// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
)

var errClientClosed = fmt.Errorf("oxia: client closed: %w", constant.ErrResourceUnavailable)

// openWriteStream opens a write stream to the leader of a shard, or to the
// leader named by the hint. Cancelling the context ends the stream.
type openWriteStream func(ctx context.Context, hint constant.ErrorMetadata) (proto.OxiaClient_WriteStreamClient, error)

// asyncWrite is a write request sent on the stream and not answered yet.
type asyncWrite struct {
	request  *proto.WriteRequest
	response concurrent.Future[*proto.WriteResponse]
}

// asyncWriteStream sends the writes of a shard on a stream to its leader
// without waiting for their responses, and matches the responses to the
// writes in the order they were sent.
//
// When the stream ends, the writes in flight are settled together, in order:
//   - the leader rejected the writes, e.g. it is no longer the leader: it
//     applied none from the rejected one on, so they are sent again, in order,
//     on a new stream, steered by the leader hint, before any newer write;
//   - otherwise, e.g. the connection broke, the leader may have applied any of
//     them: they fail, and are never sent again.
//
// A write that is not answered before its deadline aborts the stream: the
// writes in flight fail at once instead of each waiting for its own deadline.
type asyncWriteStream struct {
	ctx   context.Context
	shard int64
	open  openWriteStream

	mu sync.Mutex
	// stream is nil when there is none: the next send opens one
	stream proto.OxiaClient_WriteStreamClient
	cancel context.CancelFunc
	// generation changes whenever the stream is replaced or aborted, so that
	// the goroutines working for an older stream stop
	generation uint64
	inflight   []*asyncWrite
	// settled is set while the writes in flight on an ended stream are being
	// settled, and closed once they are: no write is sent meanwhile
	settled chan struct{}
	// resendBackoff spaces the attempts to send the writes in flight again,
	// also when the new leader rejects them too; a response resets it
	resendBackoff backoff.BackOff
}

func newAsyncWriteStream(ctx context.Context, shard int64, open openWriteStream) *asyncWriteStream {
	return &asyncWriteStream{ctx: ctx, shard: shard, open: open}
}

// send sends the request, opening a stream if there is none, and returns the
// future of its response. An error means that the request was not sent.
func (s *asyncWriteStream) send(ctx context.Context, hint constant.ErrorMetadata, request *proto.WriteRequest) (concurrent.Future[*proto.WriteResponse], error) {
	if err := s.lockSettled(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()

	if s.stream == nil {
		stream, cancel, err := s.openStream(hint) //nolint:contextcheck // The stream outlives the request.
		if err != nil {
			return nil, err
		}
		s.install(stream, cancel) //nolint:contextcheck // The stream outlives the request.
	}
	if err := s.stream.Send(request); err != nil {
		// The stream ended: its reader settles the writes in flight on it
		// before the next one is sent
		s.settled = make(chan struct{})
		return nil, err
	}
	write := &asyncWrite{request: request, response: concurrent.NewFuture[*proto.WriteResponse]()}
	s.inflight = append(s.inflight, write)
	return write.response, nil
}

// lockSettled locks the stream once no writes in flight are being settled.
func (s *asyncWriteStream) lockSettled(ctx context.Context) error {
	for {
		s.mu.Lock()
		settled := s.settled
		if settled == nil {
			return nil
		}
		s.mu.Unlock()
		select {
		case <-settled:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// abort fails the writes in flight with err, and ends the stream.
func (s *asyncWriteStream) abort(err error) {
	s.mu.Lock()
	inflight := s.inflight
	s.inflight = nil
	s.endLocked()
	s.mu.Unlock()

	for _, write := range inflight {
		write.response.Fail(err)
	}
}

func (s *asyncWriteStream) openStream(hint constant.ErrorMetadata) (proto.OxiaClient_WriteStreamClient, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(s.ctx)
	stream, err := s.open(ctx, hint)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return stream, cancel, nil
}

// install makes the stream the current one, and starts reading its responses.
func (s *asyncWriteStream) install(stream proto.OxiaClient_WriteStreamClient, cancel context.CancelFunc) {
	s.generation++
	s.stream, s.cancel = stream, cancel
	generation := s.generation
	go process.DoWithLabels(s.ctx, map[string]string{
		"oxia":  "write-stream-async",
		"shard": fmt.Sprintf("%d", s.shard),
	}, func() { s.read(generation, stream) })
}

// endLocked ends the current stream, if any, and the settling of an earlier
// one.
func (s *asyncWriteStream) endLocked() {
	s.generation++
	if s.cancel != nil {
		s.cancel()
	}
	s.stream, s.cancel = nil, nil
	if s.settled != nil {
		close(s.settled)
		s.settled = nil
	}
}

// read completes the writes in flight with the responses of the stream, in
// order, until the stream ends; then it settles the writes still in flight.
func (s *asyncWriteStream) read(generation uint64, stream proto.OxiaClient_WriteStreamClient) {
	for {
		response, err := stream.Recv()

		s.mu.Lock()
		if s.generation != generation {
			// The stream was aborted or replaced
			s.mu.Unlock()
			return
		}
		if err != nil {
			s.settleLocked(err)
			return
		}
		if len(s.inflight) == 0 {
			s.mu.Unlock()
			slog.Warn("Received a write response with no write in flight", slog.Int64("shard", s.shard))
			continue
		}
		write := s.inflight[0]
		s.inflight = s.inflight[1:]
		s.resendBackoff = nil
		s.mu.Unlock()

		write.response.Complete(response)
	}
}

// settleLocked settles the writes in flight on the stream that ended with err:
// they are sent again on a new stream if the leader rejected them, and fail
// otherwise. It is invoked with the lock held, and releases it.
func (s *asyncWriteStream) settleLocked(err error) {
	if s.settled == nil {
		s.settled = make(chan struct{})
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.stream, s.cancel = nil, nil

	rejection, hint := constant.FromGrpcError(err)
	if !isRejection(rejection) || len(s.inflight) == 0 {
		inflight := s.inflight
		s.inflight = nil
		close(s.settled)
		s.settled = nil
		s.mu.Unlock()
		for _, write := range inflight {
			write.response.Fail(err)
		}
		return
	}

	// The first rejection after a response is retried at once; a new leader
	// that rejects the writes too is retried with a growing delay
	immediate := s.resendBackoff == nil
	if immediate {
		s.resendBackoff = commontime.NewBackOff(s.ctx)
		s.resendBackoff.Reset()
	}
	bo := s.resendBackoff
	generation := s.generation
	s.mu.Unlock()

	slog.Info("The leader rejected the writes in flight, sending them again",
		slog.Int64("shard", s.shard),
		slog.Any("error", rejection))
	if !immediate && !s.waitToResend(generation, bo) {
		return
	}
	s.resend(generation, hint, bo)
}

// resend sends the writes in flight again, in order, on a new stream, until it
// succeeds or the stream is aborted. The first attempt is immediate; the next
// ones follow the backoff.
func (s *asyncWriteStream) resend(generation uint64, hint constant.ErrorMetadata, bo backoff.BackOff) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 && !s.waitToResend(generation, bo) {
			return
		}

		stream, cancel, err := s.openStream(hint)
		if err == nil {
			s.mu.Lock()
			if s.generation != generation {
				// Aborted meanwhile: the writes in flight failed
				s.mu.Unlock()
				cancel()
				return
			}
			if err = sendAll(stream, s.inflight); err == nil {
				s.install(stream, cancel)
				close(s.settled)
				s.settled = nil
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
			cancel()
		}

		// A failed attempt sent nothing that could be applied: no stream
		// was opened, or it ended before taking the writes. Its leader hint
		// steers the next attempt, else the shard assignments do. An error
		// that another attempt cannot fix, e.g. the shard is gone after a
		// split, fails the writes, which their batches reroute.
		var translated error
		translated, hint = constant.FromGrpcError(err)
		if !isRetryableShardRequest(translated) {
			s.abortGeneration(generation, translated)
			return
		}
	}
}

// waitToResend waits for the next attempt to send the writes in flight again.
// It reports false if the stream was aborted, or the backoff gave up, which
// fails the writes.
func (s *asyncWriteStream) waitToResend(generation uint64, bo backoff.BackOff) bool {
	delay := bo.NextBackOff()
	if delay == backoff.Stop {
		s.abortGeneration(generation, errors.New("oxia: gave up sending the writes in flight again"))
		return false
	}
	select {
	case <-time.After(delay):
	case <-s.ctx.Done():
		s.abortGeneration(generation, s.ctx.Err())
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation == generation
}

// abortGeneration aborts the stream unless it was replaced or aborted since
// the given generation.
func (s *asyncWriteStream) abortGeneration(generation uint64, err error) {
	s.mu.Lock()
	if s.generation != generation {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.abort(err)
}

func sendAll(stream proto.OxiaClient_WriteStreamClient, writes []*asyncWrite) error {
	for _, write := range writes {
		if err := stream.Send(write.request); err != nil {
			return err
		}
	}
	return nil
}

// isRejection reports whether a stream ended because the leader rejected the
// writes, so that it applied none of them from the rejected one on: the
// retryable errors the server returns, as opposed to a broken connection.
func isRejection(err error) bool {
	return !errors.Is(err, io.EOF) && constant.IsRetryable(err)
}
