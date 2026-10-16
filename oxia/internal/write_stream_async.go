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
	"slices"
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

// resolveWriteTarget returns the leader of the shard that the next write goes
// to: the one named by the hint, if any, else the one the shard assignments
// name.
type resolveWriteTarget func(hint constant.ErrorMetadata) (string, error)

// openWriteStream opens a write stream to the leader of the shard at target.
// Cancelling the context ends the stream.
type openWriteStream func(ctx context.Context, target string) (proto.OxiaClient_WriteStreamClient, error)

// asyncWrite is a write request sent on the stream and not answered yet.
type asyncWrite struct {
	request  *proto.WriteRequest
	response concurrent.Future[*proto.WriteResponse]
}

// asyncWriteStream sends the writes of a shard on a stream to its leader
// without waiting for their responses, and matches the responses to the
// writes in the order they were sent.
//
// The writes follow the shard assignments: a write goes on the stream only if
// the stream goes to the leader of its attempt. Otherwise the stream is
// retired: its writes in flight wait for their responses, because that leader
// may still apply them, and the write goes on a new stream after them.
//
// When the stream ends, the writes in flight are settled together, in order:
//   - the end proves that the server processed none of them (see
//     unprocessed): they are sent again, in order, on a new stream, steered by
//     the leader hint, before any newer write;
//   - otherwise, e.g. the connection broke or the leader stepped down
//     mid-stream, the server may have applied any of them: they fail, and are
//     never sent again.
//
// A write that is not answered before its deadline aborts the stream: the
// writes in flight fail at once instead of each waiting for its own deadline.
type asyncWriteStream struct {
	ctx     context.Context
	shard   int64
	resolve resolveWriteTarget
	open    openWriteStream

	// mu guards the fields below. Owner: the asyncWriteStream. Its senders,
	// the reader of its current stream, the resend that follows the end of a
	// stream, and the aborts read and change them only under mu, and the
	// goroutines started for an older stream stop once they see generation
	// changed.
	mu sync.Mutex
	// stream is nil when there is none: the next send opens one
	stream proto.OxiaClient_WriteStreamClient
	// target is the leader the stream goes to
	target string
	cancel context.CancelFunc
	// generation changes whenever the stream is replaced or aborted, so that
	// the goroutines working for an older stream stop
	generation uint64
	inflight   []*asyncWrite
	// settled is set while the writes in flight on an ended or retired stream
	// are being settled, and closed once they are: no write is sent meanwhile
	settled chan struct{}
	// retired is set while the stream is kept for the responses of its writes
	// in flight only: its leader was replaced, and it ends once they are
	// answered or failed
	retired bool
	// resendBackoff spaces the attempts to send the writes in flight again,
	// also when the new leader rejects them too; a response resets it
	resendBackoff backoff.BackOff
	// hint is the leader hint of the last stream that ended unprocessed: the
	// next stream follows it unless its send brings its own; a response
	// clears it
	hint constant.ErrorMetadata
}

func newAsyncWriteStream(ctx context.Context, shard int64, resolve resolveWriteTarget, open openWriteStream) *asyncWriteStream {
	return &asyncWriteStream{ctx: ctx, shard: shard, resolve: resolve, open: open}
}

// send hands the request returned by prepare to the stream, opening one to
// the leader of the attempt if there is none, and returns the write in flight,
// whose response completes it. prepare is invoked right before, unless ctx is
// done: a request is never sent after its deadline. An error means that the
// request was not sent.
func (s *asyncWriteStream) send(ctx context.Context, hint constant.ErrorMetadata,
	prepare func() (*proto.WriteRequest, error)) (*asyncWrite, error) {
	target, err := s.lockTarget(ctx, hint)
	if err != nil {
		return nil, err
	}
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.stream == nil {
		stream, cancel, err := s.openStream(ctx, target)
		if err != nil {
			return nil, err
		}
		s.install(stream, target, cancel)
	}
	request, err := prepare()
	if err != nil {
		return nil, err
	}
	if err := s.stream.Send(request); err != nil {
		// The stream ended: its reader settles the writes in flight on it
		// before the next one is sent
		s.settled = make(chan struct{})
		return nil, err
	}
	write := &asyncWrite{request: request, response: concurrent.NewFuture[*proto.WriteResponse]()}
	s.inflight = append(s.inflight, write)
	return write, nil
}

// lockTarget locks the stream once no writes in flight are being settled, and
// returns the leader the next write goes to. A stream to another leader, which
// was replaced, is retired first: the next write waits for its writes in
// flight to be settled, and goes to the new leader after them.
func (s *asyncWriteStream) lockTarget(ctx context.Context, hint constant.ErrorMetadata) (string, error) {
	for {
		if err := s.lockSettled(ctx); err != nil {
			return "", err
		}
		if _, _, ok := hint.GetLeaderHint(); !ok {
			hint = s.hint
		}
		target, err := s.resolve(hint)
		if err != nil {
			s.mu.Unlock()
			return "", err
		}
		if s.stream == nil || s.target == target {
			return target, nil
		}
		s.retireLocked()
		s.mu.Unlock()
	}
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

// retireLocked keeps the stream for the responses of its writes in flight
// only: nothing more is sent on it, and it ends once they are answered or
// failed. A stream with nothing in flight ends at once.
func (s *asyncWriteStream) retireLocked() {
	if len(s.inflight) == 0 {
		s.endLocked()
		return
	}
	s.retired = true
	s.settled = make(chan struct{})
}

// abortWith fails the writes in flight with err, and ends the stream, if the
// write is still in flight: a write that was settled since must not end the
// stream that replaced its own.
func (s *asyncWriteStream) abortWith(write *asyncWrite, err error) {
	s.mu.Lock()
	if !slices.Contains(s.inflight, write) {
		s.mu.Unlock()
		return
	}
	inflight := s.detachLocked()
	s.mu.Unlock()
	failAll(inflight, err)
}

// abort fails the writes in flight with err, and ends the stream.
func (s *asyncWriteStream) abort(err error) {
	s.mu.Lock()
	inflight := s.detachLocked()
	s.mu.Unlock()
	failAll(inflight, err)
}

// detachLocked ends the stream and returns the writes that were in flight on
// it, for the caller to fail once it released the lock.
func (s *asyncWriteStream) detachLocked() []*asyncWrite {
	inflight := s.inflight
	s.inflight = nil
	s.endLocked()
	return inflight
}

// openStream opens a stream to target. The stream outlives the request, but
// its setup must not: connecting to an unreachable leader can take up to the
// gRPC connect timeout, so the setup is cancelled when ctx ends.
func (s *asyncWriteStream) openStream(ctx context.Context, target string) (proto.OxiaClient_WriteStreamClient, context.CancelFunc, error) {
	streamCtx, cancel := context.WithCancel(s.ctx)
	stop := context.AfterFunc(ctx, cancel)
	stream, err := s.open(streamCtx, target)
	if !stop() {
		// The request ended during the setup, and cancelled the stream
		cancel()
		return nil, nil, ctx.Err()
	}
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return stream, cancel, nil
}

// install makes the stream to target the current one, and starts reading its
// responses.
func (s *asyncWriteStream) install(stream proto.OxiaClient_WriteStreamClient, target string, cancel context.CancelFunc) {
	s.generation++
	s.stream, s.target, s.cancel = stream, target, cancel
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
	s.stream, s.target, s.cancel = nil, "", nil
	s.retired = false
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
		s.resendBackoff, s.hint = nil, nil
		if s.retired && len(s.inflight) == 0 {
			// The retired stream answered every write it had in flight
			s.endLocked()
		}
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
	s.stream, s.target, s.cancel = nil, "", nil
	s.retired = false

	translated, md := constant.FromGrpcError(err)
	unprocessed := s.unprocessed(md, translated)
	if unprocessed {
		// The next stream follows the hint, also when nothing is in flight
		s.hint = md
	}
	if !unprocessed || len(s.inflight) == 0 {
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

	// The first rejection after a response is retried at once; a leader
	// that rejects the writes too is retried with a growing delay
	immediate := s.resendBackoff == nil
	if immediate {
		s.resendBackoff = commontime.NewBackOff(s.ctx)
		s.resendBackoff.Reset()
	}
	bo := s.resendBackoff
	generation := s.generation
	s.mu.Unlock()

	slog.Info("The server processed none of the writes in flight, sending them again",
		slog.Int64("shard", s.shard),
		slog.Any("error", translated))
	if !immediate && !s.waitToResend(generation, bo) {
		return
	}
	s.resend(generation, md, bo)
}

// unprocessed reports whether the end of a stream proves that the server
// processed none of the writes still in flight on it, so that sending them
// again cannot apply any twice:
//   - the server marked the end as such: it answered every write it appended
//     before rejecting one, and appended none after;
//   - a node that is not the leader rejected the stream at its setup, before
//     reading any write, and hinted the leader of the shard. A leader that
//     rejects a write mid-stream gives no hint: it may have appended the writes
//     before it.
func (s *asyncWriteStream) unprocessed(md constant.ErrorMetadata, translated error) bool {
	if md.Unprocessed() {
		return true
	}
	if !errors.Is(translated, constant.ErrNodeIsNotLeader) {
		return false
	}
	shard, _, ok := md.GetLeaderHint()
	return ok && shard == s.shard
}

// resend sends the writes in flight again, in order, on a new stream, until it
// succeeds or the stream is aborted. The first attempt is immediate; the next
// ones follow the backoff.
func (s *asyncWriteStream) resend(generation uint64, hint constant.ErrorMetadata, bo backoff.BackOff) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 && !s.waitToResend(generation, bo) {
			return
		}

		err := s.resendOnce(generation, hint)
		if err == nil {
			return
		}

		// The attempt applied nothing: no stream was opened, or the server
		// processed none of the writes. Its leader hint steers the next
		// attempt, else the shard assignments do. An error that another
		// attempt cannot fix, e.g. the shard is gone after a split, fails
		// the writes, which their batches reroute.
		var translated error
		translated, hint = constant.FromGrpcError(err)
		if !isRetryableShardRequest(translated) {
			s.abortGeneration(generation, translated)
			return
		}
	}
}

// resendOnce makes one attempt to send the writes in flight again on a new
// stream to the leader named by the hint, or by the shard assignments. It
// returns nil once they are all in flight on it, and the error of an attempt
// that applied nothing. Some writes may be applied after an attempt that took
// some of them and broke without proving them unprocessed: the stream is
// aborted, and nothing is returned.
func (s *asyncWriteStream) resendOnce(generation uint64, hint constant.ErrorMetadata) error {
	target, err := s.resolve(hint)
	if err != nil {
		return err
	}
	stream, cancel, err := s.openStream(s.ctx, target)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.generation != generation {
		// Aborted meanwhile: the writes in flight failed
		s.mu.Unlock()
		cancel()
		return nil
	}
	sent, err := sendAll(stream, s.inflight)
	if err == nil {
		s.install(stream, target, cancel)
		close(s.settled)
		s.settled = nil
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	// The stream ended while taking the writes: its status says whether the
	// server processed any of those it took
	err = streamStatus(stream, err)
	cancel()
	if translated, md := constant.FromGrpcError(err); sent > 0 && !s.unprocessed(md, translated) {
		s.abortGeneration(generation, err)
		return nil
	}
	return err
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
// the given generation: the check and the abort are one critical section, so
// that a stale abort never ends a newer stream.
func (s *asyncWriteStream) abortGeneration(generation uint64, err error) {
	s.mu.Lock()
	if s.generation != generation {
		s.mu.Unlock()
		return
	}
	inflight := s.detachLocked()
	s.mu.Unlock()
	failAll(inflight, err)
}

// sendAll sends the writes in order, and reports how many it handed to the
// stream before an error.
func sendAll(stream proto.OxiaClient_WriteStreamClient, writes []*asyncWrite) (int, error) {
	for i, write := range writes {
		if err := stream.Send(write.request); err != nil {
			return i, err
		}
	}
	return len(writes), nil
}

// streamStatus returns the status a stream ended with, after one of its sends
// failed: gRPC reports io.EOF on the send, and the status on the receive.
func streamStatus(stream proto.OxiaClient_WriteStreamClient, sendErr error) error {
	if !errors.Is(sendErr, io.EOF) {
		return sendErr
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
	}
}

func failAll(writes []*asyncWrite, err error) {
	for _, write := range writes {
		write.response.Fail(err)
	}
}
