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
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

// The writes rejected at a stream's setup are sent again on a new stream. If
// that stream takes some of them and then breaks with a status that does not
// prove they were left unprocessed, they may be applied: every write in
// flight fails, and none is sent a third time.
func TestAsyncWriteStreamPartialResendFailsTheWrites(t *testing.T) {
	ctx := t.Context()
	first := newScriptedStream(ctx, 2, setupRejection())
	resend := newScriptedStream(ctx, 1, status.Error(codes.Unavailable, "connection reset"))
	close(resend.release)
	streams := &scriptedStreams{streams: []*scriptedStream{first, resend}}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, streams.open)

	shard := int64(0)
	a, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	b, err := stream.send(ctx, nil, fixed(putRequest(&shard, "b")))
	require.NoError(t, err)

	close(first.release)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = a.response.Wait(waitCtx)
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = b.response.Wait(waitCtx)
	require.Equal(t, codes.Unavailable, status.Code(err))

	assert.Equal(t, []string{"a"}, resend.sentKeys(), "the resend took one write before breaking")
	assert.EqualValues(t, 2, streams.opened.Load(), "the writes were not sent a third time")
}

// A resend that breaks while taking the writes, with a status proving that
// the server processed none of them, is retried: no write can be applied
// twice.
func TestAsyncWriteStreamPartialResendRejectedAtSetupIsRetried(t *testing.T) {
	ctx := t.Context()
	first := newScriptedStream(ctx, 2, setupRejection())
	rejected := newScriptedStream(ctx, 1, setupRejection())
	close(rejected.release)
	accepted := newScriptedStream(ctx, 2, io.EOF)
	streams := &scriptedStreams{streams: []*scriptedStream{first, rejected, accepted}}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, streams.open)

	shard := int64(0)
	_, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	_, err = stream.send(ctx, nil, fixed(putRequest(&shard, "b")))
	require.NoError(t, err)

	close(first.release)
	require.Eventually(t, func() bool { return len(accepted.sentKeys()) == 2 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a", "b"}, accepted.sentKeys())
}

// A request whose deadline passed before it could be sent, e.g. while it was
// waiting for the writes in flight on a retired stream, must not be sent
// anymore, and its request is not even prepared.
func TestAsyncWriteStreamDoesNotSendAfterTheDeadline(t *testing.T) {
	stream := &testWriteStream{ctx: t.Context()}
	open := func(context.Context, string) (proto.OxiaClient_WriteStreamClient, error) { return stream, nil }
	async := newAsyncWriteStream(t.Context(), 0, resolveHintedLeader, open)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	prepared := false
	_, err := async.send(ctx, nil, func() (*proto.WriteRequest, error) {
		prepared = true
		return &proto.WriteRequest{}, nil
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, prepared)
	assert.EqualValues(t, 0, stream.sent.Load())
}

// A stream to a leader that the shard assignments replaced is retired: the
// next write waits for the writes in flight on it, which the old leader may
// still apply, and goes to the new leader after them, on a new stream.
func TestAsyncWriteStreamFollowsTheShardAssignments(t *testing.T) {
	ctx := t.Context()
	old := newScriptedStream(ctx, 1, io.EOF)
	replacement := newScriptedStream(ctx, 1, io.EOF)
	streams := &scriptedStreams{streams: []*scriptedStream{old, replacement}}
	var leader atomic.Pointer[string]
	resolve := func(constant.ErrorMetadata) (string, error) { return *leader.Load(), nil }
	stream := newAsyncWriteStream(ctx, 0, resolve, streams.open)

	oldLeader, newLeader := "old:6648", "new:6648"
	leader.Store(&oldLeader)
	shard := int64(0)
	a, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)

	leader.Store(&newLeader)
	sent := make(chan error, 1)
	go func() {
		_, err := stream.send(ctx, nil, fixed(putRequest(&shard, "b")))
		sent <- err
	}()
	assert.Never(t, func() bool { return streams.opened.Load() > 1 }, 100*time.Millisecond, 10*time.Millisecond,
		"the write to the new leader must wait for the write in flight to the old one")

	// The old leader answers, and the retired stream to it ends
	old.answer()
	_, err = a.response.Wait(ctx)
	require.NoError(t, err)

	require.NoError(t, <-sent)
	assert.Equal(t, []string{"a"}, old.sentKeys())
	assert.Equal(t, []string{"b"}, replacement.sentKeys())
}

// A stream to a leader that the shard assignments replaced, with nothing in
// flight, ends at once: the next write goes to the new leader.
func TestAsyncWriteStreamEndsAnIdleStreamToAReplacedLeader(t *testing.T) {
	ctx := t.Context()
	old := newScriptedStream(ctx, 1, io.EOF)
	replacement := newScriptedStream(ctx, 1, io.EOF)
	streams := &scriptedStreams{streams: []*scriptedStream{old, replacement}}
	var leader atomic.Pointer[string]
	resolve := func(constant.ErrorMetadata) (string, error) { return *leader.Load(), nil }
	stream := newAsyncWriteStream(ctx, 0, resolve, streams.open)

	oldLeader, newLeader := "old:6648", "new:6648"
	leader.Store(&oldLeader)
	shard := int64(0)
	a, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	old.answer()
	_, err = a.response.Wait(ctx)
	require.NoError(t, err)

	leader.Store(&newLeader)
	_, err = stream.send(ctx, nil, fixed(putRequest(&shard, "b")))
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, old.sentKeys())
	assert.Equal(t, []string{"b"}, replacement.sentKeys())
}

// A response with no write in flight is logged and ignored: it completes no
// later write.
func TestAsyncWriteStreamIgnoresAResponseWithNoWriteInFlight(t *testing.T) {
	logs := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	ctx := t.Context()
	scripted := newScriptedStream(ctx, 2, io.EOF)
	streams := &scriptedStreams{streams: []*scriptedStream{scripted}}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, streams.open)

	shard := int64(0)
	a, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	scripted.answer()
	scripted.answer()
	_, err = a.response.Wait(ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), "Received a write response with no write in flight")
	}, 10*time.Second, time.Millisecond)

	b, err := stream.send(ctx, nil, fixed(putRequest(&shard, "b")))
	require.NoError(t, err)
	pending, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = b.response.Wait(pending)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the ignored response must not complete a later write")
	scripted.answer()
	_, err = b.response.Wait(ctx)
	require.NoError(t, err)
}

// A send whose request fails to be prepared, e.g. because every caller of its
// batch gave up, sends nothing.
func TestAsyncWriteStreamSendFailsWhenThePreparationFails(t *testing.T) {
	ctx := t.Context()
	scripted := newScriptedStream(ctx, 1, io.EOF)
	streams := &scriptedStreams{streams: []*scriptedStream{scripted}}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, streams.open)

	failure := errors.New("every caller gave up")
	_, err := stream.send(ctx, nil, func() (*proto.WriteRequest, error) { return nil, failure })
	require.ErrorIs(t, err, failure)
	assert.Empty(t, scripted.sentKeys())
}

// Aborting the stream for a write that was answered since leaves the stream,
// and the writes in flight on it, alone.
func TestAsyncWriteStreamAbortWithASettledWriteKeepsTheStream(t *testing.T) {
	ctx := t.Context()
	scripted := newScriptedStream(ctx, 2, io.EOF)
	streams := &scriptedStreams{streams: []*scriptedStream{scripted}}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, streams.open)

	shard := int64(0)
	a, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	scripted.answer()
	_, err = a.response.Wait(ctx)
	require.NoError(t, err)
	b, err := stream.send(ctx, nil, fixed(putRequest(&shard, "b")))
	require.NoError(t, err)

	stream.abortWith(a, errors.New("a stale deadline"))
	scripted.answer()
	_, err = b.response.Wait(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, streams.opened.Load())
}

// A send that has to wait for the writes in flight being sent again gives up
// when its context ends.
func TestAsyncWriteStreamSendWaitingForAResendEndsWithItsContext(t *testing.T) {
	ctx := t.Context()
	first := newScriptedStream(ctx, 1, setupRejection())
	accepted := newScriptedStream(ctx, 2, io.EOF)
	gate := make(chan struct{})
	var opened atomic.Int32
	open := func(context.Context, string) (proto.OxiaClient_WriteStreamClient, error) {
		if opened.Add(1) == 1 {
			return first, nil
		}
		<-gate
		return accepted, nil
	}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, open)

	shard := int64(0)
	_, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	close(first.release)
	require.Eventually(t, func() bool { return opened.Load() == 2 }, 10*time.Second, time.Millisecond)

	gaveUp, cancel := context.WithCancel(ctx)
	cancel()
	_, err = stream.send(gaveUp, nil, fixed(putRequest(&shard, "b")))
	require.ErrorIs(t, err, context.Canceled)

	close(gate)
	require.Eventually(t, func() bool { return len(accepted.sentKeys()) == 1 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a"}, accepted.sentKeys())
}

// A send whose leader can't be resolved fails without opening a stream.
func TestAsyncWriteStreamSendFailsWhenTheLeaderIsUnknown(t *testing.T) {
	ctx := t.Context()
	unknown := errors.New("no leader")
	streams := &scriptedStreams{}
	stream := newAsyncWriteStream(ctx, 0, func(constant.ErrorMetadata) (string, error) { return "", unknown }, streams.open)

	shard := int64(0)
	_, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.ErrorIs(t, err, unknown)
	assert.EqualValues(t, 0, streams.opened.Load())
}

// A send on a stream that ended fails, and the next write waits for the writes
// in flight on it to be settled, and goes on a new stream.
func TestAsyncWriteStreamSendFailsWhenTheStreamEnded(t *testing.T) {
	ctx := t.Context()
	ended := newScriptedStream(ctx, 1, status.Error(codes.Unavailable, "connection reset"))
	replacement := newScriptedStream(ctx, 1, io.EOF)
	streams := &scriptedStreams{streams: []*scriptedStream{ended, replacement}}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, streams.open)

	shard := int64(0)
	a, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	_, err = stream.send(ctx, nil, fixed(putRequest(&shard, "b")))
	require.ErrorIs(t, err, io.EOF)

	close(ended.release)
	_, err = a.response.Wait(ctx)
	require.Equal(t, codes.Unavailable, status.Code(err), "the stream broke: a may have been applied")
	_, err = stream.send(ctx, nil, fixed(putRequest(&shard, "c")))
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, replacement.sentKeys())
}

// The writes in flight are sent again until the client closes: an attempt that
// waits for the next one then fails them.
func TestAsyncWriteStreamResendStopsWhenTheClientCloses(t *testing.T) {
	clientCtx, closeClient := context.WithCancel(t.Context())
	first := newScriptedStream(t.Context(), 1, setupRejection())
	streams := &scriptedStreams{streams: []*scriptedStream{first}}
	var resending atomic.Bool
	resolve := func(hint constant.ErrorMetadata) (string, error) {
		if resending.Load() {
			// The client closes while the attempt fails, before the next one
			closeClient()
			return "", constant.ErrNodeIsNotLeader
		}
		return resolveHintedLeader(hint)
	}
	stream := newAsyncWriteStream(clientCtx, 0, resolve, streams.open)

	shard := int64(0)
	a, err := stream.send(t.Context(), nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	resending.Store(true)
	close(first.release)

	waitCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = a.response.Wait(waitCtx)
	require.ErrorContains(t, err, "gave up sending the writes in flight again")
	assert.EqualValues(t, 1, streams.opened.Load())
}

// An attempt to send the writes in flight again whose stream fails to open is
// retried.
func TestAsyncWriteStreamResendRetriesWhenTheStreamFailsToOpen(t *testing.T) {
	ctx := t.Context()
	first := newScriptedStream(ctx, 1, setupRejection())
	accepted := newScriptedStream(ctx, 1, io.EOF)
	var opened atomic.Int32
	open := func(context.Context, string) (proto.OxiaClient_WriteStreamClient, error) {
		switch opened.Add(1) {
		case 1:
			return first, nil
		case 2:
			return nil, status.Error(codes.Unavailable, "connection refused")
		default:
			return accepted, nil
		}
	}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, open)

	shard := int64(0)
	_, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	close(first.release)

	require.Eventually(t, func() bool { return len(accepted.sentKeys()) == 1 }, 10*time.Second, time.Millisecond)
	assert.EqualValues(t, 3, opened.Load())
}

// A stream aborted while an attempt to send its writes again opens a new one
// sends nothing on it: the writes failed already.
func TestAsyncWriteStreamResendAbortedMeanwhileSendsNothing(t *testing.T) {
	ctx := t.Context()
	first := newScriptedStream(ctx, 1, setupRejection())
	resend := newScriptedStream(ctx, 1, io.EOF)
	gate := make(chan struct{})
	var opened atomic.Int32
	open := func(context.Context, string) (proto.OxiaClient_WriteStreamClient, error) {
		if opened.Add(1) == 1 {
			return first, nil
		}
		<-gate
		return resend, nil
	}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, open)

	shard := int64(0)
	a, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	close(first.release)
	require.Eventually(t, func() bool { return opened.Load() == 2 }, 10*time.Second, time.Millisecond)

	aborted := errors.New("aborted")
	stream.abort(aborted)
	_, err = a.response.Wait(ctx)
	require.ErrorIs(t, err, aborted)

	close(gate)
	assert.Never(t, func() bool { return len(resend.sentKeys()) > 0 }, 100*time.Millisecond, 10*time.Millisecond)
}

// A resend whose stream breaks on a send that took some of the writes, with an
// error of its own rather than the stream's status, may have applied them:
// every write in flight fails.
func TestAsyncWriteStreamPartialResendBrokenOnASendFailsTheWrites(t *testing.T) {
	ctx := t.Context()
	first := newScriptedStream(ctx, 2, setupRejection())
	resend := newScriptedStream(ctx, 1, io.EOF)
	resend.sendErr = status.Error(codes.Unavailable, "connection reset")
	streams := &scriptedStreams{streams: []*scriptedStream{first, resend}}
	stream := newAsyncWriteStream(ctx, 0, resolveHintedLeader, streams.open)

	shard := int64(0)
	a, err := stream.send(ctx, nil, fixed(putRequest(&shard, "a")))
	require.NoError(t, err)
	b, err := stream.send(ctx, nil, fixed(putRequest(&shard, "b")))
	require.NoError(t, err)

	close(first.release)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = a.response.Wait(waitCtx)
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = b.response.Wait(waitCtx)
	require.Equal(t, codes.Unavailable, status.Code(err))
	assert.EqualValues(t, 2, streams.opened.Load(), "the writes were not sent a third time")
}

// scriptedStream is the client side of a write stream: it takes sendOK sends,
// and fails the next ones with sendErr, or io.EOF. Its receive returns the
// responses pushed with answer, and ends with end, as the stream's status,
// once released.
type scriptedStream struct {
	grpc.ClientStream
	ctx       context.Context
	sendOK    int
	sendErr   error
	end       error
	release   chan struct{}
	responses chan *proto.WriteResponse

	mu   sync.Mutex
	sent []string
}

func newScriptedStream(ctx context.Context, sendOK int, end error) *scriptedStream {
	return &scriptedStream{ctx: ctx, sendOK: sendOK, end: end, release: make(chan struct{}),
		responses: make(chan *proto.WriteResponse, 4)}
}

func (s *scriptedStream) Send(request *proto.WriteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sent) == s.sendOK {
		if s.sendErr != nil {
			return s.sendErr
		}
		return io.EOF
	}
	s.sent = append(s.sent, request.Puts[0].Key)
	return nil
}

func (s *scriptedStream) Recv() (*proto.WriteResponse, error) {
	select {
	case response := <-s.responses:
		return response, nil
	case <-s.release:
		return nil, s.end
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (*scriptedStream) CloseSend() error { return nil }

// answer answers the oldest write in flight on the stream.
func (s *scriptedStream) answer() { s.responses <- &proto.WriteResponse{} }

func (s *scriptedStream) Context() context.Context { return s.ctx }

func (s *scriptedStream) sentKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

// scriptedStreams opens the given streams in order, and counts the opens.
type scriptedStreams struct {
	streams []*scriptedStream
	opened  atomic.Int32
}

func (o *scriptedStreams) open(context.Context, string) (proto.OxiaClient_WriteStreamClient, error) {
	i := int(o.opened.Add(1)) - 1
	if i >= len(o.streams) {
		return nil, status.Error(codes.Unavailable, "no more streams")
	}
	return o.streams[i], nil
}

// resolveHintedLeader resolves the leader named by the hint, else a fixed one.
func resolveHintedLeader(hint constant.ErrorMetadata) (string, error) {
	if _, leader, ok := hint.GetLeaderHint(); ok {
		return leader, nil
	}
	return "leader:6648", nil
}

// fixed prepares a request that is already built.
func fixed(request *proto.WriteRequest) func() (*proto.WriteRequest, error) {
	return func() (*proto.WriteRequest, error) { return request, nil }
}

// setupRejection is how a node that is not the leader ends a stream it
// rejected at its setup.
func setupRejection() error {
	return constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader, constant.WithLeaderHint(0, "leader:6648"))
}
