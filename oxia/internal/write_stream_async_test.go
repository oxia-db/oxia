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
	"io"
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

// scriptedStream is a write stream that takes sendOK requests, then fails its
// sends with io.EOF; its receive answers nothing, and ends with end, as the
// stream's status, once released.
type scriptedStream struct {
	grpc.ClientStream
	ctx     context.Context
	sendOK  int
	end     error
	release chan struct{}

	mu   sync.Mutex
	sent []string
}

func newScriptedStream(ctx context.Context, sendOK int, end error) *scriptedStream {
	return &scriptedStream{ctx: ctx, sendOK: sendOK, end: end, release: make(chan struct{})}
}

func (s *scriptedStream) Send(request *proto.WriteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sent) == s.sendOK {
		return io.EOF
	}
	s.sent = append(s.sent, request.Puts[0].Key)
	return nil
}

func (s *scriptedStream) Recv() (*proto.WriteResponse, error) {
	select {
	case <-s.release:
		return nil, s.end
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (*scriptedStream) CloseSend() error { return nil }

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

func (o *scriptedStreams) open(context.Context, constant.ErrorMetadata) (proto.OxiaClient_WriteStreamClient, error) {
	i := int(o.opened.Add(1)) - 1
	if i >= len(o.streams) {
		return nil, status.Error(codes.Unavailable, "no more streams")
	}
	return o.streams[i], nil
}

// setupRejection is how a node that is not the leader ends a stream it
// rejected at its setup.
func setupRejection() error {
	return constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader, constant.WithLeaderHint(0, "leader:6648"))
}

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
	stream := newAsyncWriteStream(ctx, 0, streams.open)

	shard := int64(0)
	a, err := stream.send(ctx, nil, putRequest(&shard, "a"))
	require.NoError(t, err)
	b, err := stream.send(ctx, nil, putRequest(&shard, "b"))
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
	stream := newAsyncWriteStream(ctx, 0, streams.open)

	shard := int64(0)
	_, err := stream.send(ctx, nil, putRequest(&shard, "a"))
	require.NoError(t, err)
	_, err = stream.send(ctx, nil, putRequest(&shard, "b"))
	require.NoError(t, err)

	close(first.release)
	require.Eventually(t, func() bool { return len(accepted.sentKeys()) == 2 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a", "b"}, accepted.sentKeys())
}
