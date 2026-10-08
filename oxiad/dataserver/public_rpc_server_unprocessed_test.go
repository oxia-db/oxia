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

package dataserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
)

// rejectingWriteLeaderController holds the callbacks of the writes it
// appends, and rejects the write of key "reject" before appending it, the way
// a leader that steps down or freezes does.
type rejectingWriteLeaderController struct {
	lead.LeaderController
	appended chan concurrent.Callback[*proto.WriteResponse]
	received chan string
}

func (m *rejectingWriteLeaderController) Write(_ context.Context, request *proto.WriteRequest, cb concurrent.Callback[*proto.WriteResponse]) {
	key := request.Puts[0].Key
	m.received <- key
	if key == "reject" {
		cb.OnCompleteError(lead.NotAppended(constant.ErrNodeIsNotLeader))
		return
	}
	m.appended <- cb
}

type writeStreamUnderTest struct {
	stream   *mockWriteStream
	lc       *rejectingWriteLeaderController
	finished chan error
}

// startWriteStream runs a write stream over the keys, whose client reads the
// responses as they come.
func startWriteStream(t *testing.T, keys ...string) *writeStreamUnderTest {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &writeStreamUnderTest{
		stream: &mockWriteStream{
			requests: make(chan *proto.WriteRequest, len(keys)),
			sent:     make(chan *proto.WriteResponse, len(keys)),
			sendGate: make(chan struct{}),
		},
		lc: &rejectingWriteLeaderController{
			appended: make(chan concurrent.Callback[*proto.WriteResponse], len(keys)),
			received: make(chan string, len(keys)),
		},
		finished: make(chan error, 1),
	}
	close(w.stream.sendGate)
	for _, key := range keys {
		w.stream.requests <- &proto.WriteRequest{Puts: []*proto.PutRequest{{Key: key}}}
	}

	pipeline := newWriteStreamPipeline()
	go processWriteStream(ctx, w.finished, w.stream, w.lc, pipeline)
	go sendWriteStreamResponses(ctx, w.finished, w.stream, pipeline)
	return w
}

// A write rejected before it was appended ends the stream as unprocessed, once
// the writes appended before it are answered, and no write after it is read:
// the client can send every write left unanswered again.
func TestWriteStreamRejectedWriteEndsTheStreamUnprocessed(t *testing.T) {
	w := startWriteStream(t, "a", "b", "reject", "c")

	first := receiveWithTimeout(t, w.lc.appended, "the first write was not appended")
	second := receiveWithTimeout(t, w.lc.appended, "the second write was not appended")
	select {
	case err := <-w.finished:
		require.Fail(t, "the stream ended before the appended writes were answered", "err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	first.OnComplete(&proto.WriteResponse{})
	second.OnComplete(&proto.WriteResponse{})
	end := receiveWithTimeout(t, w.finished, "the stream did not end")
	err, md := constant.FromGrpcError(writeStreamStatus(end))
	assert.ErrorIs(t, err, constant.ErrNodeIsNotLeader)
	assert.True(t, md.Unprocessed(), "the end marks the unanswered writes unprocessed")

	assert.Len(t, w.stream.sent, 2, "the appended writes were answered")
	assert.Equal(t, []string{"a", "b", "reject"}, drain(w.lc.received), "no write after the rejected one was read")
}

// If a write appended before the rejected one fails, its outcome is unknown:
// the stream ends with its error, not as unprocessed.
func TestWriteStreamRejectedWriteBehindAFailedOneIsNotUnprocessed(t *testing.T) {
	w := startWriteStream(t, "a", "reject")

	first := receiveWithTimeout(t, w.lc.appended, "the first write was not appended")
	failure := errors.New("commit failed")
	first.OnCompleteError(failure)

	end := receiveWithTimeout(t, w.finished, "the stream did not end")
	_, md := constant.FromGrpcError(writeStreamStatus(end))
	assert.False(t, md.Unprocessed(), "an appended write failed: its outcome is unknown")
}

func drain(ch chan string) []string {
	var out []string
	for {
		select {
		case v := <-ch:
			out = append(out, v)
		default:
			return out
		}
	}
}
