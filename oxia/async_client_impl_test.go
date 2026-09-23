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

package oxia

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	commonbatch "github.com/oxia-db/oxia/oxia/batch"
	"github.com/oxia-db/oxia/oxia/internal/batch"
	"github.com/oxia-db/oxia/oxia/internal/model"
)

type capturingGetBatcher struct {
	calls chan model.GetCall
}

func (b *capturingGetBatcher) Add(call any) {
	b.calls <- call.(model.GetCall)
}

func (*capturingGetBatcher) Close() error { return nil }
func (*capturingGetBatcher) Run()         {}

type staticShardManager struct {
	shards []int64
}

func (*staticShardManager) Close() error       { return nil }
func (s *staticShardManager) Get(string) int64 { return s.shards[0] }
func (s *staticShardManager) GetAll() []int64  { return s.shards }
func (*staticShardManager) Leader(int64) string {
	return ""
}
func (*staticShardManager) Exists(int64) bool { return true }

func newGetTestClient(shards ...int64) (*clientImpl, *capturingGetBatcher) {
	b := &capturingGetBatcher{calls: make(chan model.GetCall, len(shards))}
	return &clientImpl{
		shardManager: &staticShardManager{shards: shards},
		readBatchManager: batch.NewManager(context.Background(), func(context.Context, *int64) commonbatch.Batcher {
			return b
		}),
	}, b
}

func requireGetCallbackDoesNotBlock(t *testing.T, callback func(), resultCh <-chan GetResult) GetResult {
	t.Helper()

	done := make(chan struct{})
	go func() {
		callback()
		close(done)
	}()

	select {
	case <-done:
		return <-resultCh
	case <-time.After(time.Second):
		// Consume the result to release the callback before failing the test.
		<-resultCh
		<-done
		t.Fatal("get callback blocked until the result was consumed")
		return GetResult{}
	}
}

func TestGetCallbackDoesNotBlockWhenSingleShardResultIsAbandoned(t *testing.T) {
	client, batcher := newGetTestClient(1)
	resultCh := client.Get("key-a")
	call := <-batcher.calls

	result := requireGetCallbackDoesNotBlock(t, func() {
		call.Callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	}, resultCh)

	assert.ErrorIs(t, result.Err, ErrKeyNotFound)
	_, open := <-resultCh
	assert.False(t, open)
}

func TestGetCallbackDoesNotBlockWhenMultiShardResultIsAbandoned(t *testing.T) {
	client, batcher := newGetTestClient(1, 2)
	resultCh := client.Get("key-a", ComparisonFloor())
	firstCall := <-batcher.calls
	secondCall := <-batcher.calls

	firstCall.Callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	result := requireGetCallbackDoesNotBlock(t, func() {
		secondCall.Callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	}, resultCh)

	assert.ErrorIs(t, result.Err, ErrKeyNotFound)
	_, open := <-resultCh
	assert.False(t, open)
}

// The first shard error terminates the result channel; the responses of the
// remaining shards — errors included — must be discarded, not panic with a
// send on the closed channel (the error path used to fall through and leave
// the counter sentinel negative, defeating the response-already-sent guard).
func TestMultiShardGetCallback_ErrorsAfterFirstAreDiscarded(t *testing.T) {
	ch := make(chan GetResult, 1)
	callback := multiShardGetCallback("key-a", proto.KeyComparisonType_FLOOR, 3, ch)

	callback(nil, errors.New("shard-0 failed"))
	result := <-ch
	require.Error(t, result.Err)

	assert.NotPanics(t, func() {
		callback(nil, errors.New("shard-1 failed"))
		callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	})

	// The channel was closed exactly once, after the single error result
	_, open := <-ch
	assert.False(t, open)
}

func TestMultiShardGetCallback_AllShardsRespond(t *testing.T) {
	ch := make(chan GetResult, 1)
	callback := multiShardGetCallback("key-a", proto.KeyComparisonType_FLOOR, 3, ch)

	for i := 0; i < 3; i++ {
		callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	}

	result := <-ch
	assert.ErrorIs(t, result.Err, ErrKeyNotFound)
	_, open := <-ch
	assert.False(t, open)
}
