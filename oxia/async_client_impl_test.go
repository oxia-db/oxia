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

	"github.com/oxia-db/oxia/common/constant"
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

// staticShardManager routes every key to its first shard.
type staticShardManager struct {
	shards     []int64
	leader     string
	successors map[int64][]int64
}

func (*staticShardManager) Close() error       { return nil }
func (s *staticShardManager) Get(string) int64 { return s.shards[0] }
func (s *staticShardManager) GetAll() []int64  { return s.shards }
func (s *staticShardManager) Leader(int64) string {
	return s.leader
}
func (*staticShardManager) Exists(int64) bool { return true }
func (s *staticShardManager) GetSuccessors(shardId int64) []int64 {
	return s.successors[shardId]
}

// GetSuccessor routes every key to the first successor.
func (s *staticShardManager) GetSuccessor(shardId int64, _ string) (int64, bool) {
	if successors := s.successors[shardId]; len(successors) > 0 {
		return successors[0], true
	}
	return 0, false
}
func (*staticShardManager) Changed() <-chan struct{} { return nil }

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

type capturingWriteBatcher struct {
	calls []any
}

func (b *capturingWriteBatcher) Add(call any) {
	b.calls = append(b.calls, call)
}

func (*capturingWriteBatcher) Close() error { return nil }
func (*capturingWriteBatcher) Run()         {}

func newWriteTestClient(shardManager *staticShardManager) (*clientImpl, map[int64]*capturingWriteBatcher) {
	batchers := map[int64]*capturingWriteBatcher{}
	c := &clientImpl{
		ctx:          context.Background(),
		shardManager: shardManager,
	}
	c.writeBatchManager = batch.NewWriteManager(context.Background(), func(_ context.Context, shardId *int64) commonbatch.Batcher {
		b := &capturingWriteBatcher{}
		batchers[*shardId] = b
		return b
	}, c.forwardWrite)
	return c, batchers
}

func deleteRangeCallAt(t *testing.T, batchers map[int64]*capturingWriteBatcher, shardId int64) model.DeleteRangeCall {
	t.Helper()
	require.Contains(t, batchers, shardId)
	require.Len(t, batchers[shardId].calls, 1)
	call, ok := batchers[shardId].calls[0].(model.DeleteRangeCall)
	require.True(t, ok)
	return call
}

var deleteRangeOK = &proto.DeleteRangeResponse{Status: proto.Status_OK}

func TestRerouteDeleteRangeToSuccessors(t *testing.T) {
	// Shard 0 was split into shards 3 and 4, shards 1 and 2 are unrelated
	client, batchers := newWriteTestClient(&staticShardManager{
		shards:     []int64{3, 4, 1, 2},
		successors: map[int64][]int64{0: {3, 4}},
	})

	callbacks := 0
	var result error
	put := model.PutCall{Key: "a", OpIndex: 0}
	deleteRange := model.DeleteRangeCall{
		MinKeyInclusive: "a",
		MaxKeyExclusive: "z",
		OpIndex:         1,
		Callback: func(_ *proto.DeleteRangeResponse, err error) {
			callbacks++
			result = err
		},
	}
	del := model.DeleteCall{Key: "b", OpIndex: 2}
	client.rerouteWrites(0, []model.PutCall{put}, []model.DeleteCall{del}, []model.DeleteRangeCall{deleteRange})

	assert.NotContains(t, batchers, int64(1))
	assert.NotContains(t, batchers, int64(2))
	require.Contains(t, batchers, int64(3))
	require.Len(t, batchers[3].calls, 3)
	assert.IsType(t, model.PutCall{}, batchers[3].calls[0])
	assert.IsType(t, model.DeleteRangeCall{}, batchers[3].calls[1])
	assert.IsType(t, model.DeleteCall{}, batchers[3].calls[2])
	onShard4 := deleteRangeCallAt(t, batchers, 4)

	// The callback is invoked once, after both successors responded
	batchers[3].calls[1].(model.DeleteRangeCall).Callback(deleteRangeOK, nil)
	assert.Equal(t, 0, callbacks)
	onShard4.Callback(deleteRangeOK, nil)
	assert.Equal(t, 1, callbacks)
	assert.NoError(t, result)
}

func TestRerouteDeleteRangeWithPartitionKey(t *testing.T) {
	shardManager := &staticShardManager{shards: []int64{0, 1}}
	client, batchers := newWriteTestClient(shardManager)
	resultCh := client.DeleteRange("a", "z", PartitionKey("pk"))
	call := deleteRangeCallAt(t, batchers, 0)

	// Shard 0 was split into shards 2 and 3, and the partition key is now on shard 2
	shardManager.shards = []int64{2, 3, 1}
	shardManager.successors = map[int64][]int64{0: {2, 3}}
	client.rerouteWrites(0, nil, nil, []model.DeleteRangeCall{call})

	rerouted := deleteRangeCallAt(t, batchers, 2)
	assert.NotContains(t, batchers, int64(3))
	assert.NotContains(t, batchers, int64(1))

	rerouted.Callback(deleteRangeOK, nil)
	assert.NoError(t, <-resultCh)
}

func TestRerouteDeleteRangeWaitsForAllSuccessors(t *testing.T) {
	shardManager := &staticShardManager{shards: []int64{0, 1}}
	client, batchers := newWriteTestClient(shardManager)
	resultCh := client.DeleteRange("a", "z")
	call := deleteRangeCallAt(t, batchers, 0)
	onShard1 := deleteRangeCallAt(t, batchers, 1)

	// Shard 0 was split into shards 2 and 3. Shard 1 already got its own copy.
	shardManager.shards = []int64{1, 2, 3}
	shardManager.successors = map[int64][]int64{0: {2, 3}}
	client.rerouteWrites(0, nil, nil, []model.DeleteRangeCall{call})

	assert.Len(t, batchers[1].calls, 1)
	onShard2 := deleteRangeCallAt(t, batchers, 2)
	onShard3 := deleteRangeCallAt(t, batchers, 3)

	// The failure of the last successor is reported, not the success of the others
	failure := errors.New("shard-3 failed")
	onShard1.Callback(deleteRangeOK, nil)
	onShard2.Callback(deleteRangeOK, nil)
	onShard3.Callback(nil, failure)
	assert.ErrorIs(t, <-resultCh, failure)
}

func TestRerouteDeleteRangeWithoutSuccessors(t *testing.T) {
	client, batchers := newWriteTestClient(&staticShardManager{shards: []int64{1}})

	var results []error
	client.rerouteWrites(0, nil, nil, []model.DeleteRangeCall{{
		MinKeyInclusive: "a",
		MaxKeyExclusive: "z",
		Callback: func(_ *proto.DeleteRangeResponse, err error) {
			results = append(results, err)
		},
	}})

	assert.Empty(t, batchers)
	require.Len(t, results, 1)
	assert.ErrorIs(t, results[0], constant.ErrShardNotFound)
}

func TestMultiShardDeleteRangeCallback(t *testing.T) {
	failure := errors.New("failure")
	notFound := &proto.DeleteRangeResponse{Status: proto.Status_KEY_NOT_FOUND}

	for _, test := range []struct {
		name             string
		responses        []*proto.DeleteRangeResponse
		errs             []error
		expectedResponse *proto.DeleteRangeResponse
		expectedErr      error
	}{
		{"all-ok", []*proto.DeleteRangeResponse{deleteRangeOK, deleteRangeOK, deleteRangeOK},
			[]error{nil, nil, nil}, deleteRangeOK, nil},
		{"first-error-wins", []*proto.DeleteRangeResponse{deleteRangeOK, nil, nil},
			[]error{nil, failure, errors.New("other failure")}, nil, failure},
		{"failed-status", []*proto.DeleteRangeResponse{notFound, deleteRangeOK, deleteRangeOK},
			[]error{nil, nil, nil}, notFound, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			var responses []*proto.DeleteRangeResponse
			var errs []error
			callback := multiShardDeleteRangeCallback(3, func(response *proto.DeleteRangeResponse, err error) {
				responses = append(responses, response)
				errs = append(errs, err)
			})

			for i := range test.responses {
				callback(test.responses[i], test.errs[i])
			}

			assert.Equal(t, []*proto.DeleteRangeResponse{test.expectedResponse}, responses)
			assert.Equal(t, []error{test.expectedErr}, errs)
		})
	}
}
