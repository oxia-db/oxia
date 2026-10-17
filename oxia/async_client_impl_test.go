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
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	commonbatch "github.com/oxia-db/oxia/oxia/batch"
	"github.com/oxia-db/oxia/oxia/internal"
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
	keySorting proto.KeySorting
	closed     bool
}

func (s *staticShardManager) Close() error {
	s.closed = true
	return nil
}

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

func (s *staticShardManager) KeySorting() proto.KeySorting { return s.keySorting }

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
	callback := multiShardGetCallback("key-a", proto.KeyComparisonType_FLOOR, keyOrder{}, 3, ch)

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
	callback := multiShardGetCallback("key-a", proto.KeyComparisonType_FLOOR, keyOrder{}, 3, ch)

	for i := 0; i < 3; i++ {
		callback(&proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil)
	}

	result := <-ch
	assert.ErrorIs(t, result.Err, ErrKeyNotFound)
	_, open := <-ch
	assert.False(t, open)
}

// A get with an index goes to every shard, and the client picks one of their
// answers by comparing the secondary keys in the key sorting of the namespace.
func TestMultiShardIndexGetKeySorting(t *testing.T) {
	responses := []*proto.GetResponse{
		{Status: proto.Status_OK, Version: &proto.Version{}, Key: new("p1"), SecondaryIndexKey: new("a0")},
		{Status: proto.Status_OK, Version: &proto.Version{}, Key: new("p2"), SecondaryIndexKey: new("a/y/z")},
	}
	for _, test := range []struct {
		keySorting proto.KeySorting
		comparison GetOption
		expected   string
	}{
		// Natural sorting: "a/y/z" < "a0"
		{proto.KeySorting_KEY_SORTING_NATURAL, ComparisonFloor(), "p1"},
		{proto.KeySorting_KEY_SORTING_NATURAL, ComparisonCeiling(), "p2"},
		// Hierarchical sorting: "a0" < "a/y/z"
		{proto.KeySorting_KEY_SORTING_HIERARCHICAL, ComparisonFloor(), "p2"},
		{proto.KeySorting_KEY_SORTING_HIERARCHICAL, ComparisonCeiling(), "p1"},
		// The key sorting is not known: "a0" < "a/y/z" in the old order
		{proto.KeySorting_KEY_SORTING_UNKNOWN, ComparisonFloor(), "p2"},
		{proto.KeySorting_KEY_SORTING_UNKNOWN, ComparisonCeiling(), "p1"},
	} {
		client, batcher := newGetTestClient(1, 2)
		client.shardManager.(*staticShardManager).keySorting = test.keySorting
		resultCh := client.Get("b", test.comparison, UseIndex("idx"))
		for _, response := range responses {
			call := <-batcher.calls
			call.Callback(response, nil)
		}

		result := <-resultCh
		require.NoError(t, result.Err)
		assert.Equal(t, test.expected, result.Key, test.keySorting)
	}
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

// The shard manager receives the shard assignments in the background, until it
// is closed.
func TestCloseClosesShardManager(t *testing.T) {
	shardManager := &staticShardManager{}
	ctx, cancel := context.WithCancel(context.Background())
	rpcProvider := internal.NewRpcProvider(ctx, "default", nil, nil, "localhost:6648",
		func() internal.ShardManager { return shardManager })
	client := &clientImpl{
		shardManager:      shardManager,
		writeBatchManager: batch.NewWriteManager(ctx, nil, nil),
		readBatchManager:  batch.NewManager(ctx, nil),
		sessions:          newSessions(ctx, shardManager, rpcProvider, clientOptions{}),
		rpcProvider:       rpcProvider,
		ctx:               ctx,
		cancel:            cancel,
	}

	assert.NoError(t, client.Close())
	assert.True(t, shardManager.closed)
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

// Keys are proto3 strings, which must be valid UTF-8. The server doesn't check
// it, so the client doesn't send a write of a key, or a range bound, that isn't.
func TestWriteKeysMustBeValidUTF8(t *testing.T) {
	client, batchers := newWriteTestClient(&staticShardManager{shards: []int64{0, 1}})

	// Stored on "__oxia/term" with the natural key sorting, and on "a//" with
	// the hierarchical one. The results are there as soon as the calls return:
	// nothing was sent.
	for _, key := range []string{"\xff\xffoxia/term", "a\xff/"} {
		for _, putCh := range []<-chan PutResult{
			client.Put(key, []byte("v")),
			client.Put(key, []byte("v"), PartitionKey("pk"), SequenceKeysDeltas(1)),
		} {
			require.Len(t, putCh, 1, key)
			assert.ErrorIs(t, (<-putCh).Err, ErrInvalidOptions, key)
		}
		for _, errCh := range []<-chan error{
			client.Delete(key),
			client.DeleteRange(key, "z"),
			client.DeleteRange("", key),
			client.DeleteRange(key, "z", PartitionKey("pk")),
		} {
			require.Len(t, errCh, 1, key)
			assert.ErrorIs(t, <-errCh, ErrInvalidOptions, key)
		}
	}
	assert.Empty(t, batchers)

	// A valid UTF-8 key is sent, whatever its characters
	client.Put("ключ/日本", []byte("v"))
	client.Delete("ключ/日本")
	client.DeleteRange("ключ", "ключ/日本")
	require.Contains(t, batchers, int64(0))
	assert.Len(t, batchers[0].calls, 3)
	require.Contains(t, batchers, int64(1))
	assert.Len(t, batchers[1].calls, 1)
}

// A caller of List can stop reading once it cancelled the context, e.g. at the
// first error: the lists of the other shards end without sending the results
// they have left, and the channel is closed after them.
func TestAsyncClientListEndsOnceItsContextIsDone(t *testing.T) {
	failure := errors.New("shard unavailable")
	client := newScanTestClient(&staticExecutor{
		errs: map[int64]error{1: failure, 2: failure, 3: failure, 4: failure},
	}, 1, 2, 3, 4)

	ctx, cancel := context.WithCancel(context.Background())
	ch := client.List(ctx, "a", "z")
	assert.ErrorIs(t, (<-ch).Err, failure)

	cancel()
	assertGoroutinesEnd(t, "(*clientImpl).List")
	_, open := <-ch
	assert.False(t, open)
}

// A range scan across the shards ends at the first error: the scans of the
// other shards end with it, instead of waiting forever to send the records
// they have left, or streaming them until the end.
func TestAsyncClientRangeScanEndsTheOtherShardsAtTheFirstError(t *testing.T) {
	failure := errors.New("shard unavailable")
	client := newScanTestClient(&staticExecutor{
		keys: map[int64][]string{2: {"a", "b"}, 3: {"c"}},
		errs: map[int64]error{1: failure},
		slow: map[int64]bool{3: true},
	}, 1, 2, 3)

	var results []GetResult
	for result := range client.RangeScan(context.Background(), "a", "z") {
		results = append(results, result)
	}
	require.Len(t, results, 1)
	assert.ErrorIs(t, results[0].Err, failure)
	assertGoroutinesEnd(t, "(*clientImpl).RangeScan")
}

// A list with EndInclusive fails with ErrEndInclusiveUnsupported unless the
// leader of every shard includes the end of the range, which it says in all its
// responses, sending one at least: older servers ignore EndInclusive, and leave
// the end out. The client drops the keys of a response that doesn't say so.
func TestAsyncClientListEndInclusive(t *testing.T) {
	for _, test := range endInclusiveTests() {
		t.Run(test.name, func(t *testing.T) {
			client := newScanTestClient(test.executor, 1, 2)
			var keys []string
			var err error
			// The channel is closed once all the shards responded
			for result := range client.List(context.Background(), "a", "z", test.options...) {
				if result.Err != nil {
					err = result.Err
					continue
				}
				keys = append(keys, result.Keys...)
			}
			if test.err != nil {
				assert.ErrorIs(t, err, test.err)
				assert.NotContains(t, keys, "c")
			} else {
				require.NoError(t, err)
				assert.ElementsMatch(t, test.expected, keys)
			}
		})
	}
}

// A range scan with EndInclusive fails like a list.
func TestAsyncClientRangeScanEndInclusive(t *testing.T) {
	for _, test := range endInclusiveTests() {
		t.Run(test.name, func(t *testing.T) {
			client := newScanTestClient(test.executor, 1, 2)
			var options []RangeScanOption
			for _, option := range test.options {
				options = append(options, option)
			}
			var keys []string
			var err error
			for result := range client.RangeScan(context.Background(), "a", "z", options...) {
				if result.Err != nil {
					err = result.Err
					continue
				}
				keys = append(keys, result.Key)
			}
			if test.err != nil {
				assert.ErrorIs(t, err, test.err)
				assert.NotContains(t, keys, "c")
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expected, keys)
			}
		})
	}
}

// staticExecutor answers the lists and the range scans of each shard with its
// keys, one per response, then with its error, if it has one. It sends the
// keys whatever the context, like a stream whose responses were already
// received.
type staticExecutor struct {
	internal.Executor
	keys map[int64][]string
	errs map[int64]error
	// The shards whose scans end only once their context is done, like a scan
	// that is still streaming
	slow map[int64]bool
	// The shards that include the end of the range of the requests that ask
	// for it, like a server with FEATURE_RANGE_END_INCLUSIVE: their responses
	// say so, and they send one without keys if they have none
	endInclusive map[int64]bool
}

func (e *staticExecutor) ExecuteList(ctx context.Context, request *proto.ListRequest,
	listResponseConsumer func(*proto.ListResponse) error) error {
	responses, endInclusive := e.responses(request)
	for _, keys := range responses {
		if err := listResponseConsumer(&proto.ListResponse{Keys: keys, EndInclusive: endInclusive}); err != nil {
			return err
		}
	}
	return e.end(ctx, request.GetShard())
}

func (e *staticExecutor) ExecuteRangeScan(ctx context.Context, request *proto.RangeScanRequest,
	rangeScanResponseConsumer func(*proto.RangeScanResponse) error) error {
	responses, endInclusive := e.responses(request)
	for _, keys := range responses {
		var records []*proto.GetResponse
		for _, key := range keys {
			records = append(records, &proto.GetResponse{Key: new(key), Version: &proto.Version{}})
		}
		response := &proto.RangeScanResponse{Records: records, EndInclusive: endInclusive}
		if err := rangeScanResponseConsumer(response); err != nil {
			return err
		}
	}
	return e.end(ctx, request.GetShard())
}

// responses returns the keys of the responses of a shard to a list or a range
// scan, and whether the responses say that they include the end of the range.
func (e *staticExecutor) responses(request interface {
	GetShard() int64
	GetEndInclusive() bool
}) (responses [][]string, endInclusive bool) {
	endInclusive = request.GetEndInclusive() && e.endInclusive[request.GetShard()]
	keys := e.keys[request.GetShard()]
	if len(keys) == 0 && endInclusive {
		return [][]string{nil}, true
	}
	for _, key := range keys {
		responses = append(responses, []string{key})
	}
	return responses, endInclusive
}

func (e *staticExecutor) end(ctx context.Context, shardId int64) error {
	if e.slow[shardId] {
		<-ctx.Done()
		return ctx.Err()
	}
	return e.errs[shardId]
}

func newScanTestClient(executor *staticExecutor, shards ...int64) *clientImpl {
	return &clientImpl{
		options:      clientOptions{requestTimeout: time.Minute},
		shardManager: &staticShardManager{shards: shards},
		executor:     executor,
	}
}

// assertGoroutinesEnd asserts that the goroutines that function started end,
// e.g. that none of them waits forever to send a result that nobody receives.
func assertGoroutinesEnd(t *testing.T, function string) {
	t.Helper()

	createdBy := "created by github.com/oxia-db/oxia/oxia." + function + " in goroutine"
	assert.Eventually(t, func() bool {
		var stacks strings.Builder
		_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
		return !strings.Contains(stacks.String(), createdBy)
	}, 10*time.Second, 10*time.Millisecond, "a goroutine started by %s did not end", function)
}

type endInclusiveTest struct {
	name     string
	options  []ListOption
	executor *staticExecutor
	expected []string
	err      error
}

// endInclusiveTests returns the cases of the tests of EndInclusive, on the
// shards 1 and 2.
func endInclusiveTests() []endInclusiveTest {
	endInclusive := []ListOption{EndInclusive()}
	return []endInclusiveTest{
		{"included", endInclusive, &staticExecutor{
			keys:         map[int64][]string{1: {"a", "b"}, 2: {"c"}},
			endInclusive: map[int64]bool{1: true, 2: true},
		}, []string{"a", "b", "c"}, nil},
		{"included, empty shard", endInclusive, &staticExecutor{
			keys:         map[int64][]string{1: {"a"}},
			endInclusive: map[int64]bool{1: true, 2: true},
		}, []string{"a"}, nil},
		{"older server", endInclusive, &staticExecutor{
			keys:         map[int64][]string{1: {"a"}, 2: {"c", "d"}},
			endInclusive: map[int64]bool{1: true},
		}, nil, ErrEndInclusiveUnsupported},
		{"older server, empty shard", endInclusive, &staticExecutor{
			keys:         map[int64][]string{1: {"a"}},
			endInclusive: map[int64]bool{1: true},
		}, nil, ErrEndInclusiveUnsupported},
		{"not requested", nil, &staticExecutor{
			keys: map[int64][]string{1: {"a"}, 2: {"c"}},
		}, []string{"a", "c"}, nil},
	}
}
