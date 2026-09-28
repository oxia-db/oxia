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
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	commonbatch "github.com/oxia-db/oxia/oxia/batch"
	"github.com/oxia-db/oxia/oxia/internal"
	"github.com/oxia-db/oxia/oxia/internal/batch"
	"github.com/oxia-db/oxia/oxia/internal/metrics"
)

// splittableShardManager is a ShardManager whose shards can be split, to
// simulate the assignments update a client receives after a shard split.
type splittableShardManager struct {
	sync.RWMutex
	strategy         internal.ShardStrategy
	shards           []internal.Shard
	successors       map[int64][]internal.Shard
	onShardsReplaced internal.ShardsReplacedListener
}

func (*splittableShardManager) Close() error { return nil }

func (m *splittableShardManager) Get(key string) int64 {
	m.RLock()
	defer m.RUnlock()
	predicate := m.strategy.Get(key)
	for _, shard := range m.shards {
		if predicate(shard) {
			return shard.Id
		}
	}
	panic("shard not found")
}

func (m *splittableShardManager) GetAll() []int64 {
	m.RLock()
	defer m.RUnlock()
	ids := make([]int64, 0, len(m.shards))
	for _, shard := range m.shards {
		ids = append(ids, shard.Id)
	}
	return ids
}

func (*splittableShardManager) Leader(int64) string { return "" }

func (m *splittableShardManager) Exists(shardId int64) bool {
	m.RLock()
	defer m.RUnlock()
	for _, shard := range m.shards {
		if shard.Id == shardId {
			return true
		}
	}
	return false
}

func (m *splittableShardManager) GetSuccessors(shardId int64) []int64 {
	m.RLock()
	defer m.RUnlock()
	var ids []int64
	for _, shard := range m.successors[shardId] {
		ids = append(ids, shard.Id)
	}
	return ids
}

func (m *splittableShardManager) GetSuccessor(shardId int64, key string) (int64, bool) {
	m.RLock()
	defer m.RUnlock()
	predicate := m.strategy.Get(key)
	for _, shard := range m.successors[shardId] {
		if predicate(shard) {
			return shard.Id, true
		}
	}
	return 0, false
}

func (*splittableShardManager) Changed() <-chan struct{} { return nil }

// split replaces the parent shard with two shards covering half its hash range each.
func (m *splittableShardManager) split(parent int64, left int64, right int64) {
	m.Lock()
	defer m.Unlock()
	for i, shard := range m.shards {
		if shard.Id != parent {
			continue
		}
		r := shard.HashRange
		mid := r.MinInclusive + (r.MaxInclusive-r.MinInclusive)/2
		children := []internal.Shard{
			{Id: left, HashRange: internal.HashRange{MinInclusive: r.MinInclusive, MaxInclusive: mid}},
			{Id: right, HashRange: internal.HashRange{MinInclusive: mid + 1, MaxInclusive: r.MaxInclusive}},
		}
		m.shards = append(append(m.shards[:i], m.shards[i+1:]...), children...)
		m.successors[parent] = children
		m.onShardsReplaced(map[int64][]int64{parent: {left, right}})
		return
	}
}

// splitExecutor applies the puts it receives to an in-memory log. Writes to a
// shard that is no longer in the client's shard map fail with
// ErrShardNotFound, as in the RPC provider. Writes to the frozen shard block
// until it is released, like a batch that the client keeps retrying while the
// parent is frozen for the split cutover.
type splitExecutor struct {
	internal.Executor

	shardManager *splittableShardManager
	frozenShard  int64
	frozenWrite  chan struct{}
	release      chan struct{}

	sync.Mutex
	applied []string
}

func (e *splitExecutor) ExecuteWrite(_ context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	if *request.Shard == e.frozenShard && e.shardManager.Exists(e.frozenShard) {
		e.frozenWrite <- struct{}{}
		<-e.release
	}
	if !e.shardManager.Exists(*request.Shard) {
		return nil, constant.ErrShardNotFound
	}

	e.Lock()
	defer e.Unlock()
	response := &proto.WriteResponse{}
	for _, put := range request.Puts {
		e.applied = append(e.applied, fmt.Sprintf("%s=%s", put.Key, put.Value))
		response.Puts = append(response.Puts, &proto.PutResponse{Status: proto.Status_OK, Version: &proto.Version{}})
	}
	return response, nil
}

func (e *splitExecutor) appliedWrites() []string {
	e.Lock()
	defer e.Unlock()
	return append([]string(nil), e.applied...)
}

func newRerouteTestClient(shardManager *splittableShardManager, executor internal.Executor) *clientImpl {
	c := &clientImpl{shardManager: shardManager}
	batcherFactory := batch.NewBatcherFactory(executor, constant.DefaultNamespace, time.Millisecond,
		DefaultMaxRequestsPerBatch, metrics.NewMetrics(noop.NewMeterProvider()), DefaultRequestTimeout)
	batcherFactory.WriteRerouter = c.rerouteWrites
	c.writeBatchManager = batch.NewWriteManager(context.Background(), func(ctx context.Context, shard *int64) commonbatch.Batcher {
		return batcherFactory.NewWriteBatcher(ctx, shard, DefaultMaxBatchSize)
	}, c.forwardWrite)
	shardManager.onShardsReplaced = c.writeBatchManager.ShardsReplaced
	return c
}

// A write that is in flight to a shard when the shard splits is rerouted to
// the child shard. A write to the same key issued after the client received
// the post-split assignments goes to the child shard directly, and must still
// be applied after the rerouted one.
func TestRerouteKeepsWriteOrderAcrossSplit(t *testing.T) {
	shardManager := &splittableShardManager{
		strategy:   internal.NewShardStrategy(),
		shards:     []internal.Shard{{Id: 0, HashRange: internal.HashRange{MinInclusive: 0, MaxInclusive: math.MaxUint32}}},
		successors: map[int64][]internal.Shard{},
	}
	executor := &splitExecutor{
		shardManager: shardManager,
		frozenShard:  0,
		frozenWrite:  make(chan struct{}, 1),
		release:      make(chan struct{}),
	}
	client := newRerouteTestClient(shardManager, executor)
	defer func() { assert.NoError(t, client.writeBatchManager.Close()) }()

	put1 := client.Put("k", []byte("v1"))
	<-executor.frozenWrite

	shardManager.split(0, 1, 2)
	// The child shard holds put(k, v2) until put(k, v1) is rerouted to it
	put2 := make(chan PutResult, 1)
	go func() { put2 <- <-client.Put("k", []byte("v2")) }()

	// Give put(k, v2) the time to overtake put(k, v1), if the client lets it
	deadline := time.Now().Add(100 * time.Millisecond)
	for len(executor.appliedWrites()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	close(executor.release)
	require.NoError(t, (<-put1).Err)
	require.NoError(t, (<-put2).Err)

	assert.Equal(t, []string{"k=v1", "k=v2"}, executor.appliedWrites())
}
