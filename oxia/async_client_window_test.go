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

// heldWriteExecutor records the write requests it is sent, and answers each
// one when the test releases it.
type heldWriteExecutor struct {
	internal.Executor

	mu       sync.Mutex
	sent     []string
	releases []chan struct{}
}

func (e *heldWriteExecutor) ExecuteWriteAsync(_ context.Context, request *proto.WriteRequest) func() (*proto.WriteResponse, error) {
	release := make(chan struct{})
	e.mu.Lock()
	for _, put := range request.Puts {
		e.sent = append(e.sent, put.Key)
	}
	e.releases = append(e.releases, release)
	e.mu.Unlock()

	return func() (*proto.WriteResponse, error) {
		<-release
		response := &proto.WriteResponse{}
		for range request.Puts {
			response.Puts = append(response.Puts, &proto.PutResponse{Status: proto.Status_OK, Version: &proto.Version{}})
		}
		return response, nil
	}
}

func (e *heldWriteExecutor) sentKeys() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.sent...)
}

func (e *heldWriteExecutor) releaseAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, release := range e.releases {
		close(release)
	}
	e.releases = nil
}

// splitWindowExecutor applies a write when it receives it, unless the write
// goes to the frozen shard: the writes to it are held, and once its shard is
// gone they fail with ErrShardNotFound without being applied, as the leader of
// a split shard rejects them.
type splitWindowExecutor struct {
	internal.Executor

	shardManager *splittableShardManager
	frozenShard  int64
	held         chan struct{}
	release      chan struct{}

	mu      sync.Mutex
	applied []string
}

func (e *splitWindowExecutor) ExecuteWriteAsync(_ context.Context, request *proto.WriteRequest) func() (*proto.WriteResponse, error) {
	if *request.Shard == e.frozenShard && e.shardManager.Exists(e.frozenShard) {
		e.held <- struct{}{}
		return func() (*proto.WriteResponse, error) {
			<-e.release
			return nil, constant.ErrShardNotFound
		}
	}
	if !e.shardManager.Exists(*request.Shard) {
		return func() (*proto.WriteResponse, error) { return nil, constant.ErrShardNotFound }
	}

	e.mu.Lock()
	response := &proto.WriteResponse{}
	for _, put := range request.Puts {
		e.applied = append(e.applied, put.Key+"="+string(put.Value))
		response.Puts = append(response.Puts, &proto.PutResponse{Status: proto.Status_OK, Version: &proto.Version{}})
	}
	e.mu.Unlock()
	return func() (*proto.WriteResponse, error) { return response, nil }
}

func (e *splitWindowExecutor) appliedWrites() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.applied...)
}

func newWindowTestClient(t *testing.T, shardManager *splittableShardManager, executor internal.Executor, inFlight int) *clientImpl {
	t.Helper()

	c := &clientImpl{shardManager: shardManager}
	batcherFactory := batch.NewBatcherFactory(executor, constant.DefaultNamespace, 0,
		1, metrics.NewMetrics(noop.NewMeterProvider()), DefaultRequestTimeout)
	batcherFactory.MaxWriteBatchesInFlight = inFlight
	batcherFactory.WriteRerouter = c.rerouteWrites
	c.writeBatchManager = batch.NewWriteManager(context.Background(), func(ctx context.Context, shard *int64) commonbatch.Batcher {
		return batcherFactory.NewWriteBatcher(ctx, shard, DefaultMaxBatchSize)
	}, c.forwardWrite)
	shardManager.onShardsReplaced = c.writeBatchManager.ShardsReplaced
	t.Cleanup(func() { assert.NoError(t, c.writeBatchManager.Close()) })
	return c
}

// With a window of write batches in flight, the client sends the next write
// of a shard without waiting for the response of the previous one.
func TestClientKeepsWriteBatchesInFlight(t *testing.T) {
	executor := &heldWriteExecutor{}
	client := newWindowTestClient(t, newSplittableShardManager(""), executor, 2)

	put1 := client.Put("a", []byte("1"))
	put2 := client.Put("b", []byte("2"))

	require.Eventually(t, func() bool { return len(executor.sentKeys()) == 2 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a", "b"}, executor.sentKeys())

	executor.releaseAll()
	require.NoError(t, (<-put1).Err)
	require.NoError(t, (<-put2).Err)
}

// The writes in flight to a shard when it splits are rerouted to the child
// shards in order, and a write to the same key issued after the split is still
// applied after them, however many writes the window had in flight.
func TestClientKeepsWriteOrderAcrossSplitWithWritesInFlight(t *testing.T) {
	shardManager := newSplittableShardManager("")
	executor := &splitWindowExecutor{
		shardManager: shardManager,
		frozenShard:  0,
		held:         make(chan struct{}, 2),
		release:      make(chan struct{}),
	}
	client := newWindowTestClient(t, shardManager, executor, 2)

	put1 := client.Put("k", []byte("v1"))
	put2 := client.Put("k", []byte("v2"))
	<-executor.held
	<-executor.held

	shardManager.split(0, 1, 2)
	put3 := make(chan PutResult, 1)
	go func() { put3 <- <-client.Put("k", []byte("v3")) }()

	// Give put(k, v3) the time to overtake the writes in flight, if the
	// client lets it
	<-time.After(100 * time.Millisecond)
	assert.Empty(t, executor.appliedWrites())

	close(executor.release)
	require.NoError(t, (<-put1).Err)
	require.NoError(t, (<-put2).Err)
	require.NoError(t, (<-put3).Err)
	assert.Equal(t, []string{"k=v1", "k=v2", "k=v3"}, executor.appliedWrites())
}
