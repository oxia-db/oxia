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

package batch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxia/batch"
)

// recordingBatcher records the calls added to it, and the ones added after it
// was closed, which a batcher fails. The test completes the barriers.
type recordingBatcher struct {
	sync.Mutex
	calls    []any
	failed   []any
	closed   bool
	barriers chan batch.Barrier

	// If set, adding a call blocks until unblock is closed
	entered chan struct{}
	unblock chan struct{}
}

func (b *recordingBatcher) Add(call any) {
	if barrier, ok := call.(batch.Barrier); ok {
		b.barriers <- barrier
		return
	}
	if b.unblock != nil {
		b.entered <- struct{}{}
		<-b.unblock
	}
	b.Lock()
	defer b.Unlock()
	if b.closed {
		b.failed = append(b.failed, call)
	} else {
		b.calls = append(b.calls, call)
	}
}

func (b *recordingBatcher) recorded() []any {
	b.Lock()
	defer b.Unlock()
	return append([]any(nil), b.calls...)
}

func (*recordingBatcher) Run() {}

func (b *recordingBatcher) Close() error {
	b.Lock()
	defer b.Unlock()
	b.closed = true
	return nil
}

type testWriteManager struct {
	*WriteManager
	sync.Mutex
	batchers  map[int64]*recordingBatcher
	forwarded []any
}

// newTestWriteManager creates a WriteManager that forwards the writes of a
// removed shard to the successor given for it.
func newTestWriteManager(successors map[int64]int64) *testWriteManager {
	m := &testWriteManager{batchers: map[int64]*recordingBatcher{}}
	m.WriteManager = NewWriteManager(context.Background(), func(_ context.Context, shardId *int64) batch.Batcher {
		m.Lock()
		defer m.Unlock()
		b := &recordingBatcher{barriers: make(chan batch.Barrier, 1)}
		m.batchers[*shardId] = b
		return b
	}, func(removedShardId int64, call any) {
		m.Lock()
		m.forwarded = append(m.forwarded, call)
		m.Unlock()
		m.Add(successors[removedShardId], call)
	})
	return m
}

func (m *testWriteManager) batcher(shardId int64) *recordingBatcher {
	m.Lock()
	defer m.Unlock()
	return m.batchers[shardId]
}

func (m *testWriteManager) forwardedCalls() []any {
	m.Lock()
	defer m.Unlock()
	return append([]any(nil), m.forwarded...)
}

// addAsync adds a write from another goroutine: the returned channel is
// closed once Add returns.
func (m *testWriteManager) addAsync(shardId int64, call any) <-chan struct{} {
	added := make(chan struct{})
	go func() {
		m.Add(shardId, call)
		close(added)
	}()
	return added
}

func requireBlocked(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		require.Fail(t, "not blocked")
	case <-time.After(50 * time.Millisecond):
	}
}

func requireDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "still blocked")
	}
}

func receiveBarrier(t *testing.T, b *recordingBatcher) batch.Barrier {
	t.Helper()
	select {
	case barrier := <-b.barriers:
		return barrier
	case <-time.After(10 * time.Second):
		require.Fail(t, "no barrier")
		return batch.Barrier{}
	}
}

func TestWriteManagerAdd(t *testing.T) {
	m := newTestWriteManager(nil)
	m.Add(0, "a")
	m.Add(0, "b")
	m.Add(1, "c")

	assert.Equal(t, []any{"a", "b"}, m.batcher(0).recorded())
	assert.Equal(t, []any{"c"}, m.batcher(1).recorded())
	assert.NoError(t, m.Close())
}

func TestWriteManagerHoldsWritesUntilHandOff(t *testing.T) {
	m := newTestWriteManager(nil)
	m.Add(0, "a")

	// Shard 0 is split into shards 1 and 2
	m.ShardsReplaced(map[int64][]int64{0: {1, 2}})
	added := m.addAsync(1, "c")
	requireBlocked(t, added)

	// The batch of shard 0 fails and hands off its write, which is not held
	m.HandOff(1, "a")
	assert.Equal(t, []any{"a"}, m.batcher(1).recorded())

	// Shard 0 has completed all its batches
	barrier := receiveBarrier(t, m.batcher(0))
	requireBlocked(t, added)
	barrier.Done()

	requireDone(t, added)
	assert.Equal(t, []any{"a", "c"}, m.batcher(1).recorded())
	m.Add(2, "d")
	assert.Equal(t, []any{"d"}, m.batcher(2).recorded())
	assert.Empty(t, m.forwardedCalls())
	assert.NoError(t, m.Close())
}

func TestWriteManagerForwardsWritesToRemovedShard(t *testing.T) {
	m := newTestWriteManager(map[int64]int64{0: 1})
	m.Add(0, "a")
	m.ShardsReplaced(map[int64][]int64{0: {1}})
	receiveBarrier(t, m.batcher(0)).Done()

	// A write routed with the old shard map is forwarded before Add returns,
	// ahead of any write issued after it
	m.Add(0, "b")
	m.Add(1, "c")
	assert.Equal(t, []any{"b"}, m.forwardedCalls())
	assert.Equal(t, []any{"a"}, m.batcher(0).recorded())
	assert.Equal(t, []any{"b", "c"}, m.batcher(1).recorded())
	assert.NoError(t, m.Close())
}

func TestWriteManagerRemovedShardWithoutWrites(t *testing.T) {
	m := newTestWriteManager(map[int64]int64{0: 1})

	// Shard 0 has nothing to hand off, so shard 1 does not hold its writes
	m.ShardsReplaced(map[int64][]int64{0: {1}})
	m.Add(1, "a")
	m.Add(0, "b")

	assert.Nil(t, m.batcher(0))
	assert.Equal(t, []any{"b"}, m.forwardedCalls())
	assert.Equal(t, []any{"a", "b"}, m.batcher(1).recorded())
	assert.NoError(t, m.Close())
}

// Retiring a shard waits for the writes being added to its batcher, so they
// are handed off before the shards that replaced it stop holding their writes.
func TestWriteManagerRetireWaitsForWritesBeingAdded(t *testing.T) {
	m := newTestWriteManager(nil)
	m.Add(0, "a")
	b := m.batcher(0)
	b.entered = make(chan struct{})
	b.unblock = make(chan struct{})

	// The batcher of shard 0 is full: adding the write blocks
	added := m.addAsync(0, "b")
	<-b.entered

	m.ShardsReplaced(map[int64][]int64{0: {1}})
	select {
	case <-b.barriers:
		require.Fail(t, "the shard was retired while a write was being added")
	case <-time.After(50 * time.Millisecond):
	}

	close(b.unblock)
	requireDone(t, added)
	receiveBarrier(t, b)
	assert.Equal(t, []any{"a", "b"}, b.recorded())
	assert.NoError(t, m.Close())
}

// A shard that replaced another one is split in turn before that one has handed
// off all its writes: the writes keep their order across both hand-offs.
func TestWriteManagerNestedSplit(t *testing.T) {
	m := newTestWriteManager(map[int64]int64{1: 3})
	m.Add(0, "a")
	m.ShardsReplaced(map[int64][]int64{0: {1, 2}})
	barrier0 := receiveBarrier(t, m.batcher(0))
	m.HandOff(1, "a")
	heldOn1 := m.addAsync(1, "b")
	requireBlocked(t, heldOn1)

	// Shard 1 is split into shards 3 and 4. The write held by shard 1 is
	// forwarded, and held by shard 3 until shard 1 has handed off its writes.
	m.ShardsReplaced(map[int64][]int64{1: {3, 4}})
	require.Eventually(t, func() bool {
		return len(m.forwardedCalls()) == 1
	}, 10*time.Second, time.Millisecond)
	heldOn3 := m.addAsync(3, "c")
	requireBlocked(t, heldOn1)
	requireBlocked(t, heldOn3)

	// Shard 1 hands off its writes only after it received the ones of shard 0
	select {
	case <-m.batcher(1).barriers:
		require.Fail(t, "shard 1 was drained before shard 0 handed off its writes")
	case <-time.After(50 * time.Millisecond):
	}
	barrier0.Done()
	barrier1 := receiveBarrier(t, m.batcher(1))
	m.HandOff(3, "a")
	requireBlocked(t, heldOn3)
	barrier1.Done()

	requireDone(t, heldOn1)
	requireDone(t, heldOn3)
	recorded := m.batcher(3).recorded()
	require.Len(t, recorded, 3)
	assert.Equal(t, "a", recorded[0])
	assert.ElementsMatch(t, []any{"b", "c"}, recorded[1:])
	assert.Equal(t, []any{"b"}, m.forwardedCalls())
	assert.NoError(t, m.Close())
}

func TestWriteManagerCloseReleasesHeldWrites(t *testing.T) {
	m := newTestWriteManager(nil)
	m.Add(0, "a")
	m.ShardsReplaced(map[int64][]int64{0: {1}})
	added := m.addAsync(1, "b")
	requireBlocked(t, added)

	assert.NoError(t, m.Close())
	requireDone(t, added)

	// The closed batcher fails the write
	b := m.batcher(1)
	b.Lock()
	defer b.Unlock()
	assert.Empty(t, b.calls)
	assert.Equal(t, []any{"b"}, b.failed)
}
