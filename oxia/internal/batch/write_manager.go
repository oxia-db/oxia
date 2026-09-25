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

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxia/batch"
)

// WriteManager keeps a batcher per shard for the writes, and keeps the writes
// in the order they were issued when the shard map changes.
//
// When a shard is removed from the shard map, after a split, the writes still
// pending on it are handed off to the shards that replaced it, as its batches
// fail with ErrShardNotFound. The writes issued after the shard map changed go
// to the new shards directly, so a new shard holds them until every shard it
// replaced has handed off all its writes: otherwise they could be applied
// before an older write to the same key.
type WriteManager struct {
	sync.RWMutex
	ctx            context.Context
	batcherFactory func(context.Context, *int64) batch.Batcher
	forward        func(removedShardId int64, call any)
	gates          map[int64]*writeGate
	closed         chan struct{}
}

// NewWriteManager creates a WriteManager. forward sends a write that was routed
// to a shard removed from the shard map to the shards that replaced it, with Add.
func NewWriteManager(ctx context.Context, batcherFactory func(context.Context, *int64) batch.Batcher,
	forward func(removedShardId int64, call any)) *WriteManager {
	return &WriteManager{
		ctx:            ctx,
		batcherFactory: batcherFactory,
		forward:        forward,
		gates:          make(map[int64]*writeGate),
		closed:         make(chan struct{}),
	}
}

// Add adds a write routed to a shard to its batcher. It blocks while the shard
// holds its writes, and it forwards the writes routed to a removed shard.
func (m *WriteManager) Add(shardId int64, call any) {
	m.gate(shardId).add(call)
}

// HandOff adds a write that was pending on a removed shard to the batcher of a
// shard that replaced it, ahead of the writes that shard holds.
func (m *WriteManager) HandOff(shardId int64, call any) {
	g := m.gate(shardId)
	if g.batcher == nil {
		// The shard was removed before any write was added to it
		m.forward(shardId, call)
		return
	}
	g.batcher.Add(call)
}

// ShardsReplaced is invoked with the shards removed from the shard map and the
// shards that replaced each of them, before any write can be routed with the
// new shard map.
func (m *WriteManager) ShardsReplaced(replaced map[int64][]int64) {
	m.Lock()
	defer m.Unlock()

	if m.isClosed() {
		// The shard manager can still apply an update after the client is closed
		return
	}

	// A new shard holds its writes until the shards it replaced have handed
	// off theirs. A removed shard that never had a write has nothing to hand off.
	pending := make(map[int64]int)
	for removedShardId, successors := range replaced {
		if _, ok := m.gates[removedShardId]; ok {
			for _, successorId := range successors {
				pending[successorId]++
			}
		}
	}
	for successorId, count := range pending {
		if _, ok := m.gates[successorId]; !ok {
			m.gates[successorId] = m.newGate(successorId, count)
		}
	}

	for removedShardId, successors := range replaced {
		removed, ok := m.gates[removedShardId]
		if !ok {
			// Forward the writes routed to it with the old shard map
			m.gates[removedShardId] = &writeGate{manager: m, shardId: removedShardId, retired: true}
			continue
		}
		holds := make([]*writeGate, 0, len(successors))
		for _, successorId := range successors {
			holds = append(holds, m.gates[successorId])
		}
		// Retiring waits for the writes being added to the batcher, which may
		// wait for a batch that needs the new shard map to complete
		go removed.retire(holds)
	}
}

func (m *WriteManager) Close() error {
	m.Lock()
	defer m.Unlock()

	if m.isClosed() {
		return nil
	}

	var err error
	for _, g := range m.gates {
		if g.batcher != nil {
			err = multierr.Append(err, g.batcher.Close())
		}
	}
	// The writes held by the gates are released, and fail on the closed batchers
	close(m.closed)
	return err
}

func (m *WriteManager) isClosed() bool {
	select {
	case <-m.closed:
		return true
	default:
		return false
	}
}

func (m *WriteManager) gate(shardId int64) *writeGate {
	m.RLock()
	g, ok := m.gates[shardId]
	m.RUnlock()

	if ok {
		return g
	}

	// Fallback on write-lock
	m.Lock()
	defer m.Unlock()

	if g, ok = m.gates[shardId]; !ok {
		g = m.newGate(shardId, 0)
		m.gates[shardId] = g
	}
	return g
}

func (m *WriteManager) newGate(shardId int64, pending int) *writeGate {
	g := &writeGate{
		manager: m,
		shardId: shardId,
		batcher: m.batcherFactory(m.ctx, &shardId),
		pending: pending,
	}
	if pending > 0 {
		g.held = make(chan struct{})
	}
	return g
}

// writeGate adds the writes routed to a shard to its batcher, unless the shard
// holds them, or it was removed from the shard map and forwards them.
type writeGate struct {
	manager *WriteManager
	shardId int64
	// batcher is nil if the shard was removed before any write was added to it
	batcher batch.Batcher

	// add holds the read lock while it adds a write to the batcher, so that
	// retiring the shard waits for the writes being added
	mu sync.RWMutex
	// pending is the number of shards replaced by this one that have not
	// handed off their writes yet: the writes are held until it drops to 0
	pending int
	// held is closed when the writes are no longer held
	held    chan struct{}
	retired bool
	// holds are the shards that replaced this one, holding their writes until
	// this one has handed off its own
	holds []*writeGate
}

func (g *writeGate) add(call any) {
	g.mu.RLock()
	for g.pending > 0 && !g.retired {
		held := g.held
		g.mu.RUnlock()
		select {
		case <-held:
		case <-g.manager.closed:
			// The batcher is closed and fails the write
			g.batcher.Add(call)
			return
		}
		g.mu.RLock()
	}

	if g.retired {
		g.mu.RUnlock()
		g.manager.forward(g.shardId, call)
		return
	}

	g.batcher.Add(call)
	g.mu.RUnlock()
}

// retire is invoked once the shard was removed from the shard map, and the
// shards that replaced it hold their writes. The writes added to the batcher
// until now are handed off to them as its batches fail, and the ones added
// from now on are forwarded.
func (g *writeGate) retire(holds []*writeGate) {
	g.mu.Lock()
	g.retired = true
	g.holds = holds
	if g.pending > 0 {
		// The held writes are forwarded to the shards that replaced this one,
		// which hold them until this one has handed off its writes
		close(g.held)
	}
	drain := g.pending == 0
	g.mu.Unlock()

	if drain {
		g.drain()
	}
}

// predecessorDone is invoked when a shard replaced by this one has handed off
// all its writes.
func (g *writeGate) predecessorDone() {
	g.mu.Lock()
	if g.pending == 0 {
		// Not holding writes for it
		g.mu.Unlock()
		return
	}
	g.pending--
	if g.pending > 0 {
		g.mu.Unlock()
		return
	}
	drain := g.retired
	if !drain {
		// The held writes are added after the handed off ones
		close(g.held)
	}
	g.mu.Unlock()

	if drain {
		g.drain()
	}
}

// drain hands off the writes still pending on a retired shard, which has
// received all the writes handed off by the shards it replaced. The batcher
// completes the batches with the writes added before the barrier, which fail
// and are handed off, then the shards that replaced this one stop holding
// their writes.
func (g *writeGate) drain() {
	g.batcher.Add(batch.Barrier{Done: func() {
		for _, successor := range g.holds {
			successor.predecessorDone()
		}
	}})
}
