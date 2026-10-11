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
)

// windowServer stands for the server of a windowed batcher: it records the
// batches in the order they are sent, and answers each one when the test
// releases it.
type windowServer struct {
	mu        sync.Mutex
	sent      [][]any
	completed [][]any
	releases  []chan struct{}
}

func (s *windowServer) newBatch() Batch {
	return &windowBatch{server: s}
}

func (s *windowServer) sentBatches() [][]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]any(nil), s.sent...)
}

func (s *windowServer) completedBatches() [][]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]any(nil), s.completed...)
}

// release answers the i-th batch sent.
func (s *windowServer) release(i int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.releases[i])
}

type windowBatch struct {
	server *windowServer
	calls  []any
}

func (*windowBatch) CanAdd(any) bool { return true }
func (b *windowBatch) Add(call any)  { b.calls = append(b.calls, call) }
func (b *windowBatch) Size() int     { return len(b.calls) }
func (*windowBatch) Fail(error)      {}

func (b *windowBatch) Complete() {
	b.Send()()
}

func (b *windowBatch) Send() func() {
	released := make(chan struct{})
	b.server.mu.Lock()
	b.server.sent = append(b.server.sent, b.calls)
	b.server.releases = append(b.server.releases, released)
	b.server.mu.Unlock()

	return func() {
		<-released
		b.server.mu.Lock()
		b.server.completed = append(b.server.completed, b.calls)
		b.server.mu.Unlock()
	}
}

func newWindowBatcher(t *testing.T, server *windowServer, inFlight int) Batcher {
	t.Helper()

	return newWindowBatcherWithFactory(t, server, &BatcherFactory{MaxRequestsPerBatch: 1, MaxBatchesInFlight: inFlight})
}

func newWindowBatcherWithFactory(t *testing.T, server *windowServer, factory *BatcherFactory) Batcher {
	t.Helper()

	batcher := factory.NewBatcher(context.Background(), 1, "test-write", server.newBatch)
	t.Cleanup(func() { assert.NoError(t, batcher.Close()) })
	return batcher
}

// A batcher with a window of two batches in flight sends the second batch
// before the first one is answered.
func TestBatcherWindowSendsBatchesBeforeTheirResponses(t *testing.T) {
	server := &windowServer{}
	batcher := newWindowBatcher(t, server, 2)

	batcher.Add(1)
	batcher.Add(2)

	require.Eventually(t, func() bool { return len(server.sentBatches()) == 2 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, [][]any{{1}, {2}}, server.sentBatches())
	assert.Empty(t, server.completedBatches())

	server.release(0)
	server.release(1)
	require.Eventually(t, func() bool { return len(server.completedBatches()) == 2 }, 10*time.Second, time.Millisecond)
}

// While the window is full, the calls that arrive keep filling one batch, sent
// as soon as a batch in flight completes: without linger, a busy shard gets
// fewer, fuller batches instead of one batch per call.
func TestBatcherWindowFillsABatchWhileFull(t *testing.T) {
	server := &windowServer{}
	batcher := newWindowBatcherWithFactory(t, server, &BatcherFactory{MaxRequestsPerBatch: 10, MaxBatchesInFlight: 1})

	batcher.Add(1)
	require.Eventually(t, func() bool { return len(server.sentBatches()) == 1 }, 10*time.Second, time.Millisecond)

	batcher.Add(2)
	batcher.Add(3)
	batcher.Add(4)
	<-time.After(100 * time.Millisecond)
	assert.Len(t, server.sentBatches(), 1, "the window holds one batch in flight")

	server.release(0)
	require.Eventually(t, func() bool { return len(server.sentBatches()) == 2 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []any{2, 3, 4}, server.sentBatches()[1])
	server.release(1)
}

// Closing the batcher fails the batch being formed and the queued calls, and
// lets the batches in flight complete: each call ends exactly once.
func TestBatcherWindowClose(t *testing.T) {
	server := &windowServer{}
	failed := make(chan []any, 10)
	factory := &BatcherFactory{MaxRequestsPerBatch: 1, MaxBatchesInFlight: 1}
	batcher := factory.NewBatcher(context.Background(), 1, "test-write", func() Batch {
		return &failingWindowBatch{windowBatch: windowBatch{server: server}, failed: failed}
	})

	batcher.Add(1)
	require.Eventually(t, func() bool { return len(server.sentBatches()) == 1 }, 10*time.Second, time.Millisecond)
	batcher.Add(2)

	require.NoError(t, batcher.Close())
	select {
	case calls := <-failed:
		assert.Equal(t, []any{2}, calls)
	case <-time.After(10 * time.Second):
		require.Fail(t, "the call waiting for the window was not failed")
	}

	server.release(0)
	require.Eventually(t, func() bool { return len(server.completedBatches()) == 1 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, [][]any{{1}}, server.completedBatches())
	assert.Empty(t, failed)
}

type failingWindowBatch struct {
	windowBatch
	failed chan<- []any
}

func (b *failingWindowBatch) Fail(error) { b.failed <- b.calls }

// A barrier is done once the batches sent before it are completed, not only
// sent: the writes handed off by a split shard must not overtake them.
func TestBatcherWindowBarrierWaitsForTheBatchesInFlight(t *testing.T) {
	server := &windowServer{}
	batcher := newWindowBatcher(t, server, 2)

	done := make(chan [][]any, 1)
	batcher.Add(1)
	batcher.Add(Barrier{Done: func() { done <- server.completedBatches() }})

	require.Eventually(t, func() bool { return len(server.sentBatches()) == 1 }, 10*time.Second, time.Millisecond)
	select {
	case <-done:
		require.Fail(t, "the barrier was done before the batch sent before it completed")
	case <-time.After(100 * time.Millisecond):
	}

	server.release(0)
	select {
	case completed := <-done:
		assert.Equal(t, [][]any{{1}}, completed)
	case <-time.After(10 * time.Second):
		require.Fail(t, "the barrier was not done")
	}
}
