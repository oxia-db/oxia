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

// A batcher with a window of two batches in flight sends the second batch
// before the first one is answered.
func TestBatcherRunWindowSendsBatchesBeforeTheirResponses(t *testing.T) {
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
func TestBatcherRunWindowFillsABatchWhileFull(t *testing.T) {
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
func TestBatcherRunWindowCloseFailsTheQueuedCalls(t *testing.T) {
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

// A barrier is done once the batches sent before it are completed, not only
// sent: the writes handed off by a split shard must not overtake them.
func TestBatcherRunWindowBarrierWaitsForTheBatchesInFlight(t *testing.T) {
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

// windowServer stands for the server of a windowed batcher: it records the
// batches in the order they are sent, and answers each one when the test
// releases it.
// With a linger, the batch being formed is sent once its linger expires, with
// every call that arrived meanwhile.
func TestBatcherRunWindowSendsABatchOnceItsLingerExpires(t *testing.T) {
	server := &windowServer{autoRelease: true}
	batcher := newWindowBatcherWithFactory(t, server,
		&BatcherFactory{Linger: 200 * time.Millisecond, MaxRequestsPerBatch: 10, MaxBatchesInFlight: 2})

	batcher.Add(1)
	batcher.Add(2)
	assert.Empty(t, server.sentBatches(), "the batch lingers")

	require.Eventually(t, func() bool { return len(server.sentBatches()) == 1 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, [][]any{{1, 2}}, server.sentBatches())
}

// A batch that is full is sent at once, without waiting for its linger.
func TestBatcherRunWindowSendsAFullBatchBeforeItsLinger(t *testing.T) {
	server := &windowServer{autoRelease: true}
	batcher := newWindowBatcherWithFactory(t, server,
		&BatcherFactory{Linger: time.Minute, MaxRequestsPerBatch: 2, MaxBatchesInFlight: 2})

	batcher.Add(1)
	batcher.Add(2)

	require.Eventually(t, func() bool { return len(server.sentBatches()) == 1 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, [][]any{{1, 2}}, server.sentBatches())
}

// A barrier sends the batch being formed without waiting for its linger, and
// is done once that batch completed.
func TestBatcherRunWindowBarrierSendsTheBatchBeingFormed(t *testing.T) {
	server := &windowServer{autoRelease: true}
	batcher := newWindowBatcherWithFactory(t, server,
		&BatcherFactory{Linger: time.Minute, MaxRequestsPerBatch: 10, MaxBatchesInFlight: 2})

	done := make(chan [][]any, 1)
	batcher.Add(1)
	batcher.Add(Barrier{Done: func() { done <- server.completedBatches() }})

	select {
	case completed := <-done:
		assert.Equal(t, [][]any{{1}}, completed)
	case <-time.After(10 * time.Second):
		require.Fail(t, "the barrier was not done")
	}
}

// A call that does not fit the batch being formed sends that batch without
// waiting for its linger, and starts the next one.
func TestBatcherRunWindowSendsTheBatchACallDoesNotFit(t *testing.T) {
	server := &windowServer{autoRelease: true, maxCalls: 1}
	batcher := newWindowBatcherWithFactory(t, server,
		&BatcherFactory{Linger: time.Minute, MaxRequestsPerBatch: 10, MaxBatchesInFlight: 2})

	batcher.Add(1)
	batcher.Add(2)

	require.Eventually(t, func() bool { return len(server.sentBatches()) == 1 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, [][]any{{1}}, server.sentBatches(), "the call that did not fit lingers in the next batch")
}

// A batch that cannot be sent asynchronously completes when it is sent, and
// frees its slot of the window right after.
func TestBatcherRunWindowCompletesASynchronousBatchWhenItIsSent(t *testing.T) {
	completed := make(chan []any, 2)
	factory := &BatcherFactory{MaxRequestsPerBatch: 1, MaxBatchesInFlight: 1}
	batcher := factory.NewBatcher(context.Background(), 1, "test-write", func() Batch {
		return &syncBatch{completed: completed}
	})
	t.Cleanup(func() { assert.NoError(t, batcher.Close()) })

	batcher.Add(1)
	batcher.Add(2)
	for _, expected := range [][]any{{1}, {2}} {
		select {
		case calls := <-completed:
			assert.Equal(t, expected, calls)
		case <-time.After(10 * time.Second):
			require.Fail(t, "the batch was not completed")
		}
	}
}

// Under load, each call completes once, in the order it was added, and each
// barrier is done only after every call added before it completed, whichever
// way the queued calls join the batches.
func TestBatcherRunWindowKeepsTheOrderOfCallsAndBarriersUnderLoad(t *testing.T) {
	const calls = 5000
	server := &windowServer{autoRelease: true}
	batcher := newWindowBatcherWithFactory(t, server, &BatcherFactory{MaxRequestsPerBatch: 1000, MaxBatchesInFlight: 1})

	barriers := make(chan bool, calls)
	for i := range calls {
		batcher.Add(i)
		if i%3 == 2 {
			addedBefore := i + 1
			batcher.Add(Barrier{Done: func() { barriers <- server.completedCalls() >= addedBefore }})
		}
	}

	for range calls / 3 {
		select {
		case inOrder := <-barriers:
			require.True(t, inOrder, "a barrier was done before the calls added before it completed")
		case <-time.After(10 * time.Second):
			require.FailNow(t, "a barrier was not done")
		}
	}
	var completed []any
	for _, batch := range server.completedBatches() {
		completed = append(completed, batch...)
	}
	require.Len(t, completed, calls)
	for i, call := range completed {
		require.Equal(t, i, call)
	}
}

// takeQueued adds the queued calls to the batch until one cannot join it: a
// barrier, or a call that does not fit. It returns that call.
func TestBatcherTakeQueuedStopsAtTheFirstCallThatCannotJoin(t *testing.T) {
	barrier := Barrier{Done: func() {}}
	for _, tc := range []struct {
		name     string
		queued   []any
		maxCalls int
		leftover any
		batch    []any
	}{
		{name: "barrier", queued: []any{1, 2, barrier, 3}, leftover: barrier, batch: []any{1, 2}},
		{name: "call that does not fit", queued: []any{1, 2, 3}, maxCalls: 2, leftover: 3, batch: []any{1, 2}},
		{name: "empty queue", queued: []any{1, 2}, leftover: nil, batch: []any{1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &batcherImpl{callC: make(chan any, len(tc.queued))}
			for _, call := range tc.queued {
				b.callC <- call
			}
			batch := &windowBatch{server: &windowServer{maxCalls: tc.maxCalls}}

			leftover := b.takeQueued(batch, func() bool { return false })
			assert.Equal(t, tc.batch, batch.calls)
			if tc.leftover == nil {
				assert.Nil(t, leftover)
			} else {
				assert.IsType(t, tc.leftover, leftover)
			}
		})
	}
}

// windowServer records the batches sent and completed. Each batch completes
// once released, or at once with autoRelease. maxCalls, if set, bounds the
// calls a batch can take.
type windowServer struct {
	autoRelease bool
	maxCalls    int

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

func (s *windowServer) completedCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, batch := range s.completed {
		n += len(batch)
	}
	return n
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

func (b *windowBatch) CanAdd(any) bool {
	return b.server.maxCalls == 0 || len(b.calls) < b.server.maxCalls
}

func (b *windowBatch) Add(call any) { b.calls = append(b.calls, call) }

func (b *windowBatch) Size() int { return len(b.calls) }

func (*windowBatch) Fail(error) {}

func (b *windowBatch) Complete() {
	b.Send()()
}

func (b *windowBatch) Send() func() {
	released := make(chan struct{})
	if b.server.autoRelease {
		close(released)
	}
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

type failingWindowBatch struct {
	windowBatch
	failed chan<- []any
}

func (b *failingWindowBatch) Fail(error) { b.failed <- b.calls }

// syncBatch is a batch that can only complete synchronously.
type syncBatch struct {
	calls     []any
	completed chan<- []any
}

func (*syncBatch) CanAdd(any) bool { return true }
func (b *syncBatch) Add(call any)  { b.calls = append(b.calls, call) }
func (b *syncBatch) Size() int     { return len(b.calls) }
func (*syncBatch) Fail(error)      {}
func (b *syncBatch) Complete()     { b.completed <- b.calls }
