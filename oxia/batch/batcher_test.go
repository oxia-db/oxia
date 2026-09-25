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

type testBatch struct {
	count  int
	calls  []any
	result chan error
}

func newTestBatch() *testBatch {
	return &testBatch{
		calls:  make([]any, 0),
		result: make(chan error, 1),
	}
}

func (b *testBatch) CanAdd(call any) bool {
	return true
}

func (b *testBatch) Add(call any) {
	b.count++
}

func (b *testBatch) Size() int {
	return b.count
}

func (b *testBatch) Complete() {
	close(b.result)
}

func (b *testBatch) Fail(err error) {
	b.result <- err
	// closeC(b.result)
}

// recordingBatch appends its calls to completed when it completes.
type recordingBatch struct {
	calls     []any
	completed *[]any
}

func (*recordingBatch) CanAdd(any) bool { return true }
func (b *recordingBatch) Add(call any)  { b.calls = append(b.calls, call) }
func (b *recordingBatch) Size() int     { return len(b.calls) }
func (b *recordingBatch) Complete()     { *b.completed = append(*b.completed, b.calls...) }
func (*recordingBatch) Fail(error)      {}

func TestBatcherBarrier(t *testing.T) {
	var completed []any
	factory := &BatcherFactory{
		Linger:              time.Hour,
		MaxRequestsPerBatch: 100,
	}
	batcher := factory.NewBatcher(context.Background(), 1, "test-write", func() Batch {
		return &recordingBatch{completed: &completed}
	})

	// The barrier completes the batch with the calls added before it, without
	// waiting for the linger time
	done := make(chan []any, 1)
	batcher.Add(1)
	batcher.Add(2)
	batcher.Add(Barrier{Done: func() { done <- append([]any(nil), completed...) }})
	select {
	case calls := <-done:
		assert.Equal(t, []any{1, 2}, calls)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the barrier was not completed")
	}
	assert.NoError(t, batcher.Close())

	// A closed batcher completes the barriers right away
	closedDone := make(chan struct{})
	batcher.Add(Barrier{Done: func() { close(closedDone) }})
	select {
	case <-closedDone:
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the barrier was not completed")
	}
}

// endingBatch reports its calls when it completes or fails. Completing it
// waits for release.
type endingBatch struct {
	calls   []any
	release <-chan struct{}
	ended   func([]any)
}

func (*endingBatch) CanAdd(any) bool { return true }
func (b *endingBatch) Add(call any)  { b.calls = append(b.calls, call) }
func (b *endingBatch) Size() int     { return len(b.calls) }
func (b *endingBatch) Fail(error)    { b.ended(b.calls) }
func (b *endingBatch) Complete() {
	<-b.release
	b.ended(b.calls)
}

// Close fails the calls being added concurrently, also the ones blocked on a
// full queue while the batcher waits for a batch to complete, and every call
// is completed or failed exactly once.
func TestBatcherCloseWithConcurrentAdds(t *testing.T) {
	release := make(chan struct{})
	var mutex sync.Mutex
	ended := map[any]int{}
	factory := &BatcherFactory{MaxRequestsPerBatch: 1}
	batcher := factory.NewBatcher(context.Background(), 1, "test-write", func() Batch {
		return &endingBatch{release: release, ended: func(calls []any) {
			mutex.Lock()
			defer mutex.Unlock()
			for _, call := range calls {
				ended[call]++
			}
		}}
	})

	// The first call blocks the batcher, the next ones fill the queue, and the
	// last ones block adding to it
	calls := 2*batcherChannelBufferSize + 2
	var adding sync.WaitGroup
	for i := range calls {
		adding.Go(func() { batcher.Add(i) })
	}
	queue := batcher.(*batcherImpl).callC
	require.Eventually(t, func() bool { return len(queue) == cap(queue) }, 10*time.Second, time.Millisecond)

	assert.NoError(t, batcher.Close())
	added := make(chan struct{})
	go func() {
		adding.Wait()
		close(added)
	}()
	select {
	case <-added:
	case <-time.After(10 * time.Second):
		require.Fail(t, "adding a call blocked after the batcher was closed")
	}

	close(release)
	require.Eventually(t, func() bool {
		mutex.Lock()
		defer mutex.Unlock()
		return len(ended) == calls
	}, 10*time.Second, time.Millisecond)
	mutex.Lock()
	defer mutex.Unlock()
	for call, count := range ended {
		assert.Equal(t, 1, count, "call %v", call)
	}
}

func TestBatcher(t *testing.T) {
	for _, item := range []struct {
		name             string
		linger           time.Duration
		maxSize          int
		closeImmediately bool
		expectedErr      error
	}{
		{"complete on maxRequestsPerBatch", 1 * time.Second, 1, false, nil},
		{"complete on linger", 1 * time.Millisecond, 2, false, nil},
		{"fail on close", 1 * time.Second, 2, true, ErrShuttingDown},
	} {
		t.Run(item.name, func(t *testing.T) {
			testBatch := newTestBatch()

			batchFactory := func() Batch {
				return testBatch
			}

			factory := &BatcherFactory{
				Linger:              item.linger,
				MaxRequestsPerBatch: item.maxSize,
			}
			batcher := factory.NewBatcher(context.Background(), 1, "test-write", batchFactory)
			batcher.Add(1)

			if item.closeImmediately {
				err := batcher.Close()
				assert.NoError(t, err)
			}

			assert.ErrorIs(t, <-testBatch.result, item.expectedErr)

			if !item.closeImmediately {
				err := batcher.Close()
				assert.NoError(t, err)
			}
		})
	}
}
