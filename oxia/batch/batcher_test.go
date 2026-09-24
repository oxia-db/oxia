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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
