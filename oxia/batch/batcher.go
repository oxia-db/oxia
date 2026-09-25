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
	"errors"
	"io"
	"sync"
	"time"
)

var ErrShuttingDown = errors.New("shutting down")

type Batcher interface {
	io.Closer
	Add(request any)
	Run()
}

// Barrier is added to a batcher like a call, but it is not added to a batch:
// the batcher completes the batch with the calls added before the barrier,
// then invokes Done.
type Barrier struct {
	Done func()
}

type batcherImpl struct {
	batchFactory func() Batch
	callC        chan any
	closeC       chan bool
	// Add holds the read lock while it adds a call to callC, so that Close
	// can wait for the calls being added before the queue is drained
	addMu  sync.RWMutex
	closed bool
	// addsDone is closed once no call can be added to callC anymore
	addsDone            chan struct{}
	linger              time.Duration
	maxRequestsPerBatch int
}

func (b *batcherImpl) Close() error {
	// Also wakes up the calls being added to a full queue
	close(b.closeC)
	b.addMu.Lock()
	b.closed = true
	b.addMu.Unlock()
	close(b.addsDone)
	return nil
}

func (b *batcherImpl) Add(call any) {
	if !b.enqueue(call) {
		b.failCall(call, ErrShuttingDown)
	}
}

// enqueue adds the call to the queue, unless the batcher is closed.
func (b *batcherImpl) enqueue(call any) bool {
	b.addMu.RLock()
	defer b.addMu.RUnlock()
	if b.closed {
		return false
	}
	select {
	case b.callC <- call:
		return true
	case <-b.closeC:
		return false
	}
}

func (b *batcherImpl) failCall(call any, err error) {
	if barrier, ok := call.(Barrier); ok {
		barrier.Done()
		return
	}
	batch := b.batchFactory()
	batch.Add(call)
	batch.Fail(err)
}

func (b *batcherImpl) Run() { //nolint:revive
	var batch Batch
	var timer *time.Timer
	var timeout <-chan time.Time

	newBatch := func() {
		batch = b.batchFactory()
		if b.linger > 0 {
			timer = time.NewTimer(b.linger)
			timeout = timer.C
		}
	}
	completeBatch := func() {
		if b.linger > 0 {
			timer.Stop()
		}
		batch.Complete()
		batch = nil
	}

	for {
		select {
		case call := <-b.callC:
			if barrier, ok := call.(Barrier); ok {
				if batch != nil {
					completeBatch()
				}
				barrier.Done()
				continue
			}
			if batch == nil {
				newBatch()
			}
			canAdd := batch.CanAdd(call)
			if !canAdd {
				completeBatch()
				newBatch()
			}
			batch.Add(call)
			if batch.Size() == b.maxRequestsPerBatch || b.linger == 0 {
				completeBatch()
			}

		case <-timeout:
			if batch != nil {
				timer.Stop()
				batch.Complete()
				batch = nil
			}
		case <-b.closeC:
			if batch != nil {
				timer.Stop()
				batch.Fail(ErrShuttingDown)
				batch = nil
			}
			// Drain the queue once no call can be added to it anymore
			<-b.addsDone
			for {
				select {
				case call := <-b.callC:
					b.failCall(call, ErrShuttingDown)
				default:
					return
				}
			}
		}
	}
}
