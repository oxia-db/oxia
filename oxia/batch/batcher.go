// Copyright 2023-2025 The Oxia Authors
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
	"sync/atomic"
	"time"
)

var ErrShuttingDown = errors.New("shutting down")

type Batcher interface {
	io.Closer
	Add(request any)
	Run()
}

type batcherImpl struct {
	batchFactory        func() Batch
	callC               chan any
	closeC              chan bool
	closed              atomic.Bool
	linger              time.Duration
	maxRequestsPerBatch int
}

func (b *batcherImpl) Close() error {
	b.closed.Store(true)
	close(b.closeC)
	return nil
}

func (b *batcherImpl) Add(call any) {
	if b.closed.Load() {
		b.failCall(call, ErrShuttingDown)
	} else {
		b.callC <- call
	}
}

func (b *batcherImpl) failCall(call any, err error) {
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
