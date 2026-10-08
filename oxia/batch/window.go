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

import "time"

// window keeps the batches that a batcher has sent and not completed yet. It
// bounds how many are in flight, and completes them on its own goroutine in
// the order they were sent.
type window struct {
	// slots holds a token for each batch in flight
	slots chan struct{}
	// completions are run in order; the batcher goroutine owns the channel
	completions chan func()
}

func newWindow(size int) *window {
	return &window{
		slots:       make(chan struct{}, size),
		completions: make(chan func(), size),
	}
}

// send sends the batch once fewer than the window size are in flight.
func (w *window) send(batch Batch) {
	w.slots <- struct{}{}
	w.dispatch(batch)
}

// dispatch sends the batch, which holds a slot already, and queues its
// completion behind the batches sent before it. A batch that cannot be sent
// asynchronously completes inline.
func (w *window) dispatch(batch Batch) {
	async, ok := batch.(AsyncBatch)
	if !ok {
		batch.Complete()
		<-w.slots
		return
	}
	complete := async.Send()
	w.completions <- func() {
		complete()
		<-w.slots
	}
}

// after invokes done once the batches sent until now are completed.
func (w *window) after(done func()) {
	w.completions <- done
}

// run completes the batches in the order they were sent, until close.
func (w *window) run() {
	for complete := range w.completions {
		complete()
	}
}

// close ends run once the batches sent until now are completed.
func (w *window) close() {
	close(w.completions)
}

// takeQueued adds the calls already queued to the batch, until the queue is
// empty or the batch is full. It returns the first call that does not join
// the batch, a barrier or a call that does not fit, or nil.
func (b *batcherImpl) takeQueued(batch Batch, full func() bool) any {
	for !full() {
		select {
		case call := <-b.callC:
			if _, isBarrier := call.(Barrier); isBarrier || !batch.CanAdd(call) {
				return call
			}
			batch.Add(call)
		default:
			return nil
		}
	}
	return nil
}

// runWindow forms the batches like Run, and sends them through the window.
// A batch is ready once it is full, or its linger expired, or right away
// without linger. While the window is full, the ready batch keeps taking the
// calls that arrive until it is full too, so that a busy shard gets fewer,
// fuller batches rather than one batch per call.
func (b *batcherImpl) runWindow() { //nolint:revive
	var batch Batch
	var ready bool
	var timer *time.Timer
	var timeout <-chan time.Time

	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer, timeout = nil, nil
		}
	}
	full := func() bool {
		return b.maxRequestsPerBatch > 0 && batch.Size() >= b.maxRequestsPerBatch
	}
	sendNow := func() {
		stopTimer()
		b.window.send(batch)
		batch, ready = nil, false
	}
	add := func(call any) {
		if barrier, ok := call.(Barrier); ok {
			if batch != nil {
				sendNow()
			}
			b.window.after(barrier.Done)
			return
		}
		if batch != nil && !batch.CanAdd(call) {
			sendNow()
		}
		if batch == nil {
			batch = b.batchFactory()
			if b.linger > 0 {
				timer = time.NewTimer(b.linger)
				timeout = timer.C
			}
		}
		batch.Add(call)
		if full() || b.linger == 0 {
			stopTimer()
			ready = true
		}
	}

	for {
		calls := b.callC
		var slots chan struct{}
		if ready {
			slots = b.window.slots
			if full() {
				// Only a free slot moves a full batch on
				calls = nil
			}
		}

		select {
		case call := <-calls:
			add(call)

		case slots <- struct{}{}:
			// Without linger, the calls already queued join the batch
			// instead of waiting for the next one
			var leftover any
			if b.linger == 0 {
				leftover = b.takeQueued(batch, full)
			}
			b.window.dispatch(batch)
			batch, ready = nil, false
			if leftover != nil {
				add(leftover)
			}

		case <-timeout:
			timer, timeout = nil, nil
			ready = true

		case <-b.closeC:
			stopTimer()
			if batch != nil {
				batch.Fail(ErrShuttingDown)
			}
			b.failQueued()
			b.window.close()
			return
		}
	}
}
