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

package concurrent

import (
	"context"
	"sync"
	"sync/atomic"
)

// ConditionContext implements a condition variable, a rendezvous point
// for goroutines waiting for or announcing the occurrence
// of an event.
//
// This version of condition takes a `context.Context` in the `Wait()`
// method, to allow for timeouts and cancellations of the operation.
type ConditionContext interface {
	// Wait atomically unlocks the locker and suspends execution
	// of the calling goroutine. After later resuming execution,
	// Wait locks c.L before returning. Unlike in other systems,
	// Wait cannot return unless awoken by Broadcast or Signal.
	//
	// Because c.L is not locked when Wait first resumes, the caller
	// typically cannot assume that the condition is true when
	// Wait returns. Instead, the caller should Wait in a loop:
	//
	//	lock.Lock()
	//	for !condition() {
	//	    c.Wait(ctx)
	//	}
	//	... make use of condition ...
	//	lock.Unlock()
	Wait(ctx context.Context) error

	// Signal wakes one goroutine waiting on c, if there is any.
	//
	// It is allowed but not required for the caller to hold c.L
	// during the call.
	//
	// Signal() does not affect goroutine scheduling priority; if other goroutines
	// are attempting to lock c.L, they may be awoken before a "waiting" goroutine.
	Signal()

	// Broadcast wakes all goroutines waiting on c.
	//
	// It is allowed but not required for the caller to hold c.L
	// during the call.
	Broadcast()
}

type conditionContext struct {
	// mu serializes Signal and Broadcast: a Signal send racing with the
	// channel close in Broadcast would panic
	mu      sync.Mutex
	locker  sync.Locker
	waiters atomic.Int32

	// ch is replaced on every Broadcast, so waiters read it with an atomic
	// load instead of locking mu.
	ch atomic.Pointer[chan struct{}]
}

func NewConditionContext(locker sync.Locker) ConditionContext {
	c := &conditionContext{
		locker: locker,
	}
	ch := make(chan struct{}, 1)
	c.ch.Store(&ch)
	return c
}

func (c *conditionContext) Wait(ctx context.Context) error {
	// The waiter count is incremented while still holding c.locker, so a
	// Broadcast that holds the same locker cannot miss this waiter
	c.waiters.Add(1)
	defer c.waiters.Add(-1)

	ch := *c.ch.Load()

	// While we're waiting on the condition, the mutex is unlocked and
	// gets relocked just after the wait is done
	c.locker.Unlock()
	defer c.locker.Lock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *conditionContext) Signal() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Signal to 1 single waiter, if any is there
	select {
	case *c.ch.Load() <- struct{}{}:
	default:
	}
}

func (c *conditionContext) Broadcast() {
	if c.waiters.Load() == 0 {
		// Nobody is waiting: skip the channel close and re-allocation.
		//
		// This cannot miss a waiter as long as the condition state is
		// mutated while holding c.locker. Wait() increments the counter
		// before releasing c.locker, so a goroutine that started waiting
		// before the state change is visible to this load, while one that
		// starts waiting afterwards has already observed the new state
		// when it checked the condition under c.locker.
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Broadcast closes the channel to wake every waiter. The new channel is
	// stored before closing the old one, so a concurrent Wait loads either
	// the channel being closed (immediate wakeup) or the new one
	old := *c.ch.Load()
	ch := make(chan struct{}, 1)
	c.ch.Store(&ch)
	close(old)
}
