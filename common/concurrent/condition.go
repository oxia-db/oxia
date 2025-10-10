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

package concurrent

import (
	"context"
	"sync"
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
	sync.RWMutex
	locker sync.Locker

	ch chan bool
}

func NewConditionContext(locker sync.Locker) ConditionContext {
	return &conditionContext{
		locker: locker,
		ch:     make(chan bool, 1),
	}
}

func (c *conditionContext) Wait(ctx context.Context) error {
	c.RLock()
	ch := c.ch
	c.RUnlock()

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
	c.RLock()
	defer c.RUnlock()

	// Signal to 1 single waiter, if any is there
	select {
	case c.ch <- true:
	default:
	}
}

func (c *conditionContext) Broadcast() {
	c.Lock()
	defer c.Unlock()

	// Broadcast closes the channel to wake every waiter
	close(c.ch)
	c.ch = make(chan bool, 1)
}
