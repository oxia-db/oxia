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

package option

import (
	"context"
	"sync"
	"sync/atomic"
)

type ValueWithVersion[T any] struct {
	v   T
	ver uint64
}

// Watch provides a generic watch primitive for observing value changes
type Watch[T any] struct {
	mu     sync.RWMutex
	v      atomic.Pointer[ValueWithVersion[T]] // current value with version
	notify chan struct{}                       // notification channel for waiters
}

func (w *Watch[T]) Load() (T, uint64) {
	snapshot := w.v.Load()
	return snapshot.v, snapshot.ver
}

func (w *Watch[T]) Wait(ctx context.Context, waitVer uint64) (T, uint64, error) {
	snapshot := w.v.Load()
	if snapshot.ver > waitVer {
		return snapshot.v, snapshot.ver, nil
	}

	w.mu.RLock()
	notify := w.notify
	w.mu.RUnlock()
	select {
	case <-ctx.Done():
		notified := w.v.Load()
		return notified.v, notified.ver, ctx.Err()
	case <-notify:
		notified := w.v.Load()
		return notified.v, notified.ver, nil
	}
}

func (w *Watch[T]) Notify(value T) {
	w.mu.Lock()
	previousNotify := w.notify
	entity := w.v.Load()
	w.v.Store(&ValueWithVersion[T]{
		v:   value,
		ver: entity.ver + 1,
	})
	w.notify = make(chan struct{})
	w.mu.Unlock()

	close(previousNotify)
}

func NewWatch[T any](init T) *Watch[T] {
	w := Watch[T]{
		notify: make(chan struct{}),
		v:      atomic.Pointer[ValueWithVersion[T]]{},
	}
	w.v.Store(&ValueWithVersion[T]{
		v:   init,
		ver: 0,
	})
	return &w
}
