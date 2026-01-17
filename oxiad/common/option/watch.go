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
	"sync/atomic"
)

type ValueWithVersion[T any] struct {
	v      T
	ver    uint64
	notify chan struct{}
}

// Watch provides a generic watch primitive for observing value changes.
type Watch[T any] struct {
	v atomic.Pointer[ValueWithVersion[T]] // current value with version
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
	select {
	case <-ctx.Done():
		notified := w.v.Load()
		return notified.v, notified.ver, ctx.Err()
	case <-snapshot.notify:
		notified := w.v.Load()
		return notified.v, notified.ver, nil
	}
}

func (w *Watch[T]) Notify(value T) {
	for {
		snapshot := w.v.Load()
		if swapped := w.v.CompareAndSwap(snapshot, &ValueWithVersion[T]{
			v:      value,
			ver:    snapshot.ver + 1,
			notify: make(chan struct{}),
		}); swapped {
			close(snapshot.notify)
			return
		}
	}
}

func NewWatch[T any](init T) *Watch[T] {
	w := Watch[T]{
		v: atomic.Pointer[ValueWithVersion[T]]{},
	}
	w.v.Store(&ValueWithVersion[T]{
		v:      init,
		ver:    0,
		notify: make(chan struct{}),
	})
	return &w
}
