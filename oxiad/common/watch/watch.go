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

package watch

import (
	"sync"
	"sync/atomic"
)

type Watch[T any] struct {
	sync.Mutex

	value   atomic.Value
	changed chan struct{}
}

func New[T any](init T) *Watch[T] {
	w := &Watch[T]{
		changed: make(chan struct{}),
	}
	w.value.Store(init)
	return w
}

func (w *Watch[T]) Subscribe() *Receiver[T] {
	w.Lock()
	defer w.Unlock()

	return &Receiver[T]{
		watch:   w,
		changed: w.changed,
	}
}

func (w *Watch[T]) Publish(value T) {
	w.Lock()
	defer w.Unlock()

	w.value.Store(value)
	changed := w.changed
	w.changed = make(chan struct{})
	close(changed)
}

func (w *Watch[T]) Load() T {
	value, ok := w.value.Load().(T)
	if !ok {
		panic("watch value type mismatch")
	}
	return value
}

type Receiver[T any] struct {
	watch   *Watch[T]
	changed chan struct{}
}

// Changed returns the receiver's current notification channel.
//
// Callers must pair every wake-up from Changed with a call to Load. Load both
// returns the latest value and re-arms the receiver onto the latest watch
// generation for future notifications.
func (r *Receiver[T]) Changed() <-chan struct{} {
	r.watch.Lock()
	defer r.watch.Unlock()

	return r.changed
}

func (r *Receiver[T]) Load() T {
	r.watch.Lock()
	r.changed = r.watch.changed
	r.watch.Unlock()

	return r.watch.Load()
}
