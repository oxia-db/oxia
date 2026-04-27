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
	"errors"
	"io"
	"sync"
)

var ErrClosed = errors.New("watch closed")

var _ io.Closer = (*Receiver[any])(nil)

type Watch[T any] struct {
	sync.Mutex

	value     T
	hasValue  bool
	closed    bool
	receivers map[*Receiver[T]]chan struct{}
}

func New[T any](init T) *Watch[T] {
	return &Watch[T]{
		value:     init,
		hasValue:  true,
		receivers: map[*Receiver[T]]chan struct{}{},
	}
}

func NewEmpty[T any]() *Watch[T] {
	return &Watch[T]{
		receivers: map[*Receiver[T]]chan struct{}{},
	}
}

func (w *Watch[T]) Subscribe() (*Receiver[T], error) {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return nil, ErrClosed
	}

	ch := make(chan struct{}, 1)
	r := &Receiver[T]{
		watch:   w,
		changed: ch,
	}
	w.receivers[r] = ch
	return r, nil
}

func (w *Watch[T]) Publish(value T) {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return
	}

	w.value = value
	w.hasValue = true

	for _, ch := range w.receivers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (w *Watch[T]) Notify(value T) {
	w.Publish(value)
}

func (w *Watch[T]) Close() {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return
	}

	w.closed = true
	for r, ch := range w.receivers {
		close(ch)
		delete(w.receivers, r)
	}
}

func (w *Watch[T]) Load() (T, bool) {
	w.Lock()
	defer w.Unlock()

	var zero T
	if !w.hasValue {
		return zero, false
	}
	return w.value, true
}

type Receiver[T any] struct {
	watch   *Watch[T]
	changed chan struct{}
}

func (r *Receiver[T]) Changed() <-chan struct{} {
	return r.changed
}

func (r *Receiver[T]) Load() (T, bool) {
	return r.watch.Load()
}

func (r *Receiver[T]) Close() error {
	r.watch.closeReceiver(r)
	return nil
}

func (w *Watch[T]) closeReceiver(r *Receiver[T]) {
	w.Lock()
	defer w.Unlock()

	ch, ok := w.receivers[r]
	if !ok {
		return
	}
	delete(w.receivers, r)
	close(ch)
}
