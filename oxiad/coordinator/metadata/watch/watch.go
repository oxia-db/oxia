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
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"
)

var ErrClosed = errors.New("watch closed")

var _ io.Closer = (*Receiver[gproto.Message])(nil)

type Watch[T gproto.Message] struct {
	sync.Mutex

	value     T
	hasValue  bool
	closed    bool
	receivers map[*Receiver[T]]chan struct{}
}

func New[T gproto.Message]() *Watch[T] {
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

	w.value = cloneMessage(value)
	w.hasValue = true

	for _, ch := range w.receivers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
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

func (w *Watch[T]) load() (T, bool) {
	w.Lock()
	defer w.Unlock()

	var zero T
	if !w.hasValue {
		return zero, false
	}
	return cloneMessage(w.value), true
}

type Receiver[T gproto.Message] struct {
	watch   *Watch[T]
	changed chan struct{}
}

func (r *Receiver[T]) Changed() <-chan struct{} {
	return r.changed
}

func (r *Receiver[T]) Load() (T, bool) {
	return r.watch.load()
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

func cloneMessage[T gproto.Message](value T) T {
	var zero T
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return zero
	}
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return zero
		}
	}
	cloned, ok := gproto.Clone(value).(T)
	if !ok {
		panic(fmt.Sprintf("failed to clone watch value of type %T", value))
	}
	return cloned
}
