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

import "sync"

type Watch[T comparable] struct {
	mu        sync.RWMutex
	val       T
	ch        chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func NewWatch[T comparable](initial T) *Watch[T] {
	return &Watch[T]{val: initial, ch: make(chan struct{}), done: make(chan struct{})}
}

func (w *Watch[T]) Send(value T) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.val == value {
		return
	}
	w.val = value
	close(w.ch)
	w.ch = make(chan struct{})
}

func (w *Watch[T]) Get() T {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.val
}

func (w *Watch[T]) Changed() <-chan struct{} {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.ch
}

func (w *Watch[T]) Close() {
	w.closeOnce.Do(func() {
		close(w.done)
	})
}

func (w *Watch[T]) Done() <-chan struct{} {
	return w.done
}
