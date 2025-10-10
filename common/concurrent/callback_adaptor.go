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

type StreamCallbackAdaptor[T any] struct {
	ch chan T

	statusMutex sync.Mutex
	err         error
	completed   bool
}

func (s *StreamCallbackAdaptor[T]) OnNext(t T) error {
	s.ch <- t
	return nil
}

func (s *StreamCallbackAdaptor[T]) OnComplete(err error) {
	s.statusMutex.Lock()
	defer s.statusMutex.Unlock()
	s.completed = true
	s.err = err
}

func (s *StreamCallbackAdaptor[T]) Error() error {
	s.statusMutex.Lock()
	defer s.statusMutex.Unlock()
	return s.err
}

func (s *StreamCallbackAdaptor[T]) IsCompleted() bool {
	s.statusMutex.Lock()
	defer s.statusMutex.Unlock()
	return s.completed
}

func (s *StreamCallbackAdaptor[T]) Ch() <-chan T {
	return s.ch
}

func NewStreamCallbackAdaptor[T any]() *StreamCallbackAdaptor[T] {
	return &StreamCallbackAdaptor[T]{
		ch:  make(chan T, 1000),
		err: nil,
	}
}
