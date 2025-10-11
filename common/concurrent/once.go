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

import "sync/atomic"

// Once ensures that a specific callback is executed only once, either on completion or on error.
// It prevents further execution of the callbacks after the first call and ensures atomicity
// of the operation using the atomic package.
//
// The generic type T represents the result type of the operation, and the callbacks
// provide the behavior for handling success or failure of the operation.
//
// Fields:
// - OnComplete: A function that gets called with the result of type T when the operation completes successfully.
// - OnCompleteError: A function that gets called with an error if the operation fails.
// - completed: An atomic boolean used to track if the operation has already completed, ensuring only one callback is executed.

type Once[T any] struct {
	onComplete      func(t T)       // Callback function called on successful completion
	onCompleteError func(err error) // Callback function called when an error occurs
	completed       atomic.Bool     // Atomic flag to track completion status
}

// OnComplete is called to notify that the operation has completed successfully with the result 't'.
// It ensures that the 'OnComplete' callback is only called once.
func (c *Once[T]) OnComplete(t T) {
	if !c.completed.CompareAndSwap(false, true) {
		return
	}
	c.onComplete(t)
}

// OnCompleteError is called to notify that the operation has failed with an error 'err'.
// It ensures that the 'OnCompleteError' callback is only called once.
func (c *Once[T]) OnCompleteError(err error) {
	if !c.completed.CompareAndSwap(false, true) {
		return
	}
	c.onCompleteError(err)
}

// NewOnce creates a new instance of Once with the provided success and error callbacks.
// It ensures that the callbacks are invoked only once, either for success or failure.
func NewOnce[T any](onComplete func(t T), onError func(err error)) Callback[T] {
	return &Once[T]{
		onComplete,
		onError,
		atomic.Bool{},
	}
}

var _ StreamCallback[any] = &Stream[any]{}

type Stream[T any] struct {
	onNext     func(t T) error
	onComplete func(err error)
	completed  atomic.Bool
}

func (s *Stream[T]) OnNext(element T) error {
	return s.onNext(element)
}

func (s *Stream[T]) OnComplete(err error) {
	if !s.completed.CompareAndSwap(false, true) {
		return
	}
	s.onComplete(err)
}

func NewStreamOnce[T any](onNext func(T) error, onComplete func(err error)) StreamCallback[T] {
	return &Stream[T]{
		onNext:     onNext,
		onComplete: onComplete,
		completed:  atomic.Bool{},
	}
}
