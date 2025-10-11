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
)

type Future[T any] interface {

	// Wait until the future is either completed or failed
	// You should only call wait once
	Wait(ctx context.Context) (T, error)

	//
	Complete(result T)

	// Fail Signal that one party has failed in the operation
	Fail(err error)
}

type value[T any] struct {
	t   T
	err error
}

type future[T any] struct {
	val chan value[T]
}

func NewFuture[T any]() Future[T] {
	return &future[T]{
		val: make(chan value[T], 1),
	}
}

func (f *future[T]) Wait(ctx context.Context) (t T, err error) {
	select {
	case v := <-f.val:
		return v.t, v.err

	case <-ctx.Done():
		return t, ctx.Err()
	}
}

func (f *future[T]) Complete(result T) {
	f.val <- value[T]{result, nil}
}

func (f *future[T]) Fail(err error) {
	var t T
	f.val <- value[T]{t, err}
}
