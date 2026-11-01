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

package concurrent

import (
	"context"

	"github.com/oxia-db/oxia/common/channel"
)

type Future[T any] interface {

	// Wait until the future is either completed or failed
	// You should only call wait once
	Wait(ctx context.Context) (T, error)

	// Complete completes the future with result, unless it is already
	// completed or failed
	Complete(result T)

	// Fail Signal that one party has failed in the operation, unless the
	// future is already completed or failed
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
	// A ready value wins over a done ctx, which select would pick at random
	select {
	case v := <-f.val:
		return v.t, v.err
	default:
	}
	select {
	case v := <-f.val:
		return v.t, v.err

	case <-ctx.Done():
		return t, ctx.Err()
	}
}

// Complete and Fail keep the first value only: a later one would block on
// the full channel.
func (f *future[T]) Complete(result T) {
	channel.PushNoBlock(f.val, value[T]{result, nil})
}

func (f *future[T]) Fail(err error) {
	var t T
	channel.PushNoBlock(f.val, value[T]{t, err})
}
