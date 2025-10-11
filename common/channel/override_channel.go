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

package channel

import (
	"context"
	"io"
	"sync"
)

// OverrideChannel is a type of channel that only keeps the latest value, the write operation never blocks.
type OverrideChannel[T any] interface {
	io.Closer

	Receive(ctx context.Context) (T, error)

	//
	Ch() chan T

	// Update the latest value
	WriteLast(value T)
}

type overrideChannel[T any] struct {
	sync.Mutex
	ch chan T
}

func NewOverrideChannel[T any]() OverrideChannel[T] {
	return &overrideChannel[T]{
		ch: make(chan T, 1),
	}
}

func (o *overrideChannel[T]) Close() error {
	close(o.ch)
	return nil
}

func (o *overrideChannel[T]) Receive(ctx context.Context) (t T, err error) {
	select {
	case t = <-o.ch:
		return t, nil
	case <-ctx.Done():
		return t, ctx.Err()
	}
}

func (o *overrideChannel[T]) Ch() chan T {
	return o.ch
}

func (o *overrideChannel[T]) WriteLast(value T) {
	o.Lock()
	defer o.Unlock()

	for {
		select {
		case o.ch <- value:
			// If we were able to write on the channel, we're good: the receiver will eventually read
			// the notification from here
			return

		default:
			// If the channel buffer is full, empty the channel and retry
			select {
			case <-o.ch:
				continue
			default:
				continue
			}
		}
	}
}
