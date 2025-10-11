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

var _ StreamCallback[any] = &BatchStreamOnce[any]{}

type BatchStreamOnce[T any] struct {
	container []T

	maxBatchCount   int
	maxBatchBytes   int
	totalBatchBytes int

	completed atomic.Bool

	onFlush    func(container []T) error
	onComplete func(err error)
	getBytes   func(T) int
}

func (b *BatchStreamOnce[T]) OnNext(t T) error {
	b.container = append(b.container, t)
	b.totalBatchBytes += b.getBytes(t)
	if (b.maxBatchCount != 0 && len(b.container) >= b.maxBatchCount) || b.totalBatchBytes >= b.maxBatchBytes {
		err := b.onFlush(b.container)
		if err != nil {
			return err
		}
		b.totalBatchBytes = 0
		b.container = b.container[:0]
	}
	return nil
}

func (b *BatchStreamOnce[T]) OnComplete(err error) {
	if !b.completed.CompareAndSwap(false, true) {
		return
	}
	if err != nil {
		b.onComplete(err)
		return
	}
	if len(b.container) != 0 {
		err = b.onFlush(b.container)
	}
	b.onComplete(err)
}

func NewBatchStreamOnce[T any](
	maxBatchCount int,
	maxBatchBytes int,
	getBytes func(T) int,
	onFlush func(container []T) error,
	onComplete func(err error)) StreamCallback[T] {
	return &BatchStreamOnce[T]{
		maxBatchCount: maxBatchCount,
		maxBatchBytes: maxBatchBytes,
		onFlush:       onFlush,
		onComplete:    onComplete,
		getBytes:      getBytes,
	}
}
