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

// Batcher accumulates items and flushes them in containers bounded by count
// and by size. It is not safe for concurrent use: it is meant for synchronous
// producers, which call Add for every item and Flush once at the end.
type Batcher[T any] struct {
	container       []T
	maxBatchCount   int
	maxBatchBytes   int
	totalBatchBytes int

	onFlush  func(container []T) error
	getBytes func(T) int
}

func NewBatcher[T any](
	maxBatchCount int,
	maxBatchBytes int,
	getBytes func(T) int,
	onFlush func(container []T) error) *Batcher[T] {
	return &Batcher[T]{
		maxBatchCount: maxBatchCount,
		maxBatchBytes: maxBatchBytes,
		onFlush:       onFlush,
		getBytes:      getBytes,
	}
}

func (b *Batcher[T]) Add(t T) error {
	b.container = append(b.container, t)
	b.totalBatchBytes += b.getBytes(t)
	if (b.maxBatchCount != 0 && len(b.container) >= b.maxBatchCount) || b.totalBatchBytes >= b.maxBatchBytes {
		return b.flush()
	}
	return nil
}

// Flush sends the remaining items, if any.
func (b *Batcher[T]) Flush() error {
	if len(b.container) == 0 {
		return nil
	}
	return b.flush()
}

func (b *Batcher[T]) flush() error {
	if err := b.onFlush(b.container); err != nil {
		return err
	}
	b.totalBatchBytes = 0
	b.container = b.container[:0]
	return nil
}
