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

package object

// Borrowed wraps a value that is exposed without copying.
//
// The wrapped value remains owned by the producer. Callers must treat it as
// read-only and must not retain the borrowed reference beyond immediate use.
type Borrowed[T any] struct {
	value T
}

// Borrow wraps a value in a Borrowed marker so callers can see they do not own
// the returned reference.
func Borrow[T any](value T) Borrowed[T] {
	return Borrowed[T]{value: value}
}

// UnsafeBorrow returns the wrapped value without copying it.
//
// The returned value aliases state owned elsewhere and must not be mutated or
// retained by callers.
func (b Borrowed[T]) UnsafeBorrow() T {
	return b.value
}
