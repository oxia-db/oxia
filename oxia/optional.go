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

package oxia

// Optional represents a wrapper for some value that can be present or not.
type Optional[T any] interface {
	// Present is true if the optional value is set
	Present() bool

	// Empty is true if the optional value is not set
	Empty() bool

	// Get the value and test if it was present
	Get() (value T, ok bool)

	// MustGet get the value and panic if it's not present
	MustGet() T
}

type optional[T any] struct {
	value   T
	present bool
}

func (o *optional[T]) Present() bool {
	return o.present
}

func (o *optional[T]) Empty() bool {
	return !o.present
}

func (o *optional[T]) Get() (value T, ok bool) {
	return o.value, o.present
}

func (o *optional[T]) MustGet() T {
	if o.Empty() {
		panic("optional empty on MustGet call")
	}
	return o.value
}

func optionalOf[T any](t T) Optional[T] {
	return &optional[T]{
		present: true,
		value:   t,
	}
}

func empty[T any]() Optional[T] {
	return &optional[T]{present: false}
}
