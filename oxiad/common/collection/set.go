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

package collection

import (
	"sort"

	"golang.org/x/exp/constraints"
)

type Set[T constraints.Ordered] interface {
	Add(t T)
	Remove(t T)
	Contains(t T) bool
	Count() int
	IsEmpty() bool
	GetSorted() []T
	Complement(other Set[T]) Set[T]
}

func NewSet[T constraints.Ordered]() Set[T] {
	return &set[T]{
		Items: map[T]bool{},
	}
}

func NewSetFrom[T constraints.Ordered](i []T) Set[T] {
	s := NewSet[T]()
	for _, x := range i {
		s.Add(x)
	}
	return s
}

type set[T constraints.Ordered] struct {
	Items map[T]bool
}

func (s *set[T]) Add(t T) {
	s.Items[t] = true
}

func (s *set[T]) Remove(t T) {
	delete(s.Items, t)
}

func (s *set[T]) Contains(t T) bool {
	_, found := s.Items[t]
	return found
}

func (s *set[T]) Count() int {
	return len(s.Items)
}

func (s *set[T]) IsEmpty() bool {
	return s.Count() == 0
}

// Complement Return a new Set which is the complement of the `current` set with `other`
// eg: `res = current - other`
func (s *set[T]) Complement(other Set[T]) Set[T] {
	res := NewSet[T]()
	for k := range s.Items {
		if !other.Contains(k) {
			res.Add(k)
		}
	}
	return res
}

func (s *set[T]) GetSorted() []T {
	r := make([]T, 0, len(s.Items))
	for k := range s.Items {
		r = append(r, k)
	}

	sort.SliceStable(r, func(i, j int) bool {
		return r[i] < r[j]
	})
	return r
}
