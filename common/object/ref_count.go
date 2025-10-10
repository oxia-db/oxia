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

import (
	"io"
	"sync/atomic"
)

type RefCount[T io.Closer] interface {
	io.Closer

	Acquire() RefCount[T]

	RefCnt() int32

	Get() T
}

func NewRefCount[T io.Closer](t T) RefCount[T] {
	res := &refCount[T]{
		rc: &atomic.Int32{},
		t:  t,
	}
	res.rc.Store(1)
	return res
}

func (r refCount[T]) RefCnt() int32 {
	return r.rc.Load()
}

type refCount[T io.Closer] struct {
	rc *atomic.Int32
	t  T
}

func (r refCount[T]) Close() error {
	if count := r.rc.Add(-1); count == 0 {
		return r.t.Close()
	}

	return nil
}

func (r refCount[T]) Acquire() RefCount[T] {
	r.rc.Add(1)
	return &refCount[T]{
		rc: r.rc,
		t:  r.t,
	}
}

func (r refCount[T]) Get() T {
	return r.t
}
