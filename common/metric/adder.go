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

package metric

import (
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
)

// adder is an int64 sum that starts as a single atomic and switches to a
// striped counter the first time two adds collide: uncontended series stay
// small and as cheap as an atomic, contended ones stop sharing a cache line.
type adder struct {
	base    atomic.Int64
	stripes atomic.Pointer[xsync.Counter]
}

func (a *adder) Add(d int64) {
	if s := a.stripes.Load(); s != nil {
		s.Add(d)
		return
	}
	v := a.base.Load()
	if a.base.CompareAndSwap(v, v+d) {
		return
	}
	a.stripes.CompareAndSwap(nil, xsync.NewCounter())
	a.stripes.Load().Add(d)
}

func (a *adder) Sum() int64 {
	s := a.base.Load()
	if stripes := a.stripes.Load(); stripes != nil {
		s += stripes.Value()
	}
	return s
}
