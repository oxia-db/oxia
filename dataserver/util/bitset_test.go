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

//revive:disable-next-line:var-naming
package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitSet(t *testing.T) {
	bs := BitSet{}

	assert.Zero(t, bs.Count())

	bs.Set(0)
	assert.Equal(t, 1, bs.Count())

	bs.Set(2)
	assert.Equal(t, 2, bs.Count())

	bs.Set(1)
	assert.Equal(t, 3, bs.Count())

	bs.Set(2)
	assert.Equal(t, 3, bs.Count())
}

func TestBitSetPanic(t *testing.T) {
	bs := BitSet{}

	assert.Panics(t, func() {
		bs.Set(-2)
	})

	assert.Panics(t, func() {
		bs.Set(16)
	})

	assert.Panics(t, func() {
		bs.Set(20)
	})
}
