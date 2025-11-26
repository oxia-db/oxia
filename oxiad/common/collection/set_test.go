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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	s := NewSet[int]()
	assert.True(t, s.IsEmpty())
	assert.Equal(t, 0, s.Count())
	assert.False(t, s.Contains(5))

	s.Add(5)
	assert.False(t, s.IsEmpty())
	assert.Equal(t, 1, s.Count())
	assert.True(t, s.Contains(5))

	s.Remove(5)
	assert.True(t, s.IsEmpty())
	assert.Equal(t, 0, s.Count())
	assert.False(t, s.Contains(5))

	s.Add(1)
	s.Add(1)
	assert.False(t, s.IsEmpty())
	assert.Equal(t, 1, s.Count())
	assert.True(t, s.Contains(1))

	s.Add(3)
	s.Add(2)
	assert.Equal(t, []int{1, 2, 3}, s.GetSorted())

	o := NewSetFrom([]int{3, 4, 5})
	assert.Equal(t, []int{1, 2}, s.Complement(o).GetSorted())
}
