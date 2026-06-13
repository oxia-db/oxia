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

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestBatcher_FlushByCounter(t *testing.T) {
	var flushed [][]int
	b := NewBatcher[int](3, 2048,
		func(t int) int { return t },
		func(container []int) error {
			flushed = append(flushed, append([]int{}, container...))
			return nil
		})

	assert.NoError(t, b.Add(1))
	assert.NoError(t, b.Add(2))
	assert.Empty(t, flushed)
	assert.NoError(t, b.Add(3))
	assert.Equal(t, [][]int{{1, 2, 3}}, flushed)

	assert.NoError(t, b.Add(4))
	assert.NoError(t, b.Add(5))
	assert.NoError(t, b.Add(6))
	assert.Equal(t, [][]int{{1, 2, 3}, {4, 5, 6}}, flushed)

	assert.NoError(t, b.Add(7))
	assert.NoError(t, b.Add(8))
	assert.NoError(t, b.Flush())
	assert.Equal(t, [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8}}, flushed)

	// Nothing left: Flush is a no-op
	assert.NoError(t, b.Flush())
	assert.Equal(t, 3, len(flushed))
}

func TestBatcher_FlushByBytes(t *testing.T) {
	var flushed [][]int
	b := NewBatcher[int](10, 2048,
		func(t int) int { return t },
		func(container []int) error {
			flushed = append(flushed, append([]int{}, container...))
			return nil
		})

	assert.NoError(t, b.Add(1024))
	assert.Empty(t, flushed)
	assert.NoError(t, b.Add(1024))
	assert.Equal(t, [][]int{{1024, 1024}}, flushed)

	assert.NoError(t, b.Add(512))
	assert.NoError(t, b.Flush())
	assert.Equal(t, [][]int{{1024, 1024}, {512}}, flushed)
}

func TestBatcher_FlushError(t *testing.T) {
	expectedErr := errors.New("flush failed")
	b := NewBatcher[int](2, 2048,
		func(t int) int { return t },
		func([]int) error { return expectedErr })

	assert.NoError(t, b.Add(1))
	assert.ErrorIs(t, b.Add(2), expectedErr)
	assert.ErrorIs(t, b.Flush(), expectedErr)
}
