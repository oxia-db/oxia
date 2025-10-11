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

func TestVisibleMap(t *testing.T) {
	vm := NewVisibleMap[string, int]()

	size := vm.Size()
	assert.Equal(t, size, 0)
	assert.True(t, vm.Empty())
	vm.Put("one", 1)
	val, found := vm.Get("one")
	assert.Equal(t, val, 1)
	assert.True(t, found)

	// test repeat put
	vm.Put("one", 10)
	val, found = vm.Get("one")
	assert.Equal(t, val, 10)
	assert.True(t, found)
	assert.Equal(t, vm.Size(), 1)

	vm.Put("two", 2)
	vm.Put("three", 3)
	assert.Equal(t, vm.Size(), 3)

	keys := vm.Keys()
	values := vm.Values()
	expectedKeys := []string{"one", "two", "three"}
	expectedValues := []int{10, 2, 3}

	for _, key := range expectedKeys {
		found := false
		for _, k := range keys {
			if k == key {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected key %s not found", key)
		}
	}

	for _, value := range expectedValues {
		found := false
		for _, v := range values {
			if v == value {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected value %d not found", value)
		}
	}

	vm.Remove("two")
	_, found = vm.Get("two")
	assert.False(t, found)
	assert.Equal(t, vm.Size(), 2)

	vm.Clear()
	assert.Equal(t, vm.Size(), 0)
	assert.True(t, vm.Empty())

	vm.Put("four", 4)
	output := vm.String()
	assert.Equal(t, "{\"four\":4}", output)

	vm.Clear()
	output = vm.String()
	assert.Equal(t, "{}", output)
}
