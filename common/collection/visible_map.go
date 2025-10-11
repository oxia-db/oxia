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
	"encoding/json"
	"sync/atomic"
)

// visibleMap is a generic struct designed to provide a thread-safe mapping.
// It uses atomic operations to support safe concurrent reads of the size.
//
// K: The type of the key, which must be comparable.
// V: The type of the value, which can be any type (any).
//
// Fields:
//   - container: A map storing key-value pairs of type map[K]V.
//   - size: An atomic integer storing the number of elements in the map,
//     using atomic.Int32 to support efficient concurrent reads.
//
// This struct is designed to allow multiple goroutines to safely read size
// without needing locks, thereby improving performance. However, write operations
// (such as adding or removing elements) must still ensure thread safety,
// potentially requiring additional synchronization mechanisms.
type visibleMap[K comparable, V any] struct {
	container map[K]V
	size      atomic.Int32
}

func (v *visibleMap[K, V]) Put(key K, value V) {
	if _, exist := v.Get(key); !exist {
		v.size.Add(1)
	}
	v.container[key] = value
}

func (v *visibleMap[K, V]) Get(key K) (value V, found bool) {
	value, found = v.container[key]
	return value, found
}
func (v *visibleMap[K, V]) Remove(key K) {
	if _, exist := v.Get(key); exist {
		v.size.Add(-1)
	}
	delete(v.container, key)
}

func (v *visibleMap[K, V]) Keys() []K {
	keys := make([]K, 0, len(v.container))
	for key := range v.container {
		keys = append(keys, key)
	}
	return keys
}

func (v *visibleMap[K, V]) Values() []V {
	values := make([]V, 0, len(v.container))
	for key := range v.container {
		values = append(values, v.container[key])
	}
	return values
}

func (v *visibleMap[K, V]) Empty() bool {
	return v.size.Load() == 0
}

func (v *visibleMap[K, V]) Size() int {
	return int(v.size.Load())
}
func (v *visibleMap[K, V]) Clear() {
	v.container = make(map[K]V)
	v.size.Store(0)
}

func (v *visibleMap[K, V]) String() string {
	data, err := json.Marshal(v.container)
	if err != nil {
		panic(err)
	}
	return string(data)
}
