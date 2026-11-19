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

package internal

type ShardStrategy interface {
	// Get returns a predicate that tells whether a shard holds the key.
	Get(key string) func(Shard) bool
	// HashCode returns the code the key hashes to: the shard whose hash range
	// contains it holds the key.
	HashCode(key string) uint32
}

type Shard struct {
	Id        int64
	Leader    string
	HashRange HashRange
}

type HashRange struct {
	MinInclusive uint32
	MaxInclusive uint32
}
