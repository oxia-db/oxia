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

package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardStrategy(t *testing.T) {
	shardStrategy := &shardStrategyImpl{
		hashFunc: func(key string) uint32 {
			return 2
		},
	}
	predicate := shardStrategy.Get("foo")

	for _, item := range []struct {
		minInclusive uint32
		maxInclusive uint32
		match        bool
	}{
		{1, 3, true},
		{2, 3, true},
		{1, 2, true},
		{1, 1, false},
		{3, 3, false},
	} {
		shard := Shard{
			HashRange: HashRange{
				MinInclusive: item.minInclusive,
				MaxInclusive: item.maxInclusive,
			},
		}
		assert.Equal(t, item.match, predicate(shard))
	}
}
