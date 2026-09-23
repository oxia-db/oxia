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

import (
	"fmt"
	"log/slog"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/concurrent"
)

func TestFindOverlap(t *testing.T) {
	for _, item := range []struct {
		name        string
		shards      []Shard
		overlapping []int64
	}{
		{"no shards", nil, nil},
		{"adjacent shards", []Shard{
			{Id: 1, HashRange: hashRange(100, 199)},
			{Id: 0, HashRange: hashRange(0, 99)},
		}, nil},
		{"gap between shards", []Shard{
			{Id: 0, HashRange: hashRange(0, 99)},
			{Id: 1, HashRange: hashRange(150, 199)},
		}, nil},
		{"parent and left child", []Shard{
			{Id: 0, HashRange: hashRange(0, 99)},
			{Id: 1, HashRange: hashRange(0, 49)},
		}, []int64{0, 1}},
		{"parent and right child", []Shard{
			{Id: 2, HashRange: hashRange(50, 99)},
			{Id: 0, HashRange: hashRange(0, 99)},
		}, []int64{0, 2}},
		{"parent and both children", []Shard{
			{Id: 1, HashRange: hashRange(0, 49)},
			{Id: 2, HashRange: hashRange(50, 99)},
			{Id: 0, HashRange: hashRange(0, 99)},
		}, []int64{0, 1}},
	} {
		t.Run(item.name, func(t *testing.T) {
			a, b, found := findOverlap(item.shards)
			assert.Equal(t, item.overlapping != nil, found)
			if found {
				assert.ElementsMatch(t, item.overlapping, []int64{a.Id, b.Id})
			}
		})
	}
}

func TestShardManagerIgnoresOverlappingAssignments(t *testing.T) {
	s := &shardManagerImpl{
		shardStrategy: NewShardStrategy(),
		shards:        make(map[int64]Shard),
		updatedWg:     concurrent.NewWaitGroup(1),
		logger:        slog.Default(),
	}
	parent := Shard{Id: 0, HashRange: hashRange(0, math.MaxUint32)}
	leftChild := Shard{Id: 1, HashRange: hashRange(0, math.MaxUint32/2)}
	rightChild := Shard{Id: 2, HashRange: hashRange(math.MaxUint32/2+1, math.MaxUint32)}
	s.update([]Shard{parent})

	// Applied shard by shard, the left child would replace the parent, and
	// leave the hash range of the right child without a shard
	s.update([]Shard{parent, leftChild})
	assert.ElementsMatch(t, []int64{parent.Id}, s.GetAll())
	assert.NotPanics(t, func() {
		for i := range 100 {
			s.Get(fmt.Sprintf("key-%d", i))
		}
	})

	s.update([]Shard{leftChild, parent, rightChild})
	assert.ElementsMatch(t, []int64{parent.Id}, s.GetAll())
}
