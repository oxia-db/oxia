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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/concurrent"
)

func TestOverlap(t *testing.T) {
	for _, item := range []struct {
		a         HashRange
		b         HashRange
		isOverlap bool
	}{
		{hashRange(1, 2), hashRange(3, 6), false},
		{hashRange(1, 4), hashRange(3, 6), true},
		{hashRange(4, 5), hashRange(3, 6), true},
		{hashRange(5, 8), hashRange(3, 6), true},
		{hashRange(7, 8), hashRange(3, 6), false},
	} {
		assert.Equal(t, overlap(item.a, item.b), item.isOverlap)
	}
}

func TestShardManagerGetSuccessors(t *testing.T) {
	s := &shardManagerImpl{
		shards:        make(map[int64]Shard),
		removedShards: make(map[int64]HashRange),
		updatedWg:     concurrent.NewWaitGroup(1),
		logger:        slog.Default(),
	}
	s.update([]Shard{
		{Id: 0, HashRange: hashRange(0, 99)},
		{Id: 1, HashRange: hashRange(100, 199)},
	})
	assert.Nil(t, s.GetSuccessors(0))

	// Shard 0 is split into shards 2 and 3
	s.update([]Shard{
		{Id: 1, HashRange: hashRange(100, 199)},
		{Id: 2, HashRange: hashRange(0, 49)},
		{Id: 3, HashRange: hashRange(50, 99)},
	})
	assert.ElementsMatch(t, []int64{2, 3}, s.GetSuccessors(0))
	assert.Nil(t, s.GetSuccessors(1))

	// Shard 3 is split too, before the calls pending on shard 0 are rerouted
	s.update([]Shard{
		{Id: 1, HashRange: hashRange(100, 199)},
		{Id: 2, HashRange: hashRange(0, 49)},
		{Id: 4, HashRange: hashRange(50, 74)},
		{Id: 5, HashRange: hashRange(75, 99)},
	})
	assert.ElementsMatch(t, []int64{2, 4, 5}, s.GetSuccessors(0))
	assert.ElementsMatch(t, []int64{4, 5}, s.GetSuccessors(3))
}
