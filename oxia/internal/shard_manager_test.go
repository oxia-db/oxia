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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// numericKeyStrategy hashes a key, a number, to itself.
type numericKeyStrategy struct{}

func (numericKeyStrategy) Get(key string) func(Shard) bool {
	code, err := strconv.ParseUint(key, 10, 32)
	if err != nil {
		panic(err)
	}
	return func(shard Shard) bool {
		return shard.HashRange.MinInclusive <= uint32(code) && uint32(code) <= shard.HashRange.MaxInclusive
	}
}

func TestShardManagerSuccessors(t *testing.T) {
	var replaced []map[int64][]int64
	s := &shardManagerImpl{
		shardStrategy: numericKeyStrategy{},
		shards:        make(map[int64]Shard),
		successors:    make(map[int64][]Shard),
		changed:       make(chan struct{}),
		updatedWg:     concurrent.NewWaitGroup(1),
		logger:        slog.Default(),
	}
	s.onShardsReplaced = func(r map[int64][]int64) {
		// The new shard map is not visible yet
		assert.False(t, s.TryRLock())
		replaced = append(replaced, r)
	}
	s.update([]Shard{
		{Id: 0, HashRange: hashRange(0, 99)},
		{Id: 1, HashRange: hashRange(100, 199)},
	})
	assert.Nil(t, s.GetSuccessors(0))
	assert.Empty(t, replaced)

	// Shard 0 is split into shards 2 and 3
	s.update([]Shard{
		{Id: 1, HashRange: hashRange(100, 199)},
		{Id: 2, HashRange: hashRange(0, 49)},
		{Id: 3, HashRange: hashRange(50, 99)},
	})
	assert.ElementsMatch(t, []int64{2, 3}, s.GetSuccessors(0))
	assert.Nil(t, s.GetSuccessors(1))
	assertSuccessor(t, s, 0, "10", 2)
	assertSuccessor(t, s, 0, "60", 3)
	require.Len(t, replaced, 1)
	assert.ElementsMatch(t, []int64{2, 3}, replaced[0][0])

	// Shard 3 is split too, before the calls pending on shard 0 are rerouted:
	// they still go to shard 3 first, which reroutes them in turn, so they
	// keep their order
	s.update([]Shard{
		{Id: 1, HashRange: hashRange(100, 199)},
		{Id: 2, HashRange: hashRange(0, 49)},
		{Id: 4, HashRange: hashRange(50, 74)},
		{Id: 5, HashRange: hashRange(75, 99)},
	})
	assert.ElementsMatch(t, []int64{2, 3}, s.GetSuccessors(0))
	assert.ElementsMatch(t, []int64{4, 5}, s.GetSuccessors(3))
	assertSuccessor(t, s, 0, "60", 3)
	assertSuccessor(t, s, 3, "60", 4)
	assertSuccessor(t, s, 3, "80", 5)
	_, ok := s.GetSuccessor(1, "150")
	assert.False(t, ok)
	require.Len(t, replaced, 2)
	assert.ElementsMatch(t, []int64{4, 5}, replaced[1][3])
}

func assertSuccessor(t *testing.T, s *shardManagerImpl, shardId int64, key string, expected int64) {
	t.Helper()
	successor, ok := s.GetSuccessor(shardId, key)
	assert.True(t, ok)
	assert.Equal(t, expected, successor)
}

func TestShardManagerChanged(t *testing.T) {
	s := &shardManagerImpl{
		shards:     make(map[int64]Shard),
		successors: make(map[int64][]Shard),
		changed:    make(chan struct{}),
		updatedWg:  concurrent.NewWaitGroup(1),
		logger:     slog.Default(),
	}
	changed := s.Changed()
	select {
	case <-changed:
		assert.Fail(t, "the shard map has not changed")
	default:
	}

	s.update([]Shard{{Id: 0, HashRange: hashRange(0, 99)}})
	select {
	case <-changed:
	default:
		assert.Fail(t, "the shard map has changed")
	}
	assert.NotEqual(t, changed, s.Changed())
}
