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
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/dataserver"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"
)

type testShardStrategy struct {
}

func (s *testShardStrategy) Get(key string) func(Shard) bool {
	return func(shard Shard) bool {
		return shard.Id%2 == 0
	}
}

func TestWithStandalone(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	clientPool := rpc.NewClientPool(nil, nil)
	shardManager, err := NewShardManager(&testShardStrategy{}, clientPool, standaloneServer.ServiceAddr(),
		constant.DefaultNamespace, 30*time.Second)
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, shardManager.Close())
	}()

	shardId := shardManager.Get("foo")

	assert.EqualValues(t, 0, shardId)
}

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

func TestUpdateLeader(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	clientPool := rpc.NewClientPool(nil, nil)
	shardManager, err := NewShardManager(&testShardStrategy{}, clientPool, standaloneServer.ServiceAddr(),
		constant.DefaultNamespace, 30*time.Second)
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, shardManager.Close())
	}()

	// Get the shard to verify it exists
	shardId := shardManager.Get("foo")
	assert.EqualValues(t, 0, shardId)

	// Get current leader
	originalLeader := shardManager.Leader(shardId)
	assert.NotEmpty(t, originalLeader)

	// Update leader to a new address
	newLeader := "new-leader:6650"
	shardManager.UpdateLeader(shardId, newLeader)

	// Verify leader was updated
	assert.Equal(t, newLeader, shardManager.Leader(shardId))
}

func TestUpdateLeaderEmptyAddress(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	clientPool := rpc.NewClientPool(nil, nil)
	shardManager, err := NewShardManager(&testShardStrategy{}, clientPool, standaloneServer.ServiceAddr(),
		constant.DefaultNamespace, 30*time.Second)
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, shardManager.Close())
	}()

	// Get the shard to verify it exists
	shardId := shardManager.Get("foo")
	assert.EqualValues(t, 0, shardId)

	// Get current leader
	originalLeader := shardManager.Leader(shardId)
	assert.NotEmpty(t, originalLeader)

	// Update leader with empty address should be a no-op
	shardManager.UpdateLeader(shardId, "")

	// Verify leader was NOT updated
	assert.Equal(t, originalLeader, shardManager.Leader(shardId))
}

func TestUpdateLeaderUnknownShard(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	clientPool := rpc.NewClientPool(nil, nil)
	shardManager, err := NewShardManager(&testShardStrategy{}, clientPool, standaloneServer.ServiceAddr(),
		constant.DefaultNamespace, 30*time.Second)
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, shardManager.Close())
	}()

	// Update leader for an unknown shard should not panic
	assert.NotPanics(t, func() {
		shardManager.UpdateLeader(9999, "new-leader:6650")
	})
}

func TestUpdateLeaderSameAddress(t *testing.T) {
	standaloneServer, err := dataserver.NewStandalone(dataserver.NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	clientPool := rpc.NewClientPool(nil, nil)
	shardManager, err := NewShardManager(&testShardStrategy{}, clientPool, standaloneServer.ServiceAddr(),
		constant.DefaultNamespace, 30*time.Second)
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, shardManager.Close())
	}()

	// Get the shard to verify it exists
	shardId := shardManager.Get("foo")
	originalLeader := shardManager.Leader(shardId)

	// Update leader with the same address should be a no-op (no error)
	assert.NotPanics(t, func() {
		shardManager.UpdateLeader(shardId, originalLeader)
	})

	// Verify leader is still the same
	assert.Equal(t, originalLeader, shardManager.Leader(shardId))
}
