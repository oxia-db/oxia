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

package dataserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/dataserver/kv"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/proto"
)

func TestShardsDirector_DeleteShardLeader(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	sd := NewShardsDirector(Config{}, walFactory, kvFactory, newMockRpcClient())

	lc, _ := sd.GetOrCreateLeader(constant.DefaultNamespace, shard)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: "k1", Value: []byte("hello")}},
	})
	assert.NoError(t, err)

	_, err = lc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: constant.DefaultNamespace,
		Shard:     shard,
		Term:      1,
	})
	assert.NoError(t, err)

	// Reopen
	lc, err = sd.GetOrCreateLeader(constant.DefaultNamespace, shard)
	assert.NoError(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}

func TestShardsDirector_GetOrCreateFollower(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	sd := NewShardsDirector(Config{}, walFactory, kvFactory, newMockRpcClient())

	lc, _ := sd.GetOrCreateLeader(constant.DefaultNamespace, shard)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              2,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	assert.EqualValues(t, 2, lc.Term())

	// Should fail to get closed if the term is wrong
	fc, err := sd.GetOrCreateFollower(constant.DefaultNamespace, shard, 1)
	assert.ErrorIs(t, constant.ErrInvalidTerm, err)
	assert.Nil(t, fc)
	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	// Will get closed if term is correct
	fc, err = sd.GetOrCreateFollower(constant.DefaultNamespace, shard, 2)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	assert.NoError(t, fc.Close())
	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}
