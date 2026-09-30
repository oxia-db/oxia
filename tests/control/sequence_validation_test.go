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

package control

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/tests/mock"
)

// A sequential put can be invalid for the content of the shard, e.g. when a key
// of the same prefix has more sequences than the put has deltas. Every replica
// must apply the entry the same way: a follower that fails to apply it stops
// advancing, and could never take over from the leader.
func TestSequenceKeyValidation_InvalidPutDoesNotBlockFailover(t *testing.T) {
	s1, sa1 := mock.NewServer(t, "s1")
	s2, sa2 := mock.NewServer(t, "s2")
	s3, sa3 := mock.NewServer(t, "s3")
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}
	defer func() {
		for _, s := range servers {
			assert.NoError(t, s.Close())
		}
	}()

	metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   newDefaultClusterConfig(sa1, sa2, sa3),
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc.NewRpcProviderFactory(nil))
	defer coordinatorInstance.Close()

	shardMetadata := waitForLeaderFeature(t, coordinatorInstance.Metadata(), servers,
		proto.Feature_FEATURE_SEQUENCE_KEY_VALIDATION)
	leader := shardMetadata.Leader.GetNameOrDefault()
	lead, err := servers[leader].GetShardDirector().GetLeader(0)
	require.NoError(t, err)

	// Connect through a server that survives the leader
	survivor := sa1
	if leader == sa1.GetNameOrDefault() {
		survivor = sa2
	}
	client, err := oxia.NewSyncClient(survivor.Public, oxia.WithNamespace(constant.DefaultNamespace))
	require.NoError(t, err)
	defer client.Close()

	key, _, err := client.Put(t.Context(), "a", []byte("0"), oxia.PartitionKey("x"), oxia.SequenceKeysDeltas(1, 1))
	require.NoError(t, err)
	assert.Equal(t, "a-00000000000000000001-00000000000000000001", key)

	// The sequence has two parts: a put with a single delta is invalid
	_, _, err = client.Put(t.Context(), "a", []byte("1"), oxia.PartitionKey("x"), oxia.SequenceKeysDeltas(1))
	assert.ErrorIs(t, err, oxia.ErrInvalidOptions)

	// Commit offsets are piggybacked on the replication messages: after one more
	// write the followers know that the entries up to "b" are committed
	_, _, err = client.Put(t.Context(), "b", []byte("0"))
	require.NoError(t, err)
	commitOffset := lead.CommitOffset()
	_, _, err = client.Put(t.Context(), "flush", []byte("0"))
	require.NoError(t, err)

	for name, s := range servers {
		if name == leader {
			continue
		}
		assert.Eventually(t, func() bool {
			follow, err := s.GetShardDirector().GetFollower(0)
			return err == nil && follow.CommitOffset() >= commitOffset
		}, 10*time.Second, 100*time.Millisecond, "follower %s did not apply the committed entries", name)
	}

	// Stop the leader: one of the followers must take over
	require.NoError(t, servers[leader].Close())
	delete(servers, leader)

	require.Eventually(t, func() bool {
		shard := mock.StatusSnapshot(t, coordinatorInstance.Metadata()).Namespaces[constant.DefaultNamespace].GetShards()[0]
		return shard.GetStatusOrDefault() == proto.ShardStatusSteadyState &&
			shard.GetLeader().GetNameOrDefault() != leader
	}, 30*time.Second, 100*time.Millisecond, "no new leader was elected")

	assert.Eventually(t, func() bool {
		_, value, _, err := client.Get(t.Context(), "b")
		return err == nil && string(value) == "0"
	}, 10*time.Second, 100*time.Millisecond)
}
