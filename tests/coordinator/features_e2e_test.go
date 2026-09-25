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

package coordinator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/tests/mock"
)

// The coordinator only knows the features of a data server after a handshake
// with it. A new shard that elects a leader while a member of its ensemble is
// still down can't pin any feature: once that member starts and completes its
// handshake, a new election must pin the features the whole ensemble supports.
func TestCoordinator_PinsFeaturesOfDataServerStartedLate(t *testing.T) {
	s1, sa1 := mock.NewServer(t, "s1")
	s2, sa2 := mock.NewServer(t, "s2")
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
	}
	defer func() {
		for _, s := range servers {
			assert.NoError(t, s.Close())
		}
	}()
	// s3 only starts after the shard has elected a leader
	sa3 := &proto.DataServerIdentity{Name: new("s3"), Public: freeAddress(t), Internal: freeAddress(t)}

	metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value: newClusterConfig([]*proto.Namespace{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}}, []*proto.DataServerIdentity{sa1, sa2, sa3}),
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, coordrpc.NewRpcProviderFactory(nil))
	defer func() {
		assert.NoError(t, coordinatorInstance.Close())
	}()

	steadyShard := func() *proto.ShardMetadata {
		shard := mock.StatusSnapshot(t, coordinatorInstance.Metadata()).Namespaces[constant.DefaultNamespace].GetShards()[0]
		if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
			return nil
		}
		return shard
	}

	var shard *proto.ShardMetadata
	require.Eventually(t, func() bool {
		shard = steadyShard()
		return shard != nil
	}, 30*time.Second, 50*time.Millisecond)
	lead, err := servers[shard.GetLeader().GetNameOrDefault()].GetShardDirector().GetLeader(0)
	require.NoError(t, err)
	assert.False(t, lead.IsFeatureEnabled(proto.Feature_FEATURE_ORDERED_WRITES),
		"the features of s3 are unknown, so the term can't pin any")

	s3, _ := mock.NewServerWithAddress(t, sa3.GetName(), sa3.GetPublic(), sa3.GetInternal())
	servers[sa3.GetNameOrDefault()] = s3

	require.Eventually(t, func() bool {
		current := steadyShard()
		if current == nil || current.GetTerm() <= shard.GetTerm() {
			return false
		}
		lead, err := servers[current.GetLeader().GetNameOrDefault()].GetShardDirector().GetLeader(0)
		return err == nil && lead.IsFeatureEnabled(proto.Feature_FEATURE_ORDERED_WRITES)
	}, 30*time.Second, 50*time.Millisecond)
}
