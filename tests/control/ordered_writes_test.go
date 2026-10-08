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

func TestOrderedWrites(t *testing.T) {
	s1, sa1 := mock.NewServer(t, "s1")
	s2, sa2 := mock.NewServer(t, "s2")
	s3, sa3 := mock.NewServer(t, "s3")
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	serverInstanceIndex := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}

	metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	clusterConfig := newDefaultClusterConfig(sa1, sa2, sa3)
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   clusterConfig,
		Version: metadatacommon.NotExists,
	})
	assert.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(
		t,
		metadataProvider,
		configProvider,
		rpc.NewRpcProviderFactory(nil),
	)
	defer coordinatorInstance.Close()

	// A long linger makes the put, delete and put below go in the same write batch
	client, err := oxia.NewAsyncClient(sa1.Public, oxia.WithNamespace("default"), oxia.WithBatchLinger(1*time.Second))
	assert.NoError(t, err)
	defer client.Close()

	// The client can connect before the shard leader is elected. All the data
	// servers support the feature, so the coordinator enables it
	shardMetadata := waitForLeaderFeature(t, coordinatorInstance.Metadata(), serverInstanceIndex,
		proto.Feature_FEATURE_ORDERED_WRITES)
	leader := shardMetadata.Leader
	lead, err := serverInstanceIndex[leader.GetNameOrDefault()].GetShardDirector().GetLeader(0)
	assert.NoError(t, err)

	putResult1 := client.Put("/k", []byte("v1"))
	deleteResult := client.Delete("/k")
	putResult2 := client.Put("/k", []byte("v2"))
	assert.NoError(t, (<-putResult1).Err)
	assert.NoError(t, <-deleteResult)
	assert.NoError(t, (<-putResult2).Err)

	getResult := <-client.Get("/k")
	assert.NoError(t, getResult.Err)
	assert.Equal(t, []byte("v2"), getResult.Value)

	// The followers must apply the batch in the same order as the leader.
	// Commit notifications are piggybacked on replication messages, so one
	// more write is needed for the followers to commit the batch.
	leaderChecksum := lead.Checksum().Value()
	assert.NotZero(t, leaderChecksum)
	assert.NoError(t, (<-client.Put("/flush", []byte("v"))).Err)

	for _, dataServer := range shardMetadata.Ensemble {
		targetId := dataServer.GetNameOrDefault()
		if targetId == leader.GetNameOrDefault() {
			continue
		}
		assert.Eventually(t, func() bool {
			follow, err := serverInstanceIndex[targetId].GetShardDirector().GetFollower(0)
			return err == nil &&
				follow.IsFeatureEnabled(proto.Feature_FEATURE_ORDERED_WRITES) &&
				follow.Checksum().Value() == leaderChecksum
		}, 10*time.Second, 100*time.Millisecond)
	}
}
