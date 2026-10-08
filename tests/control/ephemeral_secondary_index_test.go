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
	"context"
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

func TestEphemeralSecondaryIndexCleanup(t *testing.T) {
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

	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace("default"))
	assert.NoError(t, err)
	defer client.Close()

	// The client can connect before the shard leader is elected. All the data
	// servers support the feature, so the coordinator enables it
	shardMetadata := waitForLeaderFeature(t, coordinatorInstance.Metadata(), serverInstanceIndex,
		proto.Feature_FEATURE_EPHEMERAL_SECONDARY_INDEX_CLEANUP)
	leader := shardMetadata.Leader
	lead, err := serverInstanceIndex[leader.GetNameOrDefault()].GetShardDirector().GetLeader(0)
	assert.NoError(t, err)

	sessionClient, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace("default"))
	assert.NoError(t, err)
	_, _, err = sessionClient.Put(context.Background(), "/ephemeral", []byte("v"),
		oxia.Ephemeral(), oxia.SecondaryIndex("idx", "a"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "/persistent", []byte("v"), oxia.SecondaryIndex("idx", "b"))
	assert.NoError(t, err)

	// Closing the client closes its session, which deletes its ephemeral record
	assert.NoError(t, sessionClient.Close())

	keys, err := client.List(context.Background(), "a", "z", oxia.UseIndex("idx"))
	assert.NoError(t, err)
	assert.Equal(t, []string{"/persistent"}, keys)

	// The followers must apply the session end like the leader. Commit
	// notifications are piggybacked on replication messages, so one more
	// write is needed for the followers to commit it.
	leaderChecksum := lead.Checksum().Value()
	assert.NotZero(t, leaderChecksum)
	_, _, err = client.Put(context.Background(), "/flush", []byte("v"))
	assert.NoError(t, err)

	for _, dataServer := range shardMetadata.Ensemble {
		targetId := dataServer.GetNameOrDefault()
		if targetId == leader.GetNameOrDefault() {
			continue
		}
		assert.Eventually(t, func() bool {
			follow, err := serverInstanceIndex[targetId].GetShardDirector().GetFollower(0)
			return err == nil &&
				follow.IsFeatureEnabled(proto.Feature_FEATURE_EPHEMERAL_SECONDARY_INDEX_CLEANUP) &&
				follow.Checksum().Value() == leaderChecksum
		}, 10*time.Second, 100*time.Millisecond)
	}
}
