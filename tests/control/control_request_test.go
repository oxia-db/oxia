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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	clientrpc "github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/tests/mock"
)

func TestControlRequestFeatureEnabled(t *testing.T) {
	s1, sa1 := mock.NewServer(t, "s1")
	s2, sa2 := mock.NewServer(t, "s2")
	s3, sa3 := mock.NewServer(t, "s3")
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	serverInstanceIndex := map[string]*dataserver.Server{
		sa1.GetIdentifier(): s1,
		sa2.GetIdentifier(): s2,
		sa3.GetIdentifier(): s3,
	}

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "default",
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc.NewRpcProvider(clientrpc.NewClientPool(nil, nil)))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace("default"))
	assert.NoError(t, err)
	defer client.Close()

	resource := coordinatorInstance.StatusResource().Load()
	shardMetadata := resource.Namespaces["default"].Shards[0]
	leader := shardMetadata.Leader

	// Write entries. The replication messages for these writes also carry
	// commit notifications for the feature-enable control entry that was
	// proposed when the leader started, ensuring all replicas activate
	// the checksum feature and start the CRC chain from the same point.
	for i := 1; i <= 7; i++ {
		_, _, err = client.Put(context.Background(), fmt.Sprintf("/key%d", i), []byte("value"))
		assert.NoError(t, err)
	}

	keys, err := client.List(context.Background(), "/", "//")
	assert.NoError(t, err)
	assert.EqualValues(t, []string{"/key1", "/key2", "/key3", "/key4", "/key5", "/key6", "/key7"}, keys)

	// Verify the checksum feature is enabled on all replicas.
	for _, dataServer := range shardMetadata.Ensemble {
		targetId := dataServer.GetIdentifier()
		if targetId == leader.GetIdentifier() {
			assert.Eventually(t, func() bool {
				lead, err := serverInstanceIndex[targetId].GetShardDirector().GetLeader(0)
				return err == nil && lead.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM)
			}, 10*time.Second, 100*time.Millisecond)
			continue
		}
		assert.Eventually(t, func() bool {
			follow, err := serverInstanceIndex[targetId].GetShardDirector().GetFollower(0)
			return err == nil && follow.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM)
		}, 10*time.Second, 100*time.Millisecond)
	}

	// Write one more entry to propagate the commit notification for key7.
	// Commit notifications are piggybacked on replication messages, so the
	// last committed entry requires a subsequent write to notify followers.
	_, _, err = client.Put(context.Background(), "/key8", []byte("value"))
	assert.NoError(t, err)

	// Capture the leader's state after all writes (including key8).
	lead, err := serverInstanceIndex[leader.GetIdentifier()].GetShardDirector().GetLeader(0)
	assert.NoError(t, err)
	leadCommitOffset := lead.CommitOffset()
	leaderChecksum := lead.Checksum().Value()

	// Wait for each follower to replicate up to the leader's commit
	// offset, then verify checksum consistency.
	for _, dataServer := range shardMetadata.Ensemble {
		targetId := dataServer.GetIdentifier()
		if targetId == leader.GetIdentifier() {
			continue
		}
		assert.Eventually(t, func() bool {
			follow, err := serverInstanceIndex[targetId].GetShardDirector().GetFollower(0)
			return err == nil && follow.CommitOffset() >= leadCommitOffset
		}, 10*time.Second, 100*time.Millisecond)

		follow, err := serverInstanceIndex[targetId].GetShardDirector().GetFollower(0)
		assert.NoError(t, err)
		assert.Equal(t, leaderChecksum, follow.Checksum().Value())
	}
}
