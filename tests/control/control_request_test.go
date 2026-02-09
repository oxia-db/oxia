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

	"github.com/emirpasic/gods/v2/sets/hashset"
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

	// Write some entries
	_, _, err = client.Put(context.Background(), "/key1", []byte("value"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "/key2", []byte("value"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "/key3", []byte("value"))
	assert.NoError(t, err)
	// monitor the data persistent
	keys, err := client.List(context.Background(), "/", "//")
	assert.NoError(t, err)
	assert.EqualValues(t, []string{"/key1", "/key2", "/key3"}, keys)

	// Check the feature has enabled for all the replicas
	resource := coordinatorInstance.StatusResource().Load()
	shardMetadata := resource.Namespaces["default"].Shards[0]
	leader := shardMetadata.Leader
	for _, dataServer := range shardMetadata.Ensemble {
		targetId := dataServer.GetIdentifier()
		if targetId == leader.GetIdentifier() {
			lead, err := serverInstanceIndex[targetId].GetShardDirector().GetLeader(0)
			assert.NoError(t, err)
			assert.True(t, lead.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM))
			continue
		}
		follow, err := serverInstanceIndex[targetId].GetShardDirector().GetFollower(0)
		assert.NoError(t, err)
		assert.True(t, follow.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM))
	}

	// Write some entries
	_, _, err = client.Put(context.Background(), "/key4", []byte("value"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "/key5", []byte("value"))
	assert.NoError(t, err)
	_, _, err = client.Put(context.Background(), "/key6", []byte("value"))
	assert.NoError(t, err)

	// monitor the data persistent
	keys, err = client.List(context.Background(), "/", "//")
	assert.NoError(t, err)
	assert.EqualValues(t, []string{"/key1", "/key2", "/key3", "/key4", "/key5", "/key6"}, keys)

	checksums := make([]uint32, 0)
	leadCommitOffset := int64(0)
	followCommitOffset := make([]int64, 0)
	for _, dataServer := range shardMetadata.Ensemble {
		targetId := dataServer.GetIdentifier()
		if targetId == leader.GetIdentifier() {
			lead, err := serverInstanceIndex[targetId].GetShardDirector().GetLeader(0)
			assert.NoError(t, err)
			checksums = append(checksums, lead.Checksum().Value())
			leadCommitOffset = lead.CommitOffset()
			continue
		}
		follow, err := serverInstanceIndex[targetId].GetShardDirector().GetFollower(0)
		assert.NoError(t, err)
		followCommitOffset = append(followCommitOffset, follow.CommitOffset())
	}

	// todo: The follower is always one step behind the leader.
	for _, offset := range followCommitOffset {
		assert.EqualValues(t, 1, leadCommitOffset-offset)
	}

	_, _, err = client.Put(context.Background(), "/key7", []byte("value"))
	assert.NoError(t, err)

	// monitor the data persistent
	keys, err = client.List(context.Background(), "/", "//")
	assert.NoError(t, err)
	assert.EqualValues(t, []string{"/key1", "/key2", "/key3", "/key4", "/key5", "/key6", "/key7"}, keys)

	for _, dataServer := range shardMetadata.Ensemble {
		targetId := dataServer.GetIdentifier()
		if targetId != leader.GetIdentifier() {
			follow, err := serverInstanceIndex[targetId].GetShardDirector().GetFollower(0)
			assert.NoError(t, err)
			checksums = append(checksums, follow.Checksum().Value())
			continue
		}
	}

	assert.Equal(t, 3, len(checksums))
	assert.Equal(t, 1, hashset.New(checksums...).Size())
}
