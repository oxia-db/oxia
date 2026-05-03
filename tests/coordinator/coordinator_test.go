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

package coordinator

import (
	"testing"

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"

	"github.com/stretchr/testify/assert"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	metadata2 "github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
)

func TestCoordinatorInitiateLeaderElection(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	metadataProvider := metadata2.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              "default",
		ReplicationFactor: 1,
		InitialShardCount: 2,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	configProvider := metadata2.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))
	defer coordinatorInstance.Close()

	shardMetadata := &proto.ShardMetadata{
		Status:                  proto.ShardStatusSteadyState,
		Term:                    999,
		Leader:                  nil,
		Ensemble:                []*proto.DataServerIdentity{},
		RemovedNodes:            []*proto.DataServerIdentity{},
		PendingDeleteShardNodes: make([]*proto.DataServerIdentity, 0),
		Int32HashRange:          &proto.HashRange{Min: 2000, Max: 100000},
	}
	metadataView := coordinatorInstance.Metadata()
	metadataView.UpdateShardStatus("default", 1, shardMetadata)

	status := metadataView.GetStatus().UnsafeBorrow()
	assert.True(t, gproto.Equal(status.Namespaces["default"].Shards[1], shardMetadata))
}
