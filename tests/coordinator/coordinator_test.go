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

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/coordinator"
	metadata2 "github.com/oxia-db/oxia/coordinator/metadata"
	rpc2 "github.com/oxia-db/oxia/coordinator/rpc"

	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/coordinator/model"
)

func TestCoordinatorInitiateLeaderElection(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	defer s1.Close()
	defer s2.Close()
	defer s3.Close()

	metadataProvider := metadata2.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "default",
			ReplicationFactor: 1,
			InitialShardCount: 2,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	metadata := model.ShardMetadata{
		Status:                  model.ShardStatusSteadyState,
		Term:                    999,
		Leader:                  nil,
		Ensemble:                []model.Server{},
		RemovedNodes:            []model.Server{},
		PendingDeleteShardNodes: make([]model.Server, 0),
		Int32HashRange:          model.Int32HashRange{Min: 2000, Max: 100000},
	}
	statusResource := coordinatorInstance.StatusResource()
	statusResource.UpdateShardMetadata("default", 1, metadata)

	status := statusResource.Load()
	assert.EqualValues(t, status.Namespaces["default"].Shards[1], metadata)
}
