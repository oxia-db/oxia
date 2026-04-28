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

package reconciler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
)

type mockCoordinator struct {
	dataServers map[string]*proto.DataServerIdentity

	createdDataServers []string
	deletedDataServers []string
	createdShards      []int64
	deletedShards      []int64
	recomputeCalls     int
	triggerCalls       int
	syncCalls          int

	status      *proto.ClusterStatus
	shardsToAdd map[int64]string
	shardsToDel []int64
}

func (m *mockCoordinator) HasDataServer(id string) bool {
	_, ok := m.dataServers[id]
	return ok
}

func (m *mockCoordinator) DataServerIDs() []string {
	ids := make([]string, 0, len(m.dataServers))
	for id := range m.dataServers {
		ids = append(ids, id)
	}
	return ids
}

func (m *mockCoordinator) CreateDataServer(dataServer *proto.DataServerIdentity) {
	id := dataServer.GetNameOrDefault()
	m.dataServers[id] = dataServer
	m.createdDataServers = append(m.createdDataServers, id)
}

func (m *mockCoordinator) DeleteDataServer(id string) {
	delete(m.dataServers, id)
	m.deletedDataServers = append(m.deletedDataServers, id)
}

func (m *mockCoordinator) SyncShardControllerServerAddresses() {
	m.syncCalls++
}

func (m *mockCoordinator) ApplyStatusChanges(*proto.ClusterConfiguration) (*proto.ClusterStatus, map[int64]string, []int64) {
	return m.status, m.shardsToAdd, m.shardsToDel
}

func (m *mockCoordinator) CreateShardController(_ string, shard int64, _ *proto.ShardMetadata) {
	m.createdShards = append(m.createdShards, shard)
}

func (m *mockCoordinator) DeleteShardController(shard int64) {
	m.deletedShards = append(m.deletedShards, shard)
}

func (m *mockCoordinator) RecomputeAssignments() {
	m.recomputeCalls++
}

func (m *mockCoordinator) TriggerLoadBalancer() {
	m.triggerCalls++
}

func TestConfigReconcilerReconcile(t *testing.T) {
	server1 := &proto.DataServerIdentity{Internal: "s1"}
	server2 := &proto.DataServerIdentity{Internal: "s2"}
	config := &proto.ClusterConfiguration{
		Servers: []*proto.DataServerIdentity{server1},
	}
	coordinator := &mockCoordinator{
		dataServers: map[string]*proto.DataServerIdentity{
			server2.GetNameOrDefault(): server2,
		},
		status: &proto.ClusterStatus{
			Namespaces: map[string]*proto.NamespaceStatus{
				"default": {
					Shards: map[int64]*proto.ShardMetadata{
						1: {},
					},
				},
			},
		},
		shardsToAdd: map[int64]string{1: "default"},
		shardsToDel: []int64{2},
	}

	reconciler := NewConfigReconciler(slog.Default(), commonwatch.New(config), coordinator)
	reconciler.Reconcile(config)

	assert.Equal(t, []string{"s1"}, coordinator.createdDataServers)
	assert.Equal(t, []string{"s2"}, coordinator.deletedDataServers)
	assert.Equal(t, 1, coordinator.syncCalls)
	assert.Equal(t, []int64{1}, coordinator.createdShards)
	assert.Equal(t, []int64{2}, coordinator.deletedShards)
	assert.Equal(t, 1, coordinator.recomputeCalls)
	assert.Equal(t, 1, coordinator.triggerCalls)
}

func TestConfigReconcilerAppliesInitialAndUpdatedConfig(t *testing.T) {
	initial := &proto.ClusterConfiguration{
		Servers: []*proto.DataServerIdentity{{Internal: "s1"}},
	}
	updated := &proto.ClusterConfiguration{
		Servers: []*proto.DataServerIdentity{{Internal: "s2"}},
	}

	coordinator := &mockCoordinator{
		dataServers: map[string]*proto.DataServerIdentity{},
		status:      &proto.ClusterStatus{Namespaces: map[string]*proto.NamespaceStatus{}},
		shardsToAdd: map[int64]string{},
	}
	watch := commonwatch.New(initial)
	reconciler := NewConfigReconciler(slog.Default(), watch, coordinator)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go reconciler.Run(ctx)

	require.Eventually(t, func() bool {
		return len(coordinator.createdDataServers) >= 1 && coordinator.createdDataServers[0] == "s1"
	}, time.Second, 10*time.Millisecond)

	watch.Publish(updated)

	require.Eventually(t, func() bool {
		return len(coordinator.createdDataServers) >= 2 && coordinator.createdDataServers[1] == "s2"
	}, time.Second, 10*time.Millisecond)
}
