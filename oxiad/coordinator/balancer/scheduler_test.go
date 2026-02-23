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

package balancer

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/selector/single"
)

// mockStatusResource implements resource.StatusResource for testing.
type mockStatusResource struct {
	status *model.ClusterStatus
}

func (m *mockStatusResource) Load() *model.ClusterStatus {
	return m.status
}

func (m *mockStatusResource) LoadWithVersion() (*model.ClusterStatus, metadata.Version) {
	return m.status, "0"
}

func (m *mockStatusResource) Swap(_ *model.ClusterStatus, _ metadata.Version) bool {
	return true
}

func (m *mockStatusResource) Update(newStatus *model.ClusterStatus) {
	m.status = newStatus
}

func (m *mockStatusResource) UpdateShardMetadata(_ string, _ int64, _ model.ShardMetadata) {}

func (m *mockStatusResource) DeleteShardMetadata(_ string, _ int64) {}

func (m *mockStatusResource) IsReady(_ *model.ClusterConfig) bool {
	return true
}

// mockClusterConfigResource implements resource.ClusterConfigResource for testing.
type mockClusterConfigResource struct {
	nodes     *linkedhashset.Set[string]
	metadata  map[string]model.ServerMetadata
	nsConfigs map[string]*model.NamespaceConfig
	nodeMap   map[string]*model.Server
	lbConfig  *model.LoadBalancer
}

func (m *mockClusterConfigResource) Close() error { return nil }

func (m *mockClusterConfigResource) Load() *model.ClusterConfig { return nil }

func (m *mockClusterConfigResource) LoadLoadBalancer() *model.LoadBalancer {
	if m.lbConfig != nil {
		return m.lbConfig
	}
	return &model.LoadBalancer{
		ScheduleInterval: 30 * time.Second,
		QuarantineTime:   5 * time.Minute,
	}
}

func (m *mockClusterConfigResource) Nodes() *linkedhashset.Set[string] { return m.nodes }

func (m *mockClusterConfigResource) NodesWithMetadata() (*linkedhashset.Set[string], map[string]model.ServerMetadata) {
	return m.nodes, m.metadata
}

func (m *mockClusterConfigResource) NamespaceConfig(namespace string) (*model.NamespaceConfig, bool) {
	nc, ok := m.nsConfigs[namespace]
	return nc, ok
}

func (m *mockClusterConfigResource) Node(id string) (*model.Server, bool) {
	n, ok := m.nodeMap[id]
	return n, ok
}

func newTestBalancer(
	ctx context.Context,
	cancel context.CancelFunc,
	statusRes *mockStatusResource,
	configRes *mockClusterConfigResource,
) *nodeBasedBalancer {
	return &nodeBasedBalancer{
		Logger:                  slog.Default(),
		WaitGroup:               &sync.WaitGroup{},
		ctx:                     ctx,
		cancel:                  cancel,
		loadBalancerConf:        configRes.LoadLoadBalancer(),
		statusResource:          statusRes,
		configResource:          configRes,
		selector:                single.NewSelector(),
		loadRatioAlgorithm:      single.DefaultShardsRank,
		nodeQuarantineNodeMap:   &sync.Map{},
		shardQuarantineShardMap: &sync.Map{},
		actionCh:                make(chan action.Action, 1000),
		triggerCh:               make(chan any, 1),
		nodeAvailableJudger:     func(_ string) bool { return true },
	}
}

func TestRebalanceLeaderProcessesMultipleShards(t *testing.T) {
	sv1 := model.Server{Internal: "sv-1"}
	sv2 := model.Server{Internal: "sv-2"}
	sv3 := model.Server{Internal: "sv-3"}

	// sv-1 is leader for 5 shards, sv-2 for 1, sv-3 for 1
	// All shards have ensemble [sv-1, sv-2, sv-3] so leader can move
	status := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {Status: model.ShardStatusSteadyState, Leader: &sv1, Ensemble: []model.Server{sv1, sv2, sv3}},
					1: {Status: model.ShardStatusSteadyState, Leader: &sv1, Ensemble: []model.Server{sv1, sv2, sv3}},
					2: {Status: model.ShardStatusSteadyState, Leader: &sv1, Ensemble: []model.Server{sv1, sv2, sv3}},
					3: {Status: model.ShardStatusSteadyState, Leader: &sv1, Ensemble: []model.Server{sv1, sv2, sv3}},
					4: {Status: model.ShardStatusSteadyState, Leader: &sv1, Ensemble: []model.Server{sv1, sv2, sv3}},
					5: {Status: model.ShardStatusSteadyState, Leader: &sv2, Ensemble: []model.Server{sv1, sv2, sv3}},
					6: {Status: model.ShardStatusSteadyState, Leader: &sv3, Ensemble: []model.Server{sv1, sv2, sv3}},
				},
			},
		},
	}

	nodes := linkedhashset.New("sv-1", "sv-2", "sv-3")
	configRes := &mockClusterConfigResource{
		nodes:    nodes,
		metadata: map[string]model.ServerMetadata{},
		nsConfigs: map[string]*model.NamespaceConfig{
			"ns": {Name: "ns", ReplicationFactor: 3},
		},
		nodeMap: map[string]*model.Server{
			"sv-1": &sv1,
			"sv-2": &sv2,
			"sv-3": &sv3,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := newTestBalancer(ctx, cancel, &mockStatusResource{status: status}, configRes)

	// Start a goroutine to consume election actions and simulate leader changes.
	// Each election moves the leader away from sv-1 to sv-2.
	var electionCount atomic.Int32
	go func() {
		for ac := range b.actionCh {
			if ea, ok := ac.(*action.ElectionAction); ok {
				electionCount.Add(1)
				// Simulate successful leader change: leader moves to sv-2
				ea.Done("sv-2")
			}
		}
	}()

	b.rebalanceLeader()

	// Without the fix, only 1 election is triggered (unconditional break).
	// With the fix, multiple elections should be triggered.
	count := electionCount.Load()
	assert.Greater(t, count, int32(1), "expected multiple elections but got %d", count)
}
