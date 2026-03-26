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
	"testing"
	"time"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/selector/single"
)

// mockStatusResource implements resource.StatusResource for testing.
type mockStatusResource struct {
	status *model.ClusterStatus
}

func (m *mockStatusResource) Load() (*model.ClusterStatus, error) {
	return m.status, nil
}

func (m *mockStatusResource) LoadWithVersion() (*model.ClusterStatus, metadata.Version, error) {
	return m.status, "0", nil
}

func (m *mockStatusResource) Swap(_ *model.ClusterStatus, _ metadata.Version) (bool, error) {
	return true, nil
}

func (m *mockStatusResource) Update(newStatus *model.ClusterStatus) error {
	m.status = newStatus
	return nil
}

func (m *mockStatusResource) UpdateShardMetadata(_ string, _ int64, _ model.ShardMetadata) error {
	return nil
}

func (m *mockStatusResource) DeleteShardMetadata(_ string, _ int64) error {
	return nil
}

func (m *mockStatusResource) IsReady(_ *model.ClusterConfig) bool {
	return true
}

func (m *mockStatusResource) ChangeNotify() <-chan struct{} {
	return make(chan struct{})
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

// alwaysErrorSelector is a selector that always returns ErrUnsatisfiedAntiAffinity.
type alwaysErrorSelector struct{}

func (*alwaysErrorSelector) Select(_ *single.Context) (string, error) {
	return "", selector.ErrUnsatisfiedAntiAffinity
}

func newTestBalancer(
	ctx context.Context,
	cancel context.CancelFunc,
	statusRes *mockStatusResource,
	configRes *mockClusterConfigResource,
	sel selector.Selector[*single.Context, string],
) *nodeBasedBalancer {
	return &nodeBasedBalancer{
		Logger:                  slog.Default(),
		WaitGroup:               &sync.WaitGroup{},
		ctx:                     ctx,
		cancel:                  cancel,
		loadBalancerConf:        configRes.LoadLoadBalancer(),
		statusResource:          statusRes,
		configResource:          configRes,
		selector:                sel,
		loadRatioAlgorithm:      single.DefaultShardsRank,
		nodeQuarantineNodeMap:   &sync.Map{},
		shardQuarantineShardMap: &sync.Map{},
		actionCh:                make(chan action.Action, 1000),
		triggerCh:               make(chan any, 1),
		nodeAvailableJudger:     func(_ string) bool { return true },
	}
}

// selectRecordingSelector records whether Select was ever called.
type selectRecordingSelector struct {
	called bool
}

func (s *selectRecordingSelector) Select(_ *single.Context) (string, error) {
	s.called = true
	return "", selector.ErrUnsatisfiedAntiAffinity
}

func TestSwapShardSkipsRF1Namespace(t *testing.T) {
	// Two nodes with an imbalanced distribution: sv-1 owns all 3 shards,
	// sv-2 owns none. With RF=3 the balancer would try to swap, but with
	// RF=1 it must skip entirely.
	sv1 := model.Server{Internal: "sv-1"}
	sv2 := model.Server{Internal: "sv-2"}

	status := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"rf1ns": {
				ReplicationFactor: 1,
				Shards: map[int64]model.ShardMetadata{
					0: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv1}},
					1: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv1}},
					2: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv1}},
				},
			},
		},
	}

	nodes := linkedhashset.New("sv-1", "sv-2")
	sel := &selectRecordingSelector{}
	configRes := &mockClusterConfigResource{
		nodes:    nodes,
		metadata: map[string]model.ServerMetadata{},
		nsConfigs: map[string]*model.NamespaceConfig{
			"rf1ns": {Name: "rf1ns", ReplicationFactor: 1},
		},
		nodeMap: map[string]*model.Server{
			"sv-1": &sv1,
			"sv-2": &sv2,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b := newTestBalancer(ctx, cancel, &mockStatusResource{status: status}, configRes, sel)

	b.rebalanceEnsemble()

	// The selector must never have been called because swapShard should
	// return early for RF=1 namespaces.
	if sel.called {
		t.Fatal("selector was called for RF=1 namespace; expected swapShard to skip it")
	}

	// No swap actions should have been emitted.
	if len(b.actionCh) != 0 {
		t.Fatalf("expected 0 actions, got %d", len(b.actionCh))
	}
}

func TestBalanceHighestNodeDoesNotHangOnSelectorError(t *testing.T) {
	sv1 := model.Server{Internal: "sv-1"}
	sv2 := model.Server{Internal: "sv-2"}
	sv3 := model.Server{Internal: "sv-3"}

	status := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv1, sv2, sv3}},
					1: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv1, sv2, sv3}},
					2: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv1, sv2, sv3}},
					3: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv1, sv2, sv3}},
					4: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv1, sv2, sv3}},
					5: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv2, sv3}},
					6: {Status: model.ShardStatusSteadyState, Ensemble: []model.Server{sv3}},
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b := newTestBalancer(ctx, cancel, &mockStatusResource{status: status}, configRes, &alwaysErrorSelector{})

	// rebalanceEnsemble should complete without hanging, even though
	// the selector always returns an error.
	done := make(chan bool, 1)
	go func() {
		b.rebalanceEnsemble()
		done <- true
	}()

	select {
	case <-done:
		// Success: rebalanceEnsemble completed
	case <-time.After(3 * time.Second):
		t.Fatal("rebalanceEnsemble hung due to infinite loop on selector error")
	}
}
