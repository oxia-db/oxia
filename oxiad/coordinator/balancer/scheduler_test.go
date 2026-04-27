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

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector/single"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
)

type mockMetadata struct {
	status    *proto.ClusterStatus
	nodes     *linkedhashset.Set[string]
	metadata  map[string]*proto.DataServerMetadata
	nsConfigs map[string]*proto.Namespace
	nodeMap   map[string]*proto.DataServerIdentity
	lbConfig  *proto.LoadBalancer
}

func dataServer(id string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{
		Internal: id,
	}
}

var _ coordmetadata.Metadata = (*mockMetadata)(nil)

func (*mockMetadata) Close() error { return nil }

func (m *mockMetadata) LoadStatus() *proto.ClusterStatus { return m.status }

func (m *mockMetadata) ApplyStatusChanges(*proto.ClusterConfiguration, coordmetadata.EnsembleSupplier) (*proto.ClusterStatus, map[int64]string, []int64) {
	return m.status, nil, nil
}

func (m *mockMetadata) UpdateStatus(newStatus *proto.ClusterStatus) { m.status = newStatus }

func (*mockMetadata) UpdateShardMetadata(string, int64, *proto.ShardMetadata) {}

func (*mockMetadata) DeleteShardMetadata(string, int64) {}

func (*mockMetadata) IsReady(*proto.ClusterConfiguration) bool { return true }

func (*mockMetadata) StatusChangeNotify() <-chan struct{} { return make(chan struct{}) }

func (*mockMetadata) LoadConfig() *proto.ClusterConfiguration { return nil }

func (*mockMetadata) ConfigWatch() *commonwatch.Watch[*proto.ClusterConfiguration] {
	return commonwatch.New[*proto.ClusterConfiguration](nil)
}

func (m *mockMetadata) LoadLoadBalancer() *proto.LoadBalancer {
	if m.lbConfig != nil {
		return m.lbConfig
	}
	return &proto.LoadBalancer{
		ScheduleInterval: "30s",
		QuarantineTime:   "5m",
	}
}

func (m *mockMetadata) Nodes() *linkedhashset.Set[string] { return m.nodes }

func (m *mockMetadata) NodesWithMetadata() (*linkedhashset.Set[string], map[string]*proto.DataServerMetadata) {
	return m.nodes, m.metadata
}

func (m *mockMetadata) Namespace(namespace string) (*proto.Namespace, bool) {
	nc, ok := m.nsConfigs[namespace]
	return nc, ok
}

func (m *mockMetadata) Node(id string) (*proto.DataServerIdentity, bool) {
	n, ok := m.nodeMap[id]
	return n, ok
}

func (m *mockMetadata) GetDataServer(id string) (*proto.DataServer, bool) {
	n, ok := m.nodeMap[id]
	if !ok {
		return nil, false
	}
	metadata, ok := m.metadata[id]
	if !ok {
		metadata = &proto.DataServerMetadata{}
	}
	return &proto.DataServer{
		Identity: n,
		Metadata: metadata,
	}, true
}

// alwaysErrorSelector is a selector that always returns ErrUnsatisfiedAntiAffinity.
type alwaysErrorSelector struct{}

func (*alwaysErrorSelector) Select(_ *single.Context) (string, error) {
	return "", selector.ErrUnsatisfiedAntiAffinity
}

func newTestBalancer(
	ctx context.Context,
	cancel context.CancelFunc,
	metadata *mockMetadata,
	sel selector.Selector[*single.Context, string],
) *nodeBasedBalancer {
	return &nodeBasedBalancer{
		Logger:                  slog.Default(),
		WaitGroup:               &sync.WaitGroup{},
		ctx:                     ctx,
		cancel:                  cancel,
		loadBalancerConf:        metadata.LoadLoadBalancer(),
		metadata:                metadata,
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
	sv1 := dataServer("sv-1")
	sv2 := dataServer("sv-2")

	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"rf1ns": {
				ReplicationFactor: 1,
				Shards: map[int64]*proto.ShardMetadata{
					0: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv1}},
					1: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv1}},
					2: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv1}},
				},
			},
		},
	}

	nodes := linkedhashset.New("sv-1", "sv-2")
	sel := &selectRecordingSelector{}
	metadata := &mockMetadata{
		status:   status,
		nodes:    nodes,
		metadata: map[string]*proto.DataServerMetadata{},
		nsConfigs: map[string]*proto.Namespace{
			"rf1ns": {Name: "rf1ns", ReplicationFactor: 1},
		},
		nodeMap: map[string]*proto.DataServerIdentity{
			"sv-1": sv1,
			"sv-2": sv2,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b := newTestBalancer(ctx, cancel, metadata, sel)

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
	sv1 := dataServer("sv-1")
	sv2 := dataServer("sv-2")
	sv3 := dataServer("sv-3")

	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv1, sv2, sv3}},
					1: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv1, sv2, sv3}},
					2: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv1, sv2, sv3}},
					3: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv1, sv2, sv3}},
					4: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv1, sv2, sv3}},
					5: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv2, sv3}},
					6: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv3}},
				},
			},
		},
	}

	nodes := linkedhashset.New("sv-1", "sv-2", "sv-3")
	metadata := &mockMetadata{
		status:   status,
		nodes:    nodes,
		metadata: map[string]*proto.DataServerMetadata{},
		nsConfigs: map[string]*proto.Namespace{
			"ns": {Name: "ns", ReplicationFactor: 3},
		},
		nodeMap: map[string]*proto.DataServerIdentity{
			"sv-1": sv1,
			"sv-2": sv2,
			"sv-3": sv3,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b := newTestBalancer(ctx, cancel, metadata, &alwaysErrorSelector{})

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
