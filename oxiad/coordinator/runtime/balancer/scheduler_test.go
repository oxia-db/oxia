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
	"github.com/stretchr/testify/assert"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/single"
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

func testNamespace(name string, replicationFactor uint32) *proto.Namespace {
	return &proto.Namespace{
		Name:   name,
		Policy: proto.NewHierarchyPolicies(1, replicationFactor, true, "hierarchical"),
	}
}

var _ coordmetadata.Metadata = (*mockMetadata)(nil)

func (*mockMetadata) Close() error { return nil }

func (m *mockMetadata) GetStatus() commonobject.Borrowed[*proto.ClusterStatus] {
	return commonobject.Borrow(m.status)
}

func (m *mockMetadata) UpdateStatus(newStatus *proto.ClusterStatus) { m.status = newStatus }

func (*mockMetadata) ReserveShardIDs(uint32) int64 { return 0 }

func (*mockMetadata) CreateNamespaceStatus(string, *proto.NamespaceStatus) bool {
	return false
}

func (*mockMetadata) ListNamespaceStatus() map[string]commonobject.Borrowed[*proto.NamespaceStatus] {
	return map[string]commonobject.Borrowed[*proto.NamespaceStatus]{}
}

func (*mockMetadata) GetNamespaceStatus(string) (commonobject.Borrowed[*proto.NamespaceStatus], bool) {
	return commonobject.Borrowed[*proto.NamespaceStatus]{}, false
}

func (*mockMetadata) DeleteNamespaceStatus(string) commonobject.Borrowed[*proto.NamespaceStatus] {
	return commonobject.Borrowed[*proto.NamespaceStatus]{}
}

func (*mockMetadata) UpdateShardStatus(string, int64, *proto.ShardMetadata) {}

func (*mockMetadata) DeleteShardStatus(string, int64) {}

func (*mockMetadata) IsReady(*proto.ClusterConfiguration) bool { return true }

func (*mockMetadata) CreateNamespace(*proto.Namespace) error {
	return nil
}

func (*mockMetadata) PatchNamespace(*proto.Namespace) (*proto.Namespace, error) {
	return &proto.Namespace{}, nil
}

func (*mockMetadata) DeleteNamespace(string) (*proto.Namespace, error) {
	return &proto.Namespace{}, nil
}

func (*mockMetadata) GetConfig() commonobject.Borrowed[*proto.ClusterConfiguration] {
	return commonobject.Borrowed[*proto.ClusterConfiguration]{}
}

func (*mockMetadata) SubscribeConfig() *commonwatch.Receiver[provider.Versioned[*proto.ClusterConfiguration]] {
	return commonwatch.New(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   &proto.ClusterConfiguration{},
		Version: "",
	}).Subscribe()
}

func (m *mockMetadata) GetLoadBalancer() commonobject.Borrowed[*proto.LoadBalancer] {
	if m.lbConfig != nil {
		return commonobject.Borrow(m.lbConfig)
	}
	return commonobject.Borrow(&proto.LoadBalancer{
		ScheduleInterval: "30s",
		QuarantineTime:   "5m",
	})
}

func (*mockMetadata) GetClusterPolicy() *proto.HierarchyPolicies {
	return proto.NewDefaultHierarchyPolicies()
}

func (*mockMetadata) PatchClusterPolicy(*proto.HierarchyPolicies) (*proto.HierarchyPolicies, error) {
	return proto.NewDefaultHierarchyPolicies(), nil
}

func (*mockMetadata) CreateDataServer(*proto.DataServer) error {
	return nil
}

func (*mockMetadata) PatchDataServer(*proto.DataServer) (*proto.DataServer, error) {
	return &proto.DataServer{}, nil
}

func (*mockMetadata) DeleteDataServer(string) (*proto.DataServer, error) {
	return &proto.DataServer{}, nil
}

func (m *mockMetadata) ListDataServer() map[string]commonobject.Borrowed[*proto.DataServer] {
	dataServers := make(map[string]commonobject.Borrowed[*proto.DataServer], m.nodes.Size())
	for _, id := range m.nodes.Values() {
		name := id
		identity, ok := m.nodeMap[id]
		if !ok {
			identity = &proto.DataServerIdentity{
				Name:     &name,
				Internal: id,
			}
		}
		metadata, ok := m.metadata[id]
		if !ok {
			metadata = &proto.DataServerMetadata{}
		}
		dataServers[id] = commonobject.Borrow(&proto.DataServer{
			Identity: identity,
			Metadata: metadata,
		})
	}
	return dataServers
}

func (m *mockMetadata) GetNamespace(namespace string, effective bool) (commonobject.Borrowed[*proto.Namespace], bool) {
	nc, ok := m.nsConfigs[namespace]
	if !ok {
		return commonobject.Borrowed[*proto.Namespace]{}, false
	}
	if effective {
		return commonobject.Borrow(proto.MaterializeNamespacePolicy(nil, nc)), true
	}
	return commonobject.Borrow(nc), true
}

func (m *mockMetadata) GetNamespaceEffectivePolicy(namespace string) (*proto.HierarchyPolicies, bool) {
	nc, ok := m.nsConfigs[namespace]
	if !ok {
		return nil, false
	}
	return proto.ResolveHierarchyPolicies(nil, nc), true
}

func (m *mockMetadata) GetDataServer(name string) (commonobject.Borrowed[*proto.DataServer], bool) {
	n, ok := m.nodeMap[name]
	if !ok {
		return commonobject.Borrowed[*proto.DataServer]{}, false
	}
	metadata, ok := m.metadata[name]
	if !ok {
		metadata = &proto.DataServerMetadata{}
	}
	return commonobject.Borrow(&proto.DataServer{
		Identity: n,
		Metadata: metadata,
	}), true
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
		logger:                  slog.Default(),
		ctx:                     ctx,
		ctxCancel:               cancel,
		wg:                      sync.WaitGroup{},
		loadBalancerConf:        metadata.GetLoadBalancer().UnsafeBorrow(),
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

type contextRecordingSelector struct {
	namespace string
	shard     int64
}

func (s *contextRecordingSelector) Select(ctx *single.Context) (string, error) {
	s.namespace = ctx.Namespace
	s.shard = ctx.Shard
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
			"rf1ns": testNamespace("rf1ns", 1),
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

func TestSwapShardPassesNamespaceAndShardToSelector(t *testing.T) {
	sv1 := dataServer("sv-1")
	sv2 := dataServer("sv-2")
	sv3 := dataServer("sv-3")

	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 2,
				Shards: map[int64]*proto.ShardMetadata{
					42: {Status: proto.ShardStatusSteadyState, Ensemble: []*proto.DataServerIdentity{sv1, sv2}},
				},
			},
		},
	}

	nodes := linkedhashset.New("sv-1", "sv-2", "sv-3")
	sel := &contextRecordingSelector{}
	metadata := &mockMetadata{
		status:   status,
		nodes:    nodes,
		metadata: map[string]*proto.DataServerMetadata{},
		nsConfigs: map[string]*proto.Namespace{
			"ns-1": testNamespace("ns-1", 2),
		},
		nodeMap: map[string]*proto.DataServerIdentity{
			"sv-1": sv1,
			"sv-2": sv2,
			"sv-3": sv3,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b := newTestBalancer(ctx, cancel, metadata, sel)
	candidates, candidatesMetadata := dataServersToCandidatesAndMetadata(metadata.ListDataServer())
	loadRatios := single.DefaultShardsRank(&model.RatioParams{
		NodeShardsInfos: map[string][]model.ShardInfo{
			"sv-1": {{
				Namespace: "ns-1",
				ShardID:   42,
				Ensemble:  []*proto.DataServerIdentity{sv1, sv2},
			}},
			"sv-2": {{
				Namespace: "ns-1",
				ShardID:   42,
				Ensemble:  []*proto.DataServerIdentity{sv1, sv2},
			}},
			"sv-3": {},
		},
	})

	_, err := b.swapShard(
		&model.ShardLoadRatio{
			ShardInfo: &model.ShardInfo{
				Namespace: "ns-1",
				ShardID:   42,
				Ensemble:  []*proto.DataServerIdentity{sv1, sv2},
			},
			Ratio: 1,
		},
		sv1,
		&sync.WaitGroup{},
		loadRatios,
		candidates,
		candidatesMetadata,
		status,
	)
	assert.ErrorIs(t, err, selector.ErrUnsatisfiedAntiAffinity)

	assert.Equal(t, "ns-1", sel.namespace)
	assert.EqualValues(t, 42, sel.shard)
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
			"ns": testNamespace("ns", 3),
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
