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

package balancer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/emirpasic/gods/v2/lists/arraylist"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/assert"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector/single"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type mockMetadata struct {
	status    *proto.ClusterStatus
	nodes     *linkedhashset.Set[string]
	metadata  map[string]*proto.DataServerMetadata
	nsConfigs map[string]*proto.Namespace
	nodeMap   map[string]*proto.DataServerIdentity
	lbConfig  *proto.LoadBalancer

	namespaceStatusErr     error
	listNamespaceStatusErr error
	listNamespaceErr       error
	listDataServerErr      error
	getNamespaceErr        error
	getDataServerErr       error
}

func dataServer(id string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{
		Internal: id,
	}
}

var _ coordmetadata.Metadata = (*mockMetadata)(nil)

func (*mockMetadata) Close() error { return nil }

func (*mockMetadata) GetSelf() (*proto.Coordinator, error) {
	return nil, nil //nolint:nilnil
}

func (*mockMetadata) GetLeader() (*proto.Coordinator, error) {
	return nil, nil //nolint:nilnil
}

func (*mockMetadata) WaitToBecomeLeader() (<-chan struct{}, error) {
	return nil, nil //nolint:nilnil
}

func (m *mockMetadata) GetInstanceID() (string, error) { return m.status.GetInstanceId(), nil }

func (*mockMetadata) AllocateShardIDs(uint32) (int64, error) { return 0, nil }

func (*mockMetadata) CreateNamespaceStatus(string, *proto.NamespaceStatus) error {
	return errors.New("not implemented")
}

func (m *mockMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	if m.listNamespaceStatusErr != nil {
		return nil, m.listNamespaceStatusErr
	}
	statuses := make(map[string]commonobject.Borrowed[*proto.NamespaceStatus], len(m.status.GetNamespaces()))
	for name, status := range m.status.GetNamespaces() {
		statuses[name] = commonobject.Borrow(status)
	}
	return statuses, nil
}

func (m *mockMetadata) GetNamespaceStatus(namespace string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
	if m.namespaceStatusErr != nil {
		return commonobject.Borrowed[*proto.NamespaceStatus]{}, false, m.namespaceStatusErr
	}
	status, exists := m.status.GetNamespaces()[namespace]
	if !exists {
		return commonobject.Borrowed[*proto.NamespaceStatus]{}, false, nil
	}
	return commonobject.Borrow(status), true, nil
}

func (m *mockMetadata) GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*proto.ShardMetadata], bool, error) {
	namespaceStatus, exists := m.status.GetNamespaces()[namespace]
	if !exists {
		return commonobject.Borrowed[*proto.ShardMetadata]{}, false, nil
	}
	shardStatus, exists := namespaceStatus.GetShards()[shard]
	if !exists {
		return commonobject.Borrowed[*proto.ShardMetadata]{}, false, nil
	}
	return commonobject.Borrow(shardStatus), true, nil
}

func (*mockMetadata) DeleteNamespaceStatus(string) commonobject.Borrowed[*proto.NamespaceStatus] {
	return commonobject.Borrowed[*proto.NamespaceStatus]{}
}

func (m *mockMetadata) UpdateShardStatus(namespace string, shard int64, shardMetadata *proto.ShardMetadata) error {
	ns, exists := m.status.GetNamespaces()[namespace]
	if !exists {
		return nil
	}
	ns.Shards[shard] = shardMetadata
	return nil
}

func (m *mockMetadata) PatchNamespaceStatus(namespace string, modify func(*proto.NamespaceStatus) bool) error {
	if ns, exists := m.status.GetNamespaces()[namespace]; exists {
		modify(ns)
	}
	return nil
}

func (*mockMetadata) DeleteShardStatus(string, int64) error { return nil }

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

func (m *mockMetadata) ListNamespace() (map[string]commonobject.Borrowed[*proto.Namespace], error) {
	if m.listNamespaceErr != nil {
		return nil, m.listNamespaceErr
	}
	namespaces := make(map[string]commonobject.Borrowed[*proto.Namespace], len(m.nsConfigs))
	for name, namespace := range m.nsConfigs {
		namespaces[name] = commonobject.Borrow(namespace)
	}
	return namespaces, nil
}

func (m *mockMetadata) GetConfig() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
	namespaces := make([]*proto.Namespace, 0, len(m.nsConfigs))
	for _, namespace := range m.nsConfigs {
		namespaces = append(namespaces, namespace)
	}
	return commonobject.Borrow(&proto.ClusterConfiguration{
		Namespaces: namespaces,
	}), nil
}

// SubscribeConfig is not used by these tests.
func (*mockMetadata) SubscribeConfig() *cache.Subscription[provider.Versioned[*proto.ClusterConfiguration]] {
	return nil
}

func (m *mockMetadata) GetLoadBalancerConfig() (commonobject.Borrowed[*proto.LoadBalancer], error) {
	if m.lbConfig != nil {
		return commonobject.Borrow(m.lbConfig), nil
	}
	return commonobject.Borrow(&proto.LoadBalancer{
		ScheduleInterval: "30s",
		QuarantineTime:   "5m",
	}), nil
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

func (m *mockMetadata) ListDataServer() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
	if m.listDataServerErr != nil {
		return nil, m.listDataServerErr
	}
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
	return dataServers, nil
}

// GetNamespaceTermOptions is not used by these tests.
func (*mockMetadata) GetNamespaceTermOptions(string) (*proto.NewTermOptions, error) {
	return &proto.NewTermOptions{}, nil
}

func (m *mockMetadata) GetNamespace(namespace string) (commonobject.Borrowed[*proto.Namespace], bool, error) {
	if m.getNamespaceErr != nil {
		return commonobject.Borrowed[*proto.Namespace]{}, false, m.getNamespaceErr
	}
	nc, ok := m.nsConfigs[namespace]
	if !ok {
		return commonobject.Borrowed[*proto.Namespace]{}, false, nil
	}
	return commonobject.Borrow(nc), true, nil
}

func (m *mockMetadata) GetDataServer(name string) (commonobject.Borrowed[*proto.DataServer], bool, error) {
	if m.getDataServerErr != nil {
		return commonobject.Borrowed[*proto.DataServer]{}, false, m.getDataServerErr
	}
	n, ok := m.nodeMap[name]
	if !ok {
		return commonobject.Borrowed[*proto.DataServer]{}, false, nil
	}
	metadata, ok := m.metadata[name]
	if !ok {
		metadata = &proto.DataServerMetadata{}
	}
	return commonobject.Borrow(&proto.DataServer{
		Identity: n,
		Metadata: metadata,
	}), true, nil
}

// alwaysErrorSelector is a selector that always returns errs.ErrBalancerUnsatisfiedAntiAffinity.
type alwaysErrorSelector struct{}

func (*alwaysErrorSelector) Select(_ *single.Context) (string, error) {
	return "", errs.ErrBalancerUnsatisfiedAntiAffinity
}

func newTestBalancer(
	ctx context.Context,
	cancel context.CancelFunc,
	metadata *mockMetadata,
	sel selector.Selector[*single.Context, string],
) *nodeBasedBalancer {
	// The mock never fails to load the configuration
	loadBalancerConf, _ := metadata.GetLoadBalancerConfig()
	return &nodeBasedBalancer{
		logger:                  slog.Default(),
		ctx:                     ctx,
		ctxCancel:               cancel,
		wg:                      sync.WaitGroup{},
		loadBalancerConf:        loadBalancerConf.UnsafeBorrow(),
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

func TestIsBalancedRequiresBalancedLeaders(t *testing.T) {
	sv1 := dataServer("sv-1")
	sv2 := dataServer("sv-2")
	sv3 := dataServer("sv-3")

	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: []*proto.DataServerIdentity{sv1, sv2, sv3}},
					1: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: []*proto.DataServerIdentity{sv1, sv2, sv3}},
					2: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: []*proto.DataServerIdentity{sv1, sv2, sv3}},
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
	assert.False(t, b.IsBalanced())

	status.Namespaces["ns"].Shards[1].Leader = sv2
	status.Namespaces["ns"].Shards[2].Leader = sv3
	assert.True(t, b.IsBalanced())
}

func TestIsBalancedIgnoresUnavailableNodes(t *testing.T) {
	sv1 := dataServer("sv-1")
	sv2 := dataServer("sv-2")
	sv3 := dataServer("sv-3")

	ensemble := []*proto.DataServerIdentity{sv1, sv2, sv3}
	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: ensemble},
					1: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: ensemble},
					2: {Status: proto.ShardStatusSteadyState, Leader: sv2, Ensemble: ensemble},
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

	// Leaders are (2, 1, 0): not balanced while sv-3 is available
	b := newTestBalancer(ctx, cancel, metadata, &alwaysErrorSelector{})
	assert.False(t, b.IsBalanced())

	// With sv-3 not running, the rebalancer cannot move any leader onto it:
	// the remaining nodes are balanced
	b.nodeAvailableJudger = func(nodeID string) bool { return nodeID != "sv-3" }
	assert.True(t, b.IsBalanced())
}

func TestIsBalancedAtLeaderMoveFixedPoint(t *testing.T) {
	sv1 := dataServer("sv-1")
	sv2 := dataServer("sv-2")
	sv3 := dataServer("sv-3")
	sv4 := dataServer("sv-4")
	sv5 := dataServer("sv-5")

	// Every node holds 6 replicas and the leader counts are (2, 1, 2, 3, 2):
	// the spread between the most loaded node (sv-4) and the least loaded one
	// (sv-2) is 2, but no single leader move can reduce it, since sv-2 does
	// not belong to the ensemble of any shard led by sv-4. The balancer has
	// no move left to make, so it must report the cluster as balanced.
	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: {Status: proto.ShardStatusSteadyState, Leader: sv4, Ensemble: []*proto.DataServerIdentity{sv1, sv5, sv4}},
					1: {Status: proto.ShardStatusSteadyState, Leader: sv5, Ensemble: []*proto.DataServerIdentity{sv3, sv1, sv5}},
					2: {Status: proto.ShardStatusSteadyState, Leader: sv2, Ensemble: []*proto.DataServerIdentity{sv3, sv2, sv4}},
					3: {Status: proto.ShardStatusSteadyState, Leader: sv3, Ensemble: []*proto.DataServerIdentity{sv2, sv3, sv4}},
					4: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: []*proto.DataServerIdentity{sv2, sv1, sv5}},
					5: {Status: proto.ShardStatusSteadyState, Leader: sv4, Ensemble: []*proto.DataServerIdentity{sv3, sv1, sv4}},
					6: {Status: proto.ShardStatusSteadyState, Leader: sv3, Ensemble: []*proto.DataServerIdentity{sv5, sv2, sv3}},
					7: {Status: proto.ShardStatusSteadyState, Leader: sv5, Ensemble: []*proto.DataServerIdentity{sv2, sv5, sv4}},
					8: {Status: proto.ShardStatusSteadyState, Leader: sv4, Ensemble: []*proto.DataServerIdentity{sv1, sv4, sv5}},
					9: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: []*proto.DataServerIdentity{sv2, sv1, sv3}},
				},
			},
		},
	}

	nodes := linkedhashset.New("sv-1", "sv-2", "sv-3", "sv-4", "sv-5")
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
			"sv-4": sv4,
			"sv-5": sv5,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	b := newTestBalancer(ctx, cancel, metadata, &alwaysErrorSelector{})
	assert.True(t, b.IsBalanced())

	// Moving shard 9's leadership to sv-3 leaves sv-1 with a single leader:
	// now sv-4 can donate shard 0 to sv-1, so the cluster is not balanced
	status.Namespaces["ns"].Shards[9].Leader = sv3
	assert.False(t, b.IsBalanced())
}

func TestIsBalancedRequiresNamespaceStatus(t *testing.T) {
	sv1 := dataServer("sv-1")
	sv2 := dataServer("sv-2")
	sv3 := dataServer("sv-3")

	metadata := &mockMetadata{
		status: &proto.ClusterStatus{
			Namespaces: map[string]*proto.NamespaceStatus{},
		},
		nodes:    linkedhashset.New("sv-1", "sv-2", "sv-3"),
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
	assert.False(t, b.IsBalanced())

	metadata.status.Namespaces = map[string]*proto.NamespaceStatus{
		"ns":      {},
		"deleted": {},
	}
	assert.False(t, b.IsBalanced())
}

func TestIsBalancedFalseOnNamespaceStatusError(t *testing.T) {
	sv1 := dataServer("sv-1")
	sv2 := dataServer("sv-2")
	sv3 := dataServer("sv-3")
	ensemble := []*proto.DataServerIdentity{sv1, sv2, sv3}

	metadata := &mockMetadata{
		status: &proto.ClusterStatus{
			Namespaces: map[string]*proto.NamespaceStatus{
				"ns": {
					ReplicationFactor: 3,
					Shards: map[int64]*proto.ShardMetadata{
						0: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: ensemble},
						1: {Status: proto.ShardStatusSteadyState, Leader: sv2, Ensemble: ensemble},
						2: {Status: proto.ShardStatusSteadyState, Leader: sv3, Ensemble: ensemble},
					},
				},
			},
		},
		nodes:    linkedhashset.New("sv-1", "sv-2", "sv-3"),
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
	assert.True(t, b.IsBalanced())

	metadata.namespaceStatusErr = errors.New("status unavailable")
	assert.False(t, b.IsBalanced())
}

// selectRecordingSelector records whether Select was ever called.
type selectRecordingSelector struct {
	called bool
}

func (s *selectRecordingSelector) Select(_ *single.Context) (string, error) {
	s.called = true
	return "", errs.ErrBalancerUnsatisfiedAntiAffinity
}

type contextRecordingSelector struct {
	namespace string
	shard     int64
}

func (s *contextRecordingSelector) Select(ctx *single.Context) (string, error) {
	s.namespace = ctx.Namespace
	s.shard = ctx.Shard
	return "", errs.ErrBalancerUnsatisfiedAntiAffinity
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
			"ns-1": {Name: "ns-1", ReplicationFactor: 2},
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
	dataServers, err := metadata.ListDataServer()
	assert.NoError(t, err)
	candidates, candidatesMetadata := dataServersToCandidatesAndMetadata(dataServers)
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

	_, err = b.swapShard(
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
	)
	assert.ErrorIs(t, err, errs.ErrBalancerUnsatisfiedAntiAffinity)

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

func TestNodeBasedBalancerRebalanceLeaderQuarantinesShardOnFailedElection(t *testing.T) {
	sv1 := dataServer("sv-1")
	sv2 := dataServer("sv-2")
	sv3 := dataServer("sv-3")

	ensemble := []*proto.DataServerIdentity{sv1, sv2, sv3}
	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: ensemble},
					1: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: ensemble},
					2: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: ensemble},
				},
			},
		},
	}
	metadata := &mockMetadata{
		status:   status,
		nodes:    linkedhashset.New("sv-1", "sv-2", "sv-3"),
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

	electedShard := make(chan int64, 1)
	go func() {
		ac := <-b.actionCh
		electedShard <- ac.GetShard()
		ac.Error(errors.New("the election failed"))
	}()
	b.rebalanceLeader()

	assert.Equal(t, []int64{<-electedShard}, b.quarantineShards().Values())
}

func TestNodeBasedBalancerCloseStopsBackgroundTasks(t *testing.T) {
	metadata := newBalancedMockMetadata()
	lb := NewLoadBalancer(Options{
		Context:             t.Context(),
		Metadata:            metadata,
		LoadBalancerConfig:  &proto.LoadBalancer{ScheduleInterval: "30s", QuarantineTime: "5m"},
		NodeAvailableJudger: func(string) bool { return true },
	})
	lb.Start()
	lb.Trigger()

	assert.NoError(t, lb.Close())
	assert.ErrorIs(t, lb.(*nodeBasedBalancer).ctx.Err(), context.Canceled)
	assert.NotPanics(t, lb.Trigger)
}

func TestNodeBasedBalancerRebalanceEnsembleFailsOnStatusError(t *testing.T) {
	metadata := newBalancedMockMetadata()
	metadata.listNamespaceStatusErr = errors.New("status unavailable")
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})

	assert.False(t, b.rebalanceEnsemble())
	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerRebalanceEnsembleFailsOnDataServerError(t *testing.T) {
	metadata := newBalancedMockMetadata()
	metadata.listDataServerErr = errors.New("data servers unavailable")
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})

	assert.False(t, b.rebalanceEnsemble())
	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerRebalanceEnsembleLogsAtDebugLevel(t *testing.T) {
	metadata := newBalancedMockMetadata()
	// A data server without metadata is a candidate with empty metadata
	metadata.metadata["sv-1"] = nil
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})
	logs := &bytes.Buffer{}
	b.logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	b.nodeQuarantineNodeMap.Store("sv-2", time.Now())

	assert.True(t, b.rebalanceEnsemble())
	assert.Contains(t, logs.String(), "start shard rebalance")
	assert.Contains(t, logs.String(), `quarantine-nodes="LinkedHashSet\nsv-2"`)
	assert.Contains(t, logs.String(), "end shard rebalance")
}

func TestNodeBasedBalancerRebalanceEnsembleSettlesOnUnevenSpread(t *testing.T) {
	// 11 replicas on 3 nodes -> 4/4/3 should be stable
	metadata := newBalancedMockMetadata()
	sv1, sv2 := metadata.nodeMap["sv-1"], metadata.nodeMap["sv-2"]
	metadata.status.Namespaces["ns-2"] = &proto.NamespaceStatus{
		ReplicationFactor: 2,
		Shards: map[int64]*proto.ShardMetadata{
			3: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: []*proto.DataServerIdentity{sv1, sv2}},
		},
	}
	metadata.nsConfigs["ns-2"] = &proto.Namespace{Name: "ns-2", ReplicationFactor: 2}
	b := newTestBalancer(t.Context(), func() {}, metadata, single.NewSelector())
	swaps := make(chan int64, 1000)
	go func() {
		for ac := range b.actionCh {
			swaps <- ac.GetShard()
			ac.Done(nil)
		}
	}()

	assert.True(t, b.rebalanceEnsemble())
	assert.True(t, b.IsBalanced())
	close(b.actionCh)
	select {
	case shard := <-swaps:
		t.Fatalf("expected no swap, got a swap of shard %d", shard)
	default:
	}
}

func TestNodeBasedBalancerBalanceHighestNodeIgnoresEmptyRatios(t *testing.T) {
	sel := &contextRecordingSelector{}
	b := newTestBalancer(t.Context(), func() {}, newBalancedMockMetadata(), sel)
	loadRatios := single.DefaultShardsRank(&model.RatioParams{NodeShardsInfos: map[string][]model.ShardInfo{}})

	b.balanceHighestNode(loadRatios, linkedhashset.New[string](), map[string]*proto.DataServerMetadata{}, &sync.WaitGroup{})

	assert.Empty(t, sel.namespace)
	assert.Empty(t, b.quarantineNodes().Values())
}

func TestNodeBasedBalancerBalanceHighestNodeSkipsQuarantinedNode(t *testing.T) {
	sel := &contextRecordingSelector{}
	b := newTestBalancer(t.Context(), func() {}, newBalancedMockMetadata(), sel)
	b.nodeQuarantineNodeMap.Store("sv-1", time.Now())
	loadRatios := single.DefaultShardsRank(&model.RatioParams{NodeShardsInfos: map[string][]model.ShardInfo{
		"sv-1": {shardInfo(1, "sv-1"), shardInfo(2, "sv-1"), shardInfo(3, "sv-1")},
		"sv-2": {shardInfo(4, "sv-2"), shardInfo(5, "sv-2")},
		"sv-3": {},
	}})
	candidates := linkedhashset.New("sv-1", "sv-2", "sv-3")

	b.balanceHighestNode(loadRatios, candidates, map[string]*proto.DataServerMetadata{}, &sync.WaitGroup{})

	// The shards of sv-2, the most loaded node that is not quarantined, can't
	// be moved: it is quarantined in turn
	assert.Contains(t, []int64{4, 5}, sel.shard)
	assert.ElementsMatch(t, []string{"sv-1", "sv-2"}, b.quarantineNodes().Values())
}

func TestNodeBasedBalancerBalanceHighestNodeStopsWhenAllNodesQuarantined(t *testing.T) {
	sel := &contextRecordingSelector{}
	b := newTestBalancer(t.Context(), func() {}, newBalancedMockMetadata(), sel)
	for _, node := range []string{"sv-1", "sv-2", "sv-3"} {
		b.nodeQuarantineNodeMap.Store(node, time.Now())
	}
	loadRatios := single.DefaultShardsRank(&model.RatioParams{NodeShardsInfos: map[string][]model.ShardInfo{
		"sv-1": {shardInfo(1, "sv-1"), shardInfo(2, "sv-1")},
		"sv-2": {shardInfo(3, "sv-2")},
		"sv-3": {},
	}})
	candidates := linkedhashset.New("sv-1", "sv-2", "sv-3")

	b.balanceHighestNode(loadRatios, candidates, map[string]*proto.DataServerMetadata{}, &sync.WaitGroup{})

	assert.Empty(t, sel.namespace)
	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerBalanceHighestNodeIgnoresMissingNode(t *testing.T) {
	sel := &contextRecordingSelector{}
	b := newTestBalancer(t.Context(), func() {}, newBalancedMockMetadata(), sel)
	loadRatios := model.NewRatio(1, 0, 0.1, arraylist.New[*model.NodeLoadRatio](nil))

	b.balanceHighestNode(loadRatios, linkedhashset.New[string](), map[string]*proto.DataServerMetadata{}, &sync.WaitGroup{})

	assert.Empty(t, sel.namespace)
	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerBalanceHighestNodeIgnoresMissingShard(t *testing.T) {
	sel := &contextRecordingSelector{}
	b := newTestBalancer(t.Context(), func() {}, newBalancedMockMetadata(), sel)
	loadRatios := model.NewRatio(1, 0, 0.1, arraylist.New(&model.NodeLoadRatio{
		NodeID:      "sv-1",
		Node:        dataServer("sv-1"),
		Ratio:       1,
		ShardRatios: arraylist.New[*model.ShardLoadRatio](nil),
	}))

	b.balanceHighestNode(loadRatios, linkedhashset.New("sv-1"), map[string]*proto.DataServerMetadata{}, &sync.WaitGroup{})

	assert.Empty(t, sel.namespace)
	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerSwapShardKeepsShardOnSelectedSourceNode(t *testing.T) {
	metadata := newBalancedMockMetadata()
	b := newTestBalancer(t.Context(), func() {}, metadata, &fixedSelector{target: "sv-1"})

	swapped, err := swapShardOffNode(t, b, metadata, "ns")

	assert.NoError(t, err)
	assert.False(t, swapped)
	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerSwapShardFailsOnNamespaceError(t *testing.T) {
	metadata := newBalancedMockMetadata()
	metadata.getNamespaceErr = errors.New("namespace unavailable")
	b := newTestBalancer(t.Context(), func() {}, metadata, &fixedSelector{target: "sv-3"})

	swapped, err := swapShardOffNode(t, b, metadata, "ns")

	assert.ErrorIs(t, err, metadata.getNamespaceErr)
	assert.False(t, swapped)
}

func TestNodeBasedBalancerSwapShardSkipsUnknownNamespace(t *testing.T) {
	metadata := newBalancedMockMetadata()
	b := newTestBalancer(t.Context(), func() {}, metadata, &fixedSelector{target: "sv-3"})

	swapped, err := swapShardOffNode(t, b, metadata, "unknown")

	assert.NoError(t, err)
	assert.False(t, swapped)
	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerSwapShardFailsOnDataServerError(t *testing.T) {
	metadata := newBalancedMockMetadata()
	metadata.getDataServerErr = errors.New("data server unavailable")
	b := newTestBalancer(t.Context(), func() {}, metadata, &fixedSelector{target: "sv-3"})

	swapped, err := swapShardOffNode(t, b, metadata, "ns")

	assert.ErrorIs(t, err, metadata.getDataServerErr)
	assert.False(t, swapped)
}

func TestNodeBasedBalancerSwapShardFailsOnUnknownTarget(t *testing.T) {
	metadata := newBalancedMockMetadata()
	b := newTestBalancer(t.Context(), func() {}, metadata, &fixedSelector{target: "sv-9"})

	swapped, err := swapShardOffNode(t, b, metadata, "ns")

	assert.ErrorContains(t, err, "target node does not exist")
	assert.False(t, swapped)
}

func TestNodeBasedBalancerSwapShardFailsOnTargetWithoutIdentity(t *testing.T) {
	metadata := newBalancedMockMetadata()
	metadata.nodeMap["sv-3"] = nil
	b := newTestBalancer(t.Context(), func() {}, metadata, &fixedSelector{target: "sv-3"})

	swapped, err := swapShardOffNode(t, b, metadata, "ns")

	assert.ErrorContains(t, err, "target node does not have identity")
	assert.False(t, swapped)
}

func TestNodeBasedBalancerCheckQuarantineNodesReleasesExpiredNodes(t *testing.T) {
	b := newTestBalancer(t.Context(), func() {}, newBalancedMockMetadata(), &alwaysErrorSelector{})
	b.nodeQuarantineNodeMap.Store("sv-1", time.Now().Add(-time.Hour))
	b.nodeQuarantineNodeMap.Store("sv-2", time.Now())

	b.checkQuarantineNodes()

	assert.Equal(t, []string{"sv-2"}, b.quarantineNodes().Values())
}

func TestNodeBasedBalancerCheckQuarantineShardsReleasesExpiredShards(t *testing.T) {
	b := newTestBalancer(t.Context(), func() {}, newBalancedMockMetadata(), &alwaysErrorSelector{})
	b.shardQuarantineShardMap.Store(int64(1), time.Now().Add(-time.Hour))
	b.shardQuarantineShardMap.Store(int64(2), time.Now())

	b.checkQuarantineShards()

	assert.Equal(t, []int64{2}, b.quarantineShards().Values())
}

func TestIsBalancedFalseOnNamespaceConfigError(t *testing.T) {
	metadata := newBalancedMockMetadata()
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})
	assert.True(t, b.IsBalanced())

	metadata.listNamespaceErr = errors.New("config unavailable")
	assert.False(t, b.IsBalanced())
}

func TestIsBalancedFalseOnDataServerError(t *testing.T) {
	metadata := newBalancedMockMetadata()
	metadata.listDataServerErr = errors.New("data servers unavailable")
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})

	assert.False(t, b.IsBalanced())
}

func TestIsBalancedWithoutShards(t *testing.T) {
	metadata := newBalancedMockMetadata()
	metadata.nodes = linkedhashset.New[string]()
	metadata.status.Namespaces["ns"].Shards = map[int64]*proto.ShardMetadata{}
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})

	assert.True(t, b.IsBalanced())
}

func TestNodeBasedBalancerRebalanceLeaderSkipsQuarantinedShards(t *testing.T) {
	metadata := newBalancedMockMetadata()
	for _, shard := range metadata.status.Namespaces["ns"].Shards {
		shard.Leader = metadata.nodeMap["sv-1"]
	}
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})
	for shard := range metadata.status.Namespaces["ns"].Shards {
		b.shardQuarantineShardMap.Store(shard, time.Now())
	}

	b.rebalanceLeader()

	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerRebalanceLeaderFailsOnStatusError(t *testing.T) {
	metadata := newBalancedMockMetadata()
	metadata.listNamespaceStatusErr = errors.New("status unavailable")
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})

	b.rebalanceLeader()

	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerRebalanceLeaderFailsOnDataServerError(t *testing.T) {
	metadata := newBalancedMockMetadata()
	metadata.listDataServerErr = errors.New("data servers unavailable")
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})

	b.rebalanceLeader()

	assert.Empty(t, b.actionCh)
}

func TestNodeBasedBalancerRebalanceLeaderQuarantinesShardWhenLeaderUnchanged(t *testing.T) {
	metadata := newBalancedMockMetadata()
	for _, shard := range metadata.status.Namespaces["ns"].Shards {
		shard.Leader = metadata.nodeMap["sv-1"]
	}
	b := newTestBalancer(t.Context(), func() {}, metadata, &alwaysErrorSelector{})
	logs := &bytes.Buffer{}
	b.logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	electedShard := make(chan int64, 1)
	go func() {
		ac := <-b.actionCh
		electedShard <- ac.GetShard()
		ac.Done("sv-1")
	}()
	b.rebalanceLeader()

	assert.Equal(t, []int64{<-electedShard}, b.quarantineShards().Values())
	assert.Contains(t, logs.String(), "start leader rebalance")
	assert.Contains(t, logs.String(), "end leader rebalance")
}

// fixedSelector always selects the target.
type fixedSelector struct {
	target string
}

func (s *fixedSelector) Select(*single.Context) (string, error) {
	return s.target, nil
}

// newBalancedMockMetadata returns the metadata of a namespace with 3 shards
// replicated on the 3 data servers, each one leading a shard.
func newBalancedMockMetadata() *mockMetadata {
	sv1 := &proto.DataServerIdentity{Name: new("sv-1"), Internal: "sv-1"}
	sv2 := &proto.DataServerIdentity{Name: new("sv-2"), Internal: "sv-2"}
	sv3 := &proto.DataServerIdentity{Name: new("sv-3"), Internal: "sv-3"}
	ensemble := []*proto.DataServerIdentity{sv1, sv2, sv3}
	return &mockMetadata{
		status: &proto.ClusterStatus{
			Namespaces: map[string]*proto.NamespaceStatus{
				"ns": {
					ReplicationFactor: 3,
					Shards: map[int64]*proto.ShardMetadata{
						0: {Status: proto.ShardStatusSteadyState, Leader: sv1, Ensemble: ensemble},
						1: {Status: proto.ShardStatusSteadyState, Leader: sv2, Ensemble: ensemble},
						2: {Status: proto.ShardStatusSteadyState, Leader: sv3, Ensemble: ensemble},
					},
				},
			},
		},
		nodes:     linkedhashset.New("sv-1", "sv-2", "sv-3"),
		metadata:  map[string]*proto.DataServerMetadata{},
		nsConfigs: map[string]*proto.Namespace{"ns": {Name: "ns", ReplicationFactor: 3}},
		nodeMap:   map[string]*proto.DataServerIdentity{"sv-1": sv1, "sv-2": sv2, "sv-3": sv3},
	}
}

// swapShardOffNode swaps a shard of the namespace, replicated on sv-1 and
// sv-2, off sv-1.
func swapShardOffNode(t *testing.T, b *nodeBasedBalancer, metadata *mockMetadata, namespace string) (bool, error) {
	t.Helper()
	dataServers, err := metadata.ListDataServer()
	assert.NoError(t, err)
	candidates, candidatesMetadata := dataServersToCandidatesAndMetadata(dataServers)
	shard := shardInfo(42, "sv-1", "sv-2")
	shard.Namespace = namespace
	loadRatios := single.DefaultShardsRank(&model.RatioParams{NodeShardsInfos: map[string][]model.ShardInfo{
		"sv-1": {shard},
		"sv-2": {shard},
		"sv-3": {},
	}})
	return b.swapShard(&model.ShardLoadRatio{ShardInfo: &shard, Ratio: 1}, metadata.nodeMap["sv-1"],
		&sync.WaitGroup{}, loadRatios, candidates, candidatesMetadata)
}

func shardInfo(shard int64, ensemble ...string) model.ShardInfo {
	info := model.ShardInfo{Namespace: "ns", ShardID: shard}
	for _, node := range ensemble {
		info.Ensemble = append(info.Ensemble, &proto.DataServerIdentity{Name: new(node), Internal: node})
	}
	return info
}
