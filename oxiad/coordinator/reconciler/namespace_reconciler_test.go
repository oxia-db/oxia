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
	"math"
	"sort"
	"testing"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/assert"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/sharding"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"
)

func hashRange(start, end uint32) *proto.HashRange {
	return &proto.HashRange{Min: start, Max: end}
}

func shardMetadata(status string, ensemble []*proto.DataServerIdentity, start, end uint32) *proto.ShardMetadata {
	return &proto.ShardMetadata{
		Status:         status,
		Term:           -1,
		Ensemble:       ensemble,
		Int32HashRange: hashRange(start, end),
	}
}

func loadAwareEnsembleSupplier(servers []*proto.DataServerIdentity) func(*proto.Namespace, *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
	return func(nc *proto.Namespace, cs *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
		load := make(map[string]int, len(servers))
		for _, s := range servers {
			load[s.GetInternal()] = 0
		}
		for _, ns := range cs.GetNamespaces() {
			for _, sh := range ns.GetShards() {
				for _, m := range sh.GetEnsemble() {
					load[m.GetInternal()]++
				}
			}
		}
		ranked := make([]*proto.DataServerIdentity, len(servers))
		copy(ranked, servers)
		sort.SliceStable(ranked, func(i, j int) bool {
			return load[ranked[i].GetInternal()] < load[ranked[j].GetInternal()]
		})
		return ranked[:nc.GetReplicationFactor()], nil
	}
}

func simpleEnsembleSupplier(candidates []*proto.DataServerIdentity, nc *proto.Namespace, cs *proto.ClusterStatus) []*proto.DataServerIdentity {
	n := len(candidates)
	res := make([]*proto.DataServerIdentity, nc.GetReplicationFactor())
	for i := uint32(0); i < nc.GetReplicationFactor(); i++ {
		res[i] = candidates[int(cs.ServerIdx+i)%n]
	}
	return res
}

func assertStatusEqual(t *testing.T, expected, actual *proto.ClusterStatus) {
	t.Helper()
	assert.True(t, gproto.Equal(expected, actual), "expected status %v, got %v", expected, actual)
}

type mockNamespaceMetadata struct {
	status        *proto.ClusterStatus
	configServers []*proto.DataServerIdentity
	configNS      map[string]*proto.Namespace
}

func (*mockNamespaceMetadata) Close() error { return nil }

func (m *mockNamespaceMetadata) GetStatus() *proto.ClusterStatus { return m.status }

func (m *mockNamespaceMetadata) UpdateStatus(newStatus *proto.ClusterStatus) { m.status = newStatus }

func (m *mockNamespaceMetadata) ReserveShardIDs(count uint32) int64 {
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	base := cloned.ShardIdGenerator
	cloned.ShardIdGenerator += int64(count)
	m.status = cloned
	return base
}

func (m *mockNamespaceMetadata) CreateNamespaceStatus(
	name string,
	status *proto.NamespaceStatus,
) bool {
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	if cloned.Namespaces == nil {
		cloned.Namespaces = map[string]*proto.NamespaceStatus{}
	}

	if _, exists := cloned.Namespaces[name]; exists {
		return false
	}

	namespaceStatus := gproto.Clone(status).(*proto.NamespaceStatus)
	cloned.Namespaces[name] = namespaceStatus
	cloned.ServerIdx = (cloned.ServerIdx + uint32(len(namespaceStatus.Shards))*namespaceStatus.GetReplicationFactor()) % uint32(len(m.configServers))

	m.status = cloned
	return true
}

func (m *mockNamespaceMetadata) ListNamespaceStatus() map[string]*proto.NamespaceStatus {
	namespaces := make(map[string]*proto.NamespaceStatus, len(m.status.GetNamespaces()))
	for name, status := range m.status.GetNamespaces() {
		namespaces[name] = gproto.Clone(status).(*proto.NamespaceStatus)
	}
	return namespaces
}

func (m *mockNamespaceMetadata) GetNamespaceStatus(namespace string) (*proto.NamespaceStatus, bool) {
	status, exists := m.status.GetNamespaces()[namespace]
	if !exists {
		return nil, false
	}
	return gproto.Clone(status).(*proto.NamespaceStatus), true
}

func (m *mockNamespaceMetadata) DeleteNamespaceStatus(name string) *proto.NamespaceStatus {
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	namespaceStatus, exists := cloned.Namespaces[name]
	if !exists {
		return nil
	}

	for shardID, shardMetadata := range namespaceStatus.Shards {
		shardMetadata.Status = proto.ShardStatusDeleting
		namespaceStatus.Shards[shardID] = shardMetadata
	}
	m.status = cloned
	return gproto.Clone(namespaceStatus).(*proto.NamespaceStatus)
}

func (*mockNamespaceMetadata) UpdateShardStatus(string, int64, *proto.ShardMetadata) {}

func (*mockNamespaceMetadata) DeleteShardStatus(string, int64) {}

func (*mockNamespaceMetadata) GetConfig() *proto.ClusterConfiguration { return nil }

func (*mockNamespaceMetadata) ConfigWatch() *commonwatch.Watch[*proto.ClusterConfiguration] {
	return commonwatch.New(&proto.ClusterConfiguration{})
}

func (*mockNamespaceMetadata) GetLoadBalancer() *proto.LoadBalancer { return nil }

func (*mockNamespaceMetadata) ListDataServers() *linkedhashset.Set[string] {
	return linkedhashset.New[string]()
}

func (*mockNamespaceMetadata) ListDataServersWithMetadata() (*linkedhashset.Set[string], map[string]*proto.DataServerMetadata) {
	return linkedhashset.New[string](), map[string]*proto.DataServerMetadata{}
}

func (*mockNamespaceMetadata) GetDataServerIdentity(string) (*proto.DataServerIdentity, bool) {
	return nil, false
}

func (*mockNamespaceMetadata) GetDataServer(string) (*proto.DataServer, bool) {
	return nil, false
}

func (m *mockNamespaceMetadata) GetNamespace(namespace string) (*proto.Namespace, bool) {
	ns, exists := m.configNS[namespace]
	return ns, exists
}

var _ coordmetadata.Metadata = (*mockNamespaceMetadata)(nil)

type mockNamespaceRuntime struct {
	metadata            *mockNamespaceMetadata
	selectNewEnsembleFn func(*proto.Namespace, *proto.ClusterStatus) ([]*proto.DataServerIdentity, error)
	added               map[int64]string
	deleted             []int64
}

func (*mockNamespaceRuntime) Close() error { return nil }

func (*mockNamespaceRuntime) InitiateSplit(string, int64, *uint32) (left int64, right int64, err error) {
	return 0, 0, nil
}

func (*mockNamespaceRuntime) LeaderElected(int64, *proto.DataServerIdentity, []*proto.DataServerIdentity) {
}

func (*mockNamespaceRuntime) ShardDeleted(int64) {}

func (*mockNamespaceRuntime) WaitForNextUpdate(context.Context, *proto.ShardAssignments) (*proto.ShardAssignments, error) {
	return nil, context.Canceled
}

func (*mockNamespaceRuntime) BecameUnavailable(*proto.DataServerIdentity) {}

func (*mockNamespaceRuntime) PutDataServerIfAbsent(*proto.DataServer) {}

func (*mockNamespaceRuntime) DeleteDataServer(string) {}

func (*mockNamespaceRuntime) SyncShardControllerServerAddresses() {}

func (m *mockNamespaceRuntime) CreateNamespace(name string, namespaceConfig *proto.Namespace) bool {
	baseShardID := m.metadata.ReserveShardIDs(namespaceConfig.GetInitialShardCount())
	currentStatus := m.metadata.GetStatus()
	namespaceStatus := &proto.NamespaceStatus{
		Shards:            map[int64]*proto.ShardMetadata{},
		ReplicationFactor: namespaceConfig.GetReplicationFactor(),
	}
	status := &proto.ClusterStatus{
		Namespaces: make(map[string]*proto.NamespaceStatus, len(currentStatus.GetNamespaces())+1),
		ServerIdx:  currentStatus.GetServerIdx(),
	}
	for name, existingNamespaceStatus := range currentStatus.GetNamespaces() {
		status.Namespaces[name] = existingNamespaceStatus
	}
	status.Namespaces[name] = namespaceStatus

	for _, shard := range sharding.GenerateShards(baseShardID, namespaceConfig.GetInitialShardCount()) {
		ensemble, err := m.selectNewEnsembleFn(namespaceConfig, status)
		if err != nil {
			continue
		}

		namespaceStatus.Shards[shard.Id] = &proto.ShardMetadata{
			Status:   proto.ShardStatusUnknown,
			Term:     -1,
			Leader:   nil,
			Ensemble: ensemble,
			Int32HashRange: &proto.HashRange{
				Min: shard.Min,
				Max: shard.Max,
			},
		}
	}

	if !m.metadata.CreateNamespaceStatus(name, namespaceStatus) {
		return false
	}
	for shard := range namespaceStatus.GetShards() {
		if m.added == nil {
			m.added = map[int64]string{}
		}
		m.added[shard] = name
	}
	return true
}

func (m *mockNamespaceRuntime) DeleteNamespace(namespace string) {
	namespaceStatus := m.metadata.DeleteNamespaceStatus(namespace)
	if namespaceStatus == nil {
		return
	}
	for shard := range namespaceStatus.GetShards() {
		m.DeleteShard(shard)
	}
}

func (m *mockNamespaceRuntime) DeleteShard(shard int64) {
	m.deleted = append(m.deleted, shard)
}

func (*mockNamespaceRuntime) RecomputeAssignments() {}

func (*mockNamespaceRuntime) NodeControllers() map[string]controller.DataServerController { return nil }

func (*mockNamespaceRuntime) LoadBalancer() balancer.LoadBalancer { return nil }

func (m *mockNamespaceRuntime) Metadata() coordmetadata.Metadata { return m.metadata }

var (
	s1 = &proto.DataServerIdentity{Public: "s1:6648", Internal: "s1:6649"}
	s2 = &proto.DataServerIdentity{Public: "s2:6648", Internal: "s2:6649"}
	s3 = &proto.DataServerIdentity{Public: "s3:6648", Internal: "s3:6649"}
	s4 = &proto.DataServerIdentity{Public: "s4:6648", Internal: "s4:6649"}
)

func TestNamespaceReconcilerInitialPlacementSeesPriorShards(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	const shardCount = 16
	const replicationFactor = 3

	metadata := &mockNamespaceMetadata{
		status:        proto.NewClusterStatus(),
		configServers: servers,
		configNS: map[string]*proto.Namespace{
			"ns-1": {
				Name:              "ns-1",
				InitialShardCount: shardCount,
				ReplicationFactor: replicationFactor,
			},
		},
	}
	runtime := &mockNamespaceRuntime{
		metadata:            metadata,
		selectNewEnsembleFn: loadAwareEnsembleSupplier(servers),
	}

	err := (&namespaceReconciler{runtime: runtime}).Reconcile(context.Background(), &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: shardCount,
			ReplicationFactor: replicationFactor,
		}},
		Servers: servers,
	})
	assert.NoError(t, err)

	slots := map[string]int{}
	for _, s := range servers {
		slots[s.GetInternal()] = 0
	}
	for _, shard := range metadata.status.GetNamespaces()["ns-1"].GetShards() {
		assert.Len(t, shard.GetEnsemble(), replicationFactor)
		for _, member := range shard.GetEnsemble() {
			slots[member.GetInternal()]++
		}
	}

	expected := shardCount * replicationFactor / len(servers)
	for _, s := range servers {
		assert.Equal(t, expected, slots[s.GetInternal()],
			"server %s should hold exactly %d ensemble slots", s.GetInternal(), expected)
	}
}

func TestNamespaceReconcilerNamespaceAddedPersistsAggregateStatus(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	metadata := &mockNamespaceMetadata{
		status: &proto.ClusterStatus{
			Namespaces: map[string]*proto.NamespaceStatus{
				"ns-1": {
					ReplicationFactor: 3,
					Shards: map[int64]*proto.ShardMetadata{
						0: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s1, s2, s3}, 0, math.MaxUint32),
					},
				},
			},
			ShardIdGenerator: 1,
			ServerIdx:        3,
		},
		configServers: servers,
		configNS: map[string]*proto.Namespace{
			"ns-1": {
				Name:              "ns-1",
				InitialShardCount: 1,
				ReplicationFactor: 3,
			},
			"ns-2": {
				Name:              "ns-2",
				InitialShardCount: 2,
				ReplicationFactor: 3,
			},
		},
	}
	runtime := &mockNamespaceRuntime{
		metadata: metadata,
		selectNewEnsembleFn: func(namespaceConfig *proto.Namespace, status *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
			return simpleEnsembleSupplier(servers, namespaceConfig, status), nil
		},
	}

	err := (&namespaceReconciler{runtime: runtime}).Reconcile(context.Background(), &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}, {
			Name:              "ns-2",
			InitialShardCount: 2,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	})
	assert.NoError(t, err)

	assertStatusEqual(t, &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s1, s2, s3}, 0, math.MaxUint32),
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					1: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s4, s1, s2}, 0, math.MaxUint32/2),
					2: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s4, s1, s2}, math.MaxUint32/2+1, math.MaxUint32),
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}, metadata.status)

	assert.Equal(t, map[int64]string{
		1: "ns-2",
		2: "ns-2",
	}, runtime.added)
	assert.Empty(t, runtime.deleted)
}

func TestNamespaceReconcilerNamespaceRemovedMarksDeletingAndDeletesRuntimeShards(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	metadata := &mockNamespaceMetadata{
		status: &proto.ClusterStatus{
			Namespaces: map[string]*proto.NamespaceStatus{
				"ns-1": {
					ReplicationFactor: 3,
					Shards: map[int64]*proto.ShardMetadata{
						0: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s1, s2, s3}, 0, math.MaxUint32),
					},
				},
				"ns-2": {
					ReplicationFactor: 3,
					Shards: map[int64]*proto.ShardMetadata{
						1: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s4, s1, s2}, 0, math.MaxUint32/2),
						2: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s4, s1, s2}, math.MaxUint32/2+1, math.MaxUint32),
					},
				},
			},
			ShardIdGenerator: 3,
			ServerIdx:        1,
		},
		configServers: servers,
		configNS: map[string]*proto.Namespace{
			"ns-1": {
				Name:              "ns-1",
				InitialShardCount: 1,
				ReplicationFactor: 3,
			},
		},
	}
	runtime := &mockNamespaceRuntime{
		metadata: metadata,
		selectNewEnsembleFn: func(namespaceConfig *proto.Namespace, status *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
			return simpleEnsembleSupplier(servers, namespaceConfig, status), nil
		},
	}

	err := (&namespaceReconciler{runtime: runtime}).Reconcile(context.Background(), &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	})
	assert.NoError(t, err)

	assertStatusEqual(t, &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s1, s2, s3}, 0, math.MaxUint32),
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					1: shardMetadata(proto.ShardStatusDeleting, []*proto.DataServerIdentity{s4, s1, s2}, 0, math.MaxUint32/2),
					2: shardMetadata(proto.ShardStatusDeleting, []*proto.DataServerIdentity{s4, s1, s2}, math.MaxUint32/2+1, math.MaxUint32),
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}, metadata.status)

	sort.Slice(runtime.deleted, func(i, j int) bool { return runtime.deleted[i] < runtime.deleted[j] })
	assert.Equal(t, []int64{1, 2}, runtime.deleted)
	assert.Empty(t, runtime.added)
}
