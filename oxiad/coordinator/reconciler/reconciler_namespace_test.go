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

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	"github.com/oxia-db/oxia/oxiad/common/dataserver"
	"github.com/oxia-db/oxia/oxiad/common/sharding"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

func TestNamespaceReconcilerReconcileInitialPlacementSeesPriorShards(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	const shardCount = 16
	const replicationFactor = 3

	metadata := &mockNamespaceMetadata{
		status: proto.NewClusterStatus(),
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

func TestNamespaceReconcilerReconcileNamespaceAddedPersistsAggregateStatus(t *testing.T) {
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
		},
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
		selectNewEnsembleFn: func(namespaceConfig *proto.Namespace, shardID int64, _ *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
			return simpleEnsembleSupplier(servers, shardID, namespaceConfig.GetReplicationFactor()), nil
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
					1: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s2, s3, s4}, 0, math.MaxUint32/2),
					2: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s3, s4, s1}, math.MaxUint32/2+1, math.MaxUint32),
				},
			},
		},
		ShardIdGenerator: 3,
	}, metadata.status)

	assert.Equal(t, map[int64]string{
		0: "ns-1",
		1: "ns-2",
		2: "ns-2",
	}, runtime.added)
	assert.Empty(t, runtime.deleted)
}

func TestNamespaceReconcilerReconcileNamespaceRemovedMarksDeletingAndDeletesRuntimeShards(t *testing.T) {
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
		},
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
		selectNewEnsembleFn: func(namespaceConfig *proto.Namespace, shardID int64, _ *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
			return simpleEnsembleSupplier(servers, shardID, namespaceConfig.GetReplicationFactor()), nil
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
	}, metadata.status)

	sort.Slice(runtime.deleted, func(i, j int) bool { return runtime.deleted[i] < runtime.deleted[j] })
	assert.Equal(t, []int64{1, 2}, runtime.deleted)
	assert.Equal(t, map[int64]string{0: "ns-1"}, runtime.added)
}

// A namespace whose configuration can't be loaded is kept: the reconciler
// returns the error instead of deleting it.
func TestNamespaceReconcilerReconcileKeepsNamespaceOnConfigurationLoadError(t *testing.T) {
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
		},
		configNS:        map[string]*proto.Namespace{},
		getNamespaceErr: errors.New("configuration unavailable"),
	}
	runtime := &mockNamespaceRuntime{metadata: metadata}

	err := (&namespaceReconciler{runtime: runtime}).Reconcile(context.Background(), &proto.ClusterConfiguration{
		Servers: []*proto.DataServerIdentity{s1, s2, s3},
	})
	assert.ErrorContains(t, err, "configuration unavailable")
	assert.Empty(t, runtime.deleted)
	assert.Equal(t, proto.ShardStatusUnknown, metadata.status.Namespaces["ns-1"].Shards[0].GetStatusOrDefault())
}

// Namespaces added to the configuration file don't go through the management
// API, so the reconciler must not create one with a reserved name either.
func TestNamespaceReconcilerReconcileDoesNotCreateReservedNamespace(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	metadata := &mockNamespaceMetadata{
		status: proto.NewClusterStatus(),
		configNS: map[string]*proto.Namespace{
			"MANIFEST": {Name: "MANIFEST", InitialShardCount: 1, ReplicationFactor: 3},
			"ns-1":     {Name: "ns-1", InitialShardCount: 1, ReplicationFactor: 3},
		},
	}
	runtime := &mockNamespaceRuntime{
		metadata: metadata,
		selectNewEnsembleFn: func(namespaceConfig *proto.Namespace, shardID int64, _ *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
			return simpleEnsembleSupplier(servers, shardID, namespaceConfig.GetReplicationFactor()), nil
		},
	}

	err := (&namespaceReconciler{runtime: runtime}).Reconcile(context.Background(), &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{
			{Name: "MANIFEST", InitialShardCount: 1, ReplicationFactor: 3},
			{Name: "ns-1", InitialShardCount: 1, ReplicationFactor: 3},
		},
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
		},
		ShardIdGenerator: 1,
	}, metadata.status)

	assert.Equal(t, map[int64]string{0: "ns-1"}, runtime.added)
	assert.Empty(t, runtime.deleted)
}

func TestNamespaceReconcilerReconcileRepairsExistingNamespace(t *testing.T) {
	runtime := namespaceRuntimeForErrors("existing")
	namespace := runtime.metadata.configNS["existing"]
	require.NoError(t, runtime.metadata.CreateNamespaceStatus("existing", &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{42: {Term: 3}},
	}))
	require.Empty(t, runtime.added)
	shardIDGenerator := runtime.metadata.status.ShardIdGenerator

	reconciler := &namespaceReconciler{runtime: runtime}
	snapshot := &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{namespace}}
	require.NoError(t, reconciler.Reconcile(t.Context(), snapshot))
	require.Equal(t, map[int64]string{42: "existing"}, runtime.added)
	require.Equal(t, shardIDGenerator, runtime.metadata.status.ShardIdGenerator)
}

func TestNamespaceReconcilerReconcileReturnsCreationError(t *testing.T) {
	base := namespaceRuntimeForErrors("first", "second", "healthy")
	firstErr := errors.New("first namespace write failed")
	secondErr := errors.New("second namespace write failed")
	runtime := &failingNamespaceRuntime{
		Runtime: base,
		failures: map[string]error{
			"first":  firstErr,
			"second": secondErr,
		},
	}
	reconciler := &namespaceReconciler{runtime: runtime}
	snapshot := &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{
		base.metadata.configNS["healthy"], base.metadata.configNS["first"], base.metadata.configNS["second"],
	}}

	err := reconciler.Reconcile(t.Context(), snapshot)
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, secondErr)
	require.Equal(t, []string{"healthy", "first", "second"}, runtime.attempted)
	require.Len(t, base.added, 1)
	_, exists, err := base.metadata.GetNamespaceStatus("healthy")
	require.NoError(t, err)
	require.True(t, exists)

	delete(runtime.failures, "first")
	err = reconciler.Reconcile(t.Context(), snapshot)
	require.Same(t, secondErr, err)
	require.Equal(t, []string{"healthy", "first", "second", "first", "second"}, runtime.attempted)
	require.Len(t, base.added, 2)

	delete(runtime.failures, "second")
	require.NoError(t, reconciler.Reconcile(t.Context(), snapshot))
	require.Equal(t, []string{"healthy", "first", "second", "first", "second", "second"}, runtime.attempted)
	require.Len(t, base.added, 3)
	for _, name := range []string{"first", "second", "healthy"} {
		_, exists, err := base.metadata.GetNamespaceStatus(name)
		require.NoError(t, err)
		require.True(t, exists)
	}
}

func TestNamespaceReconcilerReconcileReturnsAlreadyExistsForRetry(t *testing.T) {
	base := namespaceRuntimeForErrors("existing", "healthy")
	createErr := fmt.Errorf("namespace creation: %w", errs.ErrAlreadyExists)
	runtime := &failingNamespaceRuntime{
		Runtime: base,
		failures: map[string]error{
			"existing": createErr,
		},
	}
	snapshot := &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{
		base.metadata.configNS["existing"], base.metadata.configNS["healthy"],
	}}

	reconciler := &namespaceReconciler{runtime: runtime}
	require.Same(t, createErr, reconciler.Reconcile(t.Context(), snapshot))
	require.Equal(t, []string{"existing", "healthy"}, runtime.attempted)
	require.Len(t, base.added, 1)

	delete(runtime.failures, "existing")
	require.NoError(t, reconciler.Reconcile(t.Context(), snapshot))
	require.Equal(t, []string{"existing", "healthy", "existing"}, runtime.attempted)
	require.Len(t, base.added, 2)
	_, exists, err := base.metadata.GetNamespaceStatus("healthy")
	require.NoError(t, err)
	require.True(t, exists)
}

var (
	s1 = &proto.DataServerIdentity{Public: "s1:6648", Internal: "s1:6649"}
	s2 = &proto.DataServerIdentity{Public: "s2:6648", Internal: "s2:6649"}
	s3 = &proto.DataServerIdentity{Public: "s3:6648", Internal: "s3:6649"}
	s4 = &proto.DataServerIdentity{Public: "s4:6648", Internal: "s4:6649"}
)

func hashRange(start, end uint32) *proto.HashRange {
	return &proto.HashRange{Min: start, Max: end}
}

func shardMetadata(status proto.ShardStatus, ensemble []*proto.DataServerIdentity, start, end uint32) *proto.ShardMetadata {
	return &proto.ShardMetadata{
		Status:         status,
		Term:           -1,
		Ensemble:       ensemble,
		Int32HashRange: hashRange(start, end),
	}
}

func loadAwareEnsembleSupplier(servers []*proto.DataServerIdentity) func(*proto.Namespace, int64, *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
	return func(nc *proto.Namespace, _ int64, cs *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
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

func simpleEnsembleSupplier(candidates []*proto.DataServerIdentity, shardID int64, replicationFactor uint32) []*proto.DataServerIdentity {
	n := len(candidates)
	res := make([]*proto.DataServerIdentity, replicationFactor)
	start := int(shardID % int64(n))
	for i := uint32(0); i < replicationFactor; i++ {
		res[i] = candidates[(start+int(i))%n]
	}
	return res
}

func assertStatusEqual(t *testing.T, expected, actual *proto.ClusterStatus) {
	t.Helper()
	assert.True(t, gproto.Equal(expected, actual), "expected status %v, got %v", expected, actual)
}

type mockNamespaceMetadata struct {
	status   *proto.ClusterStatus
	config   *proto.ClusterConfiguration
	configNS map[string]*proto.Namespace
	// getNamespaceErr, when set, fails the namespace loads
	getNamespaceErr error
	// listDataServerStatusErr and patchDataServerStatusErr, when set, fail
	// the reads and the writes of the statuses of the data servers, and
	// staleDataServerStatus, when set, is read instead of the stored ones
	listDataServerStatusErr  error
	patchDataServerStatusErr error
	staleDataServerStatus    map[string]*proto.DataServerStatus
}

func (*mockNamespaceMetadata) Close() error { return nil }

func (m *mockNamespaceMetadata) GetInstanceID() (string, error) { return m.status.GetInstanceId(), nil }

func (m *mockNamespaceMetadata) AllocateShardIDs(count uint32) (int64, error) {
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	base := cloned.ShardIdGenerator
	cloned.ShardIdGenerator += int64(count)
	m.status = cloned
	return base, nil
}

func (m *mockNamespaceMetadata) CreateNamespaceStatus(
	name string,
	status *proto.NamespaceStatus,
) error {
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	if cloned.Namespaces == nil {
		cloned.Namespaces = map[string]*proto.NamespaceStatus{}
	}

	if _, exists := cloned.Namespaces[name]; exists {
		return errs.ErrAlreadyExists
	}

	namespaceStatus := gproto.Clone(status).(*proto.NamespaceStatus)
	cloned.Namespaces[name] = namespaceStatus

	m.status = cloned
	return nil
}

func (m *mockNamespaceMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	namespaces := make(map[string]commonobject.Borrowed[*proto.NamespaceStatus], len(m.status.GetNamespaces()))
	for name, status := range m.status.GetNamespaces() {
		namespaces[name] = commonobject.Borrow(status)
	}
	return namespaces, nil
}

func (m *mockNamespaceMetadata) GetNamespaceStatus(namespace string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
	status, exists := m.status.GetNamespaces()[namespace]
	if !exists {
		return commonobject.Borrowed[*proto.NamespaceStatus]{}, false, nil
	}
	return commonobject.Borrow(status), true, nil
}

func (m *mockNamespaceMetadata) GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*proto.ShardMetadata], bool, error) {
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

func (m *mockNamespaceMetadata) DeleteNamespaceStatus(name string) commonobject.Borrowed[*proto.NamespaceStatus] {
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	namespaceStatus, exists := cloned.Namespaces[name]
	if !exists {
		return commonobject.Borrowed[*proto.NamespaceStatus]{}
	}

	for shardID, shardMetadata := range namespaceStatus.Shards {
		shardMetadata.Status = proto.ShardStatusDeleting
		namespaceStatus.Shards[shardID] = shardMetadata
	}
	m.status = cloned
	return commonobject.Borrow(namespaceStatus)
}

func (m *mockNamespaceMetadata) UpdateShardStatus(namespace string, shard int64, shardMetadata *proto.ShardMetadata) error {
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	ns, exists := cloned.GetNamespaces()[namespace]
	if !exists {
		return nil
	}
	ns.Shards[shard] = shardMetadata
	m.status = cloned
	return nil
}

func (m *mockNamespaceMetadata) PatchNamespaceStatus(namespace string, modify func(*proto.NamespaceStatus) (bool, error)) error {
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	ns, exists := cloned.GetNamespaces()[namespace]
	if !exists {
		return nil
	}
	if updated, err := modify(ns); err != nil || !updated {
		return err
	}
	m.status = cloned
	return nil
}

func (m *mockNamespaceMetadata) PatchShardStatus(namespace string, shard int64,
	modify func(*proto.ShardMetadata) (bool, error)) error {
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	shardMetadata, exists := cloned.GetNamespaces()[namespace].GetShards()[shard]
	if !exists {
		return nil
	}
	if updated, err := modify(shardMetadata); err != nil || !updated {
		return err
	}
	m.status = cloned
	return nil
}

func (*mockNamespaceMetadata) DeleteShardStatus(string, int64) error { return nil }

func (m *mockNamespaceMetadata) ListDataServerStatus() (map[string]commonobject.Borrowed[*proto.DataServerStatus], error) {
	if m.listDataServerStatusErr != nil {
		return nil, m.listDataServerStatusErr
	}
	stored := m.status.GetDataServers()
	if m.staleDataServerStatus != nil {
		stored = m.staleDataServerStatus
	}
	statuses := make(map[string]commonobject.Borrowed[*proto.DataServerStatus], len(stored))
	for name, status := range stored {
		statuses[name] = commonobject.Borrow(status)
	}
	return statuses, nil
}

func (m *mockNamespaceMetadata) GetDataServerStatus(name string) (commonobject.Borrowed[*proto.DataServerStatus], bool, error) {
	status, exists := m.status.GetDataServers()[name]
	return commonobject.Borrow(status), exists, nil
}

func (m *mockNamespaceMetadata) PatchDataServerStatus(name string,
	modify func(*proto.DataServerStatus) (bool, error)) error {
	if m.patchDataServerStatusErr != nil {
		return m.patchDataServerStatusErr
	}
	cloned := gproto.Clone(m.status).(*proto.ClusterStatus)
	status, exists := cloned.GetDataServers()[name]
	if !exists {
		status = &proto.DataServerStatus{}
	}
	if updated, err := modify(status); err != nil || !updated {
		return err
	}
	if cloned.DataServers == nil {
		cloned.DataServers = map[string]*proto.DataServerStatus{}
	}
	cloned.DataServers[name] = status
	m.status = cloned
	return nil
}

func (*mockNamespaceMetadata) DeleteDrainedDataServerStatus(string) error {
	return nil
}

func (*mockNamespaceMetadata) CreateNamespace(*proto.Namespace) error {
	return nil
}

func (*mockNamespaceMetadata) PatchNamespace(*proto.Namespace) (*proto.Namespace, error) {
	return &proto.Namespace{}, nil
}

func (*mockNamespaceMetadata) DeleteNamespace(string) (*proto.Namespace, error) {
	return &proto.Namespace{}, nil
}

func (m *mockNamespaceMetadata) ListNamespace() (map[string]commonobject.Borrowed[*proto.Namespace], error) {
	namespaces := make(map[string]commonobject.Borrowed[*proto.Namespace], len(m.configNS))
	for name, namespace := range m.configNS {
		namespaces[name] = commonobject.Borrow(namespace)
	}
	return namespaces, nil
}

func (m *mockNamespaceMetadata) GetConfig() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
	return commonobject.Borrow(m.config), nil
}

// SubscribeConfig is not used by these tests.
func (*mockNamespaceMetadata) SubscribeConfig() *cache.Subscription[provider.Versioned[*proto.ClusterConfiguration]] {
	return nil
}

func (*mockNamespaceMetadata) CreateDataServer(*proto.DataServer) error {
	return nil
}

func (*mockNamespaceMetadata) PatchDataServer(*proto.DataServer) (*proto.DataServer, error) {
	return &proto.DataServer{}, nil
}

func (*mockNamespaceMetadata) DeleteDataServer(string) (*proto.DataServer, error) {
	return &proto.DataServer{}, nil
}

func (*mockNamespaceMetadata) ListDataServer() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
	return map[string]commonobject.Borrowed[*proto.DataServer]{}, nil
}

func (*mockNamespaceMetadata) GetDataServer(string) (commonobject.Borrowed[*proto.DataServer], bool, error) {
	return commonobject.Borrowed[*proto.DataServer]{}, false, nil
}

// GetNamespaceTermOptions is not used by these tests.
func (*mockNamespaceMetadata) GetNamespaceTermOptions(string) (*proto.NewTermOptions, error) {
	return &proto.NewTermOptions{}, nil
}

func (m *mockNamespaceMetadata) GetNamespace(namespace string) (commonobject.Borrowed[*proto.Namespace], bool, error) {
	if m.getNamespaceErr != nil {
		return commonobject.Borrowed[*proto.Namespace]{}, false, m.getNamespaceErr
	}
	ns, exists := m.configNS[namespace]
	if !exists {
		return commonobject.Borrowed[*proto.Namespace]{}, false, nil
	}
	return commonobject.Borrow(ns), true, nil
}

var _ coordmetadata.Metadata = (*mockNamespaceMetadata)(nil)

func (*mockNamespaceMetadata) GetSelf() (*proto.Coordinator, error) {
	return nil, nil //nolint:nilnil
}

func (*mockNamespaceMetadata) GetLeader() (*proto.Coordinator, error) {
	return nil, nil //nolint:nilnil
}

func (*mockNamespaceMetadata) WaitToBecomeLeader() (<-chan struct{}, error) {
	return nil, nil //nolint:nilnil
}

type mockNamespaceRuntime struct {
	metadata            *mockNamespaceMetadata
	selectNewEnsembleFn func(*proto.Namespace, int64, *proto.ClusterStatus) ([]*proto.DataServerIdentity, error)
	added               map[int64]string
	deleted             []int64
	createdDataServers  []string
	drainedDataServers  []string
	dataServerStatus    map[string]*proto.DataServerStatus
	// forgottenDataServers are the names passed to DrainedDataServer,
	// which returns the error of forgetErrs for each
	forgottenDataServers []string
	forgetErrs           map[string]error
}

func (*mockNamespaceRuntime) Close() error { return nil }

func (*mockNamespaceRuntime) InitiateSplit(string, int64, *uint32) (left int64, right int64, err error) {
	return 0, 0, nil
}

func (*mockNamespaceRuntime) LeaderElected(int64, *proto.DataServerIdentity, []*proto.DataServerIdentity) {
}

func (*mockNamespaceRuntime) ShardDeleted(int64) {}

func (*mockNamespaceRuntime) SubscribeShardAssignments() *commonwatch.Receiver[*proto.ShardAssignments] {
	return commonwatch.New(&proto.ShardAssignments{}).Subscribe()
}

func (*mockNamespaceRuntime) BecameUnavailable(*proto.DataServerIdentity) {}

func (*mockNamespaceRuntime) FeaturesDiscovered(*proto.DataServerIdentity) {}

func (*mockNamespaceRuntime) BecameDrained(*proto.DataServerIdentity) {}

func (m *mockNamespaceRuntime) CreateDataServer(name string, _ *proto.DataServer) bool {
	if _, exists := m.dataServerStatus[name]; exists {
		return false
	}
	if m.dataServerStatus == nil {
		m.dataServerStatus = map[string]*proto.DataServerStatus{}
	}
	m.dataServerStatus[name] = &proto.DataServerStatus{State: proto.DataServerState_DATA_SERVER_STATE_RUNNING}
	m.createdDataServers = append(m.createdDataServers, name)
	return true
}

func (m *mockNamespaceRuntime) DeleteDataServer(name string) {
	status, exists := m.dataServerStatus[name]
	if !exists || dataserver.IsRemoved(status.GetState()) {
		return
	}
	status.State = proto.DataServerState_DATA_SERVER_STATE_DRAINING
	m.drainedDataServers = append(m.drainedDataServers, name)
}

func (m *mockNamespaceRuntime) DrainedDataServer(name string) error {
	m.forgottenDataServers = append(m.forgottenDataServers, name)
	return m.forgetErrs[name]
}

func (*mockNamespaceRuntime) RefreshShardControllers() {}

func (m *mockNamespaceRuntime) CreateNamespace(name string, namespaceConfig *proto.Namespace) error {
	_, exists, err := m.metadata.GetNamespaceStatus(name)
	if err != nil {
		return err
	}
	if exists {
		return m.initShardControllers(name)
	}
	baseShardID, err := m.metadata.AllocateShardIDs(namespaceConfig.GetInitialShardCount())
	if err != nil {
		return err
	}
	namespaceStatus := &proto.NamespaceStatus{
		Shards:            map[int64]*proto.ShardMetadata{},
		ReplicationFactor: namespaceConfig.GetReplicationFactor(),
	}
	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{},
	}
	existingNamespaceStatuses, err := m.metadata.ListNamespaceStatus()
	if err != nil {
		return err
	}
	for name, existingNamespaceStatus := range existingNamespaceStatuses {
		status.Namespaces[name] = existingNamespaceStatus.UnsafeBorrow()
	}
	status.Namespaces[name] = namespaceStatus

	for _, shard := range sharding.GenerateShards(baseShardID, namespaceConfig.GetInitialShardCount()) {
		ensemble, err := m.selectNewEnsembleFn(namespaceConfig, shard.Id, status)
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

	if err := m.metadata.CreateNamespaceStatus(name, namespaceStatus); err != nil {
		return err
	}
	return m.initShardControllers(name)
}

func (m *mockNamespaceRuntime) initShardControllers(name string) error {
	namespaceStatus, exists, err := m.metadata.GetNamespaceStatus(name)
	if err != nil {
		return err
	}
	if !exists {
		return errs.ErrUnavailable
	}
	for shard, shardMetadata := range namespaceStatus.UnsafeBorrow().GetShards() {
		if shardMetadata.GetStatusOrDefault() == proto.ShardStatusDeleting {
			continue
		}
		if m.added == nil {
			m.added = map[int64]string{}
		}
		m.added[shard] = name
	}
	return nil
}

func (m *mockNamespaceRuntime) DeleteNamespace(namespace string) {
	namespaceStatus := m.metadata.DeleteNamespaceStatus(namespace).UnsafeBorrow()
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

func (m *mockNamespaceRuntime) ListLiveDataServer() map[string]commonobject.Borrowed[*proto.DataServer] {
	dataServers := map[string]commonobject.Borrowed[*proto.DataServer]{}
	for name, status := range m.dataServerStatus {
		if !dataserver.IsRemoved(status.GetState()) {
			dataServers[name] = commonobject.Borrow(&proto.DataServer{})
		}
	}
	return dataServers
}

func (*mockNamespaceRuntime) ListLiveDataServerStatus() map[string]*proto.DataServerStatus {
	return nil
}

func (m *mockNamespaceRuntime) GetLiveDataServerStatus(name string) (*proto.DataServerStatus, bool) {
	status, found := m.dataServerStatus[name]
	return status, found
}

func (m *mockNamespaceRuntime) Metadata() coordmetadata.Metadata { return m.metadata }

func namespaceRuntimeForErrors(names ...string) *mockNamespaceRuntime {
	metadata := &mockNamespaceMetadata{
		status:   proto.NewClusterStatus(),
		configNS: make(map[string]*proto.Namespace),
	}
	for _, name := range names {
		metadata.configNS[name] = &proto.Namespace{Name: name, InitialShardCount: 1, ReplicationFactor: 1}
	}
	return &mockNamespaceRuntime{
		metadata: metadata,
		selectNewEnsembleFn: func(*proto.Namespace, int64, *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
			return []*proto.DataServerIdentity{s1}, nil
		},
	}
}

type failingNamespaceRuntime struct {
	coordruntime.Runtime
	failures  map[string]error
	attempted []string
}

func (r *failingNamespaceRuntime) CreateNamespace(name string, namespace *proto.Namespace) error {
	_, exists, err := r.Metadata().GetNamespaceStatus(name)
	if err != nil {
		return err
	}
	if exists {
		return r.Runtime.CreateNamespace(name, namespace)
	}
	r.attempted = append(r.attempted, name)
	if err := r.failures[name]; err != nil {
		return err
	}
	return r.Runtime.CreateNamespace(name, namespace)
}

func TestNamespaceReconcilerReconcileReturnsStatusLoadError(t *testing.T) {
	base := namespaceRuntimeForErrors("default")
	listErr := errors.New("status unavailable")
	runtime := &failingMetadataRuntime{
		mockNamespaceRuntime: base,
		metadata: &failingMetadata{
			Metadata:            base.metadata,
			listNamespaceStatus: func() error { return listErr },
		},
	}

	err := (&namespaceReconciler{runtime: runtime}).Reconcile(context.Background(), &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{base.metadata.configNS["default"]},
	})
	require.ErrorIs(t, err, listErr)
	assert.Empty(t, base.deleted)
}
