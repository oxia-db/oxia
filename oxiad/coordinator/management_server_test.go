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
	"context"
	"testing"

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
)

func dataServer(name *string, public, internal string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{
		Name:     name,
		Public:   public,
		Internal: internal,
	}
}

func testNamespace(name string, initialShardCount uint32, replicationFactor uint32, notificationsEnabled bool, keySorting string) *proto.Namespace {
	return &proto.Namespace{
		Name:   name,
		Policy: proto.NewHierarchyPolicies(initialShardCount, replicationFactor, notificationsEnabled, keySorting),
	}
}

func ptr[T any](value T) *T {
	return &value
}

func newTestMetadata(t *testing.T, config *proto.ClusterConfiguration) coordmetadata.Metadata {
	t.Helper()

	if config == nil {
		config = &proto.ClusterConfiguration{}
	}

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   config,
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	metadataFactory := coordmetadata.NewFactoryWithProviders(
		memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled),
		configProvider,
	)
	metadata, err := metadataFactory.CreateMetadata(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, metadataFactory.Close())
	})
	return metadata
}

func TestManagementServerListDataServers(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"

	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName1, "public-1", "internal-1"),
				dataServer(&serverName2, "public-2", "internal-2"),
				dataServer(nil, "public-3", "internal-3"),
			},
			ServerMetadata: map[string]*proto.DataServerMetadata{
				serverName1:  {Labels: map[string]string{"rack": "rack-1"}},
				serverName2:  {Labels: map[string]string{"rack": "rack-2"}},
				"internal-3": {Labels: map[string]string{"rack": "rack-3"}},
			},
		}),
		nil,
	)

	res, err := management.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
	require.NoError(t, err)
	require.Len(t, res.DataServers, 3)

	require.NotNil(t, res.DataServers[0].Identity)
	require.NotNil(t, res.DataServers[0].Identity.Name)
	assert.Equal(t, serverName1, *res.DataServers[0].Identity.Name)
	assert.Equal(t, "public-1", res.DataServers[0].Identity.GetPublic())
	assert.Equal(t, "internal-1", res.DataServers[0].Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, res.DataServers[0].Metadata.GetLabels())

	require.NotNil(t, res.DataServers[1].Identity)
	require.NotNil(t, res.DataServers[1].Identity.Name)
	assert.Equal(t, serverName2, *res.DataServers[1].Identity.Name)
	assert.Equal(t, "public-2", res.DataServers[1].Identity.GetPublic())
	assert.Equal(t, "internal-2", res.DataServers[1].Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-2"}, res.DataServers[1].Metadata.GetLabels())

	require.NotNil(t, res.DataServers[2].Identity)
	require.NotNil(t, res.DataServers[2].Identity.Name)
	assert.Equal(t, "internal-3", *res.DataServers[2].Identity.Name)
	assert.Equal(t, "public-3", res.DataServers[2].Identity.GetPublic())
	assert.Equal(t, "internal-3", res.DataServers[2].Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-3"}, res.DataServers[2].Metadata.GetLabels())
}

func TestManagementServerGetDataServerByName(t *testing.T) {
	serverName := "server-2"

	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-2", "internal-2"),
			},
			ServerMetadata: map[string]*proto.DataServerMetadata{
				serverName: {Labels: map[string]string{"zone": "zone-2"}},
			},
		}),
		nil,
	)

	res, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: serverName})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServer)
	require.NotNil(t, res.DataServer.Identity)
	require.NotNil(t, res.DataServer.Identity.Name)
	assert.Equal(t, serverName, *res.DataServer.Identity.Name)
	assert.Equal(t, "public-2", res.DataServer.Identity.GetPublic())
	assert.Equal(t, "internal-2", res.DataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"zone": "zone-2"}, res.DataServer.Metadata.GetLabels())
}

func TestManagementServerGetDataServerByIdentifierFallback(t *testing.T) {
	serverName := "server-2"

	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-2", "internal-2"),
				dataServer(nil, "public-3", "internal-3"),
			},
			ServerMetadata: map[string]*proto.DataServerMetadata{
				serverName:   {Labels: map[string]string{"role": "named"}},
				"internal-3": {Labels: map[string]string{"role": "fallback"}},
			},
		}),
		nil,
	)

	res, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "internal-3"})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServer)
	require.NotNil(t, res.DataServer.Identity)
	require.NotNil(t, res.DataServer.Identity.Name)
	assert.Equal(t, "internal-3", *res.DataServer.Identity.Name)
	assert.Equal(t, "public-3", res.DataServer.Identity.GetPublic())
	assert.Equal(t, "internal-3", res.DataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"role": "fallback"}, res.DataServer.Metadata.GetLabels())

	_, err = management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "internal-2"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerGetDataServerNotFound(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(nil, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	_, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerGetDataServerRejectsEmptyLookup(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
}

func TestManagementServerGetNamespace(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:   "ns-1",
				Policy: proto.NewHierarchyPolicies(4, 3, true, proto.KeySortingType_NATURAL.String()),
			}},
		}),
		nil,
	)

	res, err := management.GetNamespace(context.Background(), &proto.GetNamespaceRequest{Namespace: "ns-1"})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Namespace)
	policy := proto.ResolveHierarchyPolicies(nil, res.Namespace)
	assert.Equal(t, "ns-1", res.Namespace.GetName())
	assert.EqualValues(t, 4, policy.GetInitialShardCount())
	assert.EqualValues(t, 3, policy.GetReplicationFactor())
	assert.Equal(t, proto.KeySortingType_NATURAL.String(), policy.GetKeySorting())
}

func TestManagementServerListNamespaces(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{
				testNamespace("ns-1", 4, 3, true, "hierarchical"),
				testNamespace("ns-2", 2, 1, true, "hierarchical"),
			},
		}),
		nil,
	)

	res, err := management.ListNamespaces(context.Background(), &proto.ListNamespacesRequest{})
	require.NoError(t, err)
	require.Len(t, res.Namespaces, 2)
	assert.Equal(t, "ns-1", res.Namespaces[0].GetName())
	assert.Equal(t, "ns-2", res.Namespaces[1].GetName())
}

func TestManagementServerGetClusterPolicy(t *testing.T) {
	policy := &proto.HierarchyPolicies{}
	policy.SetInitialShardCount(4)
	policy.SetReplicationFactor(3)
	policy.SetNotificationsEnabled(false)
	policy.SetKeySorting("natural")
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Policy: policy,
		}),
		nil,
	)

	res, err := management.GetClusterPolicy(context.Background(), &proto.GetClusterPolicyRequest{})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.EqualValues(t, 4, res.GetPolicy().GetInitialShardCount())
	assert.EqualValues(t, 3, res.GetPolicy().GetReplicationFactor())
	assert.False(t, res.GetPolicy().GetNotificationsEnabled())
	assert.Equal(t, "natural", res.GetPolicy().GetKeySorting())
}

func TestManagementServerPatchClusterPolicy(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	policy := &proto.HierarchyPolicies{}
	policy.SetInitialShardCount(2)
	policy.SetReplicationFactor(1)
	policy.SetNotificationsEnabled(false)
	policy.SetKeySorting("natural")
	res, err := management.PatchClusterPolicy(context.Background(), &proto.PatchClusterPolicyRequest{Policy: policy})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.EqualValues(t, 2, res.GetPolicy().GetInitialShardCount())
	assert.EqualValues(t, 1, res.GetPolicy().GetReplicationFactor())
	assert.False(t, res.GetPolicy().GetNotificationsEnabled())
	assert.Equal(t, "natural", res.GetPolicy().GetKeySorting())
	assert.Equal(t, "natural", management.metadata.GetConfig().UnsafeBorrow().GetPolicy().GetKeySorting())
}

func TestManagementServerCreateNamespace(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"
	serverName3 := "server-3"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName1, "public-1", "internal-1"),
				dataServer(&serverName2, "public-2", "internal-2"),
				dataServer(&serverName3, "public-3", "internal-3"),
			},
		}),
		nil,
	)

	notificationsEnabled := false
	res, err := management.CreateNamespace(context.Background(), &proto.CreateNamespaceRequest{
		Namespace: testNamespace("ns-1", 4, 3, notificationsEnabled, "natural"),
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Namespace)
	policy := proto.ResolveHierarchyPolicies(nil, res.Namespace)
	assert.Equal(t, "ns-1", res.Namespace.GetName())
	assert.EqualValues(t, 4, policy.GetInitialShardCount())
	assert.EqualValues(t, 3, policy.GetReplicationFactor())
	assert.False(t, policy.GetNotificationsEnabled())
	assert.Equal(t, "natural", policy.GetKeySorting())

	namespace, found := management.metadata.GetNamespace("ns-1", false)
	require.True(t, found)
	assert.Equal(t, "ns-1", namespace.UnsafeBorrow().GetName())
}

func TestManagementServerCreateNamespaceRejectsInvalidRequest(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	zero := uint32(0)
	emptyKeySorting := ""
	testCases := []struct {
		name string
		req  *proto.CreateNamespaceRequest
		code codes.Code
	}{
		{name: "nil request", req: nil, code: codes.InvalidArgument},
		{name: "nil namespace", req: &proto.CreateNamespaceRequest{}, code: codes.InvalidArgument},
		{name: "empty name", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{}}, code: codes.InvalidArgument},
		{name: "invalid name", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:   "../ns",
			Policy: proto.NewHierarchyPolicies(1, 1, true, "hierarchical"),
		}}, code: codes.InvalidArgument},
		{name: "zero policy initial shard count", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name: "ns-1",
			Policy: &proto.HierarchyPolicies{
				InitialShardCount: &zero,
			},
		}}, code: codes.InvalidArgument},
		{name: "zero policy replication factor", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name: "ns-1",
			Policy: &proto.HierarchyPolicies{
				ReplicationFactor: &zero,
			},
		}}, code: codes.InvalidArgument},
		{name: "invalid key sorting", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:   "ns-1",
			Policy: proto.NewHierarchyPolicies(1, 1, true, "invalid"),
		}}, code: codes.InvalidArgument},
		{name: "empty policy key sorting", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name: "ns-1",
			Policy: &proto.HierarchyPolicies{
				KeySorting: &emptyKeySorting,
			},
		}}, code: codes.InvalidArgument},
		{name: "unknown key sorting", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:   "ns-1",
			Policy: proto.NewHierarchyPolicies(1, 1, true, proto.KeySortingType_UNKNOWN.String()),
		}}, code: codes.InvalidArgument},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := management.CreateNamespace(context.Background(), tt.req)
			require.Error(t, err)
			assert.Equal(t, tt.code, grpcstatus.Code(err))
		})
	}
}

func TestManagementServerCreateNamespacePreservesKeySorting(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	res, err := management.CreateNamespace(context.Background(), &proto.CreateNamespaceRequest{
		Namespace: testNamespace("ns-1", 1, 1, true, "NATURAL"),
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, "NATURAL", proto.ResolveHierarchyPolicies(nil, res.Namespace).GetKeySorting())
}

func TestManagementServerCreateNamespaceAlreadyExists(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:   "ns-1",
				Policy: proto.NewHierarchyPolicies(1, 1, true, "hierarchical"),
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	_, err := management.CreateNamespace(context.Background(), &proto.CreateNamespaceRequest{
		Namespace: testNamespace("ns-1", 1, 1, true, "natural"),
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, grpcstatus.Code(err))
}

func TestManagementServerCreateNamespaceFailedPrecondition(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	_, err := management.CreateNamespace(context.Background(), &proto.CreateNamespaceRequest{
		Namespace: testNamespace("ns-1", 1, 2, true, "natural"),
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, grpcstatus.Code(err))
}

func TestManagementServerPatchNamespace(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:   "ns-1",
				Policy: proto.NewHierarchyPolicies(1, 1, true, "hierarchical"),
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName1, "public-1", "internal-1"),
				dataServer(&serverName2, "public-2", "internal-2"),
			},
		}),
		nil,
	)

	notificationsEnabled := false
	patchPolicy := &proto.HierarchyPolicies{}
	patchPolicy.SetReplicationFactor(2)
	patchPolicy.SetNotificationsEnabled(notificationsEnabled)
	res, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:   "ns-1",
			Policy: patchPolicy,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Namespace)
	policy := proto.ResolveHierarchyPolicies(nil, res.Namespace)
	assert.Equal(t, "ns-1", res.Namespace.GetName())
	assert.EqualValues(t, 1, policy.GetInitialShardCount())
	assert.EqualValues(t, 2, policy.GetReplicationFactor())
	assert.False(t, policy.GetNotificationsEnabled())
	assert.Equal(t, "hierarchical", policy.GetKeySorting())

	namespace, found := management.metadata.GetNamespace("ns-1", false)
	require.True(t, found)
	storedPolicy := proto.ResolveHierarchyPolicies(nil, namespace.UnsafeBorrow())
	assert.EqualValues(t, 1, storedPolicy.GetInitialShardCount())
	assert.EqualValues(t, 2, storedPolicy.GetReplicationFactor())
	assert.False(t, storedPolicy.GetNotificationsEnabled())
	assert.Equal(t, "hierarchical", storedPolicy.GetKeySorting())
}

func TestManagementServerPatchNamespacePreservesUnspecifiedFields(t *testing.T) {
	serverName := "server-1"
	notificationsEnabled := true
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:   "ns-1",
				Policy: proto.NewHierarchyPolicies(4, 1, notificationsEnabled, "hierarchical"),
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	patchPolicy := &proto.HierarchyPolicies{}
	patchPolicy.SetNotificationsEnabled(notificationsEnabled)
	res, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:   "ns-1",
			Policy: patchPolicy,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	policy := proto.ResolveHierarchyPolicies(nil, res.Namespace)
	assert.EqualValues(t, 4, policy.GetInitialShardCount())
	assert.EqualValues(t, 1, policy.GetReplicationFactor())
	assert.True(t, policy.GetNotificationsEnabled())
	assert.Equal(t, "hierarchical", policy.GetKeySorting())
}

func TestManagementServerPatchNamespaceRejectsInvalidRequest(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	testCases := []struct {
		name string
		req  *proto.PatchNamespaceRequest
	}{
		{name: "nil request", req: nil},
		{name: "nil namespace", req: &proto.PatchNamespaceRequest{}},
		{name: "empty name", req: &proto.PatchNamespaceRequest{Namespace: &proto.Namespace{}}},
		{name: "invalid name", req: &proto.PatchNamespaceRequest{Namespace: &proto.Namespace{
			Name:   "../ns",
			Policy: proto.NewHierarchyPolicies(1, 1, true, "hierarchical"),
		}}},
		{name: "initial shard count", req: &proto.PatchNamespaceRequest{Namespace: &proto.Namespace{
			Name:   "ns-1",
			Policy: proto.NewHierarchyPolicies(1, 1, true, "hierarchical"),
		}}},
		{name: "key sorting", req: &proto.PatchNamespaceRequest{Namespace: &proto.Namespace{
			Name:   "ns-1",
			Policy: proto.NewHierarchyPolicies(0, 0, true, "invalid"),
		}}},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := management.PatchNamespace(context.Background(), tt.req)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
		})
	}
}

func TestManagementServerPatchNamespaceNotFound(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:   "missing",
			Policy: &proto.HierarchyPolicies{ReplicationFactor: ptr(uint32(1))},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerPatchNamespaceFailedPrecondition(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:   "ns-1",
				Policy: proto.NewHierarchyPolicies(1, 1, true, "natural"),
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	_, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:   "ns-1",
			Policy: &proto.HierarchyPolicies{ReplicationFactor: ptr(uint32(2))},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, grpcstatus.Code(err))
}

func TestManagementServerDeleteNamespace(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{
				testNamespace("ns-1", 1, 1, true, "natural"),
				testNamespace("ns-2", 2, 1, true, "hierarchical"),
			},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	res, err := management.DeleteNamespace(context.Background(), &proto.DeleteNamespaceRequest{Namespace: "ns-1"})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Namespace)
	policy := proto.ResolveHierarchyPolicies(nil, res.Namespace)
	assert.Equal(t, "ns-1", res.Namespace.GetName())
	assert.EqualValues(t, 1, policy.GetInitialShardCount())
	assert.Equal(t, "natural", policy.GetKeySorting())

	_, found := management.metadata.GetNamespace("ns-1", false)
	assert.False(t, found)
	namespace, found := management.metadata.GetNamespace("ns-2", false)
	require.True(t, found)
	assert.Equal(t, "ns-2", namespace.UnsafeBorrow().GetName())
}

func TestManagementServerDeleteNamespaceRejectsInvalidRequest(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	testCases := []struct {
		name string
		req  *proto.DeleteNamespaceRequest
	}{
		{name: "nil request", req: nil},
		{name: "empty name", req: &proto.DeleteNamespaceRequest{}},
		{name: "invalid name", req: &proto.DeleteNamespaceRequest{Namespace: "../ns"}},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := management.DeleteNamespace(context.Background(), tt.req)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
		})
	}
}

func TestManagementServerDeleteNamespaceNotFound(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.DeleteNamespace(context.Background(), &proto.DeleteNamespaceRequest{Namespace: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerGetNamespaceRejectsEmptyLookup(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.GetNamespace(context.Background(), &proto.GetNamespaceRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
}

func TestManagementServerGetNamespaceNotFound(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.GetNamespace(context.Background(), &proto.GetNamespaceRequest{Namespace: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerCreateDataServer(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	serverName := "server-1"
	res, err := management.CreateDataServer(context.Background(), &proto.CreateDataServerRequest{
		DataServer: &proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName,
				Public:   "public-1",
				Internal: "internal-1",
			},
			Metadata: &proto.DataServerMetadata{
				Labels: map[string]string{"rack": "rack-1"},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServer)
	require.NotNil(t, res.DataServer.Identity)
	require.NotNil(t, res.DataServer.Identity.Name)
	assert.Equal(t, serverName, *res.DataServer.Identity.Name)
	assert.Equal(t, "public-1", res.DataServer.Identity.GetPublic())
	assert.Equal(t, "internal-1", res.DataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, res.DataServer.Metadata.GetLabels())

	created, found := management.metadata.GetDataServer(serverName)
	require.True(t, found)
	assert.Equal(t, "public-1", created.UnsafeBorrow().Identity.GetPublic())
}

func TestManagementServerCreateDataServerRejectsInvalidRequest(t *testing.T) {
	management := newManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}), nil)
	serverName := "server-1"

	testCases := []struct {
		name string
		req  *proto.CreateDataServerRequest
	}{
		{name: "nil request", req: nil},
		{name: "nil dataserver", req: &proto.CreateDataServerRequest{}},
		{name: "nil identity", req: &proto.CreateDataServerRequest{DataServer: &proto.DataServer{}}},
		{name: "empty name", req: &proto.CreateDataServerRequest{DataServer: &proto.DataServer{Identity: &proto.DataServerIdentity{Public: "public", Internal: "internal"}}}},
		{name: "empty public", req: &proto.CreateDataServerRequest{DataServer: &proto.DataServer{Identity: &proto.DataServerIdentity{Name: &serverName, Internal: "internal"}}}},
		{name: "empty internal", req: &proto.CreateDataServerRequest{DataServer: &proto.DataServer{Identity: &proto.DataServerIdentity{Name: &serverName, Public: "public"}}}},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := management.CreateDataServer(context.Background(), tt.req)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
		})
	}
}

func TestManagementServerCreateDataServerAlreadyExists(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	_, err := management.CreateDataServer(context.Background(), &proto.CreateDataServerRequest{
		DataServer: &proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:     &serverName,
				Public:   "public-1",
				Internal: "internal-1",
			},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, grpcstatus.Code(err))
}

func TestManagementServerPatchDataServer(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
			ServerMetadata: map[string]*proto.DataServerMetadata{
				serverName: {Labels: map[string]string{"rack": "rack-1"}},
			},
		}),
		nil,
	)

	res, err := management.PatchDataServer(context.Background(), &proto.PatchDataServerRequest{
		DataServer: &proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:   &serverName,
				Public: "public-2",
			},
			Metadata: &proto.DataServerMetadata{
				Labels: map[string]string{"rack": "rack-2"},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServer)
	require.NotNil(t, res.DataServer.Identity)
	require.NotNil(t, res.DataServer.Identity.Name)
	assert.Equal(t, serverName, *res.DataServer.Identity.Name)
	assert.Equal(t, "public-2", res.DataServer.Identity.GetPublic())
	assert.Equal(t, "internal-1", res.DataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-2"}, res.DataServer.Metadata.GetLabels())

	patched, found := management.metadata.GetDataServer(serverName)
	require.True(t, found)
	assert.Equal(t, "public-2", patched.UnsafeBorrow().Identity.GetPublic())
	assert.Equal(t, "internal-1", patched.UnsafeBorrow().Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-2"}, patched.UnsafeBorrow().Metadata.GetLabels())
}

func TestManagementServerPatchDataServerRejectsInvalidRequest(t *testing.T) {
	management := newManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}), nil)

	testCases := []struct {
		name string
		req  *proto.PatchDataServerRequest
	}{
		{name: "nil request", req: nil},
		{name: "nil dataserver", req: &proto.PatchDataServerRequest{}},
		{name: "nil identity", req: &proto.PatchDataServerRequest{DataServer: &proto.DataServer{}}},
		{name: "empty name", req: &proto.PatchDataServerRequest{DataServer: &proto.DataServer{Identity: &proto.DataServerIdentity{Public: "public"}}}},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := management.PatchDataServer(context.Background(), tt.req)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
		})
	}
}

func TestManagementServerPatchDataServerNotFound(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.PatchDataServer(context.Background(), &proto.PatchDataServerRequest{
		DataServer: &proto.DataServer{
			Identity: &proto.DataServerIdentity{
				Name:   &serverName,
				Public: "public-2",
			},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerDeleteDataServer(t *testing.T) {
	serverName := "server-1"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
			ServerMetadata: map[string]*proto.DataServerMetadata{
				serverName: {Labels: map[string]string{"rack": "rack-1"}},
			},
		}),
		nil,
	)

	res, err := management.DeleteDataServer(context.Background(), &proto.DeleteDataServerRequest{DataServer: serverName})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.DataServer)
	require.NotNil(t, res.DataServer.Identity)
	require.NotNil(t, res.DataServer.Identity.Name)
	assert.Equal(t, serverName, *res.DataServer.Identity.Name)
	assert.Equal(t, "public-1", res.DataServer.Identity.GetPublic())
	assert.Equal(t, "internal-1", res.DataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, res.DataServer.Metadata.GetLabels())

	_, found := management.metadata.GetDataServer(serverName)
	assert.False(t, found)
}

func TestManagementServerDeleteDataServerRejectsInvalidRequest(t *testing.T) {
	management := newManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}), nil)

	testCases := []struct {
		name string
		req  *proto.DeleteDataServerRequest
	}{
		{name: "nil request", req: nil},
		{name: "empty name", req: &proto.DeleteDataServerRequest{}},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := management.DeleteDataServer(context.Background(), tt.req)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
		})
	}
}

func TestManagementServerDeleteDataServerNotFound(t *testing.T) {
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.DeleteDataServer(context.Background(), &proto.DeleteDataServerRequest{DataServer: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerDeleteDataServerFailedPrecondition(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"
	management := newManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:   "default",
				Policy: proto.NewHierarchyPolicies(1, 2, true, "hierarchical"),
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName1, "public-1", "internal-1"),
				dataServer(&serverName2, "public-2", "internal-2"),
			},
		}),
		nil,
	)

	_, err := management.DeleteDataServer(context.Background(), &proto.DeleteDataServerRequest{DataServer: serverName1})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, grpcstatus.Code(err))

	_, found := management.metadata.GetDataServer(serverName1)
	assert.True(t, found)
}
