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

package coordinator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer"
)

func dataServer(name *string, public, internal string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{
		Name:     name,
		Public:   public,
		Internal: internal,
	}
}

type testLeaderMetadata struct {
	coordmetadata.Metadata
	info *proto.Coordinator
	self *proto.Coordinator
	err  error
}

func (p *testLeaderMetadata) GetSelf() (*proto.Coordinator, error) {
	if p.self == nil {
		return nil, nil //nolint:nilnil
	}
	return p.self.CloneVT(), nil
}

func (p *testLeaderMetadata) GetLeaderInfo() (*proto.Coordinator, error) {
	return p.info, p.err
}

var _ coordruntime.Runtime = (*testRuntime)(nil)

type testRuntime struct {
	metadata coordmetadata.Metadata
}

func (*testRuntime) Close() error { return nil }

func (*testRuntime) InitiateSplit(string, int64, *uint32) (leftShardID int64, rightShardID int64, err error) {
	return 0, 0, errors.New("unexpected split shard request")
}

func (*testRuntime) LeaderElected(int64, *proto.DataServerIdentity, []*proto.DataServerIdentity) {}

func (*testRuntime) ShardDeleted(int64) {}

func (*testRuntime) WaitForNextUpdate(context.Context, *proto.ShardAssignments) (*proto.ShardAssignments, error) {
	return nil, context.Canceled
}

func (*testRuntime) BecameUnavailable(*proto.DataServerIdentity) {}

func (*testRuntime) CreateDataServer(string, *proto.DataServer) bool { return false }

func (*testRuntime) DeleteDataServer(string) {}

func (*testRuntime) ListDataServer() map[string]commonobject.Borrowed[*proto.DataServer] { return nil }

func (*testRuntime) ListDataServerStatus() map[string]*proto.DataServerStatus { return nil }

func (*testRuntime) GetDataServerStatus(string) (*proto.DataServerStatus, bool) { return nil, false }

func (*testRuntime) CreateNamespace(string, *proto.Namespace) bool { return false }

func (*testRuntime) DeleteNamespace(string) {}

func (*testRuntime) LoadBalancer() balancer.LoadBalancer { return nil }

func (r *testRuntime) Metadata() coordmetadata.Metadata { return r.metadata }

func (*testRuntime) RecomputeAssignments() {}

func (*testRuntime) SyncShardControllerServerAddresses() {}

func newTestMetadata(t *testing.T, config *proto.ClusterConfiguration) coordmetadata.Metadata {
	t.Helper()

	if config == nil {
		config = &proto.ClusterConfiguration{}
	}

	dir := t.TempDir()
	data, err := metadatacodec.ClusterConfigCodec.MarshalYAML(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, coordoption.DefaultFileConfigName), data, 0o600))
	metadataFactory, err := coordmetadata.New(t.Context(), &coordoption.Options{
		Metadata: coordoption.MetadataOptions{
			ProviderOptions: coordoption.ProviderOptions{
				ProviderName: metadatacommon.NameFile,
				File: coordoption.FileMetadata{
					Dir: dir,
				},
			},
		},
	})
	require.NoError(t, err)
	metadata, err := metadataFactory.CreateMetadata(t.Context())
	require.NoError(t, err)
	_, err = metadata.WaitToBecomeLeader()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, metadataFactory.Close())
	})
	return metadata
}

func newReadyManagementServer(
	metadata coordmetadata.Metadata,
	runtime coordruntime.Runtime,
) *managementServer {
	if runtime == nil {
		runtime = &testRuntime{metadata: metadata}
	}
	management := newManagementServer(metadata)
	management.setRuntime(runtime)
	return management
}

func TestManagementServerRedirectsRequestsToCoordinatorLeader(t *testing.T) {
	management := newManagementServer(&testLeaderMetadata{
		info: &proto.Coordinator{
			Name:          "coordinator-1",
			PublicAddress: "coordinator-1.example.com:6651",
		},
		self: &proto.Coordinator{
			Name:          "coordinator-0",
			PublicAddress: "coordinator-0.example.com:6651",
		},
	})

	testCases := []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "list data servers",
			call: func(ctx context.Context) error {
				_, err := management.ListDataServers(ctx, &proto.ListDataServersRequest{})
				return err
			},
		},
		{
			name: "get data server",
			call: func(ctx context.Context) error {
				_, err := management.GetDataServer(ctx, &proto.GetDataServerRequest{DataServer: "server-1"})
				return err
			},
		},
		{
			name: "create data server",
			call: func(ctx context.Context) error {
				_, err := management.CreateDataServer(ctx, &proto.CreateDataServerRequest{})
				return err
			},
		},
		{
			name: "patch data server",
			call: func(ctx context.Context) error {
				_, err := management.PatchDataServer(ctx, &proto.PatchDataServerRequest{})
				return err
			},
		},
		{
			name: "delete data server",
			call: func(ctx context.Context) error {
				_, err := management.DeleteDataServer(ctx, &proto.DeleteDataServerRequest{DataServer: "server-1"})
				return err
			},
		},
		{
			name: "list namespaces",
			call: func(ctx context.Context) error {
				_, err := management.ListNamespaces(ctx, &proto.ListNamespacesRequest{})
				return err
			},
		},
		{
			name: "create namespace",
			call: func(ctx context.Context) error {
				_, err := management.CreateNamespace(ctx, &proto.CreateNamespaceRequest{})
				return err
			},
		},
		{
			name: "patch namespace",
			call: func(ctx context.Context) error {
				_, err := management.PatchNamespace(ctx, &proto.PatchNamespaceRequest{})
				return err
			},
		},
		{
			name: "delete namespace",
			call: func(ctx context.Context) error {
				_, err := management.DeleteNamespace(ctx, &proto.DeleteNamespaceRequest{Namespace: "ns-1"})
				return err
			},
		},
		{
			name: "get namespace",
			call: func(ctx context.Context) error {
				_, err := management.GetNamespace(ctx, &proto.GetNamespaceRequest{Namespace: "ns-1"})
				return err
			},
		},
		{
			name: "split shard",
			call: func(ctx context.Context) error {
				_, err := management.SplitShard(ctx, &proto.SplitShardRequest{Namespace: "ns-1", Shard: 1})
				return err
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call(t.Context())
			require.Error(t, err)

			oxiaErr, metadata := constant.FromGrpcError(err)
			leader, ok := metadata.GetCoordinatorLeaderHint()
			assert.ErrorIs(t, oxiaErr, constant.ErrNodeIsNotLeader)
			assert.True(t, ok)
			assert.Equal(t, "coordinator-1.example.com:6651", leader)
		})
	}
}

func TestManagementServerReturnsUnavailableWithoutLeader(t *testing.T) {
	management := newManagementServer(&testLeaderMetadata{
		err: errors.New("lease not found"),
	})

	_, err := management.ListNamespaces(t.Context(), &proto.ListNamespacesRequest{})
	require.Error(t, err)

	oxiaErr, metadata := constant.FromGrpcError(err)
	_, ok := metadata.GetCoordinatorLeaderHint()
	assert.ErrorIs(t, oxiaErr, constant.ErrNotInitialized)
	assert.False(t, ok)
}

func TestManagementServerReturnsUnavailableWhenLocalLeaderIsNotReady(t *testing.T) {
	management := newManagementServer(&testLeaderMetadata{
		info: &proto.Coordinator{
			Name:          "coordinator-0",
			PublicAddress: "coordinator-0.example.com:6651",
		},
		self: &proto.Coordinator{
			Name:          "coordinator-0",
			PublicAddress: "coordinator-0.example.com:6651",
		},
	})

	_, err := management.ListNamespaces(t.Context(), &proto.ListNamespacesRequest{})
	require.Error(t, err)

	oxiaErr, metadata := constant.FromGrpcError(err)
	_, ok := metadata.GetCoordinatorLeaderHint()
	assert.ErrorIs(t, oxiaErr, constant.ErrNotInitialized)
	assert.False(t, ok)
}

func TestManagementServerServesAfterShardSplitterReady(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: "ns-1"}},
	})
	management := newManagementServer(metadata)
	management.setRuntime(&testRuntime{metadata: metadata})

	res, err := management.ListNamespaces(t.Context(), &proto.ListNamespacesRequest{})
	require.NoError(t, err)
	require.Len(t, res.Namespaces, 1)
	assert.Equal(t, "ns-1", res.Namespaces[0].GetNamespace().GetName())
}

func TestManagementServerReturnsUnavailableBeforeRuntimeReady(t *testing.T) {
	management := newManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}))

	_, err := management.SplitShard(t.Context(), &proto.SplitShardRequest{})
	require.Error(t, err)

	oxiaErr, _ := constant.FromGrpcError(err)
	assert.ErrorIs(t, oxiaErr, constant.ErrNotInitialized)
}

func TestManagementServerListDataServers(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"

	management := newReadyManagementServer(
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

	ds := res.DataServers[0].GetDataServer()
	require.NotNil(t, ds.GetIdentity())
	require.NotNil(t, ds.GetIdentity().Name)
	assert.Equal(t, serverName1, ds.GetIdentity().GetName())
	assert.Equal(t, "public-1", ds.GetIdentity().GetPublic())
	assert.Equal(t, "internal-1", ds.GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, ds.GetMetadata().GetLabels())

	ds = res.DataServers[1].GetDataServer()
	require.NotNil(t, ds.GetIdentity())
	require.NotNil(t, ds.GetIdentity().Name)
	assert.Equal(t, serverName2, ds.GetIdentity().GetName())
	assert.Equal(t, "public-2", ds.GetIdentity().GetPublic())
	assert.Equal(t, "internal-2", ds.GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-2"}, ds.GetMetadata().GetLabels())

	ds = res.DataServers[2].GetDataServer()
	require.NotNil(t, ds.GetIdentity())
	require.NotNil(t, ds.GetIdentity().Name)
	assert.Equal(t, "internal-3", ds.GetIdentity().GetName())
	assert.Equal(t, "public-3", ds.GetIdentity().GetPublic())
	assert.Equal(t, "internal-3", ds.GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-3"}, ds.GetMetadata().GetLabels())
}

func TestManagementServerGetDataServerByName(t *testing.T) {
	serverName := "server-2"

	management := newReadyManagementServer(
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
	ds := res.DataServer.GetDataServer()
	require.NotNil(t, ds.GetIdentity())
	require.NotNil(t, ds.GetIdentity().Name)
	assert.Equal(t, serverName, ds.GetIdentity().GetName())
	assert.Equal(t, "public-2", ds.GetIdentity().GetPublic())
	assert.Equal(t, "internal-2", ds.GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"zone": "zone-2"}, ds.GetMetadata().GetLabels())
}

func TestManagementServerGetDataServerByIdentifierFallback(t *testing.T) {
	serverName := "server-2"

	management := newReadyManagementServer(
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
	ds := res.DataServer.GetDataServer()
	require.NotNil(t, ds.GetIdentity())
	require.NotNil(t, ds.GetIdentity().Name)
	assert.Equal(t, "internal-3", ds.GetIdentity().GetName())
	assert.Equal(t, "public-3", ds.GetIdentity().GetPublic())
	assert.Equal(t, "internal-3", ds.GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"role": "fallback"}, ds.GetMetadata().GetLabels())

	_, err = management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "internal-2"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerGetDataServerNotFound(t *testing.T) {
	management := newReadyManagementServer(
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
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
}

func TestManagementServerGetNamespace(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: 4,
			ReplicationFactor: 3,
			KeySorting:        proto.KeySortingType_NATURAL.String(),
		}},
	})
	require.True(t, metadata.CreateNamespaceStatus("ns-1", &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards: map[int64]*proto.ShardMetadata{
			0: {Status: proto.ShardStatusSteadyState},
			1: {Status: proto.ShardStatusElection},
		},
	}))
	management := newReadyManagementServer(metadata, nil)

	res, err := management.GetNamespace(context.Background(), &proto.GetNamespaceRequest{Namespace: "ns-1"})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Namespace)
	assert.Equal(t, "ns-1", res.Namespace.GetNamespace().GetName())
	assert.EqualValues(t, 4, res.Namespace.GetNamespace().GetInitialShardCount())
	assert.EqualValues(t, 3, res.Namespace.GetNamespace().GetReplicationFactor())
	assert.Equal(t, proto.KeySortingType_NATURAL.String(), res.Namespace.GetNamespace().GetKeySorting())
	assert.Len(t, res.Namespace.GetNamespaceStatus().GetShards(), 2)
}

func TestManagementServerListNamespaces(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{
			{
				Name:              "ns-1",
				InitialShardCount: 4,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-2",
				InitialShardCount: 2,
				ReplicationFactor: 1,
			},
		},
	})
	require.True(t, metadata.CreateNamespaceStatus("ns-1", &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards: map[int64]*proto.ShardMetadata{
			0: {Status: proto.ShardStatusSteadyState},
		},
	}))
	require.True(t, metadata.CreateNamespaceStatus("ns-2", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			1: {Status: proto.ShardStatusElection},
			2: {Status: proto.ShardStatusElection},
		},
	}))
	management := newReadyManagementServer(metadata, nil)

	res, err := management.ListNamespaces(context.Background(), &proto.ListNamespacesRequest{})
	require.NoError(t, err)
	require.Len(t, res.Namespaces, 2)
	viewsByName := map[string]*proto.NamespaceView{}
	for _, view := range res.Namespaces {
		viewsByName[view.GetNamespace().GetName()] = view
	}
	assert.ElementsMatch(t, []string{"ns-1", "ns-2"}, []string{res.Namespaces[0].GetNamespace().GetName(), res.Namespaces[1].GetNamespace().GetName()})
	assert.Len(t, viewsByName["ns-1"].GetNamespaceStatus().GetShards(), 1)
	assert.Len(t, viewsByName["ns-2"].GetNamespaceStatus().GetShards(), 2)
}

func TestManagementServerListShards(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: 2,
			ReplicationFactor: 1,
		}},
	})
	require.True(t, metadata.CreateNamespaceStatus("ns-1", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			2: {
				Status: proto.ShardStatusElection,
				Term:   4,
			},
			1: {
				Status: proto.ShardStatusSteadyState,
				Term:   3,
			},
		},
	}))
	management := newReadyManagementServer(metadata, nil)

	res, err := management.ListShards(context.Background(), &proto.ListShardsRequest{Namespace: "ns-1"})
	require.NoError(t, err)
	require.Len(t, res.Shards, 2)
	assert.EqualValues(t, 1, res.Shards[0].GetShard())
	assert.Equal(t, proto.ShardStatusSteadyState, res.Shards[0].GetShardStatus().GetStatus())
	assert.EqualValues(t, 2, res.Shards[1].GetShard())
	assert.Equal(t, proto.ShardStatusElection, res.Shards[1].GetShardStatus().GetStatus())
}

func TestManagementServerGetShard(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: 2,
			ReplicationFactor: 1,
		}},
	})
	require.True(t, metadata.CreateNamespaceStatus("ns-1", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			1: {
				Status: proto.ShardStatusSteadyState,
				Term:   3,
				Leader: dataServer(nil, "public-1", "internal-1"),
				Ensemble: []*proto.DataServerIdentity{
					dataServer(nil, "public-1", "internal-1"),
				},
				Int32HashRange: &proto.HashRange{Min: 1, Max: 10},
			},
		},
	}))
	management := newReadyManagementServer(metadata, nil)

	res, err := management.GetShard(context.Background(), &proto.GetShardRequest{Namespace: "ns-1", Shard: 1})
	require.NoError(t, err)
	require.NotNil(t, res.Shard)
	assert.Equal(t, "ns-1", res.Shard.GetNamespace())
	assert.EqualValues(t, 1, res.Shard.GetShard())
	assert.Equal(t, proto.ShardStatusSteadyState, res.Shard.GetShardStatus().GetStatus())
	assert.EqualValues(t, 3, res.Shard.GetShardStatus().GetTerm())
	assert.Equal(t, "internal-1", res.Shard.GetShardStatus().GetLeader().GetNameOrDefault())
	assert.EqualValues(t, 1, res.Shard.GetShardStatus().GetInt32HashRange().GetMin())
	assert.EqualValues(t, 10, res.Shard.GetShardStatus().GetInt32HashRange().GetMax())
}

func TestManagementServerGetShardNotFound(t *testing.T) {
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:              "ns-1",
				InitialShardCount: 1,
				ReplicationFactor: 1,
			}},
		}),
		nil,
	)

	_, err := management.GetShard(context.Background(), &proto.GetShardRequest{Namespace: "ns-1", Shard: 1})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerCreateNamespace(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"
	serverName3 := "server-3"
	management := newReadyManagementServer(
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
		Namespace: &proto.Namespace{
			Name:                 "ns-1",
			InitialShardCount:    4,
			ReplicationFactor:    3,
			NotificationsEnabled: &notificationsEnabled,
			KeySorting:           "natural",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Namespace)
	assert.Equal(t, "ns-1", res.Namespace.GetName())
	assert.EqualValues(t, 4, res.Namespace.GetInitialShardCount())
	assert.EqualValues(t, 3, res.Namespace.GetReplicationFactor())
	assert.False(t, res.Namespace.NotificationsEnabledOrDefault())
	assert.Equal(t, "natural", res.Namespace.GetKeySorting())

	namespace, found := management.metadata.GetNamespace("ns-1")
	require.True(t, found)
	assert.Equal(t, "ns-1", namespace.UnsafeBorrow().GetName())
}

func TestManagementServerCreateNamespaceRejectsInvalidRequest(t *testing.T) {
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	testCases := []struct {
		name string
		req  *proto.CreateNamespaceRequest
	}{
		{name: "nil request", req: nil},
		{name: "nil namespace", req: &proto.CreateNamespaceRequest{}},
		{name: "empty name", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{}}},
		{name: "invalid name", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:              "../ns",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}}},
		{name: "empty initial shard count", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:              "ns-1",
			ReplicationFactor: 1,
		}}},
		{name: "empty replication factor", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 1,
		}}},
		{name: "invalid key sorting", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 1,
			KeySorting:        "invalid",
		}}},
		{name: "empty key sorting", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}}},
		{name: "unknown key sorting", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 1,
			KeySorting:        proto.KeySortingType_UNKNOWN.String(),
		}}},
		{name: "anti affinities", req: &proto.CreateNamespaceRequest{Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 1,
			KeySorting:        "hierarchical",
			AntiAffinities: []*proto.AntiAffinity{{
				Labels: []string{"zone"},
				Mode:   proto.AntiAffinityModeStrict,
			}},
		}}},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := management.CreateNamespace(context.Background(), tt.req)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
		})
	}
}

func TestManagementServerCreateNamespacePreservesKeySorting(t *testing.T) {
	serverName := "server-1"
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	res, err := management.CreateNamespace(context.Background(), &proto.CreateNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 1,
			KeySorting:        "NATURAL",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, "NATURAL", res.Namespace.GetKeySorting())
}

func TestManagementServerCreateNamespaceAlreadyExists(t *testing.T) {
	serverName := "server-1"
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:              "ns-1",
				InitialShardCount: 1,
				ReplicationFactor: 1,
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	_, err := management.CreateNamespace(context.Background(), &proto.CreateNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 1,
			KeySorting:        "natural",
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, grpcstatus.Code(err))
}

func TestManagementServerCreateNamespaceFailedPrecondition(t *testing.T) {
	serverName := "server-1"
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	_, err := management.CreateNamespace(context.Background(), &proto.CreateNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 2,
			KeySorting:        "natural",
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, grpcstatus.Code(err))
}

func TestManagementServerPatchNamespace(t *testing.T) {
	serverName1 := "server-1"
	serverName2 := "server-2"
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:              "ns-1",
				InitialShardCount: 1,
				ReplicationFactor: 1,
				KeySorting:        "hierarchical",
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName1, "public-1", "internal-1"),
				dataServer(&serverName2, "public-2", "internal-2"),
			},
		}),
		nil,
	)

	notificationsEnabled := false
	res, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:                 "ns-1",
			ReplicationFactor:    2,
			NotificationsEnabled: &notificationsEnabled,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Namespace)
	assert.Equal(t, "ns-1", res.Namespace.GetName())
	assert.EqualValues(t, 1, res.Namespace.GetInitialShardCount())
	assert.EqualValues(t, 2, res.Namespace.GetReplicationFactor())
	assert.False(t, res.Namespace.NotificationsEnabledOrDefault())
	assert.Equal(t, "hierarchical", res.Namespace.GetKeySorting())

	namespace, found := management.metadata.GetNamespace("ns-1")
	require.True(t, found)
	assert.EqualValues(t, 1, namespace.UnsafeBorrow().GetInitialShardCount())
	assert.EqualValues(t, 2, namespace.UnsafeBorrow().GetReplicationFactor())
	assert.False(t, namespace.UnsafeBorrow().NotificationsEnabledOrDefault())
	assert.Equal(t, "hierarchical", namespace.UnsafeBorrow().GetKeySorting())
}

func TestManagementServerPatchNamespaceRejectsAntiAffinities(t *testing.T) {
	serverName := "server-1"
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:              "ns-1",
				InitialShardCount: 1,
				ReplicationFactor: 1,
				KeySorting:        "hierarchical",
				AntiAffinities: []*proto.AntiAffinity{{
					Labels: []string{"rack"},
					Mode:   proto.AntiAffinityModeStrict,
				}},
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	res, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{
			Name: "ns-1",
			AntiAffinities: []*proto.AntiAffinity{{
				Labels: []string{"zone"},
				Mode:   proto.AntiAffinityModeRelaxed,
			}},
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
	assert.Nil(t, res)

	namespace, found := management.metadata.GetNamespace("ns-1")
	require.True(t, found)
	require.Len(t, namespace.UnsafeBorrow().GetAntiAffinities(), 1)
	assert.Equal(t, []string{"rack"}, namespace.UnsafeBorrow().GetAntiAffinities()[0].GetLabels())
	assert.Equal(t, proto.AntiAffinityModeStrict, namespace.UnsafeBorrow().GetAntiAffinities()[0].GetMode())
}

func TestManagementServerPatchNamespacePreservesUnspecifiedFields(t *testing.T) {
	serverName := "server-1"
	notificationsEnabled := true
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:                 "ns-1",
				InitialShardCount:    4,
				ReplicationFactor:    1,
				NotificationsEnabled: &notificationsEnabled,
				KeySorting:           "hierarchical",
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	res, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:                 "ns-1",
			NotificationsEnabled: &notificationsEnabled,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.EqualValues(t, 4, res.Namespace.GetInitialShardCount())
	assert.EqualValues(t, 1, res.Namespace.GetReplicationFactor())
	assert.True(t, res.Namespace.NotificationsEnabledOrDefault())
	assert.Equal(t, "hierarchical", res.Namespace.GetKeySorting())
}

func TestManagementServerPatchNamespaceRejectsInvalidRequest(t *testing.T) {
	management := newReadyManagementServer(
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
			Name:              "../ns",
			ReplicationFactor: 1,
		}}},
		{name: "initial shard count", req: &proto.PatchNamespaceRequest{Namespace: &proto.Namespace{
			Name:              "ns-1",
			InitialShardCount: 1,
		}}},
		{name: "key sorting", req: &proto.PatchNamespaceRequest{Namespace: &proto.Namespace{
			Name:       "ns-1",
			KeySorting: "invalid",
		}}},
		{name: "anti affinities", req: &proto.PatchNamespaceRequest{Namespace: &proto.Namespace{
			Name: "ns-1",
			AntiAffinities: []*proto.AntiAffinity{{
				Labels: []string{"zone"},
				Mode:   proto.AntiAffinityModeStrict,
			}},
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
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:              "missing",
			ReplicationFactor: 1,
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerPatchNamespaceFailedPrecondition(t *testing.T) {
	serverName := "server-1"
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:              "ns-1",
				InitialShardCount: 1,
				ReplicationFactor: 1,
				KeySorting:        "natural",
			}},
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	_, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
		Namespace: &proto.Namespace{
			Name:              "ns-1",
			ReplicationFactor: 2,
		},
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, grpcstatus.Code(err))
}

func TestManagementServerDeleteNamespace(t *testing.T) {
	serverName := "server-1"
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{
				{
					Name:              "ns-1",
					InitialShardCount: 1,
					ReplicationFactor: 1,
					KeySorting:        "natural",
				},
				{
					Name:              "ns-2",
					InitialShardCount: 2,
					ReplicationFactor: 1,
					KeySorting:        "hierarchical",
				},
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
	assert.Equal(t, "ns-1", res.Namespace.GetName())
	assert.EqualValues(t, 1, res.Namespace.GetInitialShardCount())
	assert.Equal(t, "natural", res.Namespace.GetKeySorting())

	_, found := management.metadata.GetNamespace("ns-1")
	assert.False(t, found)
	namespace, found := management.metadata.GetNamespace("ns-2")
	require.True(t, found)
	assert.Equal(t, "ns-2", namespace.UnsafeBorrow().GetName())
}

func TestManagementServerDeleteNamespaceRejectsInvalidRequest(t *testing.T) {
	management := newReadyManagementServer(
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
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.DeleteNamespace(context.Background(), &proto.DeleteNamespaceRequest{Namespace: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerGetNamespaceRejectsEmptyLookup(t *testing.T) {
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.GetNamespace(context.Background(), &proto.GetNamespaceRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
}

func TestManagementServerGetNamespaceNotFound(t *testing.T) {
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{}),
		nil,
	)

	_, err := management.GetNamespace(context.Background(), &proto.GetNamespaceRequest{Namespace: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestManagementServerCreateDataServer(t *testing.T) {
	management := newReadyManagementServer(
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
	management := newReadyManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}), nil)
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
	management := newReadyManagementServer(
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
	management := newReadyManagementServer(
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
	management := newReadyManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}), nil)

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
	management := newReadyManagementServer(
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
	management := newReadyManagementServer(
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
	management := newReadyManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}), nil)

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
	management := newReadyManagementServer(
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
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:              "default",
				ReplicationFactor: 2,
				InitialShardCount: 1,
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
