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

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
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

func (p *testLeaderMetadata) GetLeader() (*proto.Coordinator, error) {
	return p.info, p.err
}

var _ coordruntime.Runtime = (*testRuntime)(nil)

type testRuntime struct {
	metadata coordmetadata.Metadata
	// statuses are the statuses of the data servers that have a controller
	statuses map[string]*proto.DataServerStatus
	// drainedDataServer, when set, is called by DrainedDataServer
	drainedDataServer func(name string) error
}

func (*testRuntime) Close() error { return nil }

func (*testRuntime) InitiateSplit(string, int64, *uint32) (leftShardID int64, rightShardID int64, err error) {
	return 0, 0, errors.New("unexpected split shard request")
}

func (*testRuntime) LeaderElected(int64, *proto.DataServerIdentity, []*proto.DataServerIdentity) {}

func (*testRuntime) ShardDeleted(int64) {}

func (*testRuntime) SubscribeShardAssignments() *commonwatch.Receiver[*proto.ShardAssignments] {
	return commonwatch.New(&proto.ShardAssignments{}).Subscribe()
}

func (*testRuntime) BecameUnavailable(*proto.DataServerIdentity) {}

func (*testRuntime) FeaturesDiscovered(*proto.DataServerIdentity) {}

func (*testRuntime) BecameDrained(*proto.DataServerIdentity) {}

func (*testRuntime) CreateDataServer(string, *proto.DataServer) bool { return false }

func (*testRuntime) DeleteDataServer(string) {}

func (r *testRuntime) DrainedDataServer(name string) error {
	if r.drainedDataServer != nil {
		return r.drainedDataServer(name)
	}
	return errors.New("unexpected forget of a data server")
}

func (*testRuntime) ListLiveDataServer() map[string]commonobject.Borrowed[*proto.DataServer] {
	return nil
}

func (r *testRuntime) ListLiveDataServerStatus() map[string]*proto.DataServerStatus {
	return r.statuses
}

func (r *testRuntime) GetLiveDataServerStatus(name string) (*proto.DataServerStatus, bool) {
	status, found := r.statuses[name]
	return status, found
}

func (*testRuntime) CreateNamespace(string, *proto.Namespace) error {
	return errors.New("unexpected namespace creation")
}

func (*testRuntime) DeleteNamespace(string) {}

func (r *testRuntime) Metadata() coordmetadata.Metadata { return r.metadata }

func (*testRuntime) RecomputeAssignments() {}

func (*testRuntime) RefreshShardControllers() {}

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
	metadata := metadataFactory.CreateMetadata(t.Context())
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
) *ManagementServer {
	if runtime == nil {
		runtime = &testRuntime{metadata: metadata}
	}
	management := NewManagementServer(metadata)
	management.setRuntime(runtime)
	return management
}

func TestManagementServerRedirectsRequestsToCoordinatorLeader(t *testing.T) {
	management := NewManagementServer(&testLeaderMetadata{
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
			name: "drained data server",
			call: func(ctx context.Context) error {
				_, err := management.DrainedDataServer(ctx,
					&proto.DrainedDataServerRequest{DataServer: "server-1"})
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
	management := NewManagementServer(&testLeaderMetadata{
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
	management := NewManagementServer(&testLeaderMetadata{
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
	management := NewManagementServer(metadata)
	management.setRuntime(&testRuntime{metadata: metadata})

	res, err := management.ListNamespaces(t.Context(), &proto.ListNamespacesRequest{})
	require.NoError(t, err)
	require.Len(t, res.Namespaces, 1)
	assert.Equal(t, "ns-1", res.Namespaces[0].GetNamespace().GetName())
}

func TestManagementServerReturnsUnavailableBeforeRuntimeReady(t *testing.T) {
	management := NewManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}))

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

// A data server that left the configuration is listed while it drains, and
// once drained, after the configured ones.
func TestManagementServerListDataServersIncludesLeftDataServers(t *testing.T) {
	serverName := "server-1"
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers: []*proto.DataServerIdentity{dataServer(&serverName, "public-1", "internal-1")},
	})
	storeDataServerStates(t, metadata, map[string]proto.DataServerState{
		serverName: proto.DataServerState_DATA_SERVER_STATE_RUNNING,
		"draining": proto.DataServerState_DATA_SERVER_STATE_DRAINING,
		"drained":  proto.DataServerState_DATA_SERVER_STATE_DRAINED,
		"removed":  proto.DataServerState_DATA_SERVER_STATE_RUNNING,
	})
	management := newReadyManagementServer(metadata, nil)

	res, err := management.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
	require.NoError(t, err)
	var names []string
	var states []proto.DataServerState
	for _, view := range res.GetDataServers() {
		names = append(names, view.GetDataServer().GetIdentity().GetName())
		states = append(states, view.GetDataServerStatus().GetState())
	}
	assert.Equal(t, []string{serverName, "drained", "draining"}, names)
	assert.Equal(t, []proto.DataServerState{
		proto.DataServerState_DATA_SERVER_STATE_UNSPECIFIED,
		proto.DataServerState_DATA_SERVER_STATE_DRAINED,
		proto.DataServerState_DATA_SERVER_STATE_DRAINING,
	}, states)
}

// A drained data server that the configuration has again is listed once, with
// its stored state, as Get reports it.
func TestManagementServerListDataServersReportsLeftDataServerAddedAgain(t *testing.T) {
	serverName := "server-1"
	drained := "drained"
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers: []*proto.DataServerIdentity{
			dataServer(&serverName, "public-1", "internal-1"),
			dataServer(&drained, "public-2", "internal-2"),
		},
	})
	storeDataServerStates(t, metadata, map[string]proto.DataServerState{
		drained: proto.DataServerState_DATA_SERVER_STATE_DRAINED,
	})
	management := newReadyManagementServer(metadata, nil)

	res, err := management.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
	require.NoError(t, err)
	states := map[string]proto.DataServerState{}
	for _, view := range res.GetDataServers() {
		states[view.GetDataServer().GetIdentity().GetName()] = view.GetDataServerStatus().GetState()
	}
	assert.Equal(t, map[string]proto.DataServerState{
		serverName: proto.DataServerState_DATA_SERVER_STATE_UNSPECIFIED,
		drained:    proto.DataServerState_DATA_SERVER_STATE_DRAINED,
	}, states)
	assert.Len(t, res.GetDataServers(), 2)

	got, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: drained})
	require.NoError(t, err)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINED, got.GetDataServer().GetDataServerStatus().GetState())
}

func TestManagementServerListDataServersStatusLoadError(t *testing.T) {
	management := newReadyManagementServer(&failingMetadata{
		Metadata:  newTestMetadata(t, &proto.ClusterConfiguration{}),
		statusErr: errors.New("status unavailable"),
	}, nil)

	_, err := management.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
	assert.Equal(t, codes.Internal, grpcstatus.Code(err))
	assert.ErrorContains(t, err, "status unavailable")
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

// A data server that left the configuration is reported from its stored
// status, with the features its controller knows while it drains, and isn't
// found once its status is gone, or if it isn't draining.
func TestManagementServerGetDataServerReportsLeftDataServer(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	storeDataServerStates(t, metadata, map[string]proto.DataServerState{
		"draining": proto.DataServerState_DATA_SERVER_STATE_DRAINING,
		"drained":  proto.DataServerState_DATA_SERVER_STATE_DRAINED,
		"removed":  proto.DataServerState_DATA_SERVER_STATE_RUNNING,
	})
	features := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}
	management := newReadyManagementServer(metadata, &testRuntime{
		metadata: metadata,
		statuses: map[string]*proto.DataServerStatus{
			"draining": {State: proto.DataServerState_DATA_SERVER_STATE_DRAINING, SupportedFeatures: features},
		},
	})

	res, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "draining"})
	require.NoError(t, err)
	assert.Equal(t, "draining", res.GetDataServer().GetDataServer().GetIdentity().GetName())
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINING, res.GetDataServer().GetDataServerStatus().GetState())
	assert.Equal(t, features, res.GetDataServer().GetDataServerStatus().GetSupportedFeatures())

	res, err = management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "drained"})
	require.NoError(t, err)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINED, res.GetDataServer().GetDataServerStatus().GetState())
	assert.Empty(t, res.GetDataServer().GetDataServerStatus().GetSupportedFeatures())

	_, err = management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "removed"})
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

// A deleted data server is reported DRAINING by the runtime before its
// controller stores it.
func TestManagementServerGetDataServerReportsDrainingBeforeStored(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	storeDataServerStates(t, metadata, map[string]proto.DataServerState{
		"removed": proto.DataServerState_DATA_SERVER_STATE_RUNNING,
	})
	draining := &proto.DataServerStatus{State: proto.DataServerState_DATA_SERVER_STATE_DRAINING}
	management := newReadyManagementServer(metadata, &testRuntime{
		metadata: metadata,
		statuses: map[string]*proto.DataServerStatus{"removed": draining, "unstored": draining},
	})

	for _, name := range []string{"removed", "unstored"} {
		res, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: name})
		require.NoError(t, err)
		assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINING, res.GetDataServer().GetDataServerStatus().GetState())
	}

	res, err := management.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
	require.NoError(t, err)
	var names []string
	for _, view := range res.GetDataServers() {
		names = append(names, view.GetDataServer().GetIdentity().GetName())
	}
	assert.Equal(t, []string{"removed", "unstored"}, names)
}

func TestManagementServerGetDataServerStatusLoadError(t *testing.T) {
	management := newReadyManagementServer(&failingMetadata{
		Metadata:  newTestMetadata(t, &proto.ClusterConfiguration{}),
		statusErr: errors.New("status unavailable"),
	}, nil)

	_, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: "drained"})
	assert.Equal(t, codes.Internal, grpcstatus.Code(err))
	assert.ErrorContains(t, err, "status unavailable")
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
	require.NoError(t, metadata.CreateNamespaceStatus("ns-1", &proto.NamespaceStatus{
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
	assert.EqualValues(t, 3, res.Namespace.GetNamespaceStatus().GetReplicationFactor())
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
	require.NoError(t, metadata.CreateNamespaceStatus("ns-1", &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards: map[int64]*proto.ShardMetadata{
			0: {Status: proto.ShardStatusSteadyState},
		},
	}))
	require.NoError(t, metadata.CreateNamespaceStatus("ns-2", &proto.NamespaceStatus{
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
	assert.EqualValues(t, 3, viewsByName["ns-1"].GetNamespaceStatus().GetReplicationFactor())
	assert.Len(t, viewsByName["ns-1"].GetNamespaceStatus().GetShards(), 1)
	assert.EqualValues(t, 1, viewsByName["ns-2"].GetNamespaceStatus().GetReplicationFactor())
	assert.Len(t, viewsByName["ns-2"].GetNamespaceStatus().GetShards(), 2)
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

	namespace, found, err := management.metadata.GetNamespace("ns-1")
	require.NoError(t, err)
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

func TestManagementServerCreateNamespaceRejectsReservedName(t *testing.T) {
	serverName := "server-1"
	management := newReadyManagementServer(
		newTestMetadata(t, &proto.ClusterConfiguration{
			Servers: []*proto.DataServerIdentity{
				dataServer(&serverName, "public-1", "internal-1"),
			},
		}),
		nil,
	)

	for _, name := range []string{"MANIFEST", "manifest"} {
		t.Run(name, func(t *testing.T) {
			_, err := management.CreateNamespace(context.Background(), &proto.CreateNamespaceRequest{
				Namespace: &proto.Namespace{
					Name:              name,
					InitialShardCount: 1,
					ReplicationFactor: 1,
					KeySorting:        "natural",
				},
			})
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))

			_, found, err := management.metadata.GetNamespace(name)
			require.NoError(t, err)
			assert.False(t, found)
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

	namespace, found, err := management.metadata.GetNamespace("ns-1")
	require.NoError(t, err)
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

	namespace, found, err := management.metadata.GetNamespace("ns-1")
	require.NoError(t, err)
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

	_, found, err := management.metadata.GetNamespace("ns-1")
	require.NoError(t, err)
	assert.False(t, found)
	namespace, found, err := management.metadata.GetNamespace("ns-2")
	require.NoError(t, err)
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

type namespaceStatusFailingMetadata struct {
	coordmetadata.Metadata
}

func (namespaceStatusFailingMetadata) GetNamespaceStatus(string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
	return commonobject.Borrowed[*proto.NamespaceStatus]{}, false, errors.New("status unavailable")
}

func TestManagementServerGetNamespaceStatusLoadError(t *testing.T) {
	metadata := namespaceStatusFailingMetadata{Metadata: newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: "ns-1", InitialShardCount: 1, ReplicationFactor: 1}},
	})}
	management := newReadyManagementServer(metadata, nil)

	_, err := management.GetNamespace(context.Background(), &proto.GetNamespaceRequest{Namespace: "ns-1"})
	require.Error(t, err)
	assert.Equal(t, codes.Internal, grpcstatus.Code(err))
	assert.ErrorContains(t, err, "status unavailable")
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

	created, found, err := management.metadata.GetDataServer(serverName)
	require.NoError(t, err)
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

// A data server that the runtime drains can't be created again, also before
// its controller stores it DRAINING.
func TestManagementServerCreateDataServerRejectsDrainingDataServer(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	management := newReadyManagementServer(metadata, &testRuntime{
		metadata: metadata,
		statuses: map[string]*proto.DataServerStatus{
			"draining": {State: proto.DataServerState_DATA_SERVER_STATE_DRAINING},
		},
	})

	_, err := management.CreateDataServer(context.Background(), &proto.CreateDataServerRequest{
		DataServer: &proto.DataServer{
			Identity: &proto.DataServerIdentity{Name: new("draining"), Public: "public-1", Internal: "internal-1"},
		},
	})
	assert.Equal(t, codes.AlreadyExists, grpcstatus.Code(err))
	_, exists, err := metadata.GetDataServer("draining")
	require.NoError(t, err)
	assert.False(t, exists)
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

	patched, found, err := management.metadata.GetDataServer(serverName)
	require.NoError(t, err)
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

	_, found, err := management.metadata.GetDataServer(serverName)
	require.NoError(t, err)
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

	_, found, err := management.metadata.GetDataServer(serverName1)
	require.NoError(t, err)
	assert.True(t, found)
}

// A retried delete returns the data server that already left.
func TestManagementServerDeleteDataServerIsIdempotent(t *testing.T) {
	serverName := "server-1"
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers: []*proto.DataServerIdentity{dataServer(&serverName, "public-1", "internal-1")},
	})
	require.NoError(t, metadata.PatchDataServerStatus(serverName, func(status *proto.DataServerStatus) (bool, error) {
		status.State = proto.DataServerState_DATA_SERVER_STATE_RUNNING
		return true, nil
	}))
	management := newReadyManagementServer(metadata, nil)

	for range 2 {
		res, err := management.DeleteDataServer(context.Background(), &proto.DeleteDataServerRequest{DataServer: serverName})
		require.NoError(t, err)
		assert.Equal(t, serverName, res.GetDataServer().GetIdentity().GetName())
	}
}

func TestManagementServerDrainedDataServer(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	var forgotten []string
	management := newReadyManagementServer(metadata, &testRuntime{
		metadata: metadata,
		drainedDataServer: func(name string) error {
			forgotten = append(forgotten, name)
			return nil
		},
	})

	_, err := management.DrainedDataServer(context.Background(), &proto.DrainedDataServerRequest{DataServer: "server-1"})
	require.NoError(t, err)
	assert.Equal(t, []string{"server-1"}, forgotten)
}

func TestManagementServerDrainedDataServerRejectsInvalidRequest(t *testing.T) {
	management := newReadyManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}), nil)

	for _, req := range []*proto.DrainedDataServerRequest{nil, {}} {
		_, err := management.DrainedDataServer(context.Background(), req)
		assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
	}
}

func TestManagementServerDrainedDataServerReturnsRuntimeErrors(t *testing.T) {
	for _, test := range []struct {
		err  error
		code codes.Code
	}{
		{errs.ErrNotFound, codes.NotFound},
		{errs.ErrFailedPrecondition, codes.FailedPrecondition},
		{errors.New("store unavailable"), codes.Internal},
	} {
		metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
		management := newReadyManagementServer(metadata, &testRuntime{
			metadata:          metadata,
			drainedDataServer: func(string) error { return test.err },
		})

		_, err := management.DrainedDataServer(context.Background(),
			&proto.DrainedDataServerRequest{DataServer: "server-1"})
		assert.Equal(t, test.code, grpcstatus.Code(err), test.err.Error())
	}
}

func TestManagementServerSetRuntimeIgnoresNil(t *testing.T) {
	management := NewManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}))
	management.setRuntime(nil)

	_, err := management.ListNamespaces(t.Context(), &proto.ListNamespacesRequest{})
	oxiaErr, _ := constant.FromGrpcError(err)
	assert.ErrorIs(t, oxiaErr, constant.ErrNotInitialized)
}

func TestManagementServerRedirectError(t *testing.T) {
	leader := &proto.Coordinator{Name: "coordinator-1"}
	self := &proto.Coordinator{Name: "coordinator-0"}
	for _, test := range []struct {
		name     string
		metadata coordmetadata.Metadata
		expected error
	}{
		{name: "without metadata", expected: constant.ErrNotInitialized},
		{
			name:     "without self",
			metadata: &testLeaderMetadata{info: leader},
			expected: constant.ErrNotInitialized,
		},
		{
			// The leader is known, but not where to redirect to
			name:     "leader without address",
			metadata: &testLeaderMetadata{info: leader, self: self},
			expected: constant.ErrNodeIsNotLeader,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			management := NewManagementServer(test.metadata)

			_, err := management.ListNamespaces(t.Context(), &proto.ListNamespacesRequest{})
			oxiaErr, metadata := constant.FromGrpcError(err)
			assert.ErrorIs(t, oxiaErr, test.expected)
			_, ok := metadata.GetCoordinatorLeaderHint()
			assert.False(t, ok)
		})
	}
}

// The metadata errors that are not part of the API are reported as internal
// errors, and the version conflicts as aborted, for the caller to retry.
func TestManagementServerReturnsMetadataErrors(t *testing.T) {
	serverName := "server-1"
	for _, test := range []struct {
		name         string
		conflictCode codes.Code
		call         func(management *ManagementServer) error
	}{
		{"ListDataServers", codes.Internal, func(management *ManagementServer) error {
			_, err := management.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
			return err
		}},
		{"GetDataServer", codes.Internal, func(management *ManagementServer) error {
			_, err := management.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: serverName})
			return err
		}},
		{"CreateDataServer", codes.Aborted, func(management *ManagementServer) error {
			_, err := management.CreateDataServer(context.Background(), &proto.CreateDataServerRequest{
				DataServer: &proto.DataServer{Identity: dataServer(&serverName, "public-1", "internal-1")},
			})
			return err
		}},
		{"PatchDataServer", codes.Aborted, func(management *ManagementServer) error {
			_, err := management.PatchDataServer(context.Background(), &proto.PatchDataServerRequest{
				DataServer: &proto.DataServer{Identity: dataServer(&serverName, "public-1", "internal-1")},
			})
			return err
		}},
		{"DeleteDataServer", codes.Aborted, func(management *ManagementServer) error {
			_, err := management.DeleteDataServer(context.Background(),
				&proto.DeleteDataServerRequest{DataServer: serverName})
			return err
		}},
		{"ListNamespaces", codes.Internal, func(management *ManagementServer) error {
			_, err := management.ListNamespaces(context.Background(), &proto.ListNamespacesRequest{})
			return err
		}},
		{"CreateNamespace", codes.Aborted, func(management *ManagementServer) error {
			_, err := management.CreateNamespace(context.Background(), &proto.CreateNamespaceRequest{
				Namespace: &proto.Namespace{Name: "ns-1", InitialShardCount: 1, ReplicationFactor: 1, KeySorting: "natural"},
			})
			return err
		}},
		{"PatchNamespace", codes.Aborted, func(management *ManagementServer) error {
			_, err := management.PatchNamespace(context.Background(), &proto.PatchNamespaceRequest{
				Namespace: &proto.Namespace{Name: "ns-1", ReplicationFactor: 1},
			})
			return err
		}},
		{"DeleteNamespace", codes.Aborted, func(management *ManagementServer) error {
			_, err := management.DeleteNamespace(context.Background(), &proto.DeleteNamespaceRequest{Namespace: "ns-1"})
			return err
		}},
		{"GetNamespace", codes.Internal, func(management *ManagementServer) error {
			_, err := management.GetNamespace(context.Background(), &proto.GetNamespaceRequest{Namespace: "ns-1"})
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, failure := range []struct {
				err  error
				code codes.Code
			}{
				{errors.New("metadata unavailable"), codes.Internal},
				{errs.ErrMetaBadVersion, test.conflictCode},
			} {
				metadata := &failingMetadata{Metadata: newTestMetadata(t, &proto.ClusterConfiguration{}), err: failure.err}
				management := newReadyManagementServer(metadata, nil)

				err := test.call(management)
				require.Error(t, err)
				assert.Equal(t, failure.code, grpcstatus.Code(err))
			}
		})
	}
}

func TestManagementServerListNamespacesStatusLoadError(t *testing.T) {
	metadata := &failingMetadata{
		Metadata: newTestMetadata(t, &proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{Name: "ns-1", InitialShardCount: 1, ReplicationFactor: 1}},
		}),
		statusErr: errors.New("status unavailable"),
	}
	management := newReadyManagementServer(metadata, nil)

	_, err := management.ListNamespaces(context.Background(), &proto.ListNamespacesRequest{})
	assert.Equal(t, codes.Internal, grpcstatus.Code(err))
	assert.ErrorContains(t, err, "status unavailable")
}

func TestManagementServerSplitShard(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	splitPoint := uint32(100)
	management := newReadyManagementServer(metadata, &splitRuntime{testRuntime: &testRuntime{metadata: metadata}})

	res, err := management.SplitShard(context.Background(), &proto.SplitShardRequest{
		Namespace:  "ns-1",
		Shard:      1,
		SplitPoint: &splitPoint,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 2, res.LeftChildShard)
	assert.EqualValues(t, 3, res.RightChildShard)
}

func TestManagementServerSplitShardReturnsRuntimeError(t *testing.T) {
	management := newReadyManagementServer(newTestMetadata(t, &proto.ClusterConfiguration{}), nil)

	_, err := management.SplitShard(context.Background(), &proto.SplitShardRequest{Namespace: "ns-1", Shard: 1})
	require.ErrorContains(t, err, "unexpected split shard request")
}

// failingMetadata fails the configuration reads and writes with err, and the
// reads of the statuses with statusErr, when they are set.
type failingMetadata struct {
	coordmetadata.Metadata
	err       error
	statusErr error
}

func (m *failingMetadata) GetConfig() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
	if m.err != nil {
		return commonobject.Borrowed[*proto.ClusterConfiguration]{}, m.err
	}
	return m.Metadata.GetConfig()
}

func (m *failingMetadata) GetDataServer(name string) (commonobject.Borrowed[*proto.DataServer], bool, error) {
	if m.err != nil {
		return commonobject.Borrowed[*proto.DataServer]{}, false, m.err
	}
	return m.Metadata.GetDataServer(name)
}

func (m *failingMetadata) CreateDataServer(dataServer *proto.DataServer) error {
	if m.err != nil {
		return m.err
	}
	return m.Metadata.CreateDataServer(dataServer)
}

func (m *failingMetadata) PatchDataServer(dataServer *proto.DataServer) (*proto.DataServer, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.Metadata.PatchDataServer(dataServer)
}

func (m *failingMetadata) DeleteDataServer(name string) (*proto.DataServer, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.Metadata.DeleteDataServer(name)
}

func (m *failingMetadata) ListNamespace() (map[string]commonobject.Borrowed[*proto.Namespace], error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.Metadata.ListNamespace()
}

func (m *failingMetadata) ListDataServerStatus() (map[string]commonobject.Borrowed[*proto.DataServerStatus], error) {
	if m.statusErr != nil {
		return nil, m.statusErr
	}
	return m.Metadata.ListDataServerStatus()
}

func (m *failingMetadata) GetDataServerStatus(name string) (commonobject.Borrowed[*proto.DataServerStatus], bool, error) {
	if m.statusErr != nil {
		return commonobject.Borrowed[*proto.DataServerStatus]{}, false, m.statusErr
	}
	return m.Metadata.GetDataServerStatus(name)
}

func (m *failingMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	if m.statusErr != nil {
		return nil, m.statusErr
	}
	return m.Metadata.ListNamespaceStatus()
}

func (m *failingMetadata) CreateNamespace(namespace *proto.Namespace) error {
	if m.err != nil {
		return m.err
	}
	return m.Metadata.CreateNamespace(namespace)
}

func (m *failingMetadata) PatchNamespace(namespace *proto.Namespace) (*proto.Namespace, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.Metadata.PatchNamespace(namespace)
}

func (m *failingMetadata) DeleteNamespace(name string) (*proto.Namespace, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.Metadata.DeleteNamespace(name)
}

func (m *failingMetadata) GetNamespace(name string) (commonobject.Borrowed[*proto.Namespace], bool, error) {
	if m.err != nil {
		return commonobject.Borrowed[*proto.Namespace]{}, false, m.err
	}
	return m.Metadata.GetNamespace(name)
}

// splitRuntime splits every shard into the two next shard ids.
type splitRuntime struct {
	*testRuntime
}

func (*splitRuntime) InitiateSplit(_ string, shard int64, _ *uint32) (leftShardID int64, rightShardID int64, err error) {
	return shard + 1, shard + 2, nil
}

// storeDataServerStates stores the state of each data server.
func storeDataServerStates(t *testing.T, metadata coordmetadata.Metadata, states map[string]proto.DataServerState) {
	t.Helper()
	for name, state := range states {
		require.NoError(t, metadata.PatchDataServerStatus(name, func(status *proto.DataServerStatus) (bool, error) {
			status.State = state
			return true, nil
		}))
	}
}

func (*testRuntime) RequestBackup(string, *int64) (int, error) {
	return 0, coordruntime.ErrBackupsUnsupported
}

func (*testRuntime) ListBackups(context.Context, string, *int64, string, func(*proto.ListBackupsResponse) error) error {
	return coordruntime.ErrBackupsUnsupported
}

// backupTestRuntime serves the backup operations with the given results.
type backupTestRuntime struct {
	*testRuntime
	requestBackup func(namespace string, shard *int64) (int, error)
	listBackups   func(namespace string, shard *int64, destination string, visit func(*proto.ListBackupsResponse) error) error
}

func (r *backupTestRuntime) RequestBackup(namespace string, shard *int64) (int, error) {
	return r.requestBackup(namespace, shard)
}

func (r *backupTestRuntime) ListBackups(_ context.Context, namespace string, shard *int64, destination string,
	visit func(*proto.ListBackupsResponse) error) error {
	return r.listBackups(namespace, shard, destination, visit)
}

// listBackupsStream collects what the admin service sends.
type listBackupsStream struct {
	grpc.ServerStream
	backups []*proto.BackupInfo
}

func (*listBackupsStream) Context() context.Context {
	return context.Background()
}

func (s *listBackupsStream) Send(res *proto.ListBackupsResponse) error {
	s.backups = append(s.backups, res.GetBackups()...)
	return nil
}

func TestManagementServerBackupOperations(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	shard := int64(7)
	runtime := &backupTestRuntime{
		testRuntime:   &testRuntime{metadata: metadata},
		requestBackup: func(string, *int64) (int, error) { return 3, nil },
		listBackups: func(namespace string, _ *int64, destination string, visit func(*proto.ListBackupsResponse) error) error {
			return visit(&proto.ListBackupsResponse{Backups: []*proto.BackupInfo{{Shard: 7, BackupId: namespace + "@" + destination}}})
		},
	}
	management := newReadyManagementServer(metadata, runtime)

	requested, err := management.RequestBackup(context.Background(), &proto.RequestBackupRequest{Namespace: "ns", Shard: &shard})
	require.NoError(t, err)
	assert.EqualValues(t, 3, requested.GetShards())
	listed := &listBackupsStream{}
	require.NoError(t, management.ListBackups(&proto.ListBackupsRequest{Namespace: "ns", Destination: "s3://b"}, listed))
	require.Len(t, listed.backups, 1)
	assert.Equal(t, "ns@s3://b", listed.backups[0].GetBackupId())

	// The errors of the runtime map to gRPC codes
	for name, test := range map[string]struct {
		err  error
		code codes.Code
	}{
		"unsupported":    {coordruntime.ErrBackupsUnsupported, codes.Unimplemented},
		"no destination": {coordruntime.ErrNoBackupDestination, codes.FailedPrecondition},
		"other":          {errors.New("data server unavailable"), codes.Unknown},
	} {
		t.Run(name, func(t *testing.T) {
			runtime.requestBackup = func(string, *int64) (int, error) { return 0, test.err }
			runtime.listBackups = func(string, *int64, string, func(*proto.ListBackupsResponse) error) error { return test.err }
			_, err := management.RequestBackup(context.Background(), &proto.RequestBackupRequest{Namespace: "ns"})
			assert.Equal(t, test.code, grpcstatus.Code(err))
			err = management.ListBackups(&proto.ListBackupsRequest{Namespace: "ns"}, &listBackupsStream{})
			assert.Equal(t, test.code, grpcstatus.Code(err))
		})
	}

	// Without a runtime, the server is not ready
	notReady := NewManagementServer(metadata)
	_, err = notReady.RequestBackup(context.Background(), &proto.RequestBackupRequest{Namespace: "ns"})
	assert.Error(t, err)
	assert.Error(t, notReady.ListBackups(&proto.ListBackupsRequest{Namespace: "ns"}, &listBackupsStream{}))
}
