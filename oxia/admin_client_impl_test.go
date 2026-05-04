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

package oxia

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
)

type mockAdminRpcClient struct {
	listDataServersResponse *proto.ListDataServersResponse
	listDataServersErr      error
	getDataServerResponse   *proto.GetDataServerResponse
	getDataServerErr        error
	createDataServerResp    *proto.CreateDataServerResponse
	createDataServerErr     error
	patchDataServerResp     *proto.PatchDataServerResponse
	patchDataServerErr      error
	deleteDataServerResp    *proto.DeleteDataServerResponse
	deleteDataServerErr     error
	createNamespaceResp     *proto.CreateNamespaceResponse
	createNamespaceErr      error
	patchNamespaceResp      *proto.PatchNamespaceResponse
	patchNamespaceErr       error
	deleteNamespaceResp     *proto.DeleteNamespaceResponse
	deleteNamespaceErr      error
	listNamespacesResp      *proto.ListNamespacesResponse
	listNamespacesErr       error
	getNamespaceResp        *proto.GetNamespaceResponse
	getNamespaceErr         error
}

func (m *mockAdminRpcClient) ListDataServers(context.Context, *proto.ListDataServersRequest, ...grpc.CallOption) (*proto.ListDataServersResponse, error) {
	return m.listDataServersResponse, m.listDataServersErr
}

func (m *mockAdminRpcClient) GetDataServer(context.Context, *proto.GetDataServerRequest, ...grpc.CallOption) (*proto.GetDataServerResponse, error) {
	return m.getDataServerResponse, m.getDataServerErr
}

func (m *mockAdminRpcClient) CreateDataServer(context.Context, *proto.CreateDataServerRequest, ...grpc.CallOption) (*proto.CreateDataServerResponse, error) {
	return m.createDataServerResp, m.createDataServerErr
}

func (m *mockAdminRpcClient) PatchDataServer(context.Context, *proto.PatchDataServerRequest, ...grpc.CallOption) (*proto.PatchDataServerResponse, error) {
	return m.patchDataServerResp, m.patchDataServerErr
}

func (m *mockAdminRpcClient) DeleteDataServer(context.Context, *proto.DeleteDataServerRequest, ...grpc.CallOption) (*proto.DeleteDataServerResponse, error) {
	return m.deleteDataServerResp, m.deleteDataServerErr
}

func (m *mockAdminRpcClient) CreateNamespace(context.Context, *proto.CreateNamespaceRequest, ...grpc.CallOption) (*proto.CreateNamespaceResponse, error) {
	return m.createNamespaceResp, m.createNamespaceErr
}

func (m *mockAdminRpcClient) PatchNamespace(context.Context, *proto.PatchNamespaceRequest, ...grpc.CallOption) (*proto.PatchNamespaceResponse, error) {
	return m.patchNamespaceResp, m.patchNamespaceErr
}

func (m *mockAdminRpcClient) DeleteNamespace(context.Context, *proto.DeleteNamespaceRequest, ...grpc.CallOption) (*proto.DeleteNamespaceResponse, error) {
	return m.deleteNamespaceResp, m.deleteNamespaceErr
}

func (m *mockAdminRpcClient) GetNamespace(context.Context, *proto.GetNamespaceRequest, ...grpc.CallOption) (*proto.GetNamespaceResponse, error) {
	return m.getNamespaceResp, m.getNamespaceErr
}

func (m *mockAdminRpcClient) ListNamespaces(context.Context, *proto.ListNamespacesRequest, ...grpc.CallOption) (*proto.ListNamespacesResponse, error) {
	return m.listNamespacesResp, m.listNamespacesErr
}

func (*mockAdminRpcClient) SplitShard(context.Context, *proto.SplitShardRequest, ...grpc.CallOption) (*proto.SplitShardResponse, error) {
	panic("unexpected SplitShard call")
}

type mockAdminClientPool struct {
	adminClient proto.OxiaAdminClient
	err         error
}

func (m *mockAdminClientPool) Close() error {
	return nil
}

func (m *mockAdminClientPool) GetClientRpc(string) (proto.OxiaClientClient, error) {
	return nil, errors.New("unexpected GetClientRpc call")
}

func (m *mockAdminClientPool) GetHealthRpc(string) (grpc_health_v1.HealthClient, io.Closer, error) {
	return nil, nil, errors.New("unexpected GetHealthRpc call")
}

func (m *mockAdminClientPool) GetCoordinationRpc(string) (proto.OxiaCoordinationClient, error) {
	return nil, errors.New("unexpected GetCoordinationRpc call")
}

func (m *mockAdminClientPool) GetReplicationRpc(string) (proto.OxiaLogReplicationClient, error) {
	return nil, errors.New("unexpected GetReplicationRpc call")
}

func (m *mockAdminClientPool) GetAminRpc(string) (proto.OxiaAdminClient, error) {
	return m.adminClient, m.err
}

func (m *mockAdminClientPool) Clear(string) {}

var _ rpc.ClientPool = (*mockAdminClientPool)(nil)

func TestAdminClientListDataServersMapsGrpcErrors(t *testing.T) {
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				listDataServersErr: grpcstatus.Error(codes.NotFound, "data server not found"),
			},
		},
	}

	_, err := admin.ListDataServers()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknown)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
}

func TestAdminClientListDataServersMapsUnavailableConnection(t *testing.T) {
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			err: grpcstatus.Error(codes.Unavailable, "connection refused"),
		},
	}

	_, err := admin.ListDataServers()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknown)
	assert.Equal(t, codes.Unavailable, grpcstatus.Code(err))
}

func TestAdminClientListDataServersMapsAuthErrors(t *testing.T) {
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				listDataServersErr: grpcstatus.Error(codes.PermissionDenied, "forbidden"),
			},
		},
	}

	_, err := admin.ListDataServers()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnauthorized)
	assert.Equal(t, codes.PermissionDenied, grpcstatus.Code(err))

	admin.clientPool = &mockAdminClientPool{
		adminClient: &mockAdminRpcClient{
			listDataServersErr: grpcstatus.Error(codes.Unauthenticated, "missing token"),
		},
	}

	_, err = admin.ListDataServers()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnauthenticated)
	assert.Equal(t, codes.Unauthenticated, grpcstatus.Code(err))
}

func TestAdminClientListDataServersReturnsResponse(t *testing.T) {
	serverName := "server-1"
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				listDataServersResponse: &proto.ListDataServersResponse{
					DataServers: []*proto.DataServer{
						{
							Identity: &proto.DataServerIdentity{
								Name:     &serverName,
								Public:   "public-1",
								Internal: "internal-1",
							},
						},
					},
				},
			},
		},
	}

	dataServers, err := admin.ListDataServers()
	require.NoError(t, err)
	require.Len(t, dataServers, 1)
	require.NotNil(t, dataServers[0].Identity)
	require.NotNil(t, dataServers[0].Identity.Name)
	assert.Equal(t, serverName, *dataServers[0].Identity.Name)
	assert.Equal(t, "public-1", dataServers[0].Identity.GetPublic())
	assert.Equal(t, "internal-1", dataServers[0].Identity.GetInternal())
}

func TestAdminClientGetDataServerReturnsResponse(t *testing.T) {
	serverName := "server-1"
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				getDataServerResponse: &proto.GetDataServerResponse{
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
				},
			},
		},
	}

	dataServer, err := admin.GetDataServer(serverName)
	require.NoError(t, err)
	require.NotNil(t, dataServer)
	require.NotNil(t, dataServer.Identity)
	require.NotNil(t, dataServer.Identity.Name)
	assert.Equal(t, serverName, *dataServer.Identity.Name)
	assert.Equal(t, "public-1", dataServer.Identity.GetPublic())
	assert.Equal(t, "internal-1", dataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, dataServer.Metadata.GetLabels())
}

func TestAdminClientCreateDataServerReturnsResponse(t *testing.T) {
	serverName := "server-1"
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				createDataServerResp: &proto.CreateDataServerResponse{
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
				},
			},
		},
	}

	dataServer, err := admin.CreateDataServer(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &serverName,
			Public:   "public-1",
			Internal: "internal-1",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, dataServer)
	require.NotNil(t, dataServer.Identity)
	require.NotNil(t, dataServer.Identity.Name)
	assert.Equal(t, serverName, *dataServer.Identity.Name)
	assert.Equal(t, "public-1", dataServer.Identity.GetPublic())
	assert.Equal(t, "internal-1", dataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, dataServer.Metadata.GetLabels())
}

func TestAdminClientPatchDataServerReturnsResponse(t *testing.T) {
	serverName := "server-1"
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				patchDataServerResp: &proto.PatchDataServerResponse{
					DataServer: &proto.DataServer{
						Identity: &proto.DataServerIdentity{
							Name:     &serverName,
							Public:   "public-2",
							Internal: "internal-1",
						},
						Metadata: &proto.DataServerMetadata{
							Labels: map[string]string{"rack": "rack-2"},
						},
					},
				},
			},
		},
	}

	dataServer, err := admin.PatchDataServer(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:   &serverName,
			Public: "public-2",
		},
		Metadata: &proto.DataServerMetadata{
			Labels: map[string]string{"rack": "rack-2"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, dataServer)
	require.NotNil(t, dataServer.Identity)
	require.NotNil(t, dataServer.Identity.Name)
	assert.Equal(t, serverName, *dataServer.Identity.Name)
	assert.Equal(t, "public-2", dataServer.Identity.GetPublic())
	assert.Equal(t, "internal-1", dataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-2"}, dataServer.Metadata.GetLabels())
}

func TestAdminClientDeleteDataServerReturnsResponse(t *testing.T) {
	serverName := "server-1"
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				deleteDataServerResp: &proto.DeleteDataServerResponse{
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
				},
			},
		},
	}

	dataServer, err := admin.DeleteDataServer(serverName)
	require.NoError(t, err)
	require.NotNil(t, dataServer)
	require.NotNil(t, dataServer.Identity)
	require.NotNil(t, dataServer.Identity.Name)
	assert.Equal(t, serverName, *dataServer.Identity.Name)
	assert.Equal(t, "public-1", dataServer.Identity.GetPublic())
	assert.Equal(t, "internal-1", dataServer.Identity.GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, dataServer.Metadata.GetLabels())
}

func TestAdminClientCreateNamespaceReturnsResponse(t *testing.T) {
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				createNamespaceResp: &proto.CreateNamespaceResponse{
					Namespace: &proto.Namespace{
						Name:              "ns-1",
						InitialShardCount: 4,
						ReplicationFactor: 3,
						KeySorting:        proto.KeySortingType_NATURAL.String(),
					},
				},
			},
		},
	}

	namespace, err := admin.CreateNamespace(&proto.Namespace{Name: "ns-1"})
	require.NoError(t, err)
	require.NotNil(t, namespace)
	assert.Equal(t, "ns-1", namespace.GetName())
	assert.EqualValues(t, 4, namespace.GetInitialShardCount())
	assert.EqualValues(t, 3, namespace.GetReplicationFactor())
	assert.Equal(t, proto.KeySortingType_NATURAL.String(), namespace.GetKeySorting())
}

func TestAdminClientPatchNamespaceReturnsResponse(t *testing.T) {
	notificationsEnabled := false
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				patchNamespaceResp: &proto.PatchNamespaceResponse{
					Namespace: &proto.Namespace{
						Name:                 "ns-1",
						InitialShardCount:    4,
						ReplicationFactor:    3,
						NotificationsEnabled: &notificationsEnabled,
						KeySorting:           "natural",
					},
				},
			},
		},
	}

	namespace, err := admin.PatchNamespace(&proto.Namespace{Name: "ns-1", NotificationsEnabled: &notificationsEnabled})
	require.NoError(t, err)
	require.NotNil(t, namespace)
	assert.Equal(t, "ns-1", namespace.GetName())
	assert.EqualValues(t, 4, namespace.GetInitialShardCount())
	assert.EqualValues(t, 3, namespace.GetReplicationFactor())
	assert.False(t, namespace.NotificationsEnabledOrDefault())
	assert.Equal(t, "natural", namespace.GetKeySorting())
}

func TestAdminClientDeleteNamespaceReturnsResponse(t *testing.T) {
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				deleteNamespaceResp: &proto.DeleteNamespaceResponse{
					Namespace: &proto.Namespace{
						Name:              "ns-1",
						InitialShardCount: 4,
						ReplicationFactor: 3,
						KeySorting:        "natural",
					},
				},
			},
		},
	}

	namespace, err := admin.DeleteNamespace("ns-1")
	require.NoError(t, err)
	require.NotNil(t, namespace)
	assert.Equal(t, "ns-1", namespace.GetName())
	assert.EqualValues(t, 4, namespace.GetInitialShardCount())
	assert.EqualValues(t, 3, namespace.GetReplicationFactor())
	assert.Equal(t, "natural", namespace.GetKeySorting())
}

func TestAdminClientListNamespacesReturnsResponse(t *testing.T) {
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				listNamespacesResp: &proto.ListNamespacesResponse{
					Namespaces: []*proto.Namespace{
						{
							Name:              "ns-1",
							InitialShardCount: 4,
							ReplicationFactor: 3,
						},
					},
				},
			},
		},
	}

	namespaces, err := admin.ListNamespaces()
	require.NoError(t, err)
	require.Len(t, namespaces, 1)
	assert.Equal(t, "ns-1", namespaces[0].GetName())
	assert.EqualValues(t, 4, namespaces[0].GetInitialShardCount())
	assert.EqualValues(t, 3, namespaces[0].GetReplicationFactor())
}

func TestAdminClientGetNamespaceReturnsResponse(t *testing.T) {
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				getNamespaceResp: &proto.GetNamespaceResponse{
					Namespace: &proto.Namespace{
						Name:              "ns-1",
						InitialShardCount: 4,
						ReplicationFactor: 3,
						KeySorting:        proto.KeySortingType_NATURAL.String(),
					},
				},
			},
		},
	}

	namespace, err := admin.GetNamespace("ns-1")
	require.NoError(t, err)
	require.NotNil(t, namespace)
	assert.Equal(t, "ns-1", namespace.GetName())
	assert.EqualValues(t, 4, namespace.GetInitialShardCount())
	assert.EqualValues(t, 3, namespace.GetReplicationFactor())
	assert.Equal(t, proto.KeySortingType_NATURAL.String(), namespace.GetKeySorting())
}

func TestWrapAdminErrorPreservesCause(t *testing.T) {
	cause := grpcstatus.Error(codes.InvalidArgument, "invalid request")

	err := mapAdminError(cause)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknown)
	assert.True(t, errors.Is(err, cause))
}
