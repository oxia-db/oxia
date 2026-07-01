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

package oxia

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
)

type mockAdminRpcClient struct {
	listDataServersResponse *proto.ListDataServersResponse
	listDataServersCtx      context.Context
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
	patchNamespaceReq       *proto.PatchNamespaceRequest
	patchNamespaceErr       error
	deleteNamespaceResp     *proto.DeleteNamespaceResponse
	deleteNamespaceErr      error
	listNamespacesResp      *proto.ListNamespacesResponse
	listNamespacesErr       error
	getNamespaceResp        *proto.GetNamespaceResponse
	getNamespaceErr         error
}

func (m *mockAdminRpcClient) ListDataServers(ctx context.Context, _ *proto.ListDataServersRequest, _ ...grpc.CallOption) (*proto.ListDataServersResponse, error) {
	m.listDataServersCtx = ctx
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

func (m *mockAdminRpcClient) PatchNamespace(
	_ context.Context,
	req *proto.PatchNamespaceRequest,
	_ ...grpc.CallOption,
) (*proto.PatchNamespaceResponse, error) {
	m.patchNamespaceReq = req
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
	clients     map[string]proto.OxiaAdminClient
	targets     []string
	err         error
}

func (m *mockAdminClientPool) Close() error {
	return nil
}

func (m *mockAdminClientPool) GetClientRpc(string) (proto.OxiaClientClient, error) {
	return nil, errors.New("unexpected GetClientRpc call")
}

func (m *mockAdminClientPool) GetHealthRpc(string) (grpc_health_v1.HealthClient, error) {
	return nil, errors.New("unexpected GetHealthRpc call")
}

func (m *mockAdminClientPool) GetCoordinationRpc(string) (proto.OxiaCoordinationClient, error) {
	return nil, errors.New("unexpected GetCoordinationRpc call")
}

func (m *mockAdminClientPool) GetReplicationRpc(string) (proto.OxiaLogReplicationClient, error) {
	return nil, errors.New("unexpected GetReplicationRpc call")
}

func (m *mockAdminClientPool) GetAminRpc(target string) (proto.OxiaAdminClient, error) {
	m.targets = append(m.targets, target)
	if m.clients != nil {
		return m.clients[target], m.err
	}
	return m.adminClient, m.err
}

func (m *mockAdminClientPool) Clear(string) {}

var _ rpc.ClientPool = (*mockAdminClientPool)(nil)

type adminContextKey struct{}

func TestAdminClientListDataServersPassesContext(t *testing.T) {
	adminClient := &mockAdminRpcClient{
		listDataServersResponse: &proto.ListDataServersResponse{},
	}
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: adminClient,
		},
	}
	ctx := context.WithValue(context.Background(), adminContextKey{}, "request")

	_, err := admin.ListDataServers(ctx)

	require.NoError(t, err)
	assert.Equal(t, "request", adminClient.listDataServersCtx.Value(adminContextKey{}))
}

func TestAdminClientListDataServersMapsGrpcErrors(t *testing.T) {
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				listDataServersErr: grpcstatus.Error(codes.NotFound, "data server not found"),
			},
		},
	}

	_, err := admin.ListDataServers(context.Background())
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

	_, err := admin.ListDataServers(context.Background())
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

	_, err := admin.ListDataServers(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnauthorized)
	assert.Equal(t, codes.PermissionDenied, grpcstatus.Code(err))

	admin.clientPool = &mockAdminClientPool{
		adminClient: &mockAdminRpcClient{
			listDataServersErr: grpcstatus.Error(codes.Unauthenticated, "missing token"),
		},
	}

	_, err = admin.ListDataServers(context.Background())
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
					DataServers: []*proto.DataServerView{
						{
							DataServer: &proto.DataServer{
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
		},
	}

	dataServers, err := admin.ListDataServers(context.Background())
	require.NoError(t, err)
	require.Len(t, dataServers, 1)
	dataServer := dataServers[0].GetDataServer()
	require.NotNil(t, dataServer.GetIdentity())
	require.NotNil(t, dataServer.GetIdentity().Name)
	assert.Equal(t, serverName, dataServer.GetIdentity().GetName())
	assert.Equal(t, "public-1", dataServer.GetIdentity().GetPublic())
	assert.Equal(t, "internal-1", dataServer.GetIdentity().GetInternal())
}

func TestAdminClientListDataServersRedirectsToCoordinatorLeader(t *testing.T) {
	serverName := "server-1"
	pool := &mockAdminClientPool{
		clients: map[string]proto.OxiaAdminClient{
			"coordinator-0:6651": &mockAdminRpcClient{
				listDataServersErr: constant.IntoGrpcStatusError(
					constant.ErrNodeIsNotLeader,
					constant.WithCoordinatorLeaderHint("coordinator-1:6651"),
				),
			},
			"coordinator-1:6651": &mockAdminRpcClient{
				listDataServersResponse: &proto.ListDataServersResponse{
					DataServers: []*proto.DataServerView{
						{
							DataServer: &proto.DataServer{
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
		},
	}
	admin := &adminClientImpl{
		adminAddr:  "coordinator-0:6651",
		clientPool: pool,
	}

	dataServers, err := admin.ListDataServers(context.Background())
	require.NoError(t, err)
	require.Len(t, dataServers, 1)
	assert.Equal(t, serverName, dataServers[0].GetDataServer().GetNameOrDefault())
	assert.Equal(t, []string{"coordinator-0:6651", "coordinator-1:6651"}, pool.targets)
}

func TestAdminClientGetDataServerReturnsResponse(t *testing.T) {
	serverName := "server-1"
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				getDataServerResponse: &proto.GetDataServerResponse{
					DataServer: &proto.DataServerView{
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
		},
	}

	dataServer, err := admin.GetDataServer(context.Background(), serverName)
	require.NoError(t, err)
	require.NotNil(t, dataServer)
	require.NotNil(t, dataServer.GetDataServer().GetIdentity())
	require.NotNil(t, dataServer.GetDataServer().GetIdentity().Name)
	assert.Equal(t, serverName, dataServer.GetDataServer().GetIdentity().GetName())
	assert.Equal(t, "public-1", dataServer.GetDataServer().GetIdentity().GetPublic())
	assert.Equal(t, "internal-1", dataServer.GetDataServer().GetIdentity().GetInternal())
	assert.Equal(t, map[string]string{"rack": "rack-1"}, dataServer.GetDataServer().GetMetadata().GetLabels())
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

	dataServer, err := admin.CreateDataServer(context.Background(), &proto.DataServer{
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

	dataServer, err := admin.PatchDataServer(context.Background(), &proto.DataServer{
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

	dataServer, err := admin.DeleteDataServer(context.Background(), serverName)
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

	namespace, err := admin.CreateNamespace(context.Background(), &proto.Namespace{Name: "ns-1"})
	require.NoError(t, err)
	require.NotNil(t, namespace)
	assert.Equal(t, "ns-1", namespace.GetName())
	assert.EqualValues(t, 4, namespace.GetInitialShardCount())
	assert.EqualValues(t, 3, namespace.GetReplicationFactor())
	assert.Equal(t, proto.KeySortingType_NATURAL.String(), namespace.GetKeySorting())
}

func TestAdminClientPatchNamespaceReturnsResponse(t *testing.T) {
	notificationsEnabled := false
	adminClient := &mockAdminRpcClient{
		patchNamespaceResp: &proto.PatchNamespaceResponse{
			Namespace: &proto.Namespace{
				Name:                 "ns-1",
				InitialShardCount:    4,
				ReplicationFactor:    3,
				NotificationsEnabled: &notificationsEnabled,
				KeySorting:           "natural",
			},
		},
	}
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: adminClient,
		},
	}

	namespace, err := admin.PatchNamespace(context.Background(), &proto.Namespace{Name: "ns-1", NotificationsEnabled: &notificationsEnabled})
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

	namespace, err := admin.DeleteNamespace(context.Background(), "ns-1")
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
					Namespaces: []*proto.NamespaceView{
						{
							Namespace: &proto.Namespace{
								Name:              "ns-1",
								InitialShardCount: 4,
								ReplicationFactor: 3,
							},
							NamespaceStatus: &proto.NamespaceStatus{},
						},
					},
				},
			},
		},
	}

	namespaces, err := admin.ListNamespaces(context.Background())
	require.NoError(t, err)
	require.Len(t, namespaces, 1)
	assert.Equal(t, "ns-1", namespaces[0].GetNamespace().GetName())
	assert.EqualValues(t, 4, namespaces[0].GetNamespace().GetInitialShardCount())
	assert.EqualValues(t, 3, namespaces[0].GetNamespace().GetReplicationFactor())
	assert.NotNil(t, namespaces[0].GetNamespaceStatus())
}

func TestAdminClientGetNamespaceReturnsResponse(t *testing.T) {
	admin := &adminClientImpl{
		adminAddr: "admin-addr",
		clientPool: &mockAdminClientPool{
			adminClient: &mockAdminRpcClient{
				getNamespaceResp: &proto.GetNamespaceResponse{
					Namespace: &proto.NamespaceView{
						Namespace: &proto.Namespace{
							Name:              "ns-1",
							InitialShardCount: 4,
							ReplicationFactor: 3,
							KeySorting:        proto.KeySortingType_NATURAL.String(),
						},
						NamespaceStatus: &proto.NamespaceStatus{},
					},
				},
			},
		},
	}

	namespace, err := admin.GetNamespace(context.Background(), "ns-1")
	require.NoError(t, err)
	require.NotNil(t, namespace)
	assert.Equal(t, "ns-1", namespace.GetNamespace().GetName())
	assert.EqualValues(t, 4, namespace.GetNamespace().GetInitialShardCount())
	assert.EqualValues(t, 3, namespace.GetNamespace().GetReplicationFactor())
	assert.Equal(t, proto.KeySortingType_NATURAL.String(), namespace.GetNamespace().GetKeySorting())
	assert.NotNil(t, namespace.GetNamespaceStatus())
}

func TestWrapAdminErrorPreservesCause(t *testing.T) {
	cause := grpcstatus.Error(codes.InvalidArgument, "invalid request")

	err := mapAdminError(cause)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknown)
	assert.True(t, errors.Is(err, cause))
}
