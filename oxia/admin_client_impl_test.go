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
}

func (m *mockAdminRpcClient) ListDataServers(context.Context, *proto.ListDataServersRequest, ...grpc.CallOption) (*proto.ListDataServersResponse, error) {
	return m.listDataServersResponse, m.listDataServersErr
}

func (*mockAdminRpcClient) ListNamespaces(context.Context, *proto.ListNamespacesRequest, ...grpc.CallOption) (*proto.ListNamespacesResponse, error) {
	panic("unexpected ListNamespaces call")
}

func (*mockAdminRpcClient) ListNodes(context.Context, *proto.ListNodesRequest, ...grpc.CallOption) (*proto.ListNodesResponse, error) {
	panic("unexpected ListNodes call")
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
							Name:            &serverName,
							PublicAddress:   "public-1",
							InternalAddress: "internal-1",
						},
					},
				},
			},
		},
	}

	dataServers, err := admin.ListDataServers()
	require.NoError(t, err)
	require.Len(t, dataServers, 1)
	require.NotNil(t, dataServers[0].Name)
	assert.Equal(t, serverName, *dataServers[0].Name)
	assert.Equal(t, "public-1", dataServers[0].PublicAddress)
	assert.Equal(t, "internal-1", dataServers[0].InternalAddress)
}

func TestWrapAdminErrorPreservesCause(t *testing.T) {
	cause := grpcstatus.Error(codes.InvalidArgument, "invalid request")

	err := mapAdminError(cause)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknown)
	assert.True(t, errors.Is(err, cause))
}
