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

package rpc

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
)

// invalidTarget is an address that the gRPC client can't parse, so the
// provider fails before sending anything.
const invalidTarget = "invalid\x7f://target"

func TestControllerRPCProviderCallsReachDataServer(t *testing.T) {
	server := &fakeCoordinationServer{}
	node := startFakeCoordinationServer(t, server)
	provider := NewControllerRPCProviderFactory(nil)("instance-0")
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })
	ctx := context.Background()

	newTerm, err := provider.NewTerm(ctx, node, &proto.NewTermRequest{Shard: 1, Term: 2})
	require.NoError(t, err)
	assert.Equal(t, int64(2), newTerm.HeadEntryId.Term)

	_, err = provider.BecomeLeader(ctx, node, &proto.BecomeLeaderRequest{Shard: 1})
	require.NoError(t, err)

	_, err = provider.AddFollower(ctx, node, &proto.AddFollowerRequest{Shard: 1})
	require.NoError(t, err)

	status, err := provider.GetStatus(ctx, node, &proto.GetStatusRequest{Shard: 1})
	require.NoError(t, err)
	assert.Equal(t, int64(7), status.Term)

	_, err = provider.DeleteShard(ctx, node, &proto.DeleteShardRequest{Shard: 1})
	require.NoError(t, err)

	_, err = provider.RemoveObserver(ctx, node, &proto.RemoveObserverRequest{Shard: 1})
	require.NoError(t, err)

	freeze, err := provider.FreezeShard(ctx, node, &proto.FreezeShardRequest{Shard: 1})
	require.NoError(t, err)
	assert.Equal(t, int64(5), freeze.HeadOffset)

	handshake, err := provider.Handshake(ctx, node, &proto.HandshakeRequest{InstanceId: "instance-0"})
	require.NoError(t, err)
	assert.Equal(t, proto.HandshakeStatus_HANDSHAKE_STATUS_BOUND, handshake.Status)

	healthClient, err := provider.GetHealthClient(node)
	require.NoError(t, err)
	healthResponse, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, healthResponse.Status)
}

func TestControllerRPCProviderCallsFailOnInvalidTarget(t *testing.T) {
	provider := NewControllerRPCProvider(nil, "instance-0")
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })
	ctx := context.Background()
	node := &proto.DataServerIdentity{Internal: invalidTarget}

	_, err := provider.PushShardAssignments(ctx, node)
	assert.Error(t, err)
	_, err = provider.NewTerm(ctx, node, &proto.NewTermRequest{})
	assert.Error(t, err)
	_, err = provider.BecomeLeader(ctx, node, &proto.BecomeLeaderRequest{})
	assert.Error(t, err)
	_, err = provider.AddFollower(ctx, node, &proto.AddFollowerRequest{})
	assert.Error(t, err)
	_, err = provider.GetStatus(ctx, node, &proto.GetStatusRequest{})
	assert.Error(t, err)
	_, err = provider.Handshake(ctx, node, &proto.HandshakeRequest{})
	assert.Error(t, err)
	_, err = provider.DeleteShard(ctx, node, &proto.DeleteShardRequest{})
	assert.Error(t, err)
	_, err = provider.RemoveObserver(ctx, node, &proto.RemoveObserverRequest{})
	assert.Error(t, err)
	_, err = provider.FreezeShard(ctx, node, &proto.FreezeShardRequest{})
	assert.Error(t, err)
}

func TestControllerRPCProviderHandshakeFallsBackToGetInfo(t *testing.T) {
	server := &fakeCoordinationServer{
		handshake: func(*proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
			return nil, grpcstatus.Error(codes.Unimplemented, "handshake")
		},
		getInfo: func() (*proto.GetInfoResponse, error) {
			return &proto.GetInfoResponse{FeaturesSupported: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}}, nil
		},
	}
	node := startFakeCoordinationServer(t, server)
	provider := NewControllerRPCProvider(nil, "instance-0")
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })

	res, err := provider.Handshake(context.Background(), node, &proto.HandshakeRequest{InstanceId: "instance-0"})
	require.NoError(t, err)
	assert.Equal(t, proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND, res.Status)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, res.FeaturesSupported)
}

func TestControllerRPCProviderHandshakeFallbackGetInfoFails(t *testing.T) {
	server := &fakeCoordinationServer{
		handshake: func(*proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
			return nil, grpcstatus.Error(codes.Unimplemented, "handshake")
		},
		getInfo: func() (*proto.GetInfoResponse, error) {
			return nil, grpcstatus.Error(codes.Unavailable, "get info")
		},
	}
	node := startFakeCoordinationServer(t, server)
	provider := NewControllerRPCProvider(nil, "instance-0")
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })

	res, err := provider.Handshake(context.Background(), node, &proto.HandshakeRequest{InstanceId: "instance-0"})
	assert.Nil(t, res)
	assert.Equal(t, codes.Unavailable, grpcstatus.Code(err))
}

type fakeCoordinationServer struct {
	proto.UnimplementedOxiaCoordinationServer

	handshake func(req *proto.HandshakeRequest) (*proto.HandshakeResponse, error)
	getInfo   func() (*proto.GetInfoResponse, error)
}

func (*fakeCoordinationServer) NewTerm(_ context.Context, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	return &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: req.Term}}, nil
}

func (*fakeCoordinationServer) BecomeLeader(context.Context, *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	return &proto.BecomeLeaderResponse{}, nil
}

func (*fakeCoordinationServer) AddFollower(context.Context, *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	return &proto.AddFollowerResponse{}, nil
}

func (*fakeCoordinationServer) GetStatus(context.Context, *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	return &proto.GetStatusResponse{Term: 7}, nil
}

func (*fakeCoordinationServer) DeleteShard(context.Context, *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	return &proto.DeleteShardResponse{}, nil
}

func (*fakeCoordinationServer) RemoveObserver(context.Context, *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error) {
	return &proto.RemoveObserverResponse{}, nil
}

func (*fakeCoordinationServer) FreezeShard(context.Context, *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
	return &proto.FreezeShardResponse{HeadOffset: 5}, nil
}

func (f *fakeCoordinationServer) Handshake(_ context.Context, req *proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
	if f.handshake != nil {
		return f.handshake(req)
	}
	return &proto.HandshakeResponse{Status: proto.HandshakeStatus_HANDSHAKE_STATUS_BOUND}, nil
}

func (f *fakeCoordinationServer) GetInfo(context.Context, *proto.GetInfoRequest) (*proto.GetInfoResponse, error) {
	if f.getInfo != nil {
		return f.getInfo()
	}
	return &proto.GetInfoResponse{}, nil
}

func startFakeCoordinationServer(t *testing.T, server proto.OxiaCoordinationServer) *proto.DataServerIdentity {
	t.Helper()

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	proto.RegisterOxiaCoordinationServer(grpcServer, server)
	grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	return &proto.DataServerIdentity{Internal: listener.Addr().String()}
}
