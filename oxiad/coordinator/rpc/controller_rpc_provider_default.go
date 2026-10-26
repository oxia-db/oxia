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
	"crypto/tls"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"

	commonrpc "github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/common/proto"
)

type defaultControllerRPCProvider struct {
	pool    commonrpc.ClientPool
	timeout time.Duration
}

func NewControllerRPCProvider(tlsConf *tls.Config, instanceID string) ControllerRPCProvider {
	return NewControllerRPCProviderWithTimeout(tlsConf, instanceID, commonrpc.DefaultRpcTimeout)
}

// NewControllerRPCProviderWithTimeout is NewControllerRPCProvider, with the timeout that bounds the
// calls to the data servers instead of commonrpc.DefaultRpcTimeout.
func NewControllerRPCProviderWithTimeout(tlsConf *tls.Config, instanceID string, timeout time.Duration) ControllerRPCProvider {
	return &defaultControllerRPCProvider{
		pool: commonrpc.NewClientPool(tlsConf, nil, commonrpc.MetadataInjectionDialOptions(func() map[string]string {
			return map[string]string{
				constant.MetadataInstanceId: instanceID,
			}
		})...),
		timeout: timeout,
	}
}

func (r *defaultControllerRPCProvider) Close() error {
	return r.pool.Close()
}

func (r *defaultControllerRPCProvider) PushShardAssignments(ctx context.Context, node *proto.DataServerIdentity) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	stream, err := client.PushShardAssignments(ctx)
	oxiaErr, _ := constant.FromGrpcError(err)
	return stream, oxiaErr
}

func (r *defaultControllerRPCProvider) NewTerm(ctx context.Context, node *proto.DataServerIdentity, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	response, err := client.NewTerm(ctx, req)
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

func (r *defaultControllerRPCProvider) BecomeLeader(ctx context.Context, node *proto.DataServerIdentity, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	response, err := client.BecomeLeader(ctx, req)
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

func (r *defaultControllerRPCProvider) AddFollower(ctx context.Context, node *proto.DataServerIdentity, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	response, err := client.AddFollower(ctx, req)
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

func (r *defaultControllerRPCProvider) GetStatus(ctx context.Context, node *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	response, err := client.GetStatus(ctx, req)
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

func (r *defaultControllerRPCProvider) Handshake(ctx context.Context, node *proto.DataServerIdentity, req *proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	res, err := client.Handshake(ctx, &proto.HandshakeRequest{
		InstanceId: req.InstanceId,
	})
	if grpcstatus.Code(err) != codes.Unimplemented {
		oxiaErr, _ := constant.FromGrpcError(err)
		return res, oxiaErr
	}
	return getInfo(ctx, client)
}

func (r *defaultControllerRPCProvider) DeleteShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	response, err := client.DeleteShard(ctx, req)
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

func (r *defaultControllerRPCProvider) RemoveObserver(ctx context.Context, node *proto.DataServerIdentity, req *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	response, err := client.RemoveObserver(ctx, req)
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

func (r *defaultControllerRPCProvider) FreezeShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	response, err := client.FreezeShard(ctx, req)
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

func (r *defaultControllerRPCProvider) GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error) {
	return r.pool.GetHealthRpc(node.Internal)
}

// getInfo is the Handshake of the data servers that predate it, which only
// report their features.
//
// Deprecated: remove it in the next major version, together with the GetInfo RPC.
func getInfo(ctx context.Context, client proto.OxiaCoordinationClient) (*proto.HandshakeResponse, error) {
	info, err := client.GetInfo(ctx, &proto.GetInfoRequest{})
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}
	return &proto.HandshakeResponse{
		Status:            proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND,
		FeaturesSupported: info.FeaturesSupported,
	}, nil
}
