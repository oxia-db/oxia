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
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"

	commonrpc "github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/common/proto"
)

// DefaultTimeout is the timeout of the calls to the data servers. The provider
// applies it to every call but BecomeLeader (see Provider.BecomeLeader).
const DefaultTimeout = 30 * time.Second

type Provider interface {
	io.Closer

	PushShardAssignments(ctx context.Context, node *proto.DataServerIdentity) (proto.OxiaCoordination_PushShardAssignmentsClient, error)
	NewTerm(ctx context.Context, node *proto.DataServerIdentity, req *proto.NewTermRequest) (*proto.NewTermResponse, error)
	// BecomeLeader returns once the new leader has committed the entries of its
	// wal through its followers, which can include sending them a snapshot of
	// the whole shard. Only the caller knows how long that can take: the
	// provider doesn't bound it, the caller does through ctx.
	BecomeLeader(ctx context.Context, node *proto.DataServerIdentity, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)
	AddFollower(ctx context.Context, node *proto.DataServerIdentity, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error)
	GetStatus(ctx context.Context, node *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
	DeleteShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)
	Handshake(ctx context.Context, node *proto.DataServerIdentity, req *proto.HandshakeRequest) (*proto.HandshakeResponse, error)
	RemoveObserver(ctx context.Context, node *proto.DataServerIdentity, req *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error)
	FreezeShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error)

	GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error)
}

type rpcProvider struct {
	pool    commonrpc.ClientPool
	timeout time.Duration
}

func NewRpcProvider(tlsConf *tls.Config, instanceID string) Provider {
	return NewRpcProviderWithTimeout(tlsConf, instanceID, DefaultTimeout)
}

// NewRpcProviderWithTimeout is NewRpcProvider, with the timeout that bounds the
// calls to the data servers instead of DefaultTimeout.
func NewRpcProviderWithTimeout(tlsConf *tls.Config, instanceID string, timeout time.Duration) Provider {
	return &rpcProvider{
		pool: commonrpc.NewClientPool(tlsConf, nil, commonrpc.MetadataInjectionDialOptions(func() map[string]string {
			return map[string]string{
				constant.MetadataInstanceId: instanceID,
			}
		})...),
		timeout: timeout,
	}
}

func (r *rpcProvider) Close() error {
	return r.pool.Close()
}

func (r *rpcProvider) PushShardAssignments(ctx context.Context, node *proto.DataServerIdentity) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	stream, err := client.PushShardAssignments(ctx)
	oxiaErr, _ := constant.FromGrpcError(err)
	return stream, oxiaErr
}

func (r *rpcProvider) NewTerm(ctx context.Context, node *proto.DataServerIdentity, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
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

func (r *rpcProvider) BecomeLeader(ctx context.Context, node *proto.DataServerIdentity, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		oxiaErr, _ := constant.FromGrpcError(err)
		return nil, oxiaErr
	}

	response, err := client.BecomeLeader(ctx, req)
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

func (r *rpcProvider) AddFollower(ctx context.Context, node *proto.DataServerIdentity, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
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

func (r *rpcProvider) GetStatus(ctx context.Context, node *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
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

func (r *rpcProvider) Handshake(ctx context.Context, node *proto.DataServerIdentity, req *proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
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

	// Deprecated GetInfo fallback for older dataservers that do not implement
	// Handshake. Remove this branch in the next major version together with the
	// GetInfo RPC.
	info, legacyErr := client.GetInfo(ctx, &proto.GetInfoRequest{}) //nolint:staticcheck // Deprecated rolling-upgrade fallback for older dataservers.
	if legacyErr != nil {
		oxiaErr, _ := constant.FromGrpcError(legacyErr)
		return nil, oxiaErr
	}
	return &proto.HandshakeResponse{
		Status:            proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND,
		FeaturesSupported: info.FeaturesSupported,
	}, nil
}

func (r *rpcProvider) DeleteShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
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

func (r *rpcProvider) RemoveObserver(ctx context.Context, node *proto.DataServerIdentity, req *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error) {
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

func (r *rpcProvider) FreezeShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
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

func (r *rpcProvider) GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error) {
	return r.pool.GetHealthRpc(node.Internal)
}
