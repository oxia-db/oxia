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

const rpcTimeout = 30 * time.Second

type Provider interface {
	io.Closer

	PushShardAssignments(ctx context.Context, node *proto.DataServerIdentity) (proto.OxiaCoordination_PushShardAssignmentsClient, error)
	NewTerm(ctx context.Context, node *proto.DataServerIdentity, req *proto.NewTermRequest) (*proto.NewTermResponse, error)
	BecomeLeader(ctx context.Context, node *proto.DataServerIdentity, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)
	AddFollower(ctx context.Context, node *proto.DataServerIdentity, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error)
	GetStatus(ctx context.Context, node *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
	DeleteShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)
	Handshake(ctx context.Context, node *proto.DataServerIdentity, req *proto.HandshakeRequest) (*proto.HandshakeResponse, error)
	RemoveObserver(ctx context.Context, node *proto.DataServerIdentity, req *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error)

	GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error)
}

type rpcProvider struct {
	pool commonrpc.ClientPool
}

func NewRpcProvider(tlsConf *tls.Config, instanceID string) Provider {
	return &rpcProvider{
		pool: commonrpc.NewClientPool(tlsConf, nil, commonrpc.MetadataInjectionDialOptions(func() map[string]string {
			return map[string]string{
				constant.MetadataInstanceId: instanceID,
			}
		})...),
	}
}

func (r *rpcProvider) Close() error {
	return r.pool.Close()
}

func (r *rpcProvider) PushShardAssignments(ctx context.Context, node *proto.DataServerIdentity) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	return client.PushShardAssignments(ctx)
}

func (r *rpcProvider) NewTerm(ctx context.Context, node *proto.DataServerIdentity, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.NewTerm(ctx, req)
}

func (r *rpcProvider) BecomeLeader(ctx context.Context, node *proto.DataServerIdentity, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.BecomeLeader(ctx, req)
}

func (r *rpcProvider) AddFollower(ctx context.Context, node *proto.DataServerIdentity, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.AddFollower(ctx, req)
}

func (r *rpcProvider) GetStatus(ctx context.Context, node *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.GetStatus(ctx, req)
}

func (r *rpcProvider) Handshake(ctx context.Context, node *proto.DataServerIdentity, req *proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	res, err := client.Handshake(ctx, &proto.HandshakeRequest{
		InstanceId: req.InstanceId,
	})
	if grpcstatus.Code(err) != codes.Unimplemented {
		return res, err
	}

	// Deprecated GetInfo fallback for older dataservers that do not implement
	// Handshake. Remove this branch in the next major version together with the
	// GetInfo RPC.
	info, legacyErr := client.GetInfo(ctx, &proto.GetInfoRequest{}) //nolint:staticcheck // Deprecated rolling-upgrade fallback for older dataservers.
	if legacyErr != nil {
		return nil, legacyErr
	}
	return &proto.HandshakeResponse{
		Status:            proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND,
		FeaturesSupported: info.FeaturesSupported,
	}, nil
}

func (r *rpcProvider) DeleteShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.DeleteShard(ctx, req)
}

func (r *rpcProvider) RemoveObserver(ctx context.Context, node *proto.DataServerIdentity, req *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.RemoveObserver(ctx, req)
}

func (r *rpcProvider) GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error) {
	return r.pool.GetHealthRpc(node.Internal)
}
