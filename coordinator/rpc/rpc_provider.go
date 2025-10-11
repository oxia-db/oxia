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
	"io"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/proto"
)

const rpcTimeout = 30 * time.Second

type Provider interface {
	PushShardAssignments(ctx context.Context, node model.Server) (proto.OxiaCoordination_PushShardAssignmentsClient, error)
	NewTerm(ctx context.Context, node model.Server, req *proto.NewTermRequest) (*proto.NewTermResponse, error)
	BecomeLeader(ctx context.Context, node model.Server, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)
	AddFollower(ctx context.Context, node model.Server, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error)
	GetStatus(ctx context.Context, node model.Server, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
	DeleteShard(ctx context.Context, node model.Server, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)

	GetHealthClient(node model.Server) (grpc_health_v1.HealthClient, io.Closer, error)

	ClearPooledConnections(node model.Server)
}

type rpcProvider struct {
	pool rpc.ClientPool
}

func NewRpcProvider(pool rpc.ClientPool) Provider {
	return &rpcProvider{pool: pool}
}

func (r *rpcProvider) PushShardAssignments(ctx context.Context, node model.Server) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	return client.PushShardAssignments(ctx)
}

func (r *rpcProvider) NewTerm(ctx context.Context, node model.Server, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.NewTerm(ctx, req)
}

func (r *rpcProvider) BecomeLeader(ctx context.Context, node model.Server, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.BecomeLeader(ctx, req)
}

func (r *rpcProvider) AddFollower(ctx context.Context, node model.Server, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.AddFollower(ctx, req)
}

func (r *rpcProvider) GetStatus(ctx context.Context, node model.Server, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.GetStatus(ctx, req)
}

func (r *rpcProvider) DeleteShard(ctx context.Context, node model.Server, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	client, err := r.pool.GetCoordinationRpc(node.Internal)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	return client.DeleteShard(ctx, req)
}

func (r *rpcProvider) GetHealthClient(node model.Server) (grpc_health_v1.HealthClient, io.Closer, error) {
	return r.pool.GetHealthRpc(node.Internal)
}

func (r *rpcProvider) ClearPooledConnections(node model.Server) {
	r.pool.Clear(node.Internal)
}
