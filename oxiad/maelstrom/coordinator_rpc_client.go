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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/proto"
)

var (
	ErrNotImplement = errors.New("not implement")
)

type maelstromCoordinatorRpcProvider struct {
	sync.Mutex

	dispatcher        *dispatcher
	assignmentStreams map[int64]*maelstromShardAssignmentClient
}

func (m *maelstromCoordinatorRpcProvider) ClearPooledConnections(node model.Server) {
}

func newRpcProvider(dispatcher *dispatcher) rpc.Provider {
	return &maelstromCoordinatorRpcProvider{
		dispatcher:        dispatcher,
		assignmentStreams: map[int64]*maelstromShardAssignmentClient{},
	}
}

func (m *maelstromCoordinatorRpcProvider) PushShardAssignments(ctx context.Context, node model.Server) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	return newShardAssignmentClient(ctx, m, node.Internal), nil
}

func (m *maelstromCoordinatorRpcProvider) NewTerm(ctx context.Context, node model.Server, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.Internal, MsgTypeNewTermRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.NewTermResponse), nil
}

func (m *maelstromCoordinatorRpcProvider) BecomeLeader(ctx context.Context, node model.Server, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.Internal, MsgTypeBecomeLeaderRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.BecomeLeaderResponse), nil
}

func (m *maelstromCoordinatorRpcProvider) AddFollower(ctx context.Context, node model.Server, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.Internal, MsgTypeAddFollowerRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.AddFollowerResponse), nil
}

func (m *maelstromCoordinatorRpcProvider) GetStatus(ctx context.Context, node model.Server, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.Internal, MsgTypeGetStatusRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.GetStatusResponse), nil
}

func (m *maelstromCoordinatorRpcProvider) DeleteShard(ctx context.Context, node model.Server, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.Internal, MsgTypeDeleteShardRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.DeleteShardResponse), nil
}
func (m *maelstromCoordinatorRpcProvider) GetInfo(ctx context.Context, node model.Server, req *proto.GetInfoRequest) (*proto.GetInfoResponse, error) {
	return &proto.GetInfoResponse{
		FeaturesSupported: make([]proto.Feature, 0),
	}, nil
}

func (m *maelstromCoordinatorRpcProvider) GetHealthClient(node model.Server) (grpc_health_v1.HealthClient, io.Closer, error) {
	c := &maelstromHealthCheckClient{
		provider: m,
		node:     node.Internal,
	}

	return c, c, nil
}

type maelstromShardAssignmentClient struct {
	BaseStream

	provider *maelstromCoordinatorRpcProvider
	node     string
	streamId int64

	ctx    context.Context
	cancel context.CancelFunc
}

func newShardAssignmentClient(ctx context.Context, provider *maelstromCoordinatorRpcProvider, node string) proto.OxiaCoordination_PushShardAssignmentsClient {
	sac := &maelstromShardAssignmentClient{
		provider: provider,
		node:     node,
		streamId: msgIdGenerator.Add(1),
	}

	sac.ctx, sac.cancel = context.WithCancel(ctx)

	provider.Lock()
	defer provider.Unlock()
	provider.assignmentStreams[sac.streamId] = sac

	return sac
}

func (m *maelstromShardAssignmentClient) Send(response *proto.ShardAssignments) error {
	m.provider.dispatcher.currentLeader = response.Namespaces[constant.DefaultNamespace].Assignments[0].Leader
	req := &Message[OxiaStreamMessage]{
		Src:  thisNode,
		Dest: m.node,
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{
				Type:  MsgTypeShardAssignmentsResponse,
				MsgId: msgIdGenerator.Add(1),
			},
			OxiaMsg:  toJSON(response),
			StreamId: m.streamId,
		},
	}

	b, _ := json.Marshal(req)
	fmt.Fprintln(os.Stdout, string(b))
	return nil
}

func (m *maelstromShardAssignmentClient) Context() context.Context {
	return m.ctx
}

func (*maelstromShardAssignmentClient) CloseAndRecv() (*proto.CoordinationShardAssignmentsResponse, error) {
	return &proto.CoordinationShardAssignmentsResponse{}, nil
}

type BaseStream struct {
}

func (*BaseStream) Header() (metadata.MD, error) {
	panic(ErrNotImplement)
}

func (*BaseStream) Trailer() metadata.MD {
	panic(ErrNotImplement)
}

func (*BaseStream) CloseSend() error {
	return nil
}

func (*BaseStream) Context() context.Context {
	return context.Background()
}

func (*BaseStream) SendMsg(any) error {
	panic(ErrNotImplement)
}

func (*BaseStream) RecvMsg(any) error {
	panic(ErrNotImplement)
}

func (*maelstromReplicateServerStream) SendHeader(metadata.MD) error {
	panic(ErrNotImplement)
}

func (*maelstromReplicateServerStream) SetTrailer(metadata.MD) {
	panic(ErrNotImplement)
}

type maelstromHealthCheckClient struct {
	node     string
	provider *maelstromCoordinatorRpcProvider
}

type maelstromHealthCheckClientStream struct {
	BaseStream
	sentFirst bool
	ctx       context.Context
}

func (m *maelstromHealthCheckClient) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	if _, err := m.provider.dispatcher.RpcRequest(ctx, m.node, MsgTypeHealthCheck, &proto.GetStatusRequest{}); err != nil {
		// If we're failing the health-check, also close the assignments stream
		m.provider.Lock()
		defer m.provider.Unlock()
		for sid, s := range m.provider.assignmentStreams {
			if s.node == m.node {
				s.cancel()
				delete(m.provider.assignmentStreams, sid)
			}
		}
		return nil, err
	}

	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (m *maelstromHealthCheckClient) Close() error {
	return nil
}

func (m *maelstromHealthCheckClient) Watch(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	if _, err := m.Check(ctx, in, opts...); err != nil {
		return nil, err
	}

	return &maelstromHealthCheckClientStream{
		ctx: ctx,
	}, nil
}

func (m *maelstromHealthCheckClient) List(ctx context.Context, in *grpc_health_v1.HealthListRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthListResponse, error) {
	if _, err := m.Check(ctx, &grpc_health_v1.HealthCheckRequest{}, opts...); err != nil {
		return nil, err
	}
	return &grpc_health_v1.HealthListResponse{}, nil
}

func (m *maelstromHealthCheckClientStream) Recv() (*grpc_health_v1.HealthCheckResponse, error) {
	if !m.sentFirst {
		m.sentFirst = true
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
	}

	<-m.ctx.Done()
	return nil, m.ctx.Err()
}
