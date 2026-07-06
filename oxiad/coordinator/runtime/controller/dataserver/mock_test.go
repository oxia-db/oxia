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

package dataserver

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"
)

var ErrNotImplement = errors.New("not implement")

type mockShardAssignmentsProvider struct {
	sync.Mutex
	cond    concurrent.ConditionContext
	current *proto.ShardAssignments
}

func newMockShardAssignmentsProvider() *mockShardAssignmentsProvider {
	sap := &mockShardAssignmentsProvider{}
	sap.cond = concurrent.NewConditionContext(sap)
	return sap
}

func (sap *mockShardAssignmentsProvider) set(value *proto.ShardAssignments) {
	sap.Lock()
	defer sap.Unlock()

	sap.current = value
	sap.cond.Broadcast()
}

func (sap *mockShardAssignmentsProvider) WaitForNextUpdate(ctx context.Context, currentValue *proto.ShardAssignments) (*proto.ShardAssignments, error) {
	sap.Lock()
	defer sap.Unlock()

	for pb.Equal(currentValue, sap.current) {
		if err := sap.cond.Wait(ctx); err != nil {
			return nil, err
		}
	}

	return sap.current, nil
}

type mockNodeAvailabilityListener struct {
	events chan *proto.DataServerIdentity
}

func newMockNodeAvailabilityListener() *mockNodeAvailabilityListener {
	return &mockNodeAvailabilityListener{
		events: make(chan *proto.DataServerIdentity, 100),
	}
}

func (nal *mockNodeAvailabilityListener) BecameUnavailable(node *proto.DataServerIdentity) {
	nal.events <- node
}

type mockPerNodeChannels struct {
	shardAssignmentsStream *mockShardAssignmentClient
	healthClient           *mockHealthClient
	err                    error

	handshakeStatus proto.HandshakeStatus
	handshakeErr    error
	handshakeCount  atomic.Int64
}

func newMockPerNodeChannels() *mockPerNodeChannels {
	return &mockPerNodeChannels{
		shardAssignmentsStream: newMockShardAssignmentClient(),
		healthClient:           newMockHealthClient(),
		handshakeStatus:        proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND,
	}
}

type mockRpcProvider struct {
	sync.Mutex
	channels map[string]*mockPerNodeChannels
}

func newMockRpcProvider() *mockRpcProvider {
	return &mockRpcProvider{
		channels: make(map[string]*mockPerNodeChannels),
	}
}

func (r *mockRpcProvider) Close() error {
	return nil
}

func (r *mockRpcProvider) GetNode(node *proto.DataServerIdentity) *mockPerNodeChannels {
	r.Lock()
	defer r.Unlock()

	return r.getNode(node)
}

func (r *mockRpcProvider) getNode(node *proto.DataServerIdentity) *mockPerNodeChannels {
	res, ok := r.channels[node.GetInternal()]
	if ok {
		return res
	}

	res = newMockPerNodeChannels()
	r.channels[node.GetInternal()] = res
	return res
}

func (r *mockRpcProvider) PushShardAssignments(ctx context.Context, node *proto.DataServerIdentity) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	if n.err != nil {
		return nil, n.err
	}
	n.shardAssignmentsStream.ctx = ctx
	return n.shardAssignmentsStream, nil
}

func (r *mockRpcProvider) Handshake(_ context.Context, node *proto.DataServerIdentity, _ *proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	n.handshakeCount.Add(1)
	if n.handshakeErr != nil {
		return nil, n.handshakeErr
	}
	return &proto.HandshakeResponse{
		Status: n.handshakeStatus,
	}, nil
}

func (r *mockRpcProvider) GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error) {
	return r.GetNode(node).healthClient, nil
}

func (*mockRpcProvider) NewTerm(context.Context, *proto.DataServerIdentity, *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	return nil, ErrNotImplement
}

func (*mockRpcProvider) BecomeLeader(context.Context, *proto.DataServerIdentity, *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	return nil, ErrNotImplement
}

func (*mockRpcProvider) AddFollower(context.Context, *proto.DataServerIdentity, *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	return nil, ErrNotImplement
}

func (*mockRpcProvider) GetStatus(context.Context, *proto.DataServerIdentity, *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	return nil, ErrNotImplement
}

func (*mockRpcProvider) DeleteShard(context.Context, *proto.DataServerIdentity, *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	return nil, ErrNotImplement
}

func (*mockRpcProvider) RemoveObserver(context.Context, *proto.DataServerIdentity, *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error) {
	return nil, ErrNotImplement
}

func (*mockRpcProvider) FreezeShard(context.Context, *proto.DataServerIdentity, *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
	return nil, ErrNotImplement
}

type mockShardAssignmentClient struct {
	ctx context.Context
	sync.Mutex

	err     error
	updates chan *proto.ShardAssignments
}

func newMockShardAssignmentClient() *mockShardAssignmentClient {
	return &mockShardAssignmentClient{
		updates: make(chan *proto.ShardAssignments, 100),
	}
}

func (m *mockShardAssignmentClient) SetError(err error) {
	m.Lock()
	defer m.Unlock()

	m.err = err
}

func (m *mockShardAssignmentClient) Send(response *proto.ShardAssignments) error {
	m.Lock()
	defer m.Unlock()

	if m.err != nil {
		err := m.err
		m.err = nil
		return err
	}

	m.updates <- response
	return nil
}

func (*mockShardAssignmentClient) CloseAndRecv() (*proto.CoordinationShardAssignmentsResponse, error) {
	panic(ErrNotImplement)
}

func (*mockShardAssignmentClient) Header() (metadata.MD, error) {
	panic(ErrNotImplement)
}

func (*mockShardAssignmentClient) Trailer() metadata.MD {
	panic(ErrNotImplement)
}

func (*mockShardAssignmentClient) CloseSend() error {
	panic(ErrNotImplement)
}

func (m *mockShardAssignmentClient) Context() context.Context {
	return m.ctx
}

func (*mockShardAssignmentClient) SendMsg(any) error {
	panic(ErrNotImplement)
}

func (*mockShardAssignmentClient) RecvMsg(any) error {
	panic(ErrNotImplement)
}

type mockHealthClient struct {
	sync.Mutex

	status  grpc_health_v1.HealthCheckResponse_ServingStatus
	err     error
	watches []*mockHealthWatchClient
}

func newMockHealthClient() *mockHealthClient {
	return &mockHealthClient{
		status:  grpc_health_v1.HealthCheckResponse_SERVING,
		watches: make([]*mockHealthWatchClient, 0),
	}
}

func (m *mockHealthClient) SetStatus(status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	m.Lock()
	defer m.Unlock()

	m.status = status
	m.err = nil
	for _, w := range m.watches {
		m.sendWatchResponse(w)
	}
}

func (m *mockHealthClient) SetError(err error) {
	m.Lock()
	defer m.Unlock()

	m.err = err
	for _, w := range m.watches {
		m.sendWatchResponse(w)
	}
}

func (m *mockHealthClient) Check(context.Context, *grpc_health_v1.HealthCheckRequest, ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	m.Lock()
	defer m.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return &grpc_health_v1.HealthCheckResponse{Status: m.status}, nil
}

func (m *mockHealthClient) Watch(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	m.Lock()
	defer m.Unlock()

	w := newMockHealthWatchClient(ctx)
	m.sendWatchResponse(w)
	m.watches = append(m.watches, w)
	return w, nil
}

func (m *mockHealthClient) List(context.Context, *grpc_health_v1.HealthListRequest, ...grpc.CallOption) (*grpc_health_v1.HealthListResponse, error) {
	m.Lock()
	defer m.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return &grpc_health_v1.HealthListResponse{}, nil
}

func (m *mockHealthClient) sendWatchResponse(w *mockHealthWatchClient) {
	w.responses <- struct {
		*grpc_health_v1.HealthCheckResponse
		error
	}{&grpc_health_v1.HealthCheckResponse{
		Status: m.status,
	}, m.err}
}

type mockHealthWatchClient struct {
	ctx       context.Context
	responses chan struct {
		*grpc_health_v1.HealthCheckResponse
		error
	}
}

func newMockHealthWatchClient(ctx context.Context) *mockHealthWatchClient {
	return &mockHealthWatchClient{
		ctx: ctx,
		responses: make(chan struct {
			*grpc_health_v1.HealthCheckResponse
			error
		}, 100),
	}
}

func (m *mockHealthWatchClient) Recv() (*grpc_health_v1.HealthCheckResponse, error) {
	select {
	case r := <-m.responses:
		return r.HealthCheckResponse, r.error
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (*mockHealthWatchClient) Header() (metadata.MD, error) {
	panic(ErrNotImplement)
}

func (*mockHealthWatchClient) Trailer() metadata.MD {
	panic(ErrNotImplement)
}

func (*mockHealthWatchClient) CloseSend() error {
	panic(ErrNotImplement)
}

func (*mockHealthWatchClient) Context() context.Context {
	panic(ErrNotImplement)
}

func (*mockHealthWatchClient) SendMsg(any) error {
	panic(ErrNotImplement)
}

func (*mockHealthWatchClient) RecvMsg(any) error {
	panic(ErrNotImplement)
}
