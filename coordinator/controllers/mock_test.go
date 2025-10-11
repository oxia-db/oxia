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

package controllers

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/logging"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/proto"
)

func init() {
	logging.ConfigureLogger()
}

var (
	ErrNotImplement = errors.New("not implement")
)

type mockShardAssignmentsProvider struct {
	sync.Mutex
	cond    concurrent.ConditionContext
	current *proto.ShardAssignments
}

func newMockShardAssignmentsProvider() *mockShardAssignmentsProvider {
	sap := &mockShardAssignmentsProvider{
		current: nil,
	}

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
	events chan model.Server
}

func newMockNodeAvailabilityListener() *mockNodeAvailabilityListener {
	return &mockNodeAvailabilityListener{
		events: make(chan model.Server, 100),
	}
}

func (nal *mockNodeAvailabilityListener) NodeBecameUnavailable(node model.Server) {
	nal.events <- node
}

type mockPerNodeChannels struct {
	newTermRequests  chan *proto.NewTermRequest
	newTermResponses chan struct {
		*proto.NewTermResponse
		error
	}

	becomeLeaderRequests  chan *proto.BecomeLeaderRequest
	becomeLeaderResponses chan struct {
		*proto.BecomeLeaderResponse
		error
	}

	getStatusRequests  chan *proto.GetStatusRequest
	getStatusResponses chan struct {
		*proto.GetStatusResponse
		error
	}

	deleteShardRequests  chan *proto.DeleteShardRequest
	deleteShardResponses chan struct {
		*proto.DeleteShardResponse
		error
	}

	addFollowerRequests  chan *proto.AddFollowerRequest
	addFollowerResponses chan struct {
		*proto.AddFollowerResponse
		error
	}

	shardAssignmentsStream *mockShardAssignmentClient
	healthClient           *mockHealthClient
	err                    error
}

func (m *mockPerNodeChannels) expectBecomeLeaderRequest(t *testing.T, shard int64, term int64, replicationFactor uint32) {
	t.Helper()

	r := <-m.becomeLeaderRequests

	assert.Equal(t, shard, r.Shard)
	assert.Equal(t, term, r.Term)
	assert.Equal(t, replicationFactor, r.ReplicationFactor)
}

func (m *mockPerNodeChannels) expectNewTermRequest(t *testing.T, shard int64, term int64, notificationsEnabled bool) {
	t.Helper()

	r := <-m.newTermRequests

	assert.Equal(t, shard, r.Shard)
	assert.Equal(t, term, r.Term)
	assert.Equal(t, notificationsEnabled, r.Options.EnableNotifications)
}

func (m *mockPerNodeChannels) expectAddFollowerRequest(t *testing.T, shard int64, term int64) {
	t.Helper()

	r := <-m.addFollowerRequests

	assert.Equal(t, shard, r.Shard)
	assert.Equal(t, term, r.Term)
}

func (m *mockPerNodeChannels) NewTermResponse(term int64, offset int64, err error) {
	m.newTermResponses <- struct {
		*proto.NewTermResponse
		error
	}{&proto.NewTermResponse{
		HeadEntryId: &proto.EntryId{
			Term:   term,
			Offset: offset,
		},
	}, err}
}

func (m *mockPerNodeChannels) BecomeLeaderResponse(err error) {
	m.becomeLeaderResponses <- struct {
		*proto.BecomeLeaderResponse
		error
	}{&proto.BecomeLeaderResponse{}, err}
}

func (m *mockPerNodeChannels) AddFollowerResponse(err error) {
	m.addFollowerResponses <- struct {
		*proto.AddFollowerResponse
		error
	}{&proto.AddFollowerResponse{}, err}
}

func newMockPerNodeChannels() *mockPerNodeChannels {
	return &mockPerNodeChannels{
		newTermRequests: make(chan *proto.NewTermRequest, 100),
		newTermResponses: make(chan struct {
			*proto.NewTermResponse
			error
		}, 100),
		becomeLeaderRequests: make(chan *proto.BecomeLeaderRequest, 100),
		becomeLeaderResponses: make(chan struct {
			*proto.BecomeLeaderResponse
			error
		}, 100),
		getStatusRequests: make(chan *proto.GetStatusRequest, 100),
		getStatusResponses: make(chan struct {
			*proto.GetStatusResponse
			error
		}, 100),
		addFollowerRequests: make(chan *proto.AddFollowerRequest, 100),
		addFollowerResponses: make(chan struct {
			*proto.AddFollowerResponse
			error
		}, 100),
		shardAssignmentsStream: newMockShardAssignmentClient(),
		healthClient:           newMockHealthClient(),
	}
}

type mockRpcProvider struct {
	sync.Mutex
	channels map[string]*mockPerNodeChannels
}

func (r *mockRpcProvider) ClearPooledConnections(node model.Server) {
}

func newMockRpcProvider() *mockRpcProvider {
	return &mockRpcProvider{
		channels: make(map[string]*mockPerNodeChannels),
	}
}

func (r *mockRpcProvider) FailNode(node model.Server, err error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	n.err = err
}

func (r *mockRpcProvider) RecoverNode(node model.Server) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	n.err = nil
}

func (r *mockRpcProvider) GetNode(node model.Server) *mockPerNodeChannels {
	r.Lock()
	defer r.Unlock()

	return r.getNode(node)
}

func (r *mockRpcProvider) getNode(node model.Server) *mockPerNodeChannels {
	res, ok := r.channels[node.Internal]
	if ok {
		return res
	}

	res = newMockPerNodeChannels()
	r.channels[node.Internal] = res
	return res
}

func (r *mockRpcProvider) PushShardAssignments(ctx context.Context, node model.Server) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	if n.err != nil {
		return nil, n.err
	}
	n.shardAssignmentsStream.ctx = ctx
	return n.shardAssignmentsStream, nil
}

func (r *mockRpcProvider) NewTerm(ctx context.Context, node model.Server, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.newTermRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	select {
	case response := <-s.newTermResponses:
		return response.NewTermResponse, response.error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("timeout")
	}
}

func (r *mockRpcProvider) BecomeLeader(ctx context.Context, node model.Server, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.becomeLeaderRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	select {
	case response := <-s.becomeLeaderResponses:
		return response.BecomeLeaderResponse, response.error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("timeout")
	}
}

func (r *mockRpcProvider) GetStatus(ctx context.Context, node model.Server, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.getStatusRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	select {
	case response := <-s.getStatusResponses:
		return response.GetStatusResponse, response.error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("timeout")
	}
}

func (r *mockRpcProvider) DeleteShard(ctx context.Context, node model.Server, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.deleteShardRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	select {
	case response := <-s.deleteShardResponses:
		return response.DeleteShardResponse, response.error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("timeout")
	}
}

func (r *mockRpcProvider) AddFollower(ctx context.Context, node model.Server, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.addFollowerRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	select {
	case response := <-s.addFollowerResponses:
		return response.AddFollowerResponse, response.error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("timeout")
	}
}

func (r *mockRpcProvider) GetHealthClient(node model.Server) (grpc_health_v1.HealthClient, io.Closer, error) {
	c := r.GetNode(node).healthClient
	return c, c, nil
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

func (m *mockShardAssignmentClient) CloseAndRecv() (*proto.CoordinationShardAssignmentsResponse, error) {
	panic(ErrNotImplement)
}

func (m *mockShardAssignmentClient) Header() (metadata.MD, error) {
	panic(ErrNotImplement)
}

func (m *mockShardAssignmentClient) Trailer() metadata.MD {
	panic(ErrNotImplement)
}

func (m *mockShardAssignmentClient) CloseSend() error {
	panic(ErrNotImplement)
}

func (m *mockShardAssignmentClient) Context() context.Context {
	return m.ctx
}

func (m *mockShardAssignmentClient) SendMsg(any) error {
	panic(ErrNotImplement)
}

func (m *mockShardAssignmentClient) RecvMsg(any) error {
	panic(ErrNotImplement)
}

type mockHealthClient struct {
	sync.Mutex

	status  grpc_health_v1.HealthCheckResponse_ServingStatus
	err     error
	watches []*mockHealthWatchClient
}

func (m *mockHealthClient) Close() error {
	return nil
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

func (m *mockHealthClient) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	m.Lock()
	defer m.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return &grpc_health_v1.HealthCheckResponse{Status: m.status}, nil
}

func (m *mockHealthClient) Watch(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	m.Lock()
	defer m.Unlock()

	w := newMockHealthWatchClient(ctx)
	m.sendWatchResponse(w)
	m.watches = append(m.watches, w)
	return w, nil
}

func (m *mockHealthClient) List(ctx context.Context, in *grpc_health_v1.HealthListRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthListResponse, error) {
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

func (m *mockHealthWatchClient) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (m *mockHealthWatchClient) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *mockHealthWatchClient) CloseSend() error {
	panic("not implemented")
}

func (m *mockHealthWatchClient) Context() context.Context {
	panic("not implemented")
}

func (m *mockHealthWatchClient) SendMsg(msg any) error {
	panic("not implemented")
}

func (m *mockHealthWatchClient) RecvMsg(msg any) error {
	panic("not implemented")
}
