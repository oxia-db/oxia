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

package mockutils

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/common/feature"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/oxiad/common/logging"

	"github.com/oxia-db/oxia/common/proto"
)

func init() {
	logging.ConfigureLogger()
}

var (
	ErrNotImplement = errors.New("not implement")
)

type ShardAssignmentsProvider struct {
	sync.Mutex
	cond    concurrent.ConditionContext
	current *proto.ShardAssignments
}

func NewShardAssignmentsProvider() *ShardAssignmentsProvider {
	sap := &ShardAssignmentsProvider{
		current: nil,
	}

	sap.cond = concurrent.NewConditionContext(sap)
	return sap
}

func (sap *ShardAssignmentsProvider) Set(value *proto.ShardAssignments) {
	sap.Lock()
	defer sap.Unlock()

	sap.current = value
	sap.cond.Broadcast()
}

func (sap *ShardAssignmentsProvider) WaitForNextUpdate(ctx context.Context, currentValue *proto.ShardAssignments) (*proto.ShardAssignments, error) {
	sap.Lock()
	defer sap.Unlock()

	for pb.Equal(currentValue, sap.current) {
		if err := sap.cond.Wait(ctx); err != nil {
			return nil, err
		}
	}

	return sap.current, nil
}

type NodeAvailabilityListener struct {
	Events chan *proto.DataServerIdentity
}

func NewNodeAvailabilityListener() *NodeAvailabilityListener {
	return &NodeAvailabilityListener{
		Events: make(chan *proto.DataServerIdentity, 100),
	}
}

func (nal *NodeAvailabilityListener) BecameUnavailable(node *proto.DataServerIdentity) {
	nal.Events <- node
}

type PerNodeChannels struct {
	NewTermRequests  chan *proto.NewTermRequest
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

	removeObserverRequests  chan *proto.RemoveObserverRequest
	removeObserverResponses chan struct {
		*proto.RemoveObserverResponse
		error
	}

	freezeShardRequests  chan *proto.FreezeShardRequest
	freezeShardResponses chan struct {
		*proto.FreezeShardResponse
		error
	}

	ShardAssignmentsStream *ShardAssignmentClient
	HealthClient           *HealthClient
	err                    error

	// Feature negotiation support
	supportedFeatures []proto.Feature
	handshakeStatus   proto.HandshakeStatus
	handshakeErr      error
	HandshakeCount    atomic.Int64
	getInfoErr        error
	getInfoCount      atomic.Int64
}

const defaultTimeout = 10 * time.Second
const defaultNegativeTimeout = 1 * time.Second

func (m *PerNodeChannels) ExpectBecomeLeaderRequest(t *testing.T, shard int64, term int64, replicationFactor uint32) {
	t.Helper()

	var r *proto.BecomeLeaderRequest
	select {
	case r = <-m.becomeLeaderRequests:
	case <-time.After(defaultTimeout):
		assert.Fail(t, "did not receive BecomeLeader request in time")
		return
	}

	assert.Equal(t, shard, r.Shard)
	assert.Equal(t, term, r.Term)
	assert.Equal(t, replicationFactor, r.ReplicationFactor)
}

// ExpectBecomeLeaderRequestWithFeatures verifies the BecomeLeader request includes expected negotiated features.
func (m *PerNodeChannels) ExpectBecomeLeaderRequestWithFeatures(t *testing.T, shard int64, term int64, replicationFactor uint32, expectedFeatures []proto.Feature) {
	t.Helper()

	var r *proto.BecomeLeaderRequest
	select {
	case r = <-m.becomeLeaderRequests:
	case <-time.After(defaultTimeout):
		assert.Fail(t, "did not receive BecomeLeader request in time")
		return
	}

	assert.Equal(t, shard, r.Shard)
	assert.Equal(t, term, r.Term)
	assert.Equal(t, replicationFactor, r.ReplicationFactor)
	assert.ElementsMatch(t, expectedFeatures, r.FeaturesSupported, "negotiated features should match")
}

func (m *PerNodeChannels) ExpectNewTermRequest(t *testing.T, shard int64, term int64, notificationsEnabled bool) {
	t.Helper()

	var r *proto.NewTermRequest
	select {
	case r = <-m.NewTermRequests:
	case <-time.After(defaultTimeout):
		assert.Fail(t, "did not receive NewTerm request in time")
		return
	}

	assert.Equal(t, shard, r.Shard)
	assert.Equal(t, term, r.Term)
	assert.Equal(t, notificationsEnabled, r.Options.EnableNotifications)
}

func (m *PerNodeChannels) ExpectNoMoreNewTermRequest(t *testing.T) {
	t.Helper()

	select {
	case <-m.NewTermRequests:
		assert.Fail(t, "should not have received any new term request")
	case <-time.After(defaultNegativeTimeout):
		// expected
	}
}

func (m *PerNodeChannels) ExpectDeleteShardRequest(t *testing.T, shard int64, term int64) {
	t.Helper()

	var r *proto.DeleteShardRequest
	select {
	case r = <-m.deleteShardRequests:
	case <-time.After(defaultTimeout):
		assert.Fail(t, "did not receive DeleteShard request in time")
		return
	}

	assert.Equal(t, shard, r.Shard)
	assert.Equal(t, term, r.Term)
}

func (m *PerNodeChannels) ExpectAddFollowerRequest(t *testing.T, shard int64, term int64) {
	t.Helper()

	var r *proto.AddFollowerRequest
	select {
	case r = <-m.addFollowerRequests:
	case <-time.After(defaultTimeout):
		assert.Fail(t, "did not receive AddFollower request in time")
		return
	}

	assert.Equal(t, shard, r.Shard)
	assert.Equal(t, term, r.Term)
}

func (m *PerNodeChannels) ExpectGetStatusRequest(t *testing.T, shard int64) {
	t.Helper()

	var r *proto.GetStatusRequest
	select {
	case r = <-m.getStatusRequests:
	case <-time.After(defaultTimeout):
		assert.Fail(t, "did not receive GetStatus request in time")
		return
	}

	assert.Equal(t, shard, r.Shard)
}

func (m *PerNodeChannels) NewTermResponse(term int64, offset int64, err error) {
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

//nolint:revive
func (m *PerNodeChannels) GetStatusResponse(term int64, status proto.ServingStatus,
	headOffset int64, commitOffset int64) {
	m.getStatusResponses <- struct {
		*proto.GetStatusResponse
		error
	}{&proto.GetStatusResponse{
		Term:         term,
		Status:       status,
		HeadOffset:   headOffset,
		CommitOffset: commitOffset,
	}, nil}
}

func (m *PerNodeChannels) EnqueueGetStatusError(err error) {
	m.getStatusResponses <- struct {
		*proto.GetStatusResponse
		error
	}{nil, err}
}

func (m *PerNodeChannels) BecomeLeaderResponse(err error) {
	m.becomeLeaderResponses <- struct {
		*proto.BecomeLeaderResponse
		error
	}{&proto.BecomeLeaderResponse{}, err}
}

func (m *PerNodeChannels) DeleteShardResponse(err error) {
	m.deleteShardResponses <- struct {
		*proto.DeleteShardResponse
		error
	}{&proto.DeleteShardResponse{}, err}
}

func (m *PerNodeChannels) AddFollowerResponse(err error) {
	m.addFollowerResponses <- struct {
		*proto.AddFollowerResponse
		error
	}{&proto.AddFollowerResponse{}, err}
}

func (m *PerNodeChannels) RemoveObserverResponse(err error) {
	m.removeObserverResponses <- struct {
		*proto.RemoveObserverResponse
		error
	}{&proto.RemoveObserverResponse{}, err}
}

func (m *PerNodeChannels) FreezeShardResponse(headOffset int64, err error) {
	m.freezeShardResponses <- struct {
		*proto.FreezeShardResponse
		error
	}{&proto.FreezeShardResponse{HeadOffset: headOffset}, err}
}

func newPerNodeChannels() *PerNodeChannels {
	return &PerNodeChannels{
		NewTermRequests: make(chan *proto.NewTermRequest, 100),
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
		deleteShardRequests: make(chan *proto.DeleteShardRequest, 100),
		deleteShardResponses: make(chan struct {
			*proto.DeleteShardResponse
			error
		}, 100),
		removeObserverRequests: make(chan *proto.RemoveObserverRequest, 100),
		removeObserverResponses: make(chan struct {
			*proto.RemoveObserverResponse
			error
		}, 100),
		freezeShardRequests: make(chan *proto.FreezeShardRequest, 100),
		freezeShardResponses: make(chan struct {
			*proto.FreezeShardResponse
			error
		}, 100),
		ShardAssignmentsStream: newShardAssignmentClient(),
		HealthClient:           newHealthClient(),
		supportedFeatures:      feature.SupportedFeatures(), // Default to current version
		handshakeStatus:        proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND,
	}
}

// SetNodeFeatures sets the features supported by this node (simulates a specific version).
func (m *PerNodeChannels) SetNodeFeatures(features []proto.Feature) {
	m.supportedFeatures = features
}

// SetOldNode simulates an old node that doesn't support the GetInfo RPC.
func (m *PerNodeChannels) SetOldNode() {
	m.handshakeErr = ErrNotImplement
	m.getInfoErr = ErrNotImplement
	m.supportedFeatures = nil
}

type RpcProvider struct {
	sync.Mutex
	channels map[string]*PerNodeChannels
}

func (r *RpcProvider) Close() error {
	return nil
}

func NewRpcProvider() *RpcProvider {
	return &RpcProvider{
		channels: make(map[string]*PerNodeChannels),
	}
}

func (r *RpcProvider) FailNode(node *proto.DataServerIdentity, err error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	n.err = err
}
func (r *RpcProvider) GetInfo(_ context.Context, node *proto.DataServerIdentity, _ *proto.GetInfoRequest) (*proto.GetInfoResponse, error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	n.getInfoCount.Add(1)
	if n.getInfoErr != nil {
		return nil, n.getInfoErr
	}
	return &proto.GetInfoResponse{
		FeaturesSupported: n.supportedFeatures,
	}, nil
}

func (r *RpcProvider) Handshake(_ context.Context, node *proto.DataServerIdentity, _ *proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	n.HandshakeCount.Add(1)
	if n.handshakeErr != nil {
		return nil, n.handshakeErr
	}
	return &proto.HandshakeResponse{
		Status:            n.handshakeStatus,
		FeaturesSupported: n.supportedFeatures,
	}, nil
}

func (r *RpcProvider) RecoverNode(node *proto.DataServerIdentity) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	n.err = nil
}

func (r *RpcProvider) GetNode(node *proto.DataServerIdentity) *PerNodeChannels {
	r.Lock()
	defer r.Unlock()

	return r.getNode(node)
}

func (r *RpcProvider) getNode(node *proto.DataServerIdentity) *PerNodeChannels {
	res, ok := r.channels[node.GetInternal()]
	if ok {
		return res
	}

	res = newPerNodeChannels()
	r.channels[node.GetInternal()] = res
	return res
}

func (r *RpcProvider) PushShardAssignments(ctx context.Context, node *proto.DataServerIdentity) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	r.Lock()
	defer r.Unlock()

	n := r.getNode(node)
	if n.err != nil {
		return nil, n.err
	}
	n.ShardAssignmentsStream.ctx = ctx
	return n.ShardAssignmentsStream, nil
}

func (r *RpcProvider) NewTerm(ctx context.Context, node *proto.DataServerIdentity, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.NewTermRequests <- req

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

func (r *RpcProvider) BecomeLeader(ctx context.Context, node *proto.DataServerIdentity, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
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

func (r *RpcProvider) GetStatus(ctx context.Context, node *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
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

func (r *RpcProvider) DeleteShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
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

func (r *RpcProvider) AddFollower(ctx context.Context, node *proto.DataServerIdentity, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
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

func (r *RpcProvider) RemoveObserver(ctx context.Context, node *proto.DataServerIdentity, req *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.removeObserverRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	select {
	case response := <-s.removeObserverResponses:
		return response.RemoveObserverResponse, response.error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("timeout")
	}
}

func (r *RpcProvider) FreezeShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
	r.Lock()

	s := r.getNode(node)
	s.freezeShardRequests <- req

	if s.err != nil {
		r.Unlock()
		return nil, s.err
	}

	r.Unlock()

	// Non-blocking: use a scripted response if the test queued one, otherwise
	// return a default. This keeps best-effort unfreeze calls (from cutover
	// fallback or abort) from stalling tests that don't explicitly script them.
	select {
	case response := <-s.freezeShardResponses:
		return response.FreezeShardResponse, response.error
	default:
		return &proto.FreezeShardResponse{}, nil
	}
}

func (r *RpcProvider) GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error) {
	c := r.GetNode(node).HealthClient
	return c, nil
}

type ShardAssignmentClient struct {
	ctx context.Context
	sync.Mutex

	err     error
	Updates chan *proto.ShardAssignments
}

func newShardAssignmentClient() *ShardAssignmentClient {
	return &ShardAssignmentClient{
		Updates: make(chan *proto.ShardAssignments, 100),
	}
}

func (m *ShardAssignmentClient) SetError(err error) {
	m.Lock()
	defer m.Unlock()

	m.err = err
}

func (m *ShardAssignmentClient) Send(response *proto.ShardAssignments) error {
	m.Lock()
	defer m.Unlock()

	if m.err != nil {
		err := m.err
		m.err = nil
		return err
	}

	m.Updates <- response
	return nil
}

func (m *ShardAssignmentClient) CloseAndRecv() (*proto.CoordinationShardAssignmentsResponse, error) {
	panic(ErrNotImplement)
}

func (m *ShardAssignmentClient) Header() (metadata.MD, error) {
	panic(ErrNotImplement)
}

func (m *ShardAssignmentClient) Trailer() metadata.MD {
	panic(ErrNotImplement)
}

func (m *ShardAssignmentClient) CloseSend() error {
	panic(ErrNotImplement)
}

func (m *ShardAssignmentClient) Context() context.Context {
	return m.ctx
}

func (m *ShardAssignmentClient) SendMsg(any) error {
	panic(ErrNotImplement)
}

func (m *ShardAssignmentClient) RecvMsg(any) error {
	panic(ErrNotImplement)
}

type HealthClient struct {
	sync.Mutex

	status  grpc_health_v1.HealthCheckResponse_ServingStatus
	err     error
	watches []*healthWatchClient
}

func (m *HealthClient) Close() error {
	return nil
}

func newHealthClient() *HealthClient {
	return &HealthClient{
		status:  grpc_health_v1.HealthCheckResponse_SERVING,
		watches: make([]*healthWatchClient, 0),
	}
}

func (m *HealthClient) SetStatus(status grpc_health_v1.HealthCheckResponse_ServingStatus) {
	m.Lock()
	defer m.Unlock()

	m.status = status
	m.err = nil
	for _, w := range m.watches {
		m.sendWatchResponse(w)
	}
}

func (m *HealthClient) SetError(err error) {
	m.Lock()
	defer m.Unlock()

	m.err = err
	for _, w := range m.watches {
		m.sendWatchResponse(w)
	}
}

func (m *HealthClient) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	m.Lock()
	defer m.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return &grpc_health_v1.HealthCheckResponse{Status: m.status}, nil
}

func (m *HealthClient) Watch(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	m.Lock()
	defer m.Unlock()

	w := newHealthWatchClient(ctx)
	m.sendWatchResponse(w)
	m.watches = append(m.watches, w)
	return w, nil
}

func (m *HealthClient) List(ctx context.Context, in *grpc_health_v1.HealthListRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthListResponse, error) {
	m.Lock()
	defer m.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return &grpc_health_v1.HealthListResponse{}, nil
}

func (m *HealthClient) sendWatchResponse(w *healthWatchClient) {
	w.responses <- struct {
		*grpc_health_v1.HealthCheckResponse
		error
	}{&grpc_health_v1.HealthCheckResponse{
		Status: m.status,
	}, m.err}
}

type healthWatchClient struct {
	ctx       context.Context
	responses chan struct {
		*grpc_health_v1.HealthCheckResponse
		error
	}
}

func newHealthWatchClient(ctx context.Context) *healthWatchClient {
	return &healthWatchClient{
		ctx: ctx,
		responses: make(chan struct {
			*grpc_health_v1.HealthCheckResponse
			error
		}, 100),
	}
}

func (m *healthWatchClient) Recv() (*grpc_health_v1.HealthCheckResponse, error) {
	select {
	case r := <-m.responses:
		return r.HealthCheckResponse, r.error
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *healthWatchClient) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (m *healthWatchClient) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *healthWatchClient) CloseSend() error {
	panic("not implemented")
}

func (m *healthWatchClient) Context() context.Context {
	panic("not implemented")
}

func (m *healthWatchClient) SendMsg(msg any) error {
	panic("not implemented")
}

func (m *healthWatchClient) RecvMsg(msg any) error {
	panic("not implemented")
}
