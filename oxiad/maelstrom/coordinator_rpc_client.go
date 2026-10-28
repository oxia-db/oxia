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

package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/oxia-db/oxia/oxiad/common/health"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/proto"
)

type maelstromCoordinatorRpcProvider struct {
	sync.Mutex

	dispatcher        *dispatcher
	assignmentStreams map[int64]*maelstromShardAssignmentClient

	// The consecutive failed health probes of each data server, and the
	// process that answered its last probe, see probeFailed and
	// probeSucceeded. The health clients update them, under the mutex.
	failedProbes map[string]int
	processes    map[string]int64
}

func (m *maelstromCoordinatorRpcProvider) Close() error {
	return nil
}

func newRpcProvider(dispatcher *dispatcher) rpc.ControllerRPCProvider {
	provider := &maelstromCoordinatorRpcProvider{
		dispatcher:        dispatcher,
		assignmentStreams: map[int64]*maelstromShardAssignmentClient{},
		failedProbes:      map[string]int{},
		processes:         map[string]int64{},
	}
	dispatcher.coordinatorProvider = provider
	return provider
}

// HandleShardAssignmentsAck stops the resends of the shard assignments that a
// data server acknowledged.
func (m *maelstromCoordinatorRpcProvider) HandleShardAssignmentsAck(streamId int64, msgId int64) {
	m.Lock()
	stream, ok := m.assignmentStreams[streamId]
	m.Unlock()

	if ok {
		stream.ack(msgId)
	}
}

func (m *maelstromCoordinatorRpcProvider) PushShardAssignments(ctx context.Context, node *proto.DataServerIdentity) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	return newShardAssignmentClient(ctx, m, node.GetInternal()), nil
}

func (m *maelstromCoordinatorRpcProvider) NewTerm(ctx context.Context, node *proto.DataServerIdentity, req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.GetInternal(), MsgTypeNewTermRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.NewTermResponse), nil
}

func (m *maelstromCoordinatorRpcProvider) BecomeLeader(ctx context.Context, node *proto.DataServerIdentity, req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.GetInternal(), MsgTypeBecomeLeaderRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.BecomeLeaderResponse), nil
}

func (m *maelstromCoordinatorRpcProvider) AddFollower(ctx context.Context, node *proto.DataServerIdentity, req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.GetInternal(), MsgTypeAddFollowerRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.AddFollowerResponse), nil
}

func (m *maelstromCoordinatorRpcProvider) GetStatus(ctx context.Context, node *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.GetInternal(), MsgTypeGetStatusRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.GetStatusResponse), nil
}

func (m *maelstromCoordinatorRpcProvider) DeleteShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	res, err := m.dispatcher.RpcRequest(ctx, node.GetInternal(), MsgTypeDeleteShardRequest, req)
	if err != nil {
		return nil, err
	}

	return res.(*proto.DeleteShardResponse), nil
}
func (m *maelstromCoordinatorRpcProvider) RemoveObserver(ctx context.Context, node *proto.DataServerIdentity, req *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error) {
	return &proto.RemoveObserverResponse{}, nil
}

func (m *maelstromCoordinatorRpcProvider) FreezeShard(ctx context.Context, node *proto.DataServerIdentity, req *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
	return &proto.FreezeShardResponse{}, nil
}

func (m *maelstromCoordinatorRpcProvider) Handshake(ctx context.Context, node *proto.DataServerIdentity, req *proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
	return &proto.HandshakeResponse{
		Status:            proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND,
		FeaturesSupported: make([]proto.Feature, 0),
	}, nil
}

func (m *maelstromCoordinatorRpcProvider) GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error) {
	c := &maelstromHealthCheckClient{
		provider: m,
		node:     node.GetInternal(),
	}

	return c, nil
}

// How often a shard assignments stream resends the assignments that the data
// server hasn't acknowledged yet, see maelstromShardAssignmentClient.Send.
// Well above the round trip of the latencies Maelstrom tests run with (0-20ms
// each way for --latency 10 --latency-dist uniform).
var assignmentsResendInterval = time.Second

type maelstromShardAssignmentClient struct {
	BaseStream

	provider *maelstromCoordinatorRpcProvider
	node     string
	streamId int64

	ctx    context.Context
	cancel context.CancelFunc

	// The last assignments sent, until the data server acks them. Owned by
	// the stream, guarded by unackedMutex: the coordinator sets it in Send,
	// the dispatcher clears it on the ack, and the resend timer reads it.
	unackedMutex sync.Mutex
	unacked      *Message[OxiaStreamMessage]
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

// Send pushes the assignments to the data server, and resends them every
// assignmentsResendInterval until the data server acks them. A gRPC stream
// either delivers what it sends or breaks, and then the coordinator re-opens
// it and sends the assignments again. A Maelstrom message sent across a
// partition is lost instead, while the stream stays open: without the
// resends, the data server would keep routing the requests to the previous
// leader, or to none, until the assignments change again.
func (m *maelstromShardAssignmentClient) Send(response *proto.ShardAssignments) error {
	assignments := response
	if leader, ok := getDefaultNamespaceLeader(assignments); ok {
		m.provider.dispatcher.currentLeader = leader
	}
	oxiaMsg, err := toJSON(response)
	if err != nil {
		return err
	}
	req := &Message[OxiaStreamMessage]{
		Src:  thisNode,
		Dest: m.node,
		Body: OxiaStreamMessage{
			BaseMessageBody: BaseMessageBody{
				Type:  MsgTypeShardAssignmentsResponse,
				MsgId: msgIdGenerator.Add(1),
			},
			OxiaMsg:  oxiaMsg,
			StreamId: m.streamId,
		},
	}

	m.unackedMutex.Lock()
	m.unacked = req
	m.unackedMutex.Unlock()

	m.sendUntilAcked(req.Body.MsgId)
	return nil
}

// sendUntilAcked sends the assignments message msgId and schedules its next
// resend, unless the data server acked it, a later Send replaced it, or the
// stream is closed. It sends under the lock, so that a resend never goes out
// after the assignments that replaced it.
func (m *maelstromShardAssignmentClient) sendUntilAcked(msgId int64) {
	m.unackedMutex.Lock()
	defer m.unackedMutex.Unlock()
	if m.unacked == nil || m.unacked.Body.MsgId != msgId || m.ctx.Err() != nil {
		return
	}

	if err := sendResponse(m.unacked); err != nil {
		slog.Warn(
			"Failed to send the shard assignments, closing the stream",
			slog.String("node", m.node),
			slog.Any("error", err),
		)
		m.cancel()
		return
	}
	time.AfterFunc(assignmentsResendInterval, func() { m.sendUntilAcked(msgId) })
}

func (m *maelstromShardAssignmentClient) ack(msgId int64) {
	m.unackedMutex.Lock()
	defer m.unackedMutex.Unlock()
	if m.unacked != nil && m.unacked.Body.MsgId == msgId {
		m.unacked = nil
	}
}

func (m *maelstromShardAssignmentClient) Context() context.Context {
	return m.ctx
}

func (*maelstromShardAssignmentClient) CloseAndRecv() (*proto.CoordinationShardAssignmentsResponse, error) {
	return &proto.CoordinationShardAssignmentsResponse{}, nil
}

// RecvMsg blocks until the stream context is cancelled. The coordinator drains
// the assignment stream in the background to notice server-side teardown; in
// Maelstrom the stream is torn down by cancelling its context (e.g. when the
// data server stops answering the health probes, see probeFailed), so block
// until then instead of BaseStream's panic stub.
func (m *maelstromShardAssignmentClient) RecvMsg(any) error {
	<-m.ctx.Done()
	return m.ctx.Err()
}

type BaseStream struct {
}

func (*BaseStream) Header() (metadata.MD, error) {
	panic(errs.ErrNotImplemented)
}

func (*BaseStream) Trailer() metadata.MD {
	panic(errs.ErrNotImplemented)
}

func (*BaseStream) CloseSend() error {
	return nil
}

func (*BaseStream) Context() context.Context {
	return context.Background()
}

func (*BaseStream) SendMsg(any) error {
	panic(errs.ErrNotImplemented)
}

func (*BaseStream) RecvMsg(any) error {
	panic(errs.ErrNotImplemented)
}

func (*maelstromReplicateServerStream) SendHeader(metadata.MD) error {
	panic(errs.ErrNotImplemented)
}

func (*maelstromReplicateServerStream) SetTrailer(metadata.MD) {
	panic(errs.ErrNotImplemented)
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
	res, err := m.provider.dispatcher.RpcRequest(ctx, m.node, MsgTypeHealthCheck, &proto.GetStatusRequest{})
	if err != nil {
		m.provider.probeFailed(m.node)
		return nil, err
	}

	m.provider.probeSucceeded(m.node, res.(*wrapperspb.Int64Value).GetValue())
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// probeFailed closes the assignments streams of a data server once its health
// probes failed FailureThreshold times in a row, about as long as the gRPC
// keepalive takes to drop a silent connection. A single lost probe must not
// close them: the coordinator keeps the data server Running, but re-opens a
// stream only after its retry backoff, 5-15s, and a leader elected meanwhile
// would learn its role that late.
func (m *maelstromCoordinatorRpcProvider) probeFailed(node string) {
	m.Lock()
	defer m.Unlock()
	m.failedProbes[node]++
	if m.failedProbes[node] >= health.DefaultCheckPolicy.FailureThreshold {
		m.closeAssignmentStreams(node, "the data server stopped answering the health probes")
	}
}

// probeSucceeded closes the assignments streams of a data server that answers
// from another process: the kill nemesis restarted it, and the new process
// has none of the assignments that the streams already delivered. Over gRPC,
// the restart breaks the connection, and the coordinator re-opens the stream.
func (m *maelstromCoordinatorRpcProvider) probeSucceeded(node string, processId int64) {
	m.Lock()
	defer m.Unlock()
	delete(m.failedProbes, node)
	if previous, ok := m.processes[node]; ok && previous != processId {
		m.closeAssignmentStreams(node, "the data server restarted")
	}
	m.processes[node] = processId
}

// closeAssignmentStreams must be called with the mutex held.
func (m *maelstromCoordinatorRpcProvider) closeAssignmentStreams(node string, reason string) {
	for sid, s := range m.assignmentStreams {
		if s.node == node {
			slog.Info(
				"Closing the shard assignments stream",
				slog.String("node", node),
				slog.Int64("stream-id", sid),
				slog.String("reason", reason),
			)
			s.cancel()
			delete(m.assignmentStreams, sid)
		}
	}
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
