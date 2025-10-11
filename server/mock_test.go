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

package server

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/proto"
)

func newMockServerReplicateStream() *mockServerReplicateStream {
	return &mockServerReplicateStream{
		requests:  make(chan *proto.Append, 1000),
		responses: make(chan *proto.Ack, 1000),
	}
}

type mockServerReplicateStream struct {
	mockBase
	requests  chan *proto.Append
	responses chan *proto.Ack
}

func (m *mockServerReplicateStream) AddRequest(request *proto.Append) {
	m.requests <- request
}

func (m *mockServerReplicateStream) GetResponse() *proto.Ack {
	return <-m.responses
}

func (m *mockServerReplicateStream) Send(response *proto.Ack) error {
	m.responses <- response
	return nil
}

func (m *mockServerReplicateStream) Recv() (*proto.Append, error) {
	request := <-m.requests
	return request, nil
}

// Mock of the client side handler

func newMockRpcClient() *mockRpcClient {
	return &mockRpcClient{
		sendSnapshotStream: newMockSendSnapshotClientStream(context.Background()),
		appendReqs:         make(chan *proto.Append, 1000),
		ackResps:           make(chan *proto.Ack, 1000),
		truncateReqs:       make(chan *proto.TruncateRequest, 1000),
		truncateResps: make(chan struct {
			*proto.TruncateResponse
			error
		}, 1000),
	}
}

type mockRpcClient struct {
	mockBase
	sendSnapshotStream *mockSendSnapshotClientStream
	appendReqs         chan *proto.Append
	ackResps           chan *proto.Ack
	truncateReqs       chan *proto.TruncateRequest
	truncateResps      chan struct {
		*proto.TruncateResponse
		error
	}
}

func (m *mockRpcClient) Close() error {
	return nil
}

func (m *mockRpcClient) Send(request *proto.Append) error {
	m.appendReqs <- request
	return nil
}

func (m *mockRpcClient) Recv() (*proto.Ack, error) {
	res := <-m.ackResps
	return res, nil
}

func (m *mockRpcClient) CloseSend() error {
	return nil
}

func (m *mockRpcClient) GetReplicateStream(ctx context.Context, follower string, namespace string, shard int64, term int64) (proto.OxiaLogReplication_ReplicateClient, error) {
	return m, nil
}

func (m *mockRpcClient) SendSnapshot(ctx context.Context, follower string, namespace string, shard int64, term int64) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	return m.sendSnapshotStream, nil
}

func (m *mockRpcClient) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	m.truncateReqs <- req

	// Caller needs to provide response to the channel

	x := <-m.truncateResps
	return x.TruncateResponse, x.error
}

func newMockShardAssignmentClientStream() *mockShardAssignmentClientStream {
	r := &mockShardAssignmentClientStream{
		responses: make(chan *proto.ShardAssignments, 1000),
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	return r
}

type mockShardAssignmentClientStream struct {
	mockBase
	responses chan *proto.ShardAssignments
	cancel    context.CancelFunc
}

func (m *mockShardAssignmentClientStream) GetResponse() *proto.ShardAssignments {
	x := <-m.responses
	return x
}

func (m *mockShardAssignmentClientStream) Send(response *proto.ShardAssignments) error {
	m.responses <- response
	return nil
}

func newMockShardAssignmentControllerStream() *mockShardAssignmentControllerStream {
	return &mockShardAssignmentControllerStream{
		requests:  make(chan *proto.ShardAssignments, 1000),
		responses: make(chan *proto.CoordinationShardAssignmentsResponse, 1000),
	}
}

type mockShardAssignmentControllerStream struct {
	mockBase
	requests  chan *proto.ShardAssignments
	responses chan *proto.CoordinationShardAssignmentsResponse
}

func (m *mockShardAssignmentControllerStream) GetResponse() *proto.CoordinationShardAssignmentsResponse {
	return <-m.responses
}

func (m *mockShardAssignmentControllerStream) SendAndClose(empty *proto.CoordinationShardAssignmentsResponse) error {
	m.responses <- empty
	return nil
}

func (m *mockShardAssignmentControllerStream) AddRequest(request *proto.ShardAssignments) {
	m.requests <- request
}

func (m *mockShardAssignmentControllerStream) Recv() (*proto.ShardAssignments, error) {
	request := <-m.requests
	return request, nil
}

func newMockServerSendSnapshotStream() *mockServerSendSnapshotStream {
	return &mockServerSendSnapshotStream{
		chunks:    make(chan *proto.SnapshotChunk, 1000),
		responses: make(chan *proto.SnapshotResponse, 1000),
	}
}

type mockServerSendSnapshotStream struct {
	mockBase
	chunks    chan *proto.SnapshotChunk
	responses chan *proto.SnapshotResponse
}

func (m *mockServerSendSnapshotStream) AddChunk(chunk *proto.SnapshotChunk) {
	m.chunks <- chunk
}

func (m *mockServerSendSnapshotStream) GetResponse() *proto.SnapshotResponse {
	return <-m.responses
}

func (m *mockServerSendSnapshotStream) SendAndClose(empty *proto.SnapshotResponse) error {
	m.responses <- empty
	return nil
}

func (m *mockServerSendSnapshotStream) Recv() (*proto.SnapshotChunk, error) {
	return <-m.chunks, nil
}

func newMockSendSnapshotClientStream(ctx context.Context) *mockSendSnapshotClientStream {
	r := &mockSendSnapshotClientStream{
		requests: make(chan *proto.SnapshotChunk, 100),
		response: make(chan *proto.SnapshotResponse, 1),
	}

	r.ctx, r.cancel = context.WithCancel(ctx)
	return r
}

type mockSendSnapshotClientStream struct {
	mockBase
	requests chan *proto.SnapshotChunk
	response chan *proto.SnapshotResponse
	cancel   context.CancelFunc
}

func (m *mockSendSnapshotClientStream) Send(chunk *proto.SnapshotChunk) error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case m.requests <- chunk:
		return nil
	}
}

func (m *mockSendSnapshotClientStream) CloseAndRecv() (*proto.SnapshotResponse, error) {
	m.cancel()
	close(m.requests)
	return <-m.response, nil
}

func (m *mockSendSnapshotClientStream) CloseSend() error {
	m.cancel()
	return nil
}

type mockBase struct {
	md  metadata.MD
	ctx context.Context
}

func (m *mockBase) SendHeader(_ metadata.MD) error {
	panic("not implemented")
}

func (m *mockBase) RecvMsg(msg any) error {
	panic("not implemented")
}

func (m *mockBase) SendMsg(msg any) error {
	panic("not implemented")
}

func (m *mockBase) SetTrailer(md metadata.MD) {
	panic("not implemented")
}

func (m *mockBase) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (m *mockBase) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *mockBase) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *mockBase) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}
