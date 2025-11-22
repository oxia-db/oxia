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

	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/common/proto"
)

func NewMockServerReplicateStream() *MockServerReplicateStream {
	return &MockServerReplicateStream{
		Requests:  make(chan *proto.Append, 1000),
		Responses: make(chan *proto.Ack, 1000),
	}
}

type MockServerReplicateStream struct {
	MockBase
	Requests  chan *proto.Append
	Responses chan *proto.Ack
}

func (m *MockServerReplicateStream) AddRequest(request *proto.Append) {
	m.Requests <- request
}

func (m *MockServerReplicateStream) GetResponse() *proto.Ack {
	return <-m.Responses
}

func (m *MockServerReplicateStream) Send(response *proto.Ack) error {
	m.Responses <- response
	return nil
}

func (m *MockServerReplicateStream) Recv() (*proto.Append, error) {
	request := <-m.Requests
	return request, nil
}

// Mock of the client side handler

func NewMockRpcClient() *MockRpcClient {
	return &MockRpcClient{
		SendSnapshotStream: NewMockSendSnapshotClientStream(context.Background()),
		AppendReqs:         make(chan *proto.Append, 1000),
		AckResps:           make(chan *proto.Ack, 1000),
		TruncateReqs:       make(chan *proto.TruncateRequest, 1000),
		TruncateResps:      make(chan TruncateResps, 1000),
	}
}

type TruncateResps struct {
	Response *proto.TruncateResponse
	Error    error
}
type MockRpcClient struct {
	MockBase
	SendSnapshotStream *MockSendSnapshotClientStream
	AppendReqs         chan *proto.Append
	AckResps           chan *proto.Ack
	TruncateReqs       chan *proto.TruncateRequest
	TruncateResps      chan TruncateResps
}

func (*MockRpcClient) Close() error {
	return nil
}

func (m *MockRpcClient) Send(request *proto.Append) error {
	m.AppendReqs <- request
	return nil
}

func (m *MockRpcClient) Recv() (*proto.Ack, error) {
	res := <-m.AckResps
	return res, nil
}

func (*MockRpcClient) CloseSend() error {
	return nil
}

func (m *MockRpcClient) GetReplicateStream(context.Context, string, string, int64, int64) (proto.OxiaLogReplication_ReplicateClient, error) {
	return m, nil
}

func (m *MockRpcClient) SendSnapshot(context.Context, string, string, int64, int64) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	return m.SendSnapshotStream, nil
}

func (m *MockRpcClient) Truncate(_ string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	m.TruncateReqs <- req

	// Caller needs to provide Response to the channel

	x := <-m.TruncateResps
	return x.Response, x.Error
}

func NewMockShardAssignmentClientStream() *MockShardAssignmentClientStream {
	r := &MockShardAssignmentClientStream{
		responses: make(chan *proto.ShardAssignments, 1000),
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())
	return r
}

type MockShardAssignmentClientStream struct {
	MockBase
	responses chan *proto.ShardAssignments
	cancel    context.CancelFunc
}

func (m *MockShardAssignmentClientStream) Cancel() {
	m.cancel()
}

func (m *MockShardAssignmentClientStream) GetResponse() *proto.ShardAssignments {
	x := <-m.responses
	return x
}

func (m *MockShardAssignmentClientStream) Send(response *proto.ShardAssignments) error {
	m.responses <- response
	return nil
}

func NewMockShardAssignmentControllerStream() *MockShardAssignmentControllerStream {
	return &MockShardAssignmentControllerStream{
		requests:  make(chan *proto.ShardAssignments, 1000),
		responses: make(chan *proto.CoordinationShardAssignmentsResponse, 1000),
	}
}

type MockShardAssignmentControllerStream struct {
	MockBase
	requests  chan *proto.ShardAssignments
	responses chan *proto.CoordinationShardAssignmentsResponse
}

func (m *MockShardAssignmentControllerStream) GetResponse() *proto.CoordinationShardAssignmentsResponse {
	return <-m.responses
}

func (m *MockShardAssignmentControllerStream) SendAndClose(empty *proto.CoordinationShardAssignmentsResponse) error {
	m.responses <- empty
	return nil
}

func (m *MockShardAssignmentControllerStream) AddRequest(request *proto.ShardAssignments) {
	m.requests <- request
}

func (m *MockShardAssignmentControllerStream) Recv() (*proto.ShardAssignments, error) {
	request := <-m.requests
	return request, nil
}

func NewMockServerSendSnapshotStream() *MockServerSendSnapshotStream {
	return &MockServerSendSnapshotStream{
		Chunks:    make(chan *proto.SnapshotChunk, 1000),
		Responses: make(chan *proto.SnapshotResponse, 1000),
	}
}

type MockServerSendSnapshotStream struct {
	MockBase
	Chunks    chan *proto.SnapshotChunk
	Responses chan *proto.SnapshotResponse
}

func (m *MockServerSendSnapshotStream) AddChunk(chunk *proto.SnapshotChunk) {
	m.Chunks <- chunk
}

func (m *MockServerSendSnapshotStream) GetResponse() *proto.SnapshotResponse {
	return <-m.Responses
}

func (m *MockServerSendSnapshotStream) SendAndClose(empty *proto.SnapshotResponse) error {
	m.Responses <- empty
	return nil
}

func (m *MockServerSendSnapshotStream) Recv() (*proto.SnapshotChunk, error) {
	return <-m.Chunks, nil
}

func NewMockSendSnapshotClientStream(ctx context.Context) *MockSendSnapshotClientStream {
	r := &MockSendSnapshotClientStream{
		Requests: make(chan *proto.SnapshotChunk, 100),
		Response: make(chan *proto.SnapshotResponse, 1),
	}

	r.ctx, r.cancel = context.WithCancel(ctx)
	return r
}

type MockSendSnapshotClientStream struct {
	MockBase
	Requests chan *proto.SnapshotChunk
	Response chan *proto.SnapshotResponse
	cancel   context.CancelFunc
}

func (m *MockSendSnapshotClientStream) Send(chunk *proto.SnapshotChunk) error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case m.Requests <- chunk:
		return nil
	}
}

func (m *MockSendSnapshotClientStream) CloseAndRecv() (*proto.SnapshotResponse, error) {
	m.cancel()
	close(m.Requests)
	return <-m.Response, nil
}

func (m *MockSendSnapshotClientStream) CloseSend() error {
	m.cancel()
	return nil
}

type MockBase struct {
	md  metadata.MD
	ctx context.Context
}

func (*MockBase) SendHeader(_ metadata.MD) error {
	panic("not implemented")
}

func (*MockBase) RecvMsg(any) error {
	panic("not implemented")
}

func (*MockBase) SendMsg(any) error {
	panic("not implemented")
}

func (*MockBase) SetTrailer(metadata.MD) {
	panic("not implemented")
}

func (*MockBase) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (*MockBase) Trailer() metadata.MD {
	panic("not implemented")
}

func (m *MockBase) SetHeader(md metadata.MD) error {
	m.md = md
	return nil
}

func (m *MockBase) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}
