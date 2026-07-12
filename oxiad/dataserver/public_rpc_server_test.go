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
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	oxiadcommonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/follow"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
)

func init() {
	logging.LogJSON = false
	logging.ConfigureLogger()
}

type testAssignmentDispatcher struct {
	initialized      bool
	validAuthorities map[string]bool
	registerErr      error
}

func (*testAssignmentDispatcher) Close() error { return nil }
func (t *testAssignmentDispatcher) Initialized() bool {
	return t.initialized
}
func (*testAssignmentDispatcher) PushShardAssignments(proto.OxiaCoordination_PushShardAssignmentsServer) error {
	panic("unexpected call")
}
func (t *testAssignmentDispatcher) RegisterForUpdates(*proto.ShardAssignmentsRequest, assignment.Client) error {
	return t.registerErr
}
func (*testAssignmentDispatcher) GetLeader(int64) string { return "" }
func (t *testAssignmentDispatcher) HasAuthority(authority string) bool {
	return t.validAuthorities[authority]
}

type testShardsDirector struct {
	getLeader func(int64) (lead.LeaderController, error)
}

func (t *testShardsDirector) Close() error { return nil }
func (t *testShardsDirector) GetLeader(shardId int64) (lead.LeaderController, error) {
	return t.getLeader(shardId)
}
func (*testShardsDirector) GetFollower(int64) (follow.FollowerController, error) {
	panic("unexpected call")
}
func (*testShardsDirector) GetOrCreateLeader(string, int64, *proto.NewTermOptions) (lead.LeaderController, error) {
	panic("unexpected call")
}
func (*testShardsDirector) GetOrCreateFollower(string, int64, int64, *proto.NewTermOptions) (follow.FollowerController, error) {
	panic("unexpected call")
}
func (*testShardsDirector) DeleteShard(*proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	panic("unexpected call")
}
func (*testShardsDirector) GetAllLeaders() []lead.LeaderController { panic("unexpected call") }

var _ controller.ShardsDirector = (*testShardsDirector)(nil)

func TestWriteClientClose(t *testing.T) {
	standaloneServer, err := NewStandalone(NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	// Connect to the standalone dataserver
	conn, err := grpc.NewClient(standaloneServer.ServiceAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "Failed to connect to %s", standaloneServer.ServiceAddr())
	defer conn.Close()

	client := proto.NewOxiaClientClient(conn)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{
		"shard-id":  "0",
		"namespace": "default",
	}))
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stream, err := client.WriteStream(ctx)
	require.NoError(t, err, "Failed to create write stream")

	// Send a Put request
	putReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{
				Key:   "test-key",
				Value: []byte("test-value"),
			},
		},
	}
	err = stream.Send(putReq)
	require.NoError(t, err, "Failed to send put request")

	// Validate the request succeeded
	resp, err := stream.Recv()
	require.NoError(t, err, "Failed to receive response")
	assert.Len(t, resp.Puts, 1)
	assert.Equal(t, proto.Status_OK, resp.Puts[0].Status)

	// Close the client side of the stream, and then expect no more responses
	err = stream.CloseSend()
	require.NoError(t, err, "Failed to close send")

	resp, err = stream.Recv()
	t.Logf("resp %v err %v", resp, err)
	assert.Error(t, err)
	assert.ErrorIs(t, err, io.EOF)
}

type mockWriteStream struct {
	proto.OxiaClient_WriteStreamServer
	requests chan *proto.WriteRequest
	sent     chan *proto.WriteResponse
	sendGate chan struct{}
}

func (m *mockWriteStream) Recv() (*proto.WriteRequest, error) {
	req, ok := <-m.requests
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

func (m *mockWriteStream) Send(response *proto.WriteResponse) error {
	// Simulates gRPC flow control: blocks until the "client" starts reading
	<-m.sendGate
	m.sent <- response
	return nil
}

type mockWriteLeaderController struct {
	lead.LeaderController
	writes chan concurrent.Callback[*proto.WriteResponse]
}

func (m *mockWriteLeaderController) Write(_ context.Context, _ *proto.WriteRequest, cb concurrent.Callback[*proto.WriteResponse]) {
	m.writes <- cb
}

func receiveWithTimeout[T any](t *testing.T, ch <-chan T, msg string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal(msg)
		panic("unreachable")
	}
}

// The write completion callbacks can be invoked under the quorum-ack-tracker lock:
// they must never block on the client stream, otherwise one slow client stalls the
// whole shard. Instead, the stream stops accepting new writes once
// maxWriteStreamPendingWrites responses are outstanding.
func TestWriteStreamSlowClientDoesNotBlockWriteCallbacks(t *testing.T) {
	const extraWrites = 100
	total := maxWriteStreamPendingWrites + extraWrites

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &mockWriteStream{
		requests: make(chan *proto.WriteRequest, total),
		sent:     make(chan *proto.WriteResponse, total),
		sendGate: make(chan struct{}),
	}
	lc := &mockWriteLeaderController{
		writes: make(chan concurrent.Callback[*proto.WriteResponse], total),
	}

	finished := make(chan error, 1)
	pendingWrites := make(chan struct{}, maxWriteStreamPendingWrites)
	responses := make(chan *proto.WriteResponse, maxWriteStreamPendingWrites)
	go processWriteStream(ctx, finished, stream, lc, pendingWrites, responses)
	go sendWriteStreamResponses(ctx, finished, stream, pendingWrites, responses)

	for i := 0; i < total; i++ {
		stream.requests <- &proto.WriteRequest{}
	}
	close(stream.requests)

	// The client is not reading responses (stream.Send is stuck): completing the
	// writes that reached the leader must still not block.
	completedResponses := make([]*proto.WriteResponse, 0, total)
	for i := 0; i < maxWriteStreamPendingWrites; i++ {
		cb := receiveWithTimeout(t, lc.writes, "leader did not receive the expected write")
		response := &proto.WriteResponse{}
		callbackDone := make(chan struct{})
		go func() {
			cb.OnComplete(response)
			close(callbackDone)
		}()
		select {
		case <-callbackDone:
		case <-time.After(10 * time.Second):
			t.Fatal("write callback blocked on a slow client")
		}
		completedResponses = append(completedResponses, response)
	}

	// Backpressure: no further writes are submitted while the client is stalled
	select {
	case <-lc.writes:
		t.Fatal("write submitted beyond the pending-writes cap while the client is stalled")
	case <-time.After(100 * time.Millisecond):
	}

	// Unblock the client: the remaining writes get submitted and every response
	// is delivered, in completion order
	close(stream.sendGate)
	for i := 0; i < extraWrites; i++ {
		cb := receiveWithTimeout(t, lc.writes, "leader did not receive the expected write")
		response := &proto.WriteResponse{}
		cb.OnComplete(response)
		completedResponses = append(completedResponses, response)
	}

	for i := 0; i < total; i++ {
		sent := receiveWithTimeout(t, stream.sent, "response was not sent to the client")
		assert.Same(t, completedResponses[i], sent)
	}
}

func TestPublicHealthCheck(t *testing.T) {
	standaloneServer, err := NewStandalone(NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	defer standaloneServer.Close()

	conn, err := grpc.NewClient(standaloneServer.ServiceAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := grpc_health_v1.NewHealthClient(conn)
	resp, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{
		Service: oxiadcommonrpc.ReadinessProbeService,
	})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)
}

func TestValidateAuthorityRejectsWrongAuthority(t *testing.T) {
	server := &publicRpcServer{
		log:                        slog.Default(),
		authorityValidationEnabled: true,
		assignmentDispatcher: &testAssignmentDispatcher{initialized: true, validAuthorities: map[string]bool{
			"expected-host:6648": true,
		}},
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		":authority": "wrong-host:6648",
	}))

	err := server.validateAuthority(ctx)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, grpcstatus.Code(err))
}

func TestValidateAuthorityReturnsNotInitializedBeforeAssignmentsReady(t *testing.T) {
	server := &publicRpcServer{
		authorityValidationEnabled: true,
		assignmentDispatcher:       &testAssignmentDispatcher{},
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		":authority": "expected-host:6648",
	}))

	err := server.validateAuthority(ctx)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, grpcstatus.Code(err))
}

func TestValidateAuthoritySkippedWhenDisabled(t *testing.T) {
	server := &publicRpcServer{
		assignmentDispatcher: &testAssignmentDispatcher{},
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		":authority": "wrong-host:6648",
	}))

	require.NoError(t, server.validateAuthority(ctx))
}

type testShardAssignmentsServer struct {
	ctx context.Context
}

func (t *testShardAssignmentsServer) Send(*proto.ShardAssignments) error { return nil }
func (t *testShardAssignmentsServer) SetHeader(metadata.MD) error        { return nil }
func (t *testShardAssignmentsServer) SendHeader(metadata.MD) error       { return nil }
func (t *testShardAssignmentsServer) SetTrailer(metadata.MD)             {}
func (t *testShardAssignmentsServer) Context() context.Context           { return t.ctx }
func (t *testShardAssignmentsServer) SendMsg(any) error                  { return nil }
func (t *testShardAssignmentsServer) RecvMsg(any) error                  { return nil }
func (t *testShardAssignmentsServer) SendHeaderV2(protoadapt.MessageV2) error {
	return nil
}
func (t *testShardAssignmentsServer) RecvMsgV2(protoadapt.MessageV2) error {
	return nil
}
func (t *testShardAssignmentsServer) SendMsgV2(protoadapt.MessageV2) error {
	return nil
}

func TestGetShardAssignmentsValidatesAuthority(t *testing.T) {
	server := &publicRpcServer{
		log:                        slog.Default(),
		authorityValidationEnabled: true,
		assignmentDispatcher: &testAssignmentDispatcher{initialized: true, validAuthorities: map[string]bool{
			"expected-host:6648": true,
		}},
	}

	err := server.GetShardAssignments(&proto.ShardAssignmentsRequest{}, &testShardAssignmentsServer{
		ctx: metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			":authority": "wrong-host:6648",
		})),
	})

	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, grpcstatus.Code(err))
}

func TestGetShardAssignmentsSkipsAuthorityValidationWhenDisabled(t *testing.T) {
	server := &publicRpcServer{
		log:                  slog.Default(),
		assignmentDispatcher: &testAssignmentDispatcher{},
	}

	err := server.GetShardAssignments(&proto.ShardAssignmentsRequest{}, &testShardAssignmentsServer{
		ctx: metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			":authority": "wrong-host:6648",
		})),
	})

	require.NoError(t, err)
}

func TestGetShardAssignmentsConvertsRegisterError(t *testing.T) {
	server := &publicRpcServer{
		log:                        slog.Default(),
		authorityValidationEnabled: true,
		assignmentDispatcher: &testAssignmentDispatcher{
			initialized: true,
			validAuthorities: map[string]bool{
				"expected-host:6648": true,
			},
			registerErr: constant.ErrNamespaceNotFound,
		},
	}

	err := server.GetShardAssignments(&proto.ShardAssignmentsRequest{}, &testShardAssignmentsServer{
		ctx: metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			":authority": "expected-host:6648",
		})),
	})

	oxiaErr, _ := constant.FromGrpcError(err)
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, grpcstatus.Code(err))
	assert.ErrorIs(t, oxiaErr, constant.ErrNamespaceNotFound)
}

func TestResolveLeaderValidatesAuthorityBeforeLeaderLookup(t *testing.T) {
	server := &publicRpcServer{
		log:                        slog.Default(),
		authorityValidationEnabled: true,
		shardsDirector: &testShardsDirector{
			getLeader: func(int64) (lead.LeaderController, error) {
				return nil, constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader, constant.WithLeaderHint(1, "leader:6648"))
			},
		},
		assignmentDispatcher: &testAssignmentDispatcher{initialized: true, validAuthorities: map[string]bool{
			"expected-host:6648": true,
		}},
	}

	shardID := int64(1)
	_, err := server.resolveLeader(metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		":authority": "wrong-host:6648",
	})), &shardID)

	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, grpcstatus.Code(err))
}

func TestWarnOnStreamErrorSkipsClientDisconnectErrors(t *testing.T) {
	var buf bytes.Buffer
	server := &publicRpcServer{
		log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	server.warnOnStreamError(context.Background(), "read", errors.New("unexpected failure"))
	assert.Contains(t, buf.String(), "level=WARN")

	// When the client goes away mid-stream, the transport cancels the stream
	// context before the send error reaches the handler.
	doneCtx, cancel := context.WithCancel(context.Background())
	cancel()
	buf.Reset()
	server.warnOnStreamError(doneCtx, "read", grpcstatus.Error(codes.Unavailable, "transport is closing"))
	assert.Empty(t, buf.String())

	buf.Reset()
	server.warnOnStreamError(context.Background(), "read", context.Canceled)
	assert.Empty(t, buf.String())
}
