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
	"fmt"
	"io"
	"log/slog"
	"strings"
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
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/proto/compat"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	oxiadcommonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/follow"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
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
	getLeader           func(int64) (lead.LeaderController, error)
	getFollower         func(int64) (follow.FollowerController, error)
	getOrCreateLeader   func(string, int64) (lead.LeaderController, error)
	getOrCreateFollower func(string, int64, int64) (follow.FollowerController, error)
}

func (t *testShardsDirector) Close() error { return nil }
func (t *testShardsDirector) GetLeader(shardId int64) (lead.LeaderController, error) {
	return t.getLeader(shardId)
}
func (t *testShardsDirector) GetFollower(shardId int64) (follow.FollowerController, error) {
	return t.getFollower(shardId)
}
func (t *testShardsDirector) GetOrCreateLeader(namespace string, shardId int64, _ *proto.NewTermOptions) (lead.LeaderController, error) {
	return t.getOrCreateLeader(namespace, shardId)
}
func (t *testShardsDirector) GetOrCreateFollower(namespace string, shardId int64, term int64, _ *proto.NewTermOptions) (follow.FollowerController, error) {
	return t.getOrCreateFollower(namespace, shardId, term)
}
func (*testShardsDirector) DeleteShard(*proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	panic("unexpected call")
}
func (*testShardsDirector) GetAllLeaders() []lead.LeaderController { panic("unexpected call") }
func (*testShardsDirector) Checkpoint(int64) (*database.Checkpoint, error) {
	panic("unexpected call")
}
func (*testShardsDirector) CommitOffset(int64) (int64, error) { panic("unexpected call") }
func (*testShardsDirector) WithDatabase(int64, uint64, func(string) error) error {
	panic("unexpected call")
}
func (*testShardsDirector) DatabaseDir(string, int64) string { panic("unexpected call") }

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

// Closing the leader controller (the leadership moved, the shard was deleted
// after a split, or the server is shutting down) ends the write streams open on
// it: the client must see a retryable error, so that it retries the in-flight
// writes (on the new leader, or on the children of a split shard) instead of
// failing them.
func TestWriteStreamLeaderControllerClosed(t *testing.T) {
	standaloneServer, err := NewStandalone(NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	defer standaloneServer.Close()

	conn, err := grpc.NewClient(standaloneServer.ServiceAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := proto.NewOxiaClientClient(conn)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(map[string]string{
		"shard-id":  "0",
		"namespace": "default",
	}))
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stream, err := client.WriteStream(ctx)
	require.NoError(t, err)

	// Complete a write first, so the stream is established on the leader controller
	require.NoError(t, stream.Send(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: "test-key", Value: []byte("test-value")}},
	}))
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, resp.Puts, 1)
	assert.Equal(t, proto.Status_OK, resp.Puts[0].Status)

	lc, err := standaloneServer.shardsDirector.GetLeader(0)
	require.NoError(t, err)
	require.NoError(t, lc.Close())

	_, err = stream.Recv()
	oxiaErr, _ := constant.FromGrpcError(err)
	assert.ErrorIs(t, oxiaErr, constant.ErrNodeIsNotLeader)
	assert.True(t, constant.IsRetryable(oxiaErr), "the client does not retry on: %v", err)
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

	requests := make(chan *proto.WriteRequest, total)
	sent := make(chan *proto.WriteResponse, total)
	sendGate := make(chan struct{})
	stream := &testStream[proto.WriteRequest, proto.WriteResponse]{
		recv: func() (*proto.WriteRequest, error) {
			req, ok := <-requests
			if !ok {
				return nil, io.EOF
			}
			return req, nil
		},
		send: func(response *proto.WriteResponse) error {
			// Simulates gRPC flow control: blocks until the "client" starts reading
			<-sendGate
			sent <- response
			return nil
		},
	}
	writes := make(chan concurrent.Callback[*proto.WriteResponse], total)
	lc := &testLeaderController{
		write: func(_ context.Context, _ *proto.WriteRequest, cb concurrent.Callback[*proto.WriteResponse]) {
			writes <- cb
		},
	}

	finished := make(chan error, 1)
	pendingWrites := make(chan struct{}, maxWriteStreamPendingWrites)
	responses := make(chan *proto.WriteResponse, maxWriteStreamPendingWrites)
	go processWriteStream(ctx, finished, stream, lc, pendingWrites, responses)
	go sendWriteStreamResponses(ctx, finished, stream, pendingWrites, responses)

	for i := 0; i < total; i++ {
		requests <- &proto.WriteRequest{}
	}
	close(requests)

	// The client is not reading responses (stream.Send is stuck): completing the
	// writes that reached the leader must still not block.
	completedResponses := make([]*proto.WriteResponse, 0, total)
	for i := 0; i < maxWriteStreamPendingWrites; i++ {
		cb := receiveWithTimeout(t, writes, "leader did not receive the expected write")
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
	case <-writes:
		t.Fatal("write submitted beyond the pending-writes cap while the client is stalled")
	case <-time.After(100 * time.Millisecond):
	}

	// Unblock the client: the remaining writes get submitted and every response
	// is delivered, in completion order
	close(sendGate)
	for i := 0; i < extraWrites; i++ {
		cb := receiveWithTimeout(t, writes, "leader did not receive the expected write")
		response := &proto.WriteResponse{}
		cb.OnComplete(response)
		completedResponses = append(completedResponses, response)
	}

	for i := 0; i < total; i++ {
		sent := receiveWithTimeout(t, sent, "response was not sent to the client")
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

func TestGetShardAssignmentsValidatesAuthority(t *testing.T) {
	server := &publicRpcServer{
		log:                        slog.Default(),
		authorityValidationEnabled: true,
		assignmentDispatcher: &testAssignmentDispatcher{initialized: true, validAuthorities: map[string]bool{
			"expected-host:6648": true,
		}},
	}

	err := server.GetShardAssignments(&proto.ShardAssignmentsRequest{}, &testStream[any, proto.ShardAssignments]{
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

	err := server.GetShardAssignments(&proto.ShardAssignmentsRequest{}, &testStream[any, proto.ShardAssignments]{
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

	err := server.GetShardAssignments(&proto.ShardAssignmentsRequest{}, &testStream[any, proto.ShardAssignments]{
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

func TestLogFailure(t *testing.T) {
	var buf bytes.Buffer
	server := &publicRpcServer{
		log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}

	server.complain("Failed", errors.New("unexpected failure"), slog.String("operation", "read"))
	assert.Contains(t, buf.String(), "level=WARN")
	assert.Contains(t, buf.String(), "operation=read")
	assert.Contains(t, buf.String(), `error="unexpected failure`)

	for _, err := range []error{constant.ErrNodeIsNotLeader, fmt.Errorf("closing: %w", constant.ErrResourceUnavailable)} {
		buf.Reset()
		server.complain("Failed", err)
		assert.Contains(t, buf.String(), "level=DEBUG")
	}

	buf.Reset()
	server.complain("Failed", context.Canceled)
	server.complain("Failed", context.DeadlineExceeded)
	assert.Empty(t, buf.String())
}

// The leader includes the end of the range of a List or a RangeScan with
// end_inclusive only on a shard with FEATURE_RANGE_END_INCLUSIVE, and its
// responses say so: if the range is empty, it sends one with no keys. Without
// the feature, or without end_inclusive, it responds as the servers that don't
// support it, which leave the end out.
func TestPublicRpcServerListAndRangeScanEndInclusive(t *testing.T) {
	for _, test := range []struct {
		name           string
		featureEnabled bool
		endInclusive   bool
		keys           []string
		applied        bool
		responses      [][]string
	}{
		{"included", true, true, []string{"a", "b"}, true, [][]string{{"a", "b"}}},
		{"included, empty", true, true, nil, true, [][]string{nil}},
		{"without the feature", false, true, []string{"a"}, false, [][]string{{"a"}}},
		{"without the feature, empty", false, true, nil, false, nil},
		{"not requested", true, false, []string{"a"}, false, [][]string{{"a"}}},
		{"not requested, empty", true, false, nil, false, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The requests that the leader controller gets
			var listRequest *proto.ListRequest
			var scanRequest *proto.RangeScanRequest
			lc := &testLeaderController{
				isFeatureEnabled: func(f proto.Feature) bool {
					return test.featureEnabled && f == proto.Feature_FEATURE_RANGE_END_INCLUSIVE
				},
				list: func(_ context.Context, request *proto.ListRequest, onNext func(string) error) error {
					listRequest = request.CloneVT()
					for _, key := range test.keys {
						if err := onNext(key); err != nil {
							return err
						}
					}
					return nil
				},
				rangeScan: func(_ context.Context, request *proto.RangeScanRequest,
					onNext func(*proto.GetResponse) error) error {
					scanRequest = request.CloneVT()
					for _, key := range test.keys {
						if err := onNext(&proto.GetResponse{Key: &key}); err != nil {
							return err
						}
					}
					return nil
				},
			}
			shard := int64(1)
			server := &publicRpcServer{
				log:                  slog.Default(),
				assignmentDispatcher: &testAssignmentDispatcher{initialized: true},
				shardsDirector: &testShardsDirector{getLeader: func(int64) (lead.LeaderController, error) {
					return lc, nil
				}},
			}

			var listResponses []*proto.ListResponse
			require.NoError(t, server.List(&proto.ListRequest{
				Shard: &shard, StartInclusive: "a", EndExclusive: "b", EndInclusive: test.endInclusive,
			}, &testStream[any, proto.ListResponse]{ctx: t.Context(), send: func(response *proto.ListResponse) error {
				listResponses = append(listResponses, response)
				return nil
			}}))
			assert.Equal(t, test.applied, listRequest.EndInclusive)
			var listed [][]string
			for _, response := range listResponses {
				assert.Equal(t, test.applied, response.EndInclusive)
				listed = append(listed, response.Keys)
			}
			assert.Equal(t, test.responses, listed)

			var scanResponses []*proto.RangeScanResponse
			require.NoError(t, server.RangeScan(&proto.RangeScanRequest{
				Shard: &shard, StartInclusive: "a", EndExclusive: "b", EndInclusive: test.endInclusive,
			}, &testStream[any, proto.RangeScanResponse]{ctx: t.Context(), send: func(response *proto.RangeScanResponse) error {
				scanResponses = append(scanResponses, response)
				return nil
			}}))
			assert.Equal(t, test.applied, scanRequest.EndInclusive)
			var scanned [][]string
			for _, response := range scanResponses {
				assert.Equal(t, test.applied, response.EndInclusive)
				var keys []string
				for _, record := range response.Records {
					keys = append(keys, record.GetKey())
				}
				scanned = append(scanned, keys)
			}
			assert.Equal(t, test.responses, scanned)
		})
	}
}

func TestValidateAuthorityRejectsMissingAuthority(t *testing.T) {
	server := &publicRpcServer{
		authorityValidationEnabled: true,
		assignmentDispatcher:       &testAssignmentDispatcher{initialized: true},
	}

	err := server.validateAuthority(context.Background())
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
}

func TestPublicRpcServerWrite(t *testing.T) {
	logs := &bytes.Buffer{}
	writeErr := constant.ErrNodeIsNotLeader
	lc := &testLeaderController{
		writeBlock: func(_ context.Context, write *proto.WriteRequest) (*proto.WriteResponse, error) {
			if write.Puts[0].Key == "fail" {
				return nil, writeErr
			}
			return &proto.WriteResponse{Puts: []*proto.PutResponse{{Status: proto.Status_OK}}}, nil
		},
	}
	server := newTestPublicRpcServer(logs, lc)

	res, err := server.Write(context.Background(), &proto.WriteRequest{
		Shard: pb.Int64(0),
		Puts:  []*proto.PutRequest{{Key: "a"}},
	})
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
	// The request is logged at debug level
	assert.Contains(t, logs.String(), "Write request")

	_, err = server.Write(context.Background(), &proto.WriteRequest{
		Shard: pb.Int64(0),
		Puts:  []*proto.PutRequest{{Key: "fail"}},
	})
	assertOxiaError(t, err, writeErr)

	_, err = server.Write(context.Background(), &proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "a"}}})
	assert.Equal(t, codes.InvalidArgument, grpcstatus.Code(err))
}

func TestProcessWriteStreamStopsWhenStreamContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	finished := make(chan error, 1)
	stream := &testStream[proto.WriteRequest, proto.WriteResponse]{
		recv: func() (*proto.WriteRequest, error) {
			require.FailNow(t, "unexpected receive on a done stream")
			return nil, io.EOF
		},
	}

	processWriteStream(ctx, finished, stream, &testLeaderController{}, make(chan struct{}, 1),
		make(chan *proto.WriteResponse, 1))
	assert.Empty(t, finished)
}

func TestProcessWriteStreamNilRequest(t *testing.T) {
	finished := make(chan error, 1)
	stream := &testStream[proto.WriteRequest, proto.WriteResponse]{
		recv: func() (*proto.WriteRequest, error) { return nil, nil }, //nolint:nilnil // ends the stream
	}

	processWriteStream(context.Background(), finished, stream, &testLeaderController{}, make(chan struct{}, 1),
		make(chan *proto.WriteResponse, 1))
	assert.ErrorContains(t, <-finished, "stream closed")
}

func TestProcessWriteStreamContextDoneWhileWaitingForPendingWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	stream := &testStream[proto.WriteRequest, proto.WriteResponse]{
		recv: func() (*proto.WriteRequest, error) {
			// The stream ends while the write waits for a pending write slot
			cancel()
			return &proto.WriteRequest{}, nil
		},
	}

	// No free pending write slot
	processWriteStream(ctx, finished, stream, &testLeaderController{}, make(chan struct{}),
		make(chan *proto.WriteResponse, 1))
	assert.ErrorIs(t, <-finished, context.Canceled)
}

func TestSendWriteStreamResponsesSendError(t *testing.T) {
	sendErr := errors.New("send failure")
	finished := make(chan error, 1)
	responses := make(chan *proto.WriteResponse, 1)
	responses <- &proto.WriteResponse{}
	stream := &testStream[proto.WriteRequest, proto.WriteResponse]{
		send: func(*proto.WriteResponse) error { return sendErr },
	}

	sendWriteStreamResponses(context.Background(), finished, stream, make(chan struct{}, 1), responses)
	assert.ErrorIs(t, <-finished, sendErr)
}

func TestPublicRpcServerWriteStreamInvalidMetadata(t *testing.T) {
	server := newTestPublicRpcServer(nil, nil)

	for _, tt := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "no metadata", ctx: context.Background()},
		{name: "no shard", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(
			constant.MetadataNamespace, constant.DefaultNamespace))},
		{name: "no namespace", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(
			constant.MetadataShardId, "0"))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := server.WriteStream(&testStream[proto.WriteRequest, proto.WriteResponse]{ctx: tt.ctx})
			assert.Error(t, err)
		})
	}
}

func TestPublicRpcServerRead(t *testing.T) {
	logs := &bytes.Buffer{}
	readErr := constant.ErrResourceUnavailable
	lc := &testLeaderController{
		read: func(_ context.Context, request *proto.ReadRequest, onNext func(*proto.GetResponse) error) error {
			if len(request.Gets) > 0 && request.Gets[0].Key == "fail" {
				return readErr
			}
			return onNext(&proto.GetResponse{Value: []byte("v")})
		},
	}
	server := newTestPublicRpcServer(logs, lc)

	var sent []*proto.ReadResponse
	stream := &testStream[any, proto.ReadResponse]{
		ctx: context.Background(),
		send: func(response *proto.ReadResponse) error {
			sent = append(sent, response)
			return nil
		},
	}
	require.NoError(t, server.Read(&proto.ReadRequest{Shard: pb.Int64(0)}, stream))
	require.Len(t, sent, 1)
	assert.Equal(t, []byte("v"), sent[0].Gets[0].Value)
	assert.Contains(t, logs.String(), "Read request")

	err := server.Read(&proto.ReadRequest{Shard: pb.Int64(0), Gets: []*proto.GetRequest{{Key: "fail"}}}, stream)
	assertOxiaError(t, err, readErr)
}

func TestPublicRpcServerList(t *testing.T) {
	logs := &bytes.Buffer{}
	listErr := constant.ErrResourceUnavailable
	lc := &testLeaderController{
		list: func(_ context.Context, request *proto.ListRequest, onNext func(string) error) error {
			if request.StartInclusive == "fail" {
				return listErr
			}
			return onNext("a")
		},
	}
	server := newTestPublicRpcServer(logs, lc)

	var sent []*proto.ListResponse
	stream := &testStream[any, proto.ListResponse]{
		ctx: context.Background(),
		send: func(response *proto.ListResponse) error {
			sent = append(sent, response)
			return nil
		},
	}
	require.NoError(t, server.List(&proto.ListRequest{Shard: pb.Int64(0)}, stream))
	require.Len(t, sent, 1)
	assert.Equal(t, []string{"a"}, sent[0].Keys)
	assert.Contains(t, logs.String(), "List request")

	err := server.List(&proto.ListRequest{Shard: pb.Int64(0), StartInclusive: "fail"}, stream)
	assertOxiaError(t, err, listErr)
}

func TestPublicRpcServerRangeScan(t *testing.T) {
	logs := &bytes.Buffer{}
	scanErr := constant.ErrResourceUnavailable
	lc := &testLeaderController{
		rangeScan: func(_ context.Context, request *proto.RangeScanRequest,
			onNext func(*proto.GetResponse) error) error {
			if request.StartInclusive == "fail" {
				return scanErr
			}
			return onNext(&proto.GetResponse{Key: pb.String("a")})
		},
	}
	server := newTestPublicRpcServer(logs, lc)

	var sent []*proto.RangeScanResponse
	stream := &testStream[any, proto.RangeScanResponse]{
		ctx: context.Background(),
		send: func(response *proto.RangeScanResponse) error {
			sent = append(sent, response)
			return nil
		},
	}
	require.NoError(t, server.RangeScan(&proto.RangeScanRequest{Shard: pb.Int64(0)}, stream))
	require.Len(t, sent, 1)
	assert.Equal(t, "a", sent[0].Records[0].GetKey())
	assert.Contains(t, logs.String(), "RangeScan request")

	err := server.RangeScan(&proto.RangeScanRequest{Shard: pb.Int64(0), StartInclusive: "fail"}, stream)
	assertOxiaError(t, err, scanErr)
}

func TestPublicRpcServerGetNotifications(t *testing.T) {
	notificationsErr := constant.ErrNotificationsNotEnabled
	for _, tt := range []struct {
		name        string
		completeErr error
	}{
		{name: "completed"},
		{name: "failed", completeErr: notificationsErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lc := &testLeaderController{
				ctx: context.Background(),
				getNotifications: func(_ context.Context, _ *proto.NotificationsRequest,
					cb concurrent.StreamCallback[*proto.EncodedNotificationBatch]) {
					cb.OnComplete(tt.completeErr)
				},
			}
			server := newTestPublicRpcServer(nil, lc)

			err := server.GetNotifications(&proto.NotificationsRequest{},
				&testStream[any, proto.NotificationBatch]{ctx: context.Background()})
			if tt.completeErr == nil {
				assert.NoError(t, err)
			} else {
				assertOxiaError(t, err, tt.completeErr)
			}
		})
	}
}

func TestPublicRpcServerGetNotificationsLeaderClosed(t *testing.T) {
	leaderCtx, cancel := context.WithCancel(context.Background())
	cancel()
	lc := &testLeaderController{
		ctx: leaderCtx,
		getNotifications: func(context.Context, *proto.NotificationsRequest,
			concurrent.StreamCallback[*proto.EncodedNotificationBatch]) {
		},
	}
	server := newTestPublicRpcServer(nil, lc)

	err := server.GetNotifications(&proto.NotificationsRequest{},
		&testStream[any, proto.NotificationBatch]{ctx: context.Background()})
	assertOxiaError(t, err, constant.ErrResourceUnavailable)
}

func TestPublicRpcServerGetNotificationsNotLeader(t *testing.T) {
	server := newTestPublicRpcServer(nil, nil)

	err := server.GetNotifications(&proto.NotificationsRequest{},
		&testStream[any, proto.NotificationBatch]{ctx: context.Background()})
	assertOxiaError(t, err, constant.ErrNodeIsNotLeader)
}

func TestPublicRpcServerSessionsNotLeader(t *testing.T) {
	logs := &bytes.Buffer{}
	server := newTestPublicRpcServer(logs, nil)

	_, err := server.CreateSession(context.Background(), &proto.CreateSessionRequest{})
	assertOxiaError(t, err, constant.ErrNodeIsNotLeader)

	_, err = server.KeepAlive(context.Background(), &proto.SessionHeartbeat{SessionId: 5})
	assertOxiaError(t, err, constant.ErrNodeIsNotLeader)
	// The heartbeats are logged at debug level
	assert.Contains(t, logs.String(), "Session keep alive")

	_, err = server.CloseSession(context.Background(), &proto.CloseSessionRequest{})
	assertOxiaError(t, err, constant.ErrNodeIsNotLeader)
}

func TestPublicRpcServerCloseSessionError(t *testing.T) {
	for _, closeErr := range []error{constant.ErrSessionNotFound, constant.ErrResourceUnavailable} {
		logs := &bytes.Buffer{}
		lc := &testLeaderController{
			closeSession: func(*proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
				return nil, closeErr
			},
		}
		server := newTestPublicRpcServer(logs, lc)

		_, err := server.CloseSession(context.Background(), &proto.CloseSessionRequest{})
		assertOxiaError(t, err, closeErr)
		// A session that is already gone is not worth a warning
		assert.Equal(t, !errors.Is(closeErr, constant.ErrSessionNotFound),
			strings.Contains(logs.String(), "Failed to close session"))
	}
}

func TestPublicRpcServerGetSequenceUpdates(t *testing.T) {
	updates := make(chan string, 2)
	updates <- "a-0001"
	updates <- "a-0002"
	close(updates)
	waiter := &testSequenceWaiter{ch: updates}
	lc := &testLeaderController{
		getSequenceUpdates: func(context.Context, *proto.GetSequenceUpdatesRequest) (database.SequenceWaiter, error) {
			return waiter, nil
		},
	}
	server := newTestPublicRpcServer(nil, lc)

	var sent []string
	err := server.GetSequenceUpdates(&proto.GetSequenceUpdatesRequest{Key: "a"},
		&testStream[any, proto.GetSequenceUpdatesResponse]{
			ctx: context.Background(),
			send: func(response *proto.GetSequenceUpdatesResponse) error {
				sent = append(sent, response.HighestSequenceKey)
				return nil
			},
		})
	assert.NoError(t, err)
	assert.Equal(t, []string{"a-0001", "a-0002"}, sent)
	assert.True(t, waiter.closed)
}

func TestPublicRpcServerGetSequenceUpdatesSendError(t *testing.T) {
	updates := make(chan string, 1)
	updates <- "a-0001"
	waiter := &testSequenceWaiter{ch: updates}
	lc := &testLeaderController{
		getSequenceUpdates: func(context.Context, *proto.GetSequenceUpdatesRequest) (database.SequenceWaiter, error) {
			return waiter, nil
		},
	}
	server := newTestPublicRpcServer(nil, lc)

	sendErr := errors.New("send failure")
	err := server.GetSequenceUpdates(&proto.GetSequenceUpdatesRequest{Key: "a"},
		&testStream[any, proto.GetSequenceUpdatesResponse]{
			ctx:  context.Background(),
			send: func(*proto.GetSequenceUpdatesResponse) error { return sendErr },
		})
	assert.ErrorIs(t, err, sendErr)
	assert.True(t, waiter.closed)
}

func TestPublicRpcServerGetSequenceUpdatesStreamDone(t *testing.T) {
	waiter := &testSequenceWaiter{ch: make(chan string)}
	lc := &testLeaderController{
		getSequenceUpdates: func(context.Context, *proto.GetSequenceUpdatesRequest) (database.SequenceWaiter, error) {
			return waiter, nil
		},
	}
	server := newTestPublicRpcServer(nil, lc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := server.GetSequenceUpdates(&proto.GetSequenceUpdatesRequest{Key: "a"},
		&testStream[any, proto.GetSequenceUpdatesResponse]{ctx: ctx})
	assert.ErrorIs(t, err, context.Canceled)
	assert.True(t, waiter.closed)
}

func TestPublicRpcServerGetSequenceUpdatesError(t *testing.T) {
	updatesErr := constant.ErrResourceUnavailable
	lc := &testLeaderController{
		getSequenceUpdates: func(context.Context, *proto.GetSequenceUpdatesRequest) (database.SequenceWaiter, error) {
			return nil, updatesErr
		},
	}
	server := newTestPublicRpcServer(nil, lc)

	err := server.GetSequenceUpdates(&proto.GetSequenceUpdatesRequest{Key: "a"},
		&testStream[any, proto.GetSequenceUpdatesResponse]{ctx: context.Background()})
	assertOxiaError(t, err, updatesErr)

	// Not the leader of the shard
	server = newTestPublicRpcServer(nil, nil)
	err = server.GetSequenceUpdates(&proto.GetSequenceUpdatesRequest{Key: "a"},
		&testStream[any, proto.GetSequenceUpdatesResponse]{ctx: context.Background()})
	assertOxiaError(t, err, constant.ErrNodeIsNotLeader)
}

// The clients that predate the rename of the service call it by its former name.
func TestCompatPublicRpcServer(t *testing.T) {
	standaloneServer, err := NewStandalone(NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	defer standaloneServer.Close()

	conn, err := grpc.NewClient(standaloneServer.ServiceAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := compat.NewOxiaClientClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	shard := int64(0)

	assignments, err := client.GetShardAssignments(ctx, &proto.ShardAssignmentsRequest{})
	require.NoError(t, err)
	shardAssignments, err := assignments.Recv()
	require.NoError(t, err)
	assert.Len(t, shardAssignments.Namespaces[constant.DefaultNamespace].Assignments, 1)

	startOffset := int64(-1)
	notifications, err := client.GetNotifications(ctx, &proto.NotificationsRequest{
		Shard: shard, StartOffsetExclusive: &startOffset,
	})
	require.NoError(t, err)

	writeResponse, err := client.Write(ctx, &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: "a", Value: []byte("1")}},
	})
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, writeResponse.Puts[0].Status)

	// The batches of the entries without notifications come first
	notificationBatch, err := notifications.Recv()
	for err == nil && len(notificationBatch.Notifications) == 0 {
		notificationBatch, err = notifications.Recv()
	}
	require.NoError(t, err)
	require.Len(t, notificationBatch.Notifications, 1)
	assert.Equal(t, "a", notificationBatch.Notifications[0].GetKey())

	writeStream, err := client.WriteStream(metadata.AppendToOutgoingContext(ctx,
		constant.MetadataShardId, "0", constant.MetadataNamespace, constant.DefaultNamespace))
	require.NoError(t, err)
	require.NoError(t, writeStream.Send(&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "b", Value: []byte("2")}}}))
	writeResponse, err = writeStream.Recv()
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, writeResponse.Puts[0].Status)
	require.NoError(t, writeStream.CloseSend())

	read, err := client.Read(ctx, &proto.ReadRequest{Shard: &shard, Gets: []*proto.GetRequest{{Key: "a", IncludeValue: true}}})
	require.NoError(t, err)
	readResponse, err := read.Recv()
	require.NoError(t, err)
	assert.Equal(t, []byte("1"), readResponse.Gets[0].Value)

	list, err := client.List(ctx, &proto.ListRequest{Shard: &shard, StartInclusive: "a", EndExclusive: "c"})
	require.NoError(t, err)
	listResponse, err := list.Recv()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, listResponse.Keys)

	rangeScan, err := client.RangeScan(ctx, &proto.RangeScanRequest{Shard: &shard, StartInclusive: "a", EndExclusive: "c"})
	require.NoError(t, err)
	rangeScanResponse, err := rangeScan.Recv()
	require.NoError(t, err)
	assert.Len(t, rangeScanResponse.Records, 2)

	session, err := client.CreateSession(ctx, &proto.CreateSessionRequest{Shard: shard, SessionTimeoutMs: 5000})
	require.NoError(t, err)
	_, err = client.KeepAlive(ctx, &proto.SessionHeartbeat{Shard: shard, SessionId: session.SessionId})
	require.NoError(t, err)
	_, err = client.CloseSession(ctx, &proto.CloseSessionRequest{Shard: shard, SessionId: session.SessionId})
	require.NoError(t, err)

	writeResponse, err = client.Write(ctx, &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key: "s", Value: []byte("1"), PartitionKey: pb.String("s"), SequenceKeyDelta: []uint64{1},
		}},
	})
	require.NoError(t, err)
	sequenceUpdates, err := client.GetSequenceUpdates(ctx, &proto.GetSequenceUpdatesRequest{Shard: shard, Key: "s"})
	require.NoError(t, err)
	sequenceUpdate, err := sequenceUpdates.Recv()
	require.NoError(t, err)
	assert.Equal(t, writeResponse.Puts[0].GetKey(), sequenceUpdate.HighestSequenceKey)
}

// newTestPublicRpcServer returns a server that leads shard 0 with lc, or no
// shard if lc is nil, and logs at debug level to logs, if not nil.
func newTestPublicRpcServer(logs *bytes.Buffer, lc lead.LeaderController) *publicRpcServer {
	log := slog.Default()
	if logs != nil {
		log = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return &publicRpcServer{
		log:                  log,
		assignmentDispatcher: &testAssignmentDispatcher{initialized: true},
		shardsDirector: &testShardsDirector{
			getLeader: func(int64) (lead.LeaderController, error) {
				if lc == nil {
					return nil, constant.ErrNodeIsNotLeader
				}
				return lc, nil
			},
		},
	}
}

func assertOxiaError(t *testing.T, err error, expected error) {
	t.Helper()
	oxiaErr, _ := constant.FromGrpcError(err)
	assert.ErrorIs(t, oxiaErr, expected)
}

// testStream implements the server side of the unary, client, server and
// bidirectional gRPC streams.
type testStream[Req any, Res any] struct {
	grpc.ServerStream
	ctx     context.Context
	recv    func() (*Req, error)
	send    func(*Res) error
	sendMsg func(any) error
}

func (s *testStream[Req, Res]) Context() context.Context {
	return s.ctx
}

func (s *testStream[Req, Res]) Recv() (*Req, error) {
	return s.recv()
}

func (s *testStream[Req, Res]) Send(response *Res) error {
	return s.send(response)
}

func (s *testStream[Req, Res]) SendAndClose(response *Res) error {
	return s.send(response)
}

func (s *testStream[Req, Res]) SendMsg(m any) error {
	return s.sendMsg(m)
}

type testLeaderController struct {
	lead.LeaderController
	ctx                context.Context
	term               int64
	write              func(context.Context, *proto.WriteRequest, concurrent.Callback[*proto.WriteResponse])
	writeBlock         func(context.Context, *proto.WriteRequest) (*proto.WriteResponse, error)
	read               func(context.Context, *proto.ReadRequest, func(*proto.GetResponse) error) error
	list               func(context.Context, *proto.ListRequest, func(string) error) error
	rangeScan          func(context.Context, *proto.RangeScanRequest, func(*proto.GetResponse) error) error
	getNotifications   func(context.Context, *proto.NotificationsRequest, concurrent.StreamCallback[*proto.EncodedNotificationBatch])
	getSequenceUpdates func(context.Context, *proto.GetSequenceUpdatesRequest) (database.SequenceWaiter, error)
	closeSession       func(*proto.CloseSessionRequest) (*proto.CloseSessionResponse, error)
	newTerm            func(*proto.NewTermRequest) (*proto.NewTermResponse, error)
	becomeLeader       func(context.Context, *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error)
	addFollower        func(*proto.AddFollowerRequest) (*proto.AddFollowerResponse, error)
	removeObserver     func(*proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error)
	freeze             func(*proto.FreezeShardRequest) (*proto.FreezeShardResponse, error)
	isFeatureEnabled   func(proto.Feature) bool
}

func (l *testLeaderController) Context() context.Context { return l.ctx }
func (l *testLeaderController) Term() int64              { return l.term }
func (l *testLeaderController) IsFeatureEnabled(f proto.Feature) bool {
	return l.isFeatureEnabled != nil && l.isFeatureEnabled(f)
}
func (l *testLeaderController) Write(ctx context.Context, request *proto.WriteRequest,
	cb concurrent.Callback[*proto.WriteResponse]) {
	l.write(ctx, request, cb)
}
func (l *testLeaderController) WriteBlock(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	return l.writeBlock(ctx, request)
}
func (l *testLeaderController) Read(ctx context.Context, request *proto.ReadRequest,
	onNext func(*proto.GetResponse) error) error {
	return l.read(ctx, request, onNext)
}
func (l *testLeaderController) List(ctx context.Context, request *proto.ListRequest, onNext func(string) error) error {
	return l.list(ctx, request, onNext)
}
func (l *testLeaderController) RangeScan(ctx context.Context, request *proto.RangeScanRequest,
	onNext func(*proto.GetResponse) error) error {
	return l.rangeScan(ctx, request, onNext)
}

//nolint:revive // implements lead.LeaderController
func (l *testLeaderController) GetNotifications(ctx context.Context, request *proto.NotificationsRequest,
	cb concurrent.StreamCallback[*proto.EncodedNotificationBatch]) {
	l.getNotifications(ctx, request, cb)
}
func (l *testLeaderController) GetSequenceUpdates(ctx context.Context,
	request *proto.GetSequenceUpdatesRequest) (database.SequenceWaiter, error) {
	return l.getSequenceUpdates(ctx, request)
}
func (l *testLeaderController) CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	return l.closeSession(request)
}
func (l *testLeaderController) NewTerm(request *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	return l.newTerm(request)
}
func (l *testLeaderController) BecomeLeader(ctx context.Context,
	request *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	return l.becomeLeader(ctx, request)
}
func (l *testLeaderController) AddFollower(request *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	return l.addFollower(request)
}
func (l *testLeaderController) RemoveObserver(request *proto.RemoveObserverRequest) (*proto.RemoveObserverResponse, error) {
	return l.removeObserver(request)
}
func (l *testLeaderController) Freeze(request *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
	return l.freeze(request)
}

type testSequenceWaiter struct {
	ch     chan string
	closed bool
}

func (w *testSequenceWaiter) Next(ctx context.Context) (string, error) {
	select {
	case key, more := <-w.ch:
		if !more {
			return "", io.EOF
		}
		return key, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (w *testSequenceWaiter) Close() {
	w.closed = true
}
