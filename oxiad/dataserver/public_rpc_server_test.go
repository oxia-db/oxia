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

package dataserver

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
)

func init() {
	logging.LogJSON = false
	logging.ConfigureLogger()
}

type testAssignmentDispatcher struct {
	initialized      bool
	validAuthorities map[string]bool
}

func (*testAssignmentDispatcher) Close() error { return nil }
func (t *testAssignmentDispatcher) Initialized() bool {
	return t.initialized
}
func (*testAssignmentDispatcher) PushShardAssignments(proto.OxiaCoordination_PushShardAssignmentsServer) error {
	panic("unexpected call")
}
func (*testAssignmentDispatcher) RegisterForUpdates(*proto.ShardAssignmentsRequest, assignment.Client) error {
	panic("unexpected call")
}
func (*testAssignmentDispatcher) GetLeader(int64) string { return "" }
func (t *testAssignmentDispatcher) HasAuthority(authority string) bool {
	return t.validAuthorities[authority]
}

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

func TestValidateAuthorityRejectsWrongAuthority(t *testing.T) {
	server := &publicRpcServer{
		log: slog.Default(),
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
		assignmentDispatcher: &testAssignmentDispatcher{},
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		":authority": "expected-host:6648",
	}))

	err := server.validateAuthority(ctx)
	require.Error(t, err)
	assert.Equal(t, constant.CodeNotInitialized, grpcstatus.Code(err))
}

func TestValidateAuthorityCanBeDisabled(t *testing.T) {
	server := &publicRpcServer{
		disableAuthorityValidation: true,
		assignmentDispatcher:       &testAssignmentDispatcher{},
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
		log: slog.Default(),
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
