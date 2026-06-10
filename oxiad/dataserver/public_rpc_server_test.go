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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
)

func init() {
	logging.LogJSON = false
	logging.ConfigureLogger()
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
	go procesWriteStream(ctx, finished, stream, lc, pendingWrites, responses)
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
