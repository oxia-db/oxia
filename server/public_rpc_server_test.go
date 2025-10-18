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
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/common/logging"
	"github.com/oxia-db/oxia/proto"
)

func init() {
	logging.LogJSON = false
	logging.ConfigureLogger()
}

func TestWriteClientClose(t *testing.T) {
	standaloneServer, err := NewStandalone(NewTestConfig(t.TempDir()))
	assert.NoError(t, err)
	defer standaloneServer.Close()

	// Connect to the standalone server
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
