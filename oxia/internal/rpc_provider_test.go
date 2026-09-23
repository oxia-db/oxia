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

package internal

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

// testOxiaClientServer answers every data request with an empty success.
type testOxiaClientServer struct {
	proto.UnimplementedOxiaClientServer
}

func (testOxiaClientServer) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		response := &proto.WriteResponse{}
		for range request.Puts {
			response.Puts = append(response.Puts, &proto.PutResponse{})
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func (testOxiaClientServer) Read(request *proto.ReadRequest, stream proto.OxiaClient_ReadServer) error {
	response := &proto.ReadResponse{}
	for range request.Gets {
		response.Gets = append(response.Gets, &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND})
	}
	return stream.Send(response)
}

func (testOxiaClientServer) List(_ *proto.ListRequest, stream proto.OxiaClient_ListServer) error {
	return stream.Send(&proto.ListResponse{})
}

func (testOxiaClientServer) RangeScan(_ *proto.RangeScanRequest, stream proto.OxiaClient_RangeScanServer) error {
	return stream.Send(&proto.RangeScanResponse{})
}

func startTestOxiaClientServer(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	proto.RegisterOxiaClientServer(server, testOxiaClientServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// unreachableAddress returns an address where nothing is listening, so
// connecting to it fails with "connection refused".
func unreachableAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

type testShardManager struct {
	ShardManager
	leader atomic.Pointer[string]
}

func (*testShardManager) Exists(int64) bool { return true }

func (m *testShardManager) Leader(int64) string { return *m.leader.Load() }

// When the leader of a shard goes away, the client keeps sending the requests
// for that shard to the old leader until it receives the updated shard
// assignments. These attempts fail with a transport-level Unavailable error,
// which must be retried until the new leader is known.
func TestRpcProvider_RetryWhenShardLeaderIsUnreachable(t *testing.T) {
	shardId := int64(0)
	for _, tt := range []struct {
		name    string
		execute func(context.Context, RpcProvider) error
	}{
		{"write", func(ctx context.Context, provider RpcProvider) error {
			_, err := provider.ExecuteWrite(ctx, &proto.WriteRequest{
				Shard: &shardId,
				Puts:  []*proto.PutRequest{{Key: "key", Value: []byte("value")}},
			})
			return err
		}},
		{"read", func(ctx context.Context, provider RpcProvider) error {
			_, err := provider.ExecuteRead(ctx, &proto.ReadRequest{
				Shard: &shardId,
				Gets:  []*proto.GetRequest{{Key: "key"}},
			})
			return err
		}},
		{"list", func(ctx context.Context, provider RpcProvider) error {
			return provider.ExecuteList(ctx, &proto.ListRequest{Shard: &shardId}, func(*proto.ListResponse) {})
		}},
		{"range-scan", func(ctx context.Context, provider RpcProvider) error {
			return provider.ExecuteRangeScan(ctx, &proto.RangeScanRequest{Shard: &shardId}, func(*proto.RangeScanResponse) {})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			oldLeader := unreachableAddress(t)
			newLeader := startTestOxiaClientServer(t)

			shardManager := &testShardManager{}
			shardManager.leader.Store(&oldLeader)

			// The updated shard assignments arrive while the client is trying
			// to reach the old leader
			dialer := func(ctx context.Context, address string) (net.Conn, error) {
				if address == oldLeader {
					shardManager.leader.Store(&newLeader)
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			}
			provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, newLeader,
				func() ShardManager { return shardManager }, grpc.WithContextDialer(dialer))
			defer func() {
				assert.NoError(t, provider.Close())
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			assert.NoError(t, tt.execute(ctx, provider))
		})
	}
}

func TestExecuteWithRetry_ShardRequestErrors(t *testing.T) {
	for _, tt := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{"connection refused", status.Error(codes.Unavailable,
			`connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:6648: connect: connection refused"`), true},
		{"transport closing", status.Error(codes.Unavailable, "transport is closing"), true},
		{"stream closed", io.EOF, true},
		{"node is not leader", constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader), true},
		{"server not initialized", constant.IntoGrpcStatusError(constant.ErrNotInitialized), true},
		{"context canceled", status.FromContextError(context.Canceled).Err(), false},
		{"context deadline exceeded", status.FromContextError(context.DeadlineExceeded).Err(), false},
		{"client connection closing", status.Error(codes.Canceled, "grpc: the client connection is closing"), false},
		{"invalid term", constant.IntoGrpcStatusError(constant.ErrInvalidTerm), false},
		{"unknown", status.Error(codes.Unknown, "unknown"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			attempts := 0
			_, err := executeWithRetry(t.Context(), func(constant.ErrorMetadata) (struct{}, error) {
				attempts++
				if attempts == 1 {
					return struct{}{}, tt.err
				}
				return struct{}{}, nil
			}, isRetryableShardRequest)

			if tt.retryable {
				assert.NoError(t, err)
				assert.Equal(t, 2, attempts)
			} else {
				assert.Error(t, err)
				assert.Equal(t, 1, attempts)
			}
		})
	}
}

// Session requests go to a fixed target, so they must not retry an
// unreachable one: the session looks up the shard leader again before its
// next attempt.
func TestRpcProvider_KeepAliveFailsFastWhenTargetIsUnreachable(t *testing.T) {
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, "",
		func() ShardManager { return nil })
	defer func() {
		assert.NoError(t, provider.Close())
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := provider.KeepAlive(ctx, unreachableAddress(t), &proto.SessionHeartbeat{})
	assert.Equal(t, codes.Unavailable, status.Code(err), "unexpected error: %v", err)
}
