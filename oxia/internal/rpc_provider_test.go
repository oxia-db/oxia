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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

// notLeaderOxiaClientServer answers the session requests like a node that is
// no longer the leader of the shard: with ErrNodeIsNotLeader and a hint to the
// new leader.
type notLeaderOxiaClientServer struct {
	proto.UnimplementedOxiaClientServer
	calls atomic.Int64
}

func (s *notLeaderOxiaClientServer) notLeader() error {
	s.calls.Add(1)
	return constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader, constant.WithLeaderHint(0, "new-leader:6648"))
}

func (s *notLeaderOxiaClientServer) CreateSession(context.Context, *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return nil, s.notLeader()
}

func (s *notLeaderOxiaClientServer) KeepAlive(context.Context, *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error) {
	return nil, s.notLeader()
}

func (s *notLeaderOxiaClientServer) CloseSession(context.Context, *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	return nil, s.notLeader()
}

func startOxiaClientServer(t *testing.T, oxiaClientServer proto.OxiaClientServer) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	proto.RegisterOxiaClientServer(server, oxiaClientServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// Session requests go to a fixed target: the shard leader known by the client
// when the request is sent. After the leader moves, retrying them against the
// same target would keep failing, so they must fail fast: the session looks up
// the shard leader again before its next attempt.
func TestRpcProvider_SessionRequestsFailFastWhenTargetIsNotLeader(t *testing.T) {
	for _, tt := range []struct {
		name    string
		execute func(context.Context, RpcProvider, string) error
	}{
		{"create-session", func(ctx context.Context, provider RpcProvider, target string) error {
			_, err := provider.CreateSession(ctx, target, &proto.CreateSessionRequest{})
			return err
		}},
		{"keep-alive", func(ctx context.Context, provider RpcProvider, target string) error {
			_, err := provider.KeepAlive(ctx, target, &proto.SessionHeartbeat{})
			return err
		}},
		{"close-session", func(ctx context.Context, provider RpcProvider, target string) error {
			_, err := provider.CloseSession(ctx, target, &proto.CloseSessionRequest{})
			return err
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := &notLeaderOxiaClientServer{}
			target := startOxiaClientServer(t, server)

			provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, "",
				func() ShardManager { return nil })
			defer func() {
				assert.NoError(t, provider.Close())
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			assert.ErrorIs(t, tt.execute(ctx, provider, target), constant.ErrNodeIsNotLeader)
			assert.EqualValues(t, 1, server.calls.Load())
		})
	}
}
