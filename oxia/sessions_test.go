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

package oxia

import (
	"context"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/internal"
)

// An operation hitting a session that failed to start must fail its callback
// and discard the session — without re-acquiring the sessions lock that the
// caller already holds, which was a self-deadlock wedging every subsequent
// ephemeral operation of the client.
func TestSessions_FailedSessionStartDoesNotDeadlock(t *testing.T) {
	s := &sessions{
		sessionsByShard: map[int64]*clientSession{},
		log:             slog.Default(),
	}

	cs := &clientSession{
		shardId:  1,
		sessions: s,
		started:  make(chan error, 1),
		log:      slog.Default(),
	}
	cs.ctx, cs.cancel = context.WithCancel(context.Background())
	cs.started <- errors.New("session start failed")
	s.sessionsByShard[1] = cs

	done := make(chan struct{})
	go func() {
		s.executeWithSessionId(1, func(sessionId int64, err error) {
			assert.Error(t, err)
			assert.EqualValues(t, -1, sessionId)
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("executeWithSessionId deadlocked on a failed session start")
	}

	// The failed session has been discarded: the next operation will attempt
	// a fresh one
	s.Lock()
	_, found := s.sessionsByShard[1]
	s.Unlock()
	assert.False(t, found)
}

// flakyKeepAliveServer fails every other heartbeat, starting with the first
// one: a long-lived session going through occasional failures, like a node
// restart, each followed by a recovery.
type flakyKeepAliveServer struct {
	proto.UnimplementedOxiaClientServer
	heartbeats chan time.Time
	count      atomic.Int64
}

func (*flakyKeepAliveServer) CreateSession(context.Context, *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return &proto.CreateSessionResponse{SessionId: 1}, nil
}

func (s *flakyKeepAliveServer) KeepAlive(context.Context, *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error) {
	s.heartbeats <- time.Now()
	if s.count.Add(1)%2 == 1 {
		return nil, status.Error(codes.Unavailable, "transient failure")
	}
	return &proto.KeepAliveResponse{}, nil
}

// A failed heartbeat is retried after a backoff, which a successful heartbeat
// must reset: otherwise every failure over the lifetime of the session makes
// the next retry wait longer, until a single failure is enough to make the
// session expire on the server.
func TestSessions_KeepAliveRetryDelayDoesNotGrow(t *testing.T) {
	server := &flakyKeepAliveServer{heartbeats: make(chan time.Time, 100)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	proto.RegisterOxiaClientServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	options, err := newClientOptions(listener.Addr().String())
	require.NoError(t, err)
	shardManager := &staticShardManager{leader: options.serviceAddress}
	rpcProvider := internal.NewRpcProvider(t.Context(), options.namespace, nil, nil, options.serviceAddress,
		func() internal.ShardManager { return shardManager })
	defer func() {
		assert.NoError(t, rpcProvider.Close())
	}()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := newSessions(ctx, shardManager, rpcProvider, options)
	s.executeWithSessionId(0, func(_ int64, err error) {
		assert.NoError(t, err)
	})

	nextHeartbeat := func() time.Time {
		select {
		case heartbeat := <-server.heartbeats:
			return heartbeat
		case <-time.After(10 * time.Second):
			require.FailNow(t, "no heartbeat received")
			return time.Time{}
		}
	}

	// A failed heartbeat is retried after the backoff, then a full tick of the
	// keep-alive ticker, which is 2s here. The first backoff is 150ms at the
	// most: without a reset, the one after the 7th failure is 570ms at least.
	for failure := 1; failure <= 7; failure++ {
		failedAt := nextHeartbeat()
		retryDelay := nextHeartbeat().Sub(failedAt) - 2*time.Second
		assert.Less(t, retryDelay, 500*time.Millisecond, "retry delay after failure %d", failure)
	}
}
