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
	"fmt"
	"log/slog"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
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
		s.executeWithSessionId(func() int64 { return 1 }, func(_ int64, sessionId int64, err error) {
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
	s.executeWithSessionId(func() int64 { return 0 }, func(_ int64, _ int64, err error) {
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

type shardSession struct {
	shard     int64
	sessionId int64
}

// sessionRequestsServer records the session requests it receives. The id of a
// session it creates is 10 + the shard id.
type sessionRequestsServer struct {
	proto.UnimplementedOxiaClientServer
	heartbeats chan shardSession

	sync.Mutex
	created []int64
	closed  []shardSession
}

func (s *sessionRequestsServer) CreateSession(_ context.Context,
	req *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	s.Lock()
	defer s.Unlock()
	s.created = append(s.created, req.Shard)
	return &proto.CreateSessionResponse{SessionId: 10 + req.Shard}, nil
}

func (s *sessionRequestsServer) KeepAlive(_ context.Context,
	req *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error) {
	s.heartbeats <- shardSession{req.Shard, req.SessionId}
	return &proto.KeepAliveResponse{}, nil
}

func (s *sessionRequestsServer) CloseSession(_ context.Context,
	req *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	s.Lock()
	defer s.Unlock()
	s.closed = append(s.closed, shardSession{req.Shard, req.SessionId})
	return &proto.CloseSessionResponse{}, nil
}

// unnotifiedShardManager doesn't notify the changes of its shard map.
type unnotifiedShardManager struct {
	*splittableShardManager
}

func (unnotifiedShardManager) Changed() <-chan struct{} { return nil }

type followSplitTest struct {
	name string
	// The splits of shard 0 and its children, as {parent, left, right}
	splits [][3]int64
	// The shards that replaced shard 0 in the end
	shards []int64
	// Whether the client looks up the session of the last of them before the
	// change of the shard map is notified
	lookupFirst bool
}

// The shards that replace a split shard inherit its session, which owns the
// ephemeral records they get from it. The client must keep the session alive
// on them, use it for their new ephemeral records, and close it there.
func TestSessions_FollowSplit(t *testing.T) {
	for _, test := range []followSplitTest{
		{name: "notified", splits: [][3]int64{{0, 1, 2}}, shards: []int64{1, 2}},
		{name: "lookup-first", splits: [][3]int64{{0, 1, 2}}, shards: []int64{1, 2}, lookupFirst: true},
		{name: "split-twice", splits: [][3]int64{{0, 1, 2}, {1, 3, 4}}, shards: []int64{2, 3, 4}, lookupFirst: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			testSessionsFollowSplit(t, test)
		})
	}
}

func testSessionsFollowSplit(t *testing.T, test followSplitTest) {
	t.Helper()
	server := &sessionRequestsServer{heartbeats: make(chan shardSession, 100)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	proto.RegisterOxiaClientServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	options, err := newClientOptions(listener.Addr().String())
	require.NoError(t, err)
	splittable := newSplittableShardManager(options.serviceAddress)
	var shardManager internal.ShardManager = splittable
	if test.lookupFirst {
		shardManager = unnotifiedShardManager{splittable}
	}
	rpcProvider := internal.NewRpcProvider(t.Context(), options.namespace, nil, nil, options.serviceAddress,
		func() internal.ShardManager { return shardManager })
	defer func() {
		assert.NoError(t, rpcProvider.Close())
	}()

	s := newSessions(t.Context(), shardManager, rpcProvider, options)
	sessionOn := func(shardId int64) int64 {
		var sessionId int64
		s.executeWithSessionId(func() int64 { return shardId }, func(_ int64, id int64, err error) {
			require.NoError(t, err)
			sessionId = id
		})
		return sessionId
	}
	sessionId := sessionOn(0)

	for _, split := range test.splits {
		splittable.split(split[0], split[1], split[2])
	}
	if test.lookupFirst {
		assert.Equal(t, sessionId, sessionOn(test.shards[len(test.shards)-1]))
	}

	missing := map[shardSession]bool{}
	var inherited []shardSession
	for _, shard := range test.shards {
		missing[shardSession{shard, sessionId}] = true
		inherited = append(inherited, shardSession{shard, sessionId})
	}
	timeout := time.After(10 * time.Second)
	for len(missing) > 0 {
		select {
		case heartbeat := <-server.heartbeats:
			delete(missing, heartbeat)
		case <-timeout:
			require.FailNow(t, "the session is not kept alive on the shards that replaced its shard",
				"missing heartbeats: %v", missing)
		}
	}
	for _, shard := range test.shards {
		assert.Equal(t, sessionId, sessionOn(shard))
	}

	require.NoError(t, s.Close())
	server.Lock()
	defer server.Unlock()
	assert.Equal(t, []int64{0}, server.created)
	assert.ElementsMatch(t, inherited, server.closed)
}

// frozenShardServer is a sessionRequestsServer that rejects the creation of
// the sessions on shard 0 with ErrNodeIsNotLeader, like the parent of a split
// while the cutover freezes it, and the data servers once it is deleted.
type frozenShardServer struct {
	*sessionRequestsServer
	rejected atomic.Int64
}

func (s *frozenShardServer) CreateSession(ctx context.Context,
	req *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	if req.Shard == 0 {
		s.rejected.Add(1)
		return nil, constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader)
	}
	return s.sessionRequestsServer.CreateSession(ctx, req)
}

type sessionResult struct {
	shard     int64
	sessionId int64
	err       error
}

// The first ephemeral operation on a shard creates the session there, and holds
// the sessions lock until the session is established. When the shard is split
// meanwhile, the session can no longer be created on it: the operation must get
// one on the shard that took over its key, and release the lock, without
// waiting for the next attempt of the creation.
func TestSessions_ShardSplitDuringCreation(t *testing.T) {
	server := &frozenShardServer{
		sessionRequestsServer: &sessionRequestsServer{heartbeats: make(chan shardSession, 100)},
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	proto.RegisterOxiaClientServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	options, err := newClientOptions(listener.Addr().String())
	require.NoError(t, err)
	shardManager := newSplittableShardManager(options.serviceAddress)
	rpcProvider := internal.NewRpcProvider(t.Context(), options.namespace, nil, nil, options.serviceAddress,
		func() internal.ShardManager { return shardManager })
	defer func() {
		assert.NoError(t, rpcProvider.Close())
	}()

	s := newSessions(t.Context(), shardManager, rpcProvider, options)
	execute := func(key string) <-chan sessionResult {
		ch := make(chan sessionResult, 1)
		go s.executeWithSessionId(func() int64 { return shardManager.Get(key) },
			func(shardId int64, sessionId int64, err error) { ch <- sessionResult{shardId, sessionId, err} })
		return ch
	}

	// The split gives the lower half of the hash range to shard 1, the upper
	// half to shard 2
	var leftKey, rightKey string
	for i := 0; leftKey == "" || rightKey == ""; i++ {
		key := fmt.Sprintf("key-%d", i)
		if hash.Xxh332(key) <= math.MaxUint32/2 {
			leftKey = key
		} else {
			rightKey = key
		}
	}

	await := func(ch <-chan sessionResult) sessionResult {
		select {
		case result := <-ch:
			return result
		case <-time.After(10 * time.Second):
			require.FailNow(t, "the operation is stuck on the session of the split shard",
				"creations rejected on shard 0: %d", server.rejected.Load())
			return sessionResult{}
		}
	}

	first := execute(leftKey)
	// After its 8th attempt, the creation waits 854 ms at least
	require.Eventually(t, func() bool { return server.rejected.Load() >= 8 }, 30*time.Second, time.Millisecond)
	splitAt := time.Now()
	shardManager.split(0, 1, 2)
	second := execute(rightKey)
	assert.Equal(t, sessionResult{shard: 1, sessionId: 11}, await(first))
	assert.Less(t, time.Since(splitAt), 500*time.Millisecond, "the creation waited for its next attempt")
	assert.Equal(t, sessionResult{shard: 2, sessionId: 12}, await(second))

	require.NoError(t, s.Close())
	server.Lock()
	defer server.Unlock()
	assert.Equal(t, []int64{1, 2}, server.created)
	assert.ElementsMatch(t, []shardSession{{1, 11}, {2, 12}}, server.closed)
}
