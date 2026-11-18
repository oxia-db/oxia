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
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
)

// testOxiaClientServer answers every data request with an empty success, or
// runs the hook of a test for the lists and the range scans.
type testOxiaClientServer struct {
	proto.UnimplementedOxiaClientServer
	list      func(*proto.ListRequest, proto.OxiaClient_ListServer) error
	rangeScan func(*proto.RangeScanRequest, proto.OxiaClient_RangeScanServer) error
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

func (s testOxiaClientServer) List(request *proto.ListRequest, stream proto.OxiaClient_ListServer) error {
	if s.list != nil {
		return s.list(request, stream)
	}
	return stream.Send(&proto.ListResponse{})
}

func (s testOxiaClientServer) RangeScan(request *proto.RangeScanRequest,
	stream proto.OxiaClient_RangeScanServer) error {
	if s.rangeScan != nil {
		return s.rangeScan(request, stream)
	}
	return stream.Send(&proto.RangeScanResponse{})
}

// notLeaderOxiaClientServer is a node that no longer leads the shard: it
// rejects every data and session request with a leader hint.
type notLeaderOxiaClientServer struct {
	proto.UnimplementedOxiaClientServer
	leaderHint string
	onRequest  func()
}

func (s notLeaderOxiaClientServer) notLeader() error {
	s.onRequest()
	return constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader, constant.WithLeaderHint(0, s.leaderHint))
}

func (s notLeaderOxiaClientServer) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return s.notLeader()
}

func (s notLeaderOxiaClientServer) Read(*proto.ReadRequest, proto.OxiaClient_ReadServer) error {
	return s.notLeader()
}

func (s notLeaderOxiaClientServer) List(*proto.ListRequest, proto.OxiaClient_ListServer) error {
	return s.notLeader()
}

func (s notLeaderOxiaClientServer) RangeScan(*proto.RangeScanRequest, proto.OxiaClient_RangeScanServer) error {
	return s.notLeader()
}

func (s notLeaderOxiaClientServer) CreateSession(context.Context, *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return nil, s.notLeader()
}

func (s notLeaderOxiaClientServer) KeepAlive(context.Context, *proto.SessionHeartbeat) (*proto.KeepAliveResponse, error) {
	return nil, s.notLeader()
}

func (s notLeaderOxiaClientServer) CloseSession(context.Context, *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	return nil, s.notLeader()
}

// writeServer counts the write requests it receives, and answers them unless it
// is stuck, like a deposed leader that is still reachable, or a saturated one.
type writeServer struct {
	proto.UnimplementedOxiaClientServer
	stuck     bool
	onRequest func()
	requests  atomic.Int64
}

func (s *writeServer) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		s.requests.Add(1)
		if s.onRequest != nil {
			s.onRequest()
		}
		if s.stuck {
			continue
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

func startTestOxiaClientServer(t *testing.T, oxiaClientServer proto.OxiaClientServer) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	proto.RegisterOxiaClientServer(server, oxiaClientServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// unreachableAddress returns an address where nothing is listening, so
// connecting to it fails with "connection refused". The port is in
// [10000, 32768): above the fixed ports that tests bind, e.g. Oxia's default
// 6648, and below the default ephemeral port ranges of Linux (32768-60999),
// macOS and Windows (49152-65535), so the kernel doesn't give it to a socket
// that binds port 0 meanwhile, e.g. a listener of a test package that runs in
// parallel, which would accept the connection. The scan starts at a random
// port, so that it rarely picks a port that a parallel test binds.
func unreachableAddress(t *testing.T) string {
	t.Helper()

	const minFixedPort, ephemeralPortsStart = 10000, 32768
	const ports = ephemeralPortsStart - minFixedPort
	start := rand.IntN(ports)
	for i := 0; ; i++ {
		require.Less(t, i, ports, "no free port below the ephemeral port range")
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", minFixedPort+(start+i)%ports))
		if err != nil {
			continue
		}
		address := listener.Addr().String()
		require.NoError(t, listener.Close())
		return address
	}
}

type testShardManager struct {
	ShardManager
	leader atomic.Pointer[string]
}

func (*testShardManager) Exists(int64) bool { return true }

func (m *testShardManager) Leader(int64) string { return *m.leader.Load() }

func (*testShardManager) Changed() <-chan struct{} { return nil }

func executeTestWrite(ctx context.Context, provider RpcProvider, shardId int64) error {
	_, err := provider.ExecuteWrite(ctx, shardId, func() (*proto.WriteRequest, error) {
		return &proto.WriteRequest{
			Shard: &shardId,
			Puts:  []*proto.PutRequest{{Key: "key", Value: []byte("value")}},
		}, nil
	})
	return err
}

type shardRequest struct {
	name    string
	execute func(context.Context, RpcProvider) error
}

func shardRequests(shardId *int64) []shardRequest {
	return []shardRequest{
		{"write", func(ctx context.Context, provider RpcProvider) error {
			return executeTestWrite(ctx, provider, *shardId)
		}},
		{"read", func(ctx context.Context, provider RpcProvider) error {
			_, err := provider.ExecuteRead(ctx, &proto.ReadRequest{
				Shard: shardId,
				Gets:  []*proto.GetRequest{{Key: "key"}},
			})
			return err
		}},
		{"list", func(ctx context.Context, provider RpcProvider) error {
			return provider.ExecuteList(ctx, &proto.ListRequest{Shard: shardId},
				func(*proto.ListResponse) error { return nil })
		}},
		{"range-scan", func(ctx context.Context, provider RpcProvider) error {
			return provider.ExecuteRangeScan(ctx, &proto.RangeScanRequest{Shard: shardId},
				func(*proto.RangeScanResponse) error { return nil })
		}},
	}
}

// The address of a deleted pod neither refuses nor answers the connection. A request to it fails after the connect
// timeout with a status that the callers retry, instead of waiting for its whole deadline.
func TestRpcProviderRequestToHangingAddressFailsAfterConnectTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var accepted []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			accepted = append(accepted, c) // accepts the connection and never answers it
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		for _, c := range accepted {
			_ = c.Close()
		}
	})

	provider := newRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, listener.Addr().String(),
		func() ShardManager { return nil }, nil)
	t.Cleanup(func() { _ = provider.Close() })

	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	_, err = provider.CreateSession(ctx, listener.Addr().String(), &proto.CreateSessionRequest{})

	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Less(t, time.Since(start), 8*time.Second)
}

// When the leader of a shard goes away, the client keeps sending the requests
// for that shard to the old leader until it receives the updated shard
// assignments. These attempts fail with a transport-level Unavailable error,
// which must be retried until the new leader is known.
func TestRpcProvider_RetryWhenShardLeaderIsUnreachable(t *testing.T) {
	shardId := int64(0)
	for _, tt := range shardRequests(&shardId) {
		t.Run(tt.name, func(t *testing.T) {
			oldLeader := unreachableAddress(t)
			newLeader := startTestOxiaClientServer(t, testOxiaClientServer{})

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

// A leader hint only steers the next attempt: when the hinted leader is
// unreachable, the request must go back to the shard assignments, which know
// the new leader by then.
func TestRpcProvider_RetryWhenHintedLeaderIsUnreachable(t *testing.T) {
	shardId := int64(0)
	for _, tt := range shardRequests(&shardId) {
		t.Run(tt.name, func(t *testing.T) {
			newLeader := startTestOxiaClientServer(t, testOxiaClientServer{})
			shardManager := &testShardManager{}
			oldLeader := startTestOxiaClientServer(t, notLeaderOxiaClientServer{
				leaderHint: unreachableAddress(t),
				onRequest:  func() { shardManager.leader.Store(&newLeader) },
			})
			shardManager.leader.Store(&oldLeader)

			provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, newLeader,
				func() ShardManager { return shardManager })
			defer func() {
				assert.NoError(t, provider.Close())
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			assert.NoError(t, tt.execute(ctx, provider))
		})
	}
}

// A leader that was deposed, but is still reachable, keeps the write stream of
// the client open without answering the writes. Once the shard assignments name
// the new leader, the writes must go to it. The write in flight to the old
// leader must not be sent again to the new one: the old leader may apply it.
func TestRpcProvider_WritesGoToTheNewLeader(t *testing.T) {
	shardId := int64(0)
	shardManager := &testShardManager{}
	newLeader := &writeServer{}
	newLeaderAddress := startTestOxiaClientServer(t, newLeader)
	oldLeader := &writeServer{
		stuck: true,
		// The shard assignments name the new leader while the write is in flight
		onRequest: func() { shardManager.leader.Store(&newLeaderAddress) },
	}
	oldLeaderAddress := startTestOxiaClientServer(t, oldLeader)
	shardManager.leader.Store(&oldLeaderAddress)

	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, newLeaderAddress,
		func() ShardManager { return shardManager })
	defer func() {
		assert.NoError(t, provider.Close())
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, executeTestWrite(ctx, provider, shardId), context.DeadlineExceeded)

	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	assert.NoError(t, executeTestWrite(ctx, provider, shardId))

	assert.EqualValues(t, 1, oldLeader.requests.Load())
	assert.EqualValues(t, 1, newLeader.requests.Load())
}

func TestRpcProviderWritesGoToTheNewLeaderWhenTheOldStreamFailsToClose(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	shardId := int64(0)
	shardManager := &testShardManager{}
	oldLeader := &writeServer{}
	oldLeaderAddress := startTestOxiaClientServer(t, oldLeader)
	newLeader := &writeServer{}
	newLeaderAddress := startTestOxiaClientServer(t, newLeader)
	shardManager.leader.Store(&oldLeaderAddress)
	closeFailures := atomic.Int64{}
	failpoints := failpoint.New(map[string]func() error{
		"getWriteStream.closeOldStream": func() error {
			closeFailures.Add(1)
			return errors.New("injected")
		},
	})

	provider := newRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, newLeaderAddress,
		func() ShardManager { return shardManager }, failpoints)
	defer func() {
		assert.NoError(t, provider.Close())
	}()

	require.NoError(t, executeTestWrite(t.Context(), provider, shardId))
	shardManager.leader.Store(&newLeaderAddress)
	require.NoError(t, executeTestWrite(t.Context(), provider, shardId))

	assert.EqualValues(t, 1, closeFailures.Load())
	assert.EqualValues(t, 1, oldLeader.requests.Load())
	assert.EqualValues(t, 1, newLeader.requests.Load())
}

// Connecting to a leader that cannot be reached, e.g. because its address is
// blackholed, can take up to the gRPC connect timeout. Setting up the write
// stream must not outlast the request, and the request must never be sent after
// its deadline.
func TestRpcProvider_WriteStreamSetupRespectsTheDeadline(t *testing.T) {
	shardId := int64(0)
	leader := &writeServer{}
	leaderAddress := startTestOxiaClientServer(t, leader)
	shardManager := &testShardManager{}
	shardManager.leader.Store(&leaderAddress)

	// Connecting to the leader takes longer than the request timeout
	connect := make(chan struct{})
	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		select {
		case <-connect:
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, leaderAddress,
		func() ShardManager { return shardManager }, grpc.WithContextDialer(dialer))
	defer func() {
		assert.NoError(t, provider.Close())
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	assert.ErrorIs(t, executeTestWrite(ctx, provider, shardId), context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)

	// Once connected, the request is not sent
	close(connect)
	assert.Never(t, func() bool { return leader.requests.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond)
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
			var requests atomic.Int64
			target := startTestOxiaClientServer(t, notLeaderOxiaClientServer{
				leaderHint: "new-leader:6648",
				onRequest:  func() { requests.Add(1) },
			})

			provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, "",
				func() ShardManager { return nil })
			defer func() {
				assert.NoError(t, provider.Close())
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			assert.ErrorIs(t, tt.execute(ctx, provider, target), constant.ErrNodeIsNotLeader)
			assert.EqualValues(t, 1, requests.Load())
		})
	}
}

// recordingClientPool records the targets that connections are requested for,
// and fails to connect with err, if set.
type recordingClientPool struct {
	rpc.ClientPool
	mutex   sync.Mutex
	targets []string
	err     error
}

func (p *recordingClientPool) GetClientRpc(target string) (proto.OxiaClientClient, error) {
	p.mutex.Lock()
	p.targets = append(p.targets, target)
	p.mutex.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	return p.ClientPool.GetClientRpc(target)
}

func (p *recordingClientPool) Targets() []string {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return slices.Clone(p.targets)
}

// While a shard has no leader, e.g. during an election, its leader address is
// empty: the client must not connect to it. The shard requests are retried
// until the leader is known, while the session requests fail fast.
func TestRpcProvider_ShardWithoutLeader(t *testing.T) {
	shardId := int64(0)
	noLeader := ""
	leader := startTestOxiaClientServer(t, testOxiaClientServer{})
	shardManager := &testShardManager{}
	shardManager.leader.Store(&noLeader)

	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, leader,
		func() ShardManager { return shardManager })
	pool := &recordingClientPool{ClientPool: provider.(*rpcProvider).clientPool}
	provider.(*rpcProvider).clientPool = pool
	defer func() {
		assert.NoError(t, provider.Close())
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := provider.KeepAlive(ctx, shardManager.Leader(shardId), &proto.SessionHeartbeat{Shard: shardId})
	assert.Equal(t, codes.Unavailable, status.Code(err), "unexpected error: %v", err)

	time.AfterFunc(300*time.Millisecond, func() { shardManager.leader.Store(&leader) })
	_, err = provider.ExecuteRead(ctx, &proto.ReadRequest{
		Shard: &shardId,
		Gets:  []*proto.GetRequest{{Key: "key"}},
	})
	assert.NoError(t, err)

	assert.NotContains(t, pool.Targets(), "")
}

// The requests without a shard go to the service address.
func TestRpcProviderExecuteReadWithoutShard(t *testing.T) {
	serviceAddress := startTestOxiaClientServer(t, testOxiaClientServer{})
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, serviceAddress,
		func() ShardManager { return nil })
	defer func() {
		assert.NoError(t, provider.Close())
	}()

	response, err := provider.ExecuteRead(t.Context(), &proto.ReadRequest{Gets: []*proto.GetRequest{{Key: "key"}}})
	require.NoError(t, err)
	require.Len(t, response.Gets, 1)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, response.Gets[0].Status)
}

// A shard missing from the shard map was split or merged: the shard requests
// fail right away, so that the caller reroutes them.
func TestRpcProviderShardRequestsFailOnUnknownShard(t *testing.T) {
	shardId := int64(0)
	for _, tt := range shardRequests(&shardId) {
		t.Run(tt.name, func(t *testing.T) {
			provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, "",
				func() ShardManager { return nil })
			defer func() {
				assert.NoError(t, provider.Close())
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			assert.ErrorIs(t, tt.execute(ctx, provider), constant.ErrShardNotFound)
		})
	}
}

func TestRpcProviderShardRequestsFailWhenConnectionFails(t *testing.T) {
	errConnect := errors.New("invalid target")
	shardId := int64(0)
	leader := "leader:6648"
	for _, tt := range shardRequests(&shardId) {
		t.Run(tt.name, func(t *testing.T) {
			shardManager := &testShardManager{}
			shardManager.leader.Store(&leader)
			provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, "",
				func() ShardManager { return shardManager })
			pool := &recordingClientPool{ClientPool: provider.(*rpcProvider).clientPool, err: errConnect}
			provider.(*rpcProvider).clientPool = pool
			defer func() {
				assert.NoError(t, provider.Close())
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			assert.ErrorIs(t, tt.execute(ctx, provider), errConnect)
			assert.Equal(t, []string{leader}, pool.Targets())
		})
	}
}

// fixedTargetRequests are the requests that go to a target chosen by the caller.
func fixedTargetRequests() []shardRequest {
	return []shardRequest{
		{"shard-assignments", func(ctx context.Context, provider RpcProvider) error {
			_, err := provider.GetShardAssignments(ctx, "", &proto.ShardAssignmentsRequest{})
			return err
		}},
		{"sequence-updates", func(ctx context.Context, provider RpcProvider) error {
			_, err := provider.GetSequenceUpdates(ctx, "", &proto.GetSequenceUpdatesRequest{})
			return err
		}},
		{"notifications", func(ctx context.Context, provider RpcProvider) error {
			_, err := provider.GetNotifications(ctx, "", &proto.NotificationsRequest{})
			return err
		}},
		{"create-session", func(ctx context.Context, provider RpcProvider) error {
			_, err := provider.CreateSession(ctx, "", &proto.CreateSessionRequest{})
			return err
		}},
		{"close-session", func(ctx context.Context, provider RpcProvider) error {
			_, err := provider.CloseSession(ctx, "", &proto.CloseSessionRequest{})
			return err
		}},
	}
}

// The empty target is the leader of a shard that has no leader: the requests
// to a fixed target fail fast without connecting to it.
func TestRpcProviderFixedTargetRequestsFailWithoutLeader(t *testing.T) {
	for _, tt := range fixedTargetRequests() {
		t.Run(tt.name, func(t *testing.T) {
			provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, "",
				func() ShardManager { return nil })
			pool := &recordingClientPool{ClientPool: provider.(*rpcProvider).clientPool}
			provider.(*rpcProvider).clientPool = pool
			defer func() {
				assert.NoError(t, provider.Close())
			}()

			assert.ErrorIs(t, tt.execute(t.Context(), provider), errShardHasNoLeader)
			assert.Empty(t, pool.Targets())
		})
	}
}

func TestRpcProviderGetSequenceUpdates(t *testing.T) {
	target := startTestOxiaClientServer(t, testOxiaClientServer{})
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, "",
		func() ShardManager { return nil })
	defer func() {
		assert.NoError(t, provider.Close())
	}()

	// The server does not implement the request: the stream reports it
	stream, err := provider.GetSequenceUpdates(t.Context(), target, &proto.GetSequenceUpdatesRequest{Key: "key"})
	require.NoError(t, err)
	_, err = stream.Recv()
	assert.Equal(t, codes.Unimplemented, status.Code(err), "unexpected error: %v", err)
}

func TestRpcProviderStreamRequestsFailOnCanceledContext(t *testing.T) {
	target := startTestOxiaClientServer(t, testOxiaClientServer{})
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, "",
		func() ShardManager { return nil })
	defer func() {
		assert.NoError(t, provider.Close())
	}()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := provider.GetSequenceUpdates(ctx, target, &proto.GetSequenceUpdatesRequest{Key: "key"})
	assert.Equal(t, codes.Canceled, status.Code(err), "unexpected error: %v", err)
	_, err = provider.GetNotifications(ctx, target, &proto.NotificationsRequest{})
	assert.Equal(t, codes.Canceled, status.Code(err), "unexpected error: %v", err)
}

func TestShardMapTimer(t *testing.T) {
	changed := make(chan struct{})
	timer := &ShardMapTimer{Changed: changed}
	defer timer.Stop()

	timer.Start(time.Millisecond)
	select {
	case <-timer.C():
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the timer did not fire")
	}

	timer.Start(time.Hour)
	close(changed)
	select {
	case <-timer.C():
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the timer did not fire on the shard map change")
	}
}

func TestExecuteWithRetryWakesUpOnShardMapChange(t *testing.T) {
	// Without the shard map change, the retry would wait at least 50 ms
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	changed := make(chan struct{})
	timer := &ShardMapTimer{}
	attempts := 0
	result, err := executeWithRetryTimer(ctx, timer, func(constant.ErrorMetadata) (int, error) {
		timer.Changed = changed
		attempts++
		if attempts == 1 {
			// The shard map changes while the attempt is in progress
			close(changed)
			return 0, constant.ErrNodeIsNotLeader
		}
		return attempts, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, result)
}

// The consumer of the responses of a list or a range scan ends it with an
// error, which is returned without a retry.
func TestRpcProviderListAndRangeScanConsumerError(t *testing.T) {
	var requests atomic.Int32
	keys := []string{"a", "b", "c"}
	address := startTestOxiaClientServer(t, testOxiaClientServer{
		list: func(_ *proto.ListRequest, stream proto.OxiaClient_ListServer) error {
			requests.Add(1)
			for _, key := range keys {
				if err := stream.Send(&proto.ListResponse{Keys: []string{key}}); err != nil {
					return err
				}
			}
			return nil
		},
		rangeScan: func(_ *proto.RangeScanRequest, stream proto.OxiaClient_RangeScanServer) error {
			requests.Add(1)
			for _, key := range keys {
				if err := stream.Send(&proto.RangeScanResponse{Records: []*proto.GetResponse{{Key: &key}}}); err != nil {
					return err
				}
			}
			return nil
		},
	})
	shardManager := &testShardManager{}
	shardManager.leader.Store(&address)
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, address,
		func() ShardManager { return shardManager })
	defer func() {
		assert.NoError(t, provider.Close())
	}()

	shardId := int64(0)
	failure := errors.New("failure")
	consumed := 0
	err := provider.ExecuteList(t.Context(), &proto.ListRequest{Shard: &shardId}, func(*proto.ListResponse) error {
		consumed++
		return failure
	})
	assert.ErrorIs(t, err, failure)
	assert.Equal(t, 1, consumed)
	assert.Equal(t, int32(1), requests.Load())

	consumed = 0
	err = provider.ExecuteRangeScan(t.Context(), &proto.RangeScanRequest{Shard: &shardId},
		func(*proto.RangeScanResponse) error {
			consumed++
			return failure
		})
	assert.ErrorIs(t, err, failure)
	assert.Equal(t, 1, consumed)
	assert.Equal(t, int32(2), requests.Load())
}

// Writes sent asynchronously on a shard reach the leader without waiting for
// the responses of the writes sent before them, in the order they were sent.
func TestRpcProviderExecuteWriteAsyncSendsBeforeTheResponses(t *testing.T) {
	server := newHeldWriteServer()
	provider := newHeldWriteProvider(t, server)
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := asyncPut(ctx, provider, shardId, "a")
	second := asyncPut(ctx, provider, shardId, "b")

	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 2 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a", "b"}, server.receivedKeys())

	server.answer <- nil
	server.answer <- nil
	_, err := first()
	require.NoError(t, err)
	_, err = second()
	require.NoError(t, err)
}

// A write whose response does not arrive before its deadline closes the
// stream: the writes in flight behind it fail at once instead of each waiting
// for its own deadline, and the next write is sent on a new stream.
func TestRpcProviderExecuteWriteAsyncDeadlineClosesTheStream(t *testing.T) {
	server := newHeldWriteServer()
	provider := newHeldWriteProvider(t, server)
	shardId := int64(0)

	short, cancelShort := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancelShort()
	long, cancelLong := context.WithTimeout(t.Context(), time.Minute)
	defer cancelLong()
	first := asyncPut(short, provider, shardId, "a")
	second := asyncPut(long, provider, shardId, "b")
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 2 }, 10*time.Second, time.Millisecond)

	_, err := first()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	failed := make(chan error, 1)
	go func() {
		_, err := second()
		failed <- err
	}()
	select {
	case err := <-failed:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		require.Fail(t, "the write behind the expired one kept waiting on the stream")
	}

	third := asyncPut(long, provider, shardId, "c")
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 3 }, 10*time.Second, time.Millisecond)
	server.answer <- nil
	_, err = third()
	require.NoError(t, err)
}

// A leader that steps down while writes are in flight rejects the stream
// mid-way, without a hint: it may have appended the writes before the one it
// rejected, and they may still commit. Every write in flight fails, in order,
// and none is sent again.
func TestRpcProviderExecuteWriteAsyncFailsTheWritesInFlightWhenTheLeaderStepsDown(t *testing.T) {
	server := newHeldWriteServer()
	provider := newHeldWriteProvider(t, server)
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := asyncPut(ctx, provider, shardId, "a")
	second := asyncPut(ctx, provider, shardId, "b")
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 2 }, 10*time.Second, time.Millisecond)

	server.answer <- constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader)
	_, err := first()
	require.ErrorIs(t, err, constant.ErrNodeIsNotLeader)
	_, err = second()
	require.ErrorIs(t, err, constant.ErrNodeIsNotLeader)

	third := asyncPut(ctx, provider, shardId, "c")
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 3 }, 10*time.Second, time.Millisecond)
	server.answer <- nil
	_, err = third()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, server.receivedKeys(), "no write was sent again")
}

// A server that marks the end of a stream as unprocessed answered every write
// it appended before rejecting one, and appended none after: the writes left in
// flight are sent again, in order.
func TestRpcProviderExecuteWriteAsyncSendsTheUnprocessedWritesAgain(t *testing.T) {
	server := newHeldWriteServer()
	provider := newHeldWriteProvider(t, server)
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := asyncPut(ctx, provider, shardId, "a")
	second := asyncPut(ctx, provider, shardId, "b")
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 2 }, 10*time.Second, time.Millisecond)

	server.answer <- constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader, constant.WithUnprocessed())
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 4 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a", "b", "a", "b"}, server.receivedKeys())
	server.answer <- nil
	server.answer <- nil
	_, err := first()
	require.NoError(t, err)
	_, err = second()
	require.NoError(t, err)
}

// A node that is not the leader rejects a write stream at its setup, before
// reading any of its writes, and hints the leader: the writes in flight are
// sent again, in order, to the hinted leader, and the writes issued since
// follow them.
func TestRpcProviderExecuteWriteAsyncSendsTheWritesRejectedAtSetupToTheHintedLeader(t *testing.T) {
	newLeader := newHeldWriteServer()
	newLeaderAddress := startTestOxiaClientServer(t, newLeader)
	oldLeader := &setupRejectingServer{}
	oldLeader.hint.Store(&newLeaderAddress)
	oldLeaderAddress := startTestOxiaClientServer(t, oldLeader)

	shardManager := &testShardManager{}
	shardManager.leader.Store(&oldLeaderAddress)
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, oldLeaderAddress,
		func() ShardManager { return shardManager })
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := asyncPut(ctx, provider, shardId, "a")
	second := asyncPut(ctx, provider, shardId, "b")
	third := asyncPut(ctx, provider, shardId, "c")

	require.Eventually(t, func() bool { return len(newLeader.receivedKeys()) == 3 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a", "b", "c"}, newLeader.receivedKeys())
	for range 3 {
		newLeader.answer <- nil
	}
	for _, wait := range []func() (*proto.WriteResponse, error){first, second, third} {
		_, err := wait()
		require.NoError(t, err)
	}
}

// A leader that keeps rejecting the writes in flight gets them again with a
// growing delay, not in a busy loop.
func TestRpcProviderExecuteWriteAsyncResendsTheWritesRejectedAgainWithABackoff(t *testing.T) {
	server, address := startSelfRejectingServer(t)
	provider := newHeldWriteProviderAt(t, address, &testShardManager{})
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := asyncPut(ctx, provider, shardId, "a")()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, server.streams.Load(), int32(10), "the writes were sent again without a delay")
}

// When the shard of the writes that its leader rejected is gone, e.g. after a
// split, the writes fail with ErrShardNotFound, so that they are rerouted.
func TestRpcProviderExecuteWriteAsyncFailsTheWritesRejectedByAGoneShard(t *testing.T) {
	server, address := startSelfRejectingServer(t)
	shardManager := &removableShardManager{}
	provider := newHeldWriteProviderAt(t, address, &shardManager.testShardManager)
	provider.(*rpcProvider).shardManagerSupplier = func() ShardManager { return shardManager }
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	wait := asyncPut(ctx, provider, shardId, "a")
	require.Eventually(t, func() bool { return server.streams.Load() > 0 }, 10*time.Second, time.Millisecond)
	shardManager.removed.Store(true)

	_, err := wait()
	require.ErrorIs(t, err, constant.ErrShardNotFound)
}

// When the stream breaks, every write in flight on it fails, in order, and is
// never sent again: the leader may have applied it already. The next write is
// sent on a new stream.
func TestRpcProviderExecuteWriteAsyncFailsTheWritesInFlightWhenTheStreamBreaks(t *testing.T) {
	server := newHeldWriteServer()
	provider := newHeldWriteProvider(t, server)
	shardId := int64(0)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := asyncPut(ctx, provider, shardId, "a")
	second := asyncPut(ctx, provider, shardId, "b")
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 2 }, 10*time.Second, time.Millisecond)

	broken := status.Error(codes.Internal, "stream broken")
	server.answer <- broken
	_, err := first()
	require.Equal(t, codes.Internal, status.Code(err))
	_, err = second()
	require.Equal(t, codes.Internal, status.Code(err))

	third := asyncPut(ctx, provider, shardId, "c")
	require.Eventually(t, func() bool { return len(server.receivedKeys()) == 3 }, 10*time.Second, time.Millisecond)
	server.answer <- nil
	_, err = third()
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, server.receivedKeys(), "no write was sent again")
}

// A leader that was deposed, but is still reachable, keeps the write stream of
// the client open without answering the writes. Once the shard assignments name
// the new leader, the next write must go to it, after the write in flight to
// the old leader, which is not sent again: the old leader may apply it.
func TestRpcProviderExecuteWriteAsyncGoesToTheNewLeader(t *testing.T) {
	shardId := int64(0)
	shardManager := &testShardManager{}
	newLeader := &writeServer{}
	newLeaderAddress := startTestOxiaClientServer(t, newLeader)
	oldLeader := &writeServer{
		stuck: true,
		// The shard assignments name the new leader while the write is in flight
		onRequest: func() { shardManager.leader.Store(&newLeaderAddress) },
	}
	oldLeaderAddress := startTestOxiaClientServer(t, oldLeader)
	provider := newHeldWriteProviderAt(t, oldLeaderAddress, shardManager)

	short, cancelShort := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancelShort()
	first := asyncPut(short, provider, shardId, "a")
	require.Eventually(t, func() bool { return oldLeader.requests.Load() == 1 }, 10*time.Second, time.Millisecond)
	firstErr := make(chan error, 1)
	go func() {
		_, err := first()
		firstErr <- err
	}()

	long, cancelLong := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelLong()
	_, err := asyncPut(long, provider, shardId, "b")()
	require.NoError(t, err)
	assert.ErrorIs(t, <-firstErr, context.DeadlineExceeded)

	assert.EqualValues(t, 1, oldLeader.requests.Load())
	assert.EqualValues(t, 1, newLeader.requests.Load())
}

// Connecting to a leader that cannot be reached, e.g. because its address is
// blackholed, can take up to the gRPC connect timeout. Setting up the write
// stream must not outlast the request, and the request must never be sent after
// its deadline.
func TestRpcProviderExecuteWriteAsyncStreamSetupRespectsTheDeadline(t *testing.T) {
	shardId := int64(0)
	leader := &writeServer{}
	leaderAddress := startTestOxiaClientServer(t, leader)
	shardManager := &testShardManager{}
	shardManager.leader.Store(&leaderAddress)

	// Connecting to the leader takes longer than the request timeout
	connect := make(chan struct{})
	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		select {
		case <-connect:
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, leaderAddress,
		func() ShardManager { return shardManager }, grpc.WithContextDialer(dialer))
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := asyncPut(ctx, provider, shardId, "a")()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)

	// Once connected, the request is not sent
	close(connect)
	assert.Never(t, func() bool { return leader.requests.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond)
}

// heldWriteServer receives the write requests of a stream and holds their
// responses until the test answers them, so that several are in flight.
type heldWriteServer struct {
	proto.UnimplementedOxiaClientServer

	mu       sync.Mutex
	received []string
	streams  int
	// answer sends the response of the oldest request held, or ends the
	// stream with the error
	answer chan error
}

func newHeldWriteServer() *heldWriteServer {
	return &heldWriteServer{answer: make(chan error)}
}

func (s *heldWriteServer) WriteStream(stream proto.OxiaClient_WriteStreamServer) error {
	s.mu.Lock()
	s.streams++
	s.mu.Unlock()

	requests := make(chan *proto.WriteRequest, 100)
	go func() {
		defer close(requests)
		for {
			request, err := stream.Recv()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.received = append(s.received, request.Puts[0].Key)
			s.mu.Unlock()
			requests <- request
		}
	}()

	for {
		select {
		case err := <-s.answer:
			if err != nil {
				return err
			}
			request, ok := <-requests
			if !ok {
				return io.EOF
			}
			response := &proto.WriteResponse{}
			for range request.Puts {
				response.Puts = append(response.Puts, &proto.PutResponse{})
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *heldWriteServer) receivedKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...)
}

func newHeldWriteProvider(t *testing.T, server *heldWriteServer) RpcProvider {
	t.Helper()

	return newHeldWriteProviderAt(t, startTestOxiaClientServer(t, server), &testShardManager{})
}

// newHeldWriteProviderAt returns a provider whose shards are led by address.
func newHeldWriteProviderAt(t *testing.T, address string, shardManager *testShardManager) RpcProvider {
	t.Helper()

	shardManager.leader.Store(&address)
	provider := NewRpcProvider(t.Context(), constant.DefaultNamespace, nil, nil, address,
		func() ShardManager { return shardManager })
	t.Cleanup(func() { assert.NoError(t, provider.Close()) })
	return provider
}

func putRequest(shardId *int64, key string) *proto.WriteRequest {
	return &proto.WriteRequest{Shard: shardId, Puts: []*proto.PutRequest{{Key: key, Value: []byte(key)}}}
}

// asyncPut sends the put of key on the shard without waiting for its response.
func asyncPut(ctx context.Context, provider RpcProvider, shardId int64, key string) func() (*proto.WriteResponse, error) {
	return provider.ExecuteWriteAsync(ctx, shardId, fixed(putRequest(&shardId, key)))
}

// setupRejectingServer rejects every write stream at its setup, as a node that
// is not the leader, before reading any of its writes, and hints the leader in
// hint. It counts the streams.
type setupRejectingServer struct {
	proto.UnimplementedOxiaClientServer
	hint    atomic.Pointer[string]
	streams atomic.Int32
}

func (s *setupRejectingServer) WriteStream(proto.OxiaClient_WriteStreamServer) error {
	s.streams.Add(1)
	return constant.IntoGrpcStatusError(constant.ErrNodeIsNotLeader, constant.WithLeaderHint(0, *s.hint.Load()))
}

// startSelfRejectingServer starts a setupRejectingServer that hints itself.
func startSelfRejectingServer(t *testing.T) (*setupRejectingServer, string) {
	t.Helper()

	server := &setupRejectingServer{}
	address := startTestOxiaClientServer(t, server)
	server.hint.Store(&address)
	return server, address
}

// removableShardManager is a testShardManager whose shard can be removed, as
// after a split.
type removableShardManager struct {
	testShardManager
	removed atomic.Bool
}

func (m *removableShardManager) Exists(int64) bool { return !m.removed.Load() }
