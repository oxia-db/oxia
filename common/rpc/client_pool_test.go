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

package rpc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/auth"
)

func TestClientPool_RemovesConnectionAfterHealthFailure(t *testing.T) {
	_, _, target := newTestHealthServer(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	conn, err := newTestConnectionWithHealthConfig(pool, target,
		10*time.Millisecond, 10*time.Millisecond)
	require.NoError(t, err)

	pool.Lock()
	pool.connections[target] = conn
	pool.Unlock()

	require.Eventually(t, func() bool {
		pool.RLock()
		defer pool.RUnlock()
		_, ok := pool.connections[target]
		return !ok
	}, time.Second, 10*time.Millisecond)
}

func TestClientPool_RemovesConnectionWhenHealthPingHangs(t *testing.T) {
	_, target, _ := newTestBlockingHealthServer(t)

	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	conn, err := newTestConnectionWithHealthConfig(pool, target,
		10*time.Millisecond, 10*time.Millisecond)
	require.NoError(t, err)

	pool.Lock()
	pool.connections[target] = conn
	pool.Unlock()

	require.Eventually(t, func() bool {
		pool.RLock()
		defer pool.RUnlock()
		_, ok := pool.connections[target]
		return !ok
	}, time.Second, 10*time.Millisecond)
}

func TestClientPool_KeepsConnectionOnTransientHealthFailures(t *testing.T) {
	_, target := newTestFlakyHealthServer(t, connectionHealthFailureThreshold-1)

	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	conn, err := newTestConnectionWithHealthConfig(pool, target,
		10*time.Millisecond, time.Second)
	require.NoError(t, err)

	pool.Lock()
	pool.connections[target] = conn
	pool.Unlock()

	// Fewer consecutive failures than the threshold must not tear the
	// connection down
	require.Never(t, func() bool {
		pool.RLock()
		defer pool.RUnlock()
		return pool.connections[target] != conn
	}, 300*time.Millisecond, 10*time.Millisecond)
}

func TestClientPool_RemovesConnectionAfterConsecutiveHealthFailures(t *testing.T) {
	_, target := newTestFlakyHealthServer(t, 1000)

	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	conn, err := newTestConnectionWithHealthConfig(pool, target,
		10*time.Millisecond, 10*time.Millisecond)
	require.NoError(t, err)

	pool.Lock()
	pool.connections[target] = conn
	pool.Unlock()

	require.Eventually(t, func() bool {
		pool.RLock()
		defer pool.RUnlock()
		_, ok := pool.connections[target]
		return !ok
	}, time.Second, 10*time.Millisecond)
}

func TestClientPool_KeepsConnectionWhenHealthIsServing(t *testing.T) {
	_, _, target := newTestHealthServer(t, grpc_health_v1.HealthCheckResponse_SERVING)

	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	conn, err := newTestConnectionWithHealthConfig(pool, target,
		10*time.Millisecond, 10*time.Millisecond)
	require.NoError(t, err)

	pool.Lock()
	pool.connections[target] = conn
	pool.Unlock()

	require.Never(t, func() bool {
		pool.RLock()
		defer pool.RUnlock()
		return pool.connections[target] != conn
	}, 250*time.Millisecond, 10*time.Millisecond)
}

func TestClientPool_KeepsConnectionWhenHealthCheckIsUnsupported(t *testing.T) {
	_, target := newTestGrpcServerWithoutHealth(t)

	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	conn, err := newTestConnectionWithHealthConfig(pool, target,
		10*time.Millisecond, 10*time.Millisecond)
	require.NoError(t, err)

	pool.Lock()
	pool.connections[target] = conn
	pool.Unlock()

	require.Never(t, func() bool {
		pool.RLock()
		defer pool.RUnlock()
		return pool.connections[target] != conn
	}, 100*time.Millisecond, 10*time.Millisecond)
}

func TestClientPoolGetRpcReturnsConnectionError(t *testing.T) {
	// Credentials that require TLS can't go over the plaintext connection to the target
	pool := NewClientPool(nil, auth.NewTokenAuthenticationWithToken("token", true))
	defer pool.Close()

	const target = "localhost:1"
	_, err := pool.GetClientRpc(target)
	assert.ErrorContains(t, err, "require transport level security")
	_, err = pool.GetHealthRpc(target)
	assert.ErrorContains(t, err, "require transport level security")
	_, err = pool.GetCoordinationRpc(target)
	assert.ErrorContains(t, err, "require transport level security")
	_, err = pool.GetReplicationRpc(target)
	assert.ErrorContains(t, err, "require transport level security")
	_, err = pool.GetAminRpc(target)
	assert.ErrorContains(t, err, "require transport level security")

	assert.Empty(t, pool.(*clientPool).connections)
}

func TestClientPoolGetRpcReusesConnection(t *testing.T) {
	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	const target = "localhost:1"
	_, err := pool.GetAminRpc(target)
	require.NoError(t, err)
	_, err = pool.GetReplicationRpc(target)
	require.NoError(t, err)
	assert.Len(t, pool.connections, 1)
}

func TestClientPoolCloseIgnoresConnectionCloseError(t *testing.T) {
	pool := NewClientPool(nil, nil).(*clientPool)

	const target = "localhost:1"
	_, err := pool.GetClientRpc(target)
	require.NoError(t, err)

	// The gRPC connection is already closed, so closing it again fails
	require.NoError(t, pool.connections[target].ClientConn.Close())
	assert.NoError(t, pool.Close())
}

func TestClientPoolRemoveConnectionIgnoresConnectionCloseError(t *testing.T) {
	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	const target = "localhost:1"
	_, err := pool.GetClientRpc(target)
	require.NoError(t, err)

	require.NoError(t, pool.connections[target].ClientConn.Close())
	pool.removeConnection(target)
	assert.Empty(t, pool.connections)

	// The next call opens a new connection
	_, err = pool.GetClientRpc(target)
	require.NoError(t, err)
	assert.Len(t, pool.connections, 1)
}

func TestClientPoolRemoveConnectionKeepsOtherTargets(t *testing.T) {
	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	const target = "localhost:1"
	_, err := pool.GetClientRpc(target)
	require.NoError(t, err)
	cnx := pool.connections[target]

	pool.removeConnection("localhost:2")
	assert.Same(t, cnx, pool.connections[target])
}

func newTestConnectionWithHealthConfig(
	pool *clientPool,
	target string,
	healthInterval time.Duration,
	healthPingTimeout time.Duration,
) (*connection, error) {
	return newConnectionWithHealthConfig(
		target,
		pool.tls,
		pool.authentication,
		pool.dialOptions,
		pool.removeConnection,
		healthInterval,
		healthPingTimeout,
	)
}

func newTestHealthServer(
	t *testing.T,
	status grpc_health_v1.HealthCheckResponse_ServingStatus,
) (*grpc.Server, *health.Server, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", status)

	server := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
	})

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	healthClient := grpc_health_v1.NewHealthClient(conn)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		return err == nil
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, conn.Close())

	return server, healthServer, listener.Addr().String()
}

// flakyHealthServer fails the first remainingFailures Check calls and then
// answers SERVING, modeling probes that fail transiently on a busy server.
type flakyHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	remainingFailures atomic.Int32
}

func (f *flakyHealthServer) Check(
	context.Context,
	*grpc_health_v1.HealthCheckRequest,
) (*grpc_health_v1.HealthCheckResponse, error) {
	if f.remainingFailures.Add(-1) >= 0 {
		return nil, grpcstatus.Error(codes.DeadlineExceeded, "health check too slow")
	}
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func newTestFlakyHealthServer(t *testing.T, failures int32) (*grpc.Server, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	healthServer := &flakyHealthServer{}
	healthServer.remainingFailures.Store(failures)

	server := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
	})

	return server, listener.Addr().String()
}

// blockingHealthServer answers no Check call, and signals each one on checks
// when it has room.
type blockingHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	checks chan struct{}
}

func (s *blockingHealthServer) Check(
	ctx context.Context,
	_ *grpc_health_v1.HealthCheckRequest,
) (*grpc_health_v1.HealthCheckResponse, error) {
	select {
	case s.checks <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func newTestBlockingHealthServer(t *testing.T) (*grpc.Server, string, <-chan struct{}) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	healthServer := &blockingHealthServer{checks: make(chan struct{}, 1)}
	server := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
	})

	return server, listener.Addr().String(), healthServer.checks
}

func newTestGrpcServerWithoutHealth(t *testing.T) (*grpc.Server, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
	})

	return server, listener.Addr().String()
}
