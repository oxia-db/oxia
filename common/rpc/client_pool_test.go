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

package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestClientPool_RemovesConnectionAfterHealthTimeout(t *testing.T) {
	_, _, target := newTestHealthServer(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	conn, err := newTestConnectionWithHealthConfig(pool, target,
		10*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
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
	_, target := newTestBlockingHealthServer(t)

	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	conn, err := newTestConnectionWithHealthConfig(pool, target,
		10*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
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

func TestClientPool_KeepsConnectionWhenHealthRecoversBeforeTimeout(t *testing.T) {
	_, healthServer, target := newTestHealthServer(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	pool := NewClientPool(nil, nil).(*clientPool)
	defer pool.Close()

	conn, err := newTestConnectionWithHealthConfig(pool, target,
		10*time.Millisecond, 10*time.Millisecond, 200*time.Millisecond)
	require.NoError(t, err)

	pool.Lock()
	pool.connections[target] = conn
	pool.Unlock()

	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	require.Never(t, func() bool {
		pool.RLock()
		defer pool.RUnlock()
		return pool.connections[target] != conn
	}, 250*time.Millisecond, 10*time.Millisecond)
}

func newTestConnectionWithHealthConfig(
	pool *clientPool,
	target string,
	healthInterval time.Duration,
	healthPingTimeout time.Duration,
	healthTimeout time.Duration,
) (*connection, error) {
	return newConnectionWithHealthConfig(
		target,
		pool.tls,
		pool.authentication,
		pool.dialOptions,
		pool.removeConnection,
		healthInterval,
		healthPingTimeout,
		healthTimeout,
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

type blockingHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (*blockingHealthServer) Check(
	ctx context.Context,
	_ *grpc_health_v1.HealthCheckRequest,
) (*grpc_health_v1.HealthCheckResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func newTestBlockingHealthServer(t *testing.T) (*grpc.Server, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(server, &blockingHealthServer{})

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
	})

	return server, listener.Addr().String()
}
