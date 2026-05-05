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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestClientPoolClearsPooledConnectionWhenHealthCheckFails(t *testing.T) {
	setHealthProbeTimingForTest(t, 20*time.Millisecond, 100*time.Millisecond)

	healthServer := health.NewServer()
	target := startTestGrpcServer(t, func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, healthServer)
	})

	pool := NewClientPool(nil, nil)
	defer pool.Close()
	poolInstance := pool.(*clientPool)

	_, err := pool.GetClientRpc(target)
	require.NoError(t, err)

	var first *connection
	require.Eventually(t, func() bool {
		poolInstance.RLock()
		defer poolInstance.RUnlock()
		first = poolInstance.connections[target]
		return first != nil
	}, time.Second, 10*time.Millisecond)

	healthServer.Shutdown()
	require.Eventually(t, func() bool {
		poolInstance.RLock()
		defer poolInstance.RUnlock()
		return poolInstance.connections[target] == nil
	}, 3*time.Second, 10*time.Millisecond)

	healthServer.Resume()
	_, err = pool.GetClientRpc(target)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		poolInstance.RLock()
		defer poolInstance.RUnlock()
		return poolInstance.connections[target] != nil && poolInstance.connections[target] != first
	}, time.Second, 10*time.Millisecond)
}

func TestClientPoolKeepsPooledConnectionWhenHealthServiceUnsupported(t *testing.T) {
	target := startTestGrpcServer(t, nil)

	pool := NewClientPool(nil, nil)
	defer pool.Close()
	poolInstance := pool.(*clientPool)

	_, err := pool.GetClientRpc(target)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		poolInstance.RLock()
		defer poolInstance.RUnlock()
		return poolInstance.connections[target] != nil
	}, time.Second, 10*time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	poolInstance.RLock()
	defer poolInstance.RUnlock()
	assert.NotNil(t, poolInstance.connections[target])
}

func setHealthProbeTimingForTest(t *testing.T, interval time.Duration, timeout time.Duration) {
	t.Helper()

	previousInterval := defaultGrpcClientHealthProbeInterval
	previousTimeout := defaultGrpcClientHealthProbeTimeout
	defaultGrpcClientHealthProbeInterval = interval
	defaultGrpcClientHealthProbeTimeout = timeout
	t.Cleanup(func() {
		defaultGrpcClientHealthProbeInterval = previousInterval
		defaultGrpcClientHealthProbeTimeout = previousTimeout
	})
}

func startTestGrpcServer(t *testing.T, register func(grpc.ServiceRegistrar)) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	if register != nil {
		register(server)
	}
	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	return listener.Addr().String()
}
