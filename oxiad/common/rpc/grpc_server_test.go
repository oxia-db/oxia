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
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"syscall"
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
)

func TestGrpcServer_CloseBeforeServeStarts(t *testing.T) {
	// Regression test: when Close() completes before the Serve() goroutine is
	// scheduled, Serve() returns grpc.ErrServerStopped. That must be treated as
	// a normal shutdown, not a serve failure.
	// GOMAXPROCS(1) keeps the Serve goroutine queued until after Close().
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	for i := 0; i < 100; i++ {
		s, err := Default.StartGrpcServer("test", "localhost:0",
			func(grpc.ServiceRegistrar) {}, nil, &auth.Options{}, nil)
		require.NoError(t, err)
		require.NoError(t, s.Close())
	}
}

func TestGrpcServerCloseReleasesPort(t *testing.T) {
	// GOMAXPROCS(1) keeps the Serve goroutine queued until Close() has stopped
	// the server
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	for i := 0; i < 100; i++ {
		s, err := Default.StartGrpcServer("test", "127.0.0.1:0",
			func(grpc.ServiceRegistrar) {}, nil, &auth.Options{}, nil)
		require.NoError(t, err)
		require.NoError(t, s.Close())

		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port()))
		require.NoError(t, err, "the port is still bound after Close")
		require.NoError(t, listener.Close())
	}
}

func TestGrpcServerStartReturnsListenError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	s, err := Default.StartGrpcServer("public", listener.Addr().String(),
		func(grpc.ServiceRegistrar) {}, nil, &auth.Options{}, nil)

	require.ErrorIs(t, err, syscall.EADDRINUSE)
	assert.ErrorContains(t, err, "failed to start the public gRPC server on "+listener.Addr().String())
	assert.Nil(t, s)
}

func TestGrpcServerStartRejectsUnsupportedAuthProvider(t *testing.T) {
	registered := false
	s, err := Default.StartGrpcServer("test", "127.0.0.1:0",
		func(grpc.ServiceRegistrar) { registered = true }, nil, &auth.Options{Provider: "ldap"}, nil)

	require.ErrorIs(t, err, errs.ErrNotImplemented)
	require.Nil(t, s)
	require.False(t, registered)
}

func TestGrpcServerServeLogsFailure(t *testing.T) {
	logs := &bytes.Buffer{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	c := &defaultGrpcServer{
		server:    grpc.NewServer(),
		log:       slog.New(slog.NewTextHandler(logs, nil)),
		serveDone: make(chan struct{}),
	}

	c.serve(listener)

	assert.Contains(t, logs.String(), "Failed to serve")
	assert.NoError(t, c.Close())
}
