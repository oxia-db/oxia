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
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
)

func TestGrpcServer_CloseBeforeServeStarts(t *testing.T) {
	// Regression test: when Close() completes before the Serve() goroutine is
	// scheduled, Serve() returns grpc.ErrServerStopped. That must be treated as
	// a normal shutdown, not a startup failure that os.Exit(1)s the process.
	// GOMAXPROCS(1) keeps the Serve goroutine queued until after Close().
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	for i := 0; i < 100; i++ {
		s, err := Default.StartGrpcServer("test", "localhost:0",
			func(grpc.ServiceRegistrar) {}, nil, &auth.Options{}, nil)
		require.NoError(t, err)
		require.NoError(t, s.Close())
	}
}
