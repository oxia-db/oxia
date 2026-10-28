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

package perf

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecStopsWhenItsContextIsDone(t *testing.T) {
	server := newStandalone(t)
	previous := config
	t.Cleanup(func() { config = previous })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	Cmd.SetArgs([]string{"--service-address", server.ServiceAddr(), "--request-timeout", "200ms"})

	assert.NoError(t, Cmd.ExecuteContext(ctx))
}

func TestRunPerfStopsOnClose(t *testing.T) {
	server := newStandalone(t)
	previous := config
	config = testConfig(server.ServiceAddr(), false)
	t.Cleanup(func() { config = previous })

	process, err := runPerf()
	require.NoError(t, err)

	client := newSyncClient(t, server.ServiceAddr())
	assert.Eventually(t, func() bool {
		keys, err := client.List(context.Background(), "key-", "key-~") //nolint:staticcheck
		return err == nil && len(keys) > 0
	}, 10*time.Second, 10*time.Millisecond)

	require.NoError(t, process.Close())
	assert.ErrorIs(t, process.(*closer).ctx.Err(), context.Canceled)
}

func TestRunPerfReturnsClientCreationError(t *testing.T) {
	previous := config
	config = testConfig("localhost:1", false)
	t.Cleanup(func() { config = previous })

	process, err := runPerf()

	assert.ErrorContains(t, err, "failed to create Oxia client")
	assert.Nil(t, process)
}
