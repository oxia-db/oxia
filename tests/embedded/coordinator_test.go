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

package embedded

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func newCoordinatorOptions(name string) *coordinatoroption.Options {
	options := coordinatoroption.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.Name = name
	return options
}

func newFileCoordinatorOptions(name, dir string) *coordinatoroption.Options {
	options := newCoordinatorOptions(name)
	options.Metadata.ProviderName = coordinatoroption.ProviderFile
	options.Metadata.File.Dir = dir
	return options
}

type startResult struct {
	server coordinator.Coordinator
	err    error
}

func startCoordinator(ctx context.Context, options *coordinatoroption.Options) <-chan startResult {
	started := make(chan startResult, 1)
	go func() {
		server, err := coordinator.New(ctx, commonwatch.New(options))
		started <- startResult{server: server, err: err}
	}()
	return started
}

func requireStartReturns(t *testing.T, started <-chan startResult) startResult {
	t.Helper()

	select {
	case result := <-started:
		return result
	case <-time.After(time.Minute):
		require.FailNow(t, "coordinator.New did not return")
		return startResult{}
	}
}

// A standby coordinator blocks in New while another holds the leadership.
// Cancelling its context ends the wait: New releases what it started and
// returns the context's error.
func TestCoordinatorStandbyStartIsCancellable(t *testing.T) {
	dir := t.TempDir()

	leader, err := coordinator.New(t.Context(), commonwatch.New(newFileCoordinatorOptions("leader", dir)))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, leader.Close()) })

	ctx, cancel := context.WithCancel(t.Context())
	started := startCoordinator(ctx, newFileCoordinatorOptions("standby", dir))
	select {
	case result := <-started:
		require.FailNow(t, "the standby started while the leader holds the leadership", "err: %v", result.err)
	case <-time.After(500 * time.Millisecond):
	}

	cancel()
	result := requireStartReturns(t, started)
	require.ErrorIs(t, result.err, context.Canceled)
	assert.Nil(t, result.server)
}
