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
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/constant"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/tests/mock"
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

// A coordinator that loses the metadata leadership stops coordinating and
// reports the loss, and the process carries on: its health turns NOT_SERVING,
// Close completes, and new coordinators in the same process, on the same raft
// addresses and directories, elect a leader again.
//
// The coordinators share a raft group of three. The one that wins is the one
// whose New returns; cancelling the starts of the other two shuts their raft
// nodes down, and the winner loses the quorum.
func TestCoordinatorLeadershipLostThenRejoins(t *testing.T) {
	addresses := mock.FreeAddresses(t, 6)
	raftAddresses, internalAddresses := addresses[:3], addresses[3:]
	raftDir := t.TempDir()
	options := make([]*coordinatoroption.Options, len(raftAddresses))
	for i, address := range raftAddresses {
		options[i] = newCoordinatorOptions(address)
		options[i].Server.Internal.BindAddress = internalAddresses[i]
		options[i].Metadata.ProviderName = coordinatoroption.ProviderRaft
		options[i].Metadata.Raft.Address = address
		options[i].Metadata.Raft.BootstrapNodes = raftAddresses
		options[i].Metadata.Raft.DataDir = filepath.Join(raftDir, strconv.Itoa(i))
	}

	leaderIndex, leader, cancelStandbys := startRaftGroup(t, options)
	requireHealth(t, internalAddresses[leaderIndex], grpc_health_v1.HealthCheckResponse_SERVING)
	select {
	case <-leader.LeadershipLost():
		require.FailNow(t, "the leader reported a loss while it holds the quorum")
	default:
	}

	cancelStandbys()
	select {
	case <-leader.LeadershipLost():
	case <-time.After(time.Minute):
		require.FailNow(t, "the leader did not report losing the leadership")
	}
	requireHealth(t, internalAddresses[leaderIndex], grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	requireClose(t, leader)

	_, rejoined, cancelStandbys := startRaftGroup(t, options)
	cancelStandbys()
	requireClose(t, rejoined)
}

// startRaftGroup starts a coordinator for each of options, and returns the one
// that won the election, the first whose New returns, with its index.
// cancelStandbys cancels the starts of the others, and waits for them to
// return.
func startRaftGroup(t *testing.T, options []*coordinatoroption.Options) (int, coordinator.Coordinator, func()) {
	t.Helper()

	type indexedResult struct {
		index int
		startResult
	}
	results := make(chan indexedResult, len(options))
	cancels := make([]context.CancelFunc, len(options))
	for i, o := range options {
		ctx, cancel := context.WithCancel(t.Context())
		cancels[i] = cancel
		t.Cleanup(cancel)
		started := startCoordinator(ctx, o)
		go func() { results <- indexedResult{index: i, startResult: <-started} }()
	}

	var leader indexedResult
	select {
	case leader = <-results:
		require.NoError(t, leader.err)
	case <-time.After(time.Minute):
		require.FailNow(t, "no coordinator became the leader")
	}

	cancelStandbys := func() {
		for i, cancel := range cancels {
			if i != leader.index {
				cancel()
			}
		}
		for range len(options) - 1 {
			select {
			case standby := <-results:
				require.ErrorIs(t, standby.err, context.Canceled)
			case <-time.After(time.Minute):
				require.FailNow(t, "a cancelled standby did not return from New")
			}
		}
	}
	return leader.index, leader.server, cancelStandbys
}

func requireHealth(t *testing.T, address string, expected grpc_health_v1.HealthCheckResponse_ServingStatus) {
	t.Helper()

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	response, err := grpc_health_v1.NewHealthClient(conn).Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, expected, response.GetStatus())
}

func requireClose(t *testing.T, server coordinator.Coordinator) {
	t.Helper()

	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		require.FailNow(t, "closing the coordinator did not complete")
	}
}
