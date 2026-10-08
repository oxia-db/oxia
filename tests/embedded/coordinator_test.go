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
	"fmt"
	"net"
	"os"
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
	"github.com/oxia-db/oxia/common/proto"
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

// freeAddress returns a loopback address with a port that was free a moment
// ago, for the listeners that cannot bind an ephemeral port: a raft node's
// address is also its identity.
func freeAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

func requireHealth(t *testing.T, port int, expected grpc_health_v1.HealthCheckResponse_ServingStatus) {
	t.Helper()

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	response, err := grpc_health_v1.NewHealthClient(conn).Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, expected, response.GetStatus())
}

func listNamespaces(t *testing.T, port int) (*proto.ListNamespacesResponse, error) {
	t.Helper()

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	return proto.NewOxiaAdminClient(conn).ListNamespaces(t.Context(), &proto.ListNamespacesRequest{})
}

type startResult struct {
	server *coordinator.GrpcServer
	err    error
}

func startCoordinator(ctx context.Context, options *coordinatoroption.Options, serverOpts ...coordinator.ServerOption) <-chan startResult {
	started := make(chan startResult, 1)
	go func() {
		server, err := coordinator.New(ctx, options, serverOpts...)
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

// An invalid initial cluster configuration fails New before the coordinator
// binds its ports or touches the metadata.
func TestCoordinatorRejectsInvalidInitialConfiguration(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, occupied.Close()) })

	dir := t.TempDir()
	options := newFileCoordinatorOptions("coordinator", dir)
	options.Server.Public.BindAddress = occupied.Addr().String()

	_, err = coordinator.New(t.Context(), options,
		coordinator.WithInitialClusterConfiguration(&proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:              constant.DefaultNamespace,
				ReplicationFactor: 3,
				InitialShardCount: 1,
			}},
			Servers: []*proto.DataServerIdentity{{Public: "localhost:6648", Internal: "localhost:6649"}},
		}))
	require.ErrorContains(t, err, "invalid initial cluster configuration")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "nothing is written to the metadata directory")
}

// A standby coordinator blocks in New while another holds the leadership.
// Cancelling its context ends the wait: New releases what it started and
// returns the context's error.
func TestCoordinatorStandbyStartIsCancellable(t *testing.T) {
	dir := t.TempDir()

	leader, err := coordinator.New(t.Context(), newFileCoordinatorOptions("leader", dir))
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

// A coordinator with a WithOnLeadershipLost handler that loses the leadership
// stops coordinating by itself: by the time the handler runs, its health is
// NOT_SERVING. Closing it afterwards completes.
//
// The coordinators share a raft group of three. The one that wins is
// discovered from its New returning; cancelling the starts of the other two
// shuts their raft nodes down, and the winner loses the quorum.
func TestCoordinatorStopsCoordinatingOnLeadershipLoss(t *testing.T) {
	raftAddresses := []string{freeAddress(t), freeAddress(t), freeAddress(t)}
	raftDir := t.TempDir()

	type indexedResult struct {
		index int
		startResult
	}
	results := make(chan indexedResult, len(raftAddresses))
	lost := make(chan struct{})
	cancels := make([]context.CancelFunc, len(raftAddresses))
	for i, address := range raftAddresses {
		options := newCoordinatorOptions(address)
		options.Metadata.ProviderName = coordinatoroption.ProviderRaft
		options.Metadata.Raft.Address = address
		options.Metadata.Raft.BootstrapNodes = raftAddresses
		options.Metadata.Raft.DataDir = filepath.Join(raftDir, strconv.Itoa(i))

		ctx, cancel := context.WithCancel(t.Context())
		cancels[i] = cancel
		t.Cleanup(cancel)
		started := startCoordinator(ctx, options, coordinator.WithOnLeadershipLost(func() { close(lost) }))
		go func() { results <- indexedResult{index: i, startResult: <-started} }()
	}

	var leader indexedResult
	select {
	case leader = <-results:
		require.NoError(t, leader.err)
	case <-time.After(time.Minute):
		require.FailNow(t, "no coordinator became leader")
	}
	requireHealth(t, leader.server.InternalPort(), grpc_health_v1.HealthCheckResponse_SERVING)
	_, err := listNamespaces(t, leader.server.PublicPort())
	require.NoError(t, err)

	for i, cancel := range cancels {
		if i != leader.index {
			cancel()
		}
	}
	for range len(raftAddresses) - 1 {
		select {
		case standby := <-results:
			require.ErrorIs(t, standby.err, context.Canceled)
		case <-time.After(time.Minute):
			require.FailNow(t, "a cancelled standby did not return from New")
		}
	}

	select {
	case <-lost:
	case <-time.After(time.Minute):
		require.FailNow(t, "the leader did not report losing the leadership")
	}
	requireHealth(t, leader.server.InternalPort(), grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	_, err = listNamespaces(t, leader.server.PublicPort())
	require.Error(t, err, "a coordinator that lost the leadership turns the admin API away")

	closed := make(chan error, 1)
	go func() { closed <- leader.server.Close() }()
	select {
	case err := <-closed:
		assert.NoError(t, err)
	case <-time.After(time.Minute):
		require.FailNow(t, "closing the coordinator after the loss did not complete")
	}
}
