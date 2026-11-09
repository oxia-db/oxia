// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package coordinator

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/proto"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestQuorumLossTrackerObserve(t *testing.T) {
	start := time.Unix(1000, 0)
	for name, tt := range map[string]struct {
		steps []quorumObservation
	}{
		"a reachable majority never steps down":                   {steps: []quorumObservation{{20 * time.Second, 2, 3, false}, {40 * time.Second, 3, 3, false}}},
		"the loss of the majority steps down after the timeout":   {steps: []quorumObservation{{20 * time.Second, 1, 3, false}, {29 * time.Second, 1, 3, false}, {30 * time.Second, 1, 3, true}}},
		"a recovery restarts the count":                           {steps: []quorumObservation{{20 * time.Second, 1, 3, false}, {25 * time.Second, 2, 3, false}, {26 * time.Second, 1, 3, false}, {35 * time.Second, 1, 3, false}, {36 * time.Second, 1, 3, true}}},
		"a new leader gets the timeout to reach the data servers": {steps: []quorumObservation{{1 * time.Second, 0, 3, false}, {9 * time.Second, 0, 3, false}, {10 * time.Second, 0, 3, false}, {19 * time.Second, 0, 3, false}, {20 * time.Second, 0, 3, true}}},
		"fewer than 3 data servers never step down":               {steps: []quorumObservation{{20 * time.Second, 0, 2, false}, {3600 * time.Second, 0, 2, false}, {3601 * time.Second, 0, 0, false}}},
		"half of an even number is not a majority":                {steps: []quorumObservation{{20 * time.Second, 2, 4, false}, {30 * time.Second, 2, 4, true}}},
	} {
		t.Run(name, func(t *testing.T) {
			tracker := quorumLossTracker{timeout: 10 * time.Second, leaderSince: start}
			for _, step := range tt.steps {
				assert.Equal(t, step.stepDown, tracker.observe(step.reachable, step.total, start.Add(step.at)),
					"at %s: %d of %d", step.at, step.reachable, step.total)
			}
		})
	}
}

// A leader that cannot reach a majority of the data servers gives up the leadership: it stops coordinating, and
// releases the lease so that a standby takes over.
func TestCoordinatorStepsDownWhenItCannotReachTheDataServers(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	options := newTestOptions()
	options.Metadata.ProviderName = option.ProviderFile
	options.Metadata.File.StatusName = filepath.Join(t.TempDir(), option.DefaultFileStatusName)
	options.Metadata.File.ConfigName = filepath.Join(t.TempDir(), option.DefaultFileConfigName)
	options.Controller.StepDownTimeout = commonoption.Duration(time.Second)
	// Three data servers that nobody listens on
	servers := make([]*proto.DataServerIdentity, 3)
	for i := range servers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		require.NoError(t, listener.Close())
		servers[i] = &proto.DataServerIdentity{Public: address, Internal: address}
	}
	config, err := metadatacodec.ClusterConfigCodec.MarshalYAML(&proto.ClusterConfiguration{Servers: servers})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(options.Metadata.File.ConfigName, config, 0o600))

	exited := make(chan struct{})
	failpoint.Enable("coordinator.stepDownExit", func() { close(exited) })
	t.Cleanup(func() { failpoint.Disable("coordinator.stepDownExit") })
	server, err := New(t.Context(), commonwatch.New(options))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	// The standby waits for the lease that the leader holds
	standbyFactory, err := coordmetadata.New(t.Context(), options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, standbyFactory.Close()) })
	standby := standbyFactory.CreateMetadata(t.Context())
	t.Cleanup(func() { require.NoError(t, standby.Close()) })
	tookOver := make(chan error, 1)
	go func() {
		_, err := standby.WaitToBecomeLeader()
		tookOver <- err
	}()

	select {
	case err := <-tookOver:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		require.FailNow(t, "the standby did not take the leadership over")
	}
	select {
	case <-exited:
	case <-time.After(time.Minute):
		require.FailNow(t, "the coordinator did not exit after it released the lease")
	}
}

// Closing the coordinator stops the watcher, which would otherwise keep Close waiting.
func TestCoordinatorCloseStopsTheQuorumWatcher(t *testing.T) {
	options := newTestOptions()
	options.Controller.StepDownTimeout = commonoption.Duration(time.Minute)
	server, err := New(t.Context(), commonwatch.New(options))
	require.NoError(t, err)

	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		require.FailNow(t, "Close did not return")
	}
}

// quorumObservation is the number of data servers that the leader reaches at a time after it started, and whether
// the tracker tells to step down then.
type quorumObservation struct {
	at        time.Duration
	reachable int
	total     int
	stepDown  bool
}
