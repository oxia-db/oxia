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

package coordinator

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestNewRejectsInvalidOptions(t *testing.T) {
	options := newTestOptions()
	options.Metadata.ProviderName = "etcd"

	_, err := New(t.Context(), commonwatch.New(options))
	require.ErrorContains(t, err, "must be one of")
}

// Each part that fails to start fails New, which releases what it started
// before.
func TestNewFailsToStart(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, occupied.Close()) })
	missingFile := filepath.Join(t.TempDir(), "missing.pem")
	invalidTLS := security.TLSOptions{
		Enabled:       &constant.FlagTrue,
		CertFile:      missingFile,
		KeyFile:       missingFile,
		TrustedCaFile: missingFile,
	}
	notADirectory := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notADirectory, nil, 0o600))

	for _, test := range []struct {
		name   string
		modify func(options *option.Options)
	}{
		{"internal TLS", func(options *option.Options) { options.Server.Internal.TLS = invalidTLS }},
		{"internal listener", func(options *option.Options) {
			options.Server.Internal.BindAddress = occupied.Addr().String()
		}},
		{"controller TLS", func(options *option.Options) { options.Controller.TLS = invalidTLS }},
		{"metadata", func(options *option.Options) {
			options.Metadata.ProviderName = option.ProviderFile
			options.Metadata.File.Dir = filepath.Join(notADirectory, "metadata")
		}},
		{"public TLS", func(options *option.Options) { options.Server.Public.TLS = invalidTLS }},
		{"public listener", func(options *option.Options) {
			options.Server.Public.BindAddress = occupied.Addr().String()
		}},
		{"leadership", func(options *option.Options) {
			// The file provider locks the status file, which it creates
			readOnly := filepath.Join(t.TempDir(), "read-only")
			require.NoError(t, os.Mkdir(readOnly, 0o500))
			options.Metadata.ProviderName = option.ProviderFile
			options.Metadata.File.StatusName = filepath.Join(readOnly, option.DefaultFileStatusName)
			options.Metadata.File.ConfigName = filepath.Join(t.TempDir(), option.DefaultFileConfigName)
		}},
		{"metrics TLS", func(options *option.Options) {
			options.Observability.Metric = commonoption.MetricOptions{
				Enabled:     &constant.FlagTrue,
				BindAddress: "localhost:0",
				TLS:         invalidTLS,
			}
		}},
		{"metrics listener", func(options *option.Options) {
			options.Observability.Metric = commonoption.MetricOptions{
				Enabled:     &constant.FlagTrue,
				BindAddress: occupied.Addr().String(),
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := newTestOptions()
			test.modify(options)

			server, err := New(t.Context(), commonwatch.New(options))
			require.Error(t, err)
			assert.Nil(t, server)
		})
	}
}

// A runtime that fails to start fails the start, after the coordinator became
// the leader.
func TestNewFailsToStartRuntime(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	failpointErr := errors.New("failpoint")
	options := newTestOptions()
	failpoint.Enable("coordinator.newRuntime", failpointErr)
	t.Cleanup(func() { failpoint.Disable("coordinator.newRuntime") })

	server, err := New(t.Context(), commonwatch.New(options))
	require.ErrorIs(t, err, failpointErr)
	assert.Nil(t, server)
}

// A cluster configuration that can't be reconciled fails the start, after the
// coordinator became the leader.
func TestNewFailsToReconcileClusterConfiguration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	statusPath := filepath.Join(t.TempDir(), option.DefaultFileStatusName)
	require.NoError(t, os.WriteFile(statusPath, []byte("instanceId: coordinator-test\n"), 0o400))
	options := newTestOptions()
	options.Metadata.ProviderName = option.ProviderFile
	options.Metadata.File.StatusName = statusPath
	options.Metadata.File.ConfigName = filepath.Join(t.TempDir(), option.DefaultFileConfigName)
	config, err := metadatacodec.ClusterConfigCodec.MarshalYAML(&proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: "default", ReplicationFactor: 1, InitialShardCount: 1}},
		Servers:    []*proto.DataServerIdentity{{Public: "s1:6648", Internal: "s1:6649"}},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(options.Metadata.File.ConfigName, config, 0o600))

	server, err := New(t.Context(), commonwatch.New(options))
	require.ErrorIs(t, err, os.ErrPermission)
	assert.ErrorContains(t, err, "failed to reconcile the cluster configuration")
	assert.Nil(t, server)
}

func TestCoordinatorAppliesLogLevelChanges(t *testing.T) {
	logging.ConfigureLogger()
	originalLevel := logging.LogLevel
	options := newTestOptions()
	options.Observability.Log.Level = originalLevel.String()
	optionsWatch := commonwatch.New(options)
	server, err := New(t.Context(), optionsWatch)
	require.NoError(t, err)

	updated := newTestOptions()
	updated.Observability.Log.Level = slog.LevelDebug.String()
	if originalLevel == slog.LevelDebug {
		updated.Observability.Log.Level = slog.LevelInfo.String()
	}
	expectedLevel, err := logging.ParseLogLevel(updated.Observability.Log.Level)
	require.NoError(t, err)
	// The server subscribes to the changes in the background: publish until it
	// sees one
	require.Eventually(t, func() bool {
		optionsWatch.Publish(updated)
		return slog.Default().Enabled(context.Background(), expectedLevel) &&
			!slog.Default().Enabled(context.Background(), expectedLevel-1)
	}, 10*time.Second, 10*time.Millisecond)

	require.NoError(t, server.Close())
	logging.ReconfigureLogger(&commonoption.LogOptions{Level: originalLevel.String()})
}

// Close releases the coordinator once: closing it again does nothing.
func TestCoordinatorCloseTwice(t *testing.T) {
	server, err := New(t.Context(), commonwatch.New(newTestOptions()))
	require.NoError(t, err)

	require.NoError(t, server.Close())
	require.NoError(t, server.Close())
}

func TestCoordinatorBgLeadershipWatcherEndsOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	server := &coordinator{ctx: ctx, logger: slog.Default()}

	server.bgLeadershipWatcher(make(chan struct{}))
}

// newTestOptions returns the options of a coordinator on the memory metadata,
// listening on free ports, without metrics.
func newTestOptions() *option.Options {
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.Name = "coordinator-test"
	options.Metadata.ProviderName = option.ProviderMemory
	return options
}
