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

package codec

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"
)

func TestRealConfigurations_ServerConfig(t *testing.T) {
	// Test loading the real server configuration file
	configPath := filepath.Join("..", "..", "test-server-config.yaml")

	// Create a new options instance
	opts := dataserveroption.NewDefaultOptions()

	// Load configuration
	err := ReadConf(configPath, opts)
	require.NoError(t, err)

	// Verify server configuration
	assert.Equal(t, "0.0.0.0:6644", opts.Server.Public.BindAddress)
	assert.Equal(t, "oidc", opts.Server.Public.Auth.Provider)
	assert.Contains(t, opts.Server.Public.Auth.ProviderParams, "https://accounts.google.com")
	assert.Contains(t, opts.Server.Public.Auth.ProviderParams, "oxia-client")

	assert.Equal(t, "/etc/oxia/certs/server.crt", opts.Server.Public.TLS.CertFile)
	assert.Equal(t, "/etc/oxia/certs/server.key", opts.Server.Public.TLS.KeyFile)
	assert.Equal(t, uint16(771), opts.Server.Public.TLS.MinVersion)
	assert.Equal(t, uint16(772), opts.Server.Public.TLS.MaxVersion)
	assert.False(t, opts.Server.Public.TLS.InsecureSkipVerify)
	assert.False(t, opts.Server.Public.TLS.ClientAuth)
	assert.Equal(t, "/etc/oxia/certs/ca.crt", opts.Server.Public.TLS.TrustedCaFile)

	assert.Equal(t, "0.0.0.0:6645", opts.Server.Internal.BindAddress)
	assert.Equal(t, "/etc/oxia/certs/internal.crt", opts.Server.Internal.TLS.CertFile)
	assert.Equal(t, "/etc/oxia/certs/internal.key", opts.Server.Internal.TLS.KeyFile)
	assert.True(t, opts.Server.Internal.TLS.ClientAuth)

	// Verify replication configuration
	assert.Equal(t, "/etc/oxia/certs/peer.crt", opts.Replication.TLS.CertFile)
	assert.Equal(t, "/etc/oxia/certs/peer.key", opts.Replication.TLS.KeyFile)
	assert.Equal(t, "oxia-peer", opts.Replication.TLS.ServerName)
	assert.True(t, opts.Replication.TLS.ClientAuth)

	// Verify storage configuration
	assert.Equal(t, "/var/lib/oxia/wal", opts.Storage.WAL.Dir)
	assert.True(t, *opts.Storage.WAL.Sync)
	assert.Equal(t, 24*time.Hour, opts.Storage.WAL.Retention)
	assert.Equal(t, "/var/lib/oxia/db", opts.Storage.Database.Dir)
	assert.Equal(t, int64(1024), opts.Storage.Database.ReadCacheSizeMB)
	assert.Equal(t, 24*time.Hour, opts.Storage.Notification.Retention)

	// Verify observability configuration
	assert.Equal(t, "0.0.0.0:9090", opts.Observability.Metric.BindAddress)
	assert.Equal(t, "info", opts.Observability.Log.Level)
}

func TestRealConfigurations_CoordinatorConfig(t *testing.T) {
	// Test loading the real coordinator configuration file
	configPath := filepath.Join("..", "..", "test-coordinator-config.yaml")

	// Create a new options instance
	opts := coordinatoroption.NewDefaultOptions()

	// Load configuration
	err := ReadConf(configPath, opts)
	require.NoError(t, err)

	// Verify cluster configuration
	assert.Equal(t, "/etc/oxia/cluster-config.json", opts.Cluster.ConfigPath)

	// Verify server configuration
	assert.Equal(t, "0.0.0.0:6643", opts.Server.Admin.BindAddress)
	assert.Equal(t, "/etc/oxia/certs/admin.crt", opts.Server.Admin.TLS.CertFile)
	assert.Equal(t, "/etc/oxia/certs/admin.key", opts.Server.Admin.TLS.KeyFile)

	assert.Equal(t, "0.0.0.0:6645", opts.Server.Internal.BindAddress)
	assert.Equal(t, "/etc/oxia/certs/internal.crt", opts.Server.Internal.TLS.CertFile)
	assert.Equal(t, "/etc/oxia/certs/internal.key", opts.Server.Internal.TLS.KeyFile)
	assert.True(t, opts.Server.Internal.TLS.ClientAuth)

	// Verify controller configuration
	assert.Equal(t, "/etc/oxia/certs/controller.crt", opts.Controller.TLS.CertFile)
	assert.Equal(t, "/etc/oxia/certs/controller.key", opts.Controller.TLS.KeyFile)
	assert.Equal(t, "oxia-controller", opts.Controller.TLS.ServerName)
	assert.True(t, opts.Controller.TLS.ClientAuth)

	// Verify metadata configuration
	assert.Equal(t, "raft", opts.Metadata.ProviderName)
	assert.Equal(t, "oxia-system", opts.Metadata.Kubernetes.Namespace)
	assert.Equal(t, "oxia-cluster-status", opts.Metadata.Kubernetes.ConfigMapName)
	assert.Equal(t, "/var/lib/oxia/cluster-status.json", opts.Metadata.File.Path)
	assert.Equal(t, []string{"oxia-1:6645", "oxia-2:6645", "oxia-3:6645"}, opts.Metadata.Raft.BootstrapNodes)
	assert.Equal(t, "oxia-1:6645", opts.Metadata.Raft.Address)
	assert.Equal(t, "/var/lib/oxia/raft", opts.Metadata.Raft.DataDir)

	// Verify observability configuration
	assert.Equal(t, "0.0.0.0:9091", opts.Observability.Metric.BindAddress)
	assert.Equal(t, "info", opts.Observability.Log.Level)
}
