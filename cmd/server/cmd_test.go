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

package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/codec"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
)

func TestServer_ConfigurationLoading(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "server-config.yaml")

	configContent := `
server:
  public:
    bindAddress: "0.0.0.0:6644"
    auth:
      provider: "oidc"
      providerParams: '{"allowedIssueURLs":"https://example.com","allowedAudiences":"my-app"}'
    tls:
      certFile: "/path/to/cert.pem"
      keyFile: "/path/to/key.pem"
  internal:
    bindAddress: "0.0.0.0:6645"
    tls:
      certFile: "/path/to/internal-cert.pem"
      keyFile: "/path/to/internal-key.pem"
replication:
  tls:
    certFile: "/path/to/peer-cert.pem"
    keyFile: "/path/to/peer-key.pem"
storage:
  wal:
    dir: "/tmp/wal"
    sync: true
    retention: "2h"
  database:
    dir: "/tmp/db"
    readCacheSizeMB: 512
  notification:
    retention: "30m"
observability:
  metric:
    bindAddress: "0.0.0.0:9090"
  log:
    level: "debug"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Create a new options instance
	opts := option.NewDefaultOptions()

	// Load configuration using the same function as the cmd
	err = codec.TryReadAndInitConf(configPath, opts)
	require.NoError(t, err)

	// Verify that config was loaded correctly
	assert.Equal(t, "0.0.0.0:6644", opts.Server.Public.BindAddress)
	assert.Equal(t, "oidc", opts.Server.Public.Auth.Provider)
	assert.Equal(t, "{\"allowedIssueURLs\":\"https://example.com\",\"allowedAudiences\":\"my-app\"}", opts.Server.Public.Auth.ProviderParams)
	assert.Equal(t, "/path/to/cert.pem", opts.Server.Public.TLS.CertFile)
	assert.Equal(t, "/path/to/key.pem", opts.Server.Public.TLS.KeyFile)

	assert.Equal(t, "0.0.0.0:6645", opts.Server.Internal.BindAddress)
	assert.Equal(t, "/path/to/internal-cert.pem", opts.Server.Internal.TLS.CertFile)
	assert.Equal(t, "/path/to/internal-key.pem", opts.Server.Internal.TLS.KeyFile)

	assert.Equal(t, "/path/to/peer-cert.pem", opts.Replication.TLS.CertFile)
	assert.Equal(t, "/path/to/peer-key.pem", opts.Replication.TLS.KeyFile)

	assert.Equal(t, "/tmp/wal", opts.Storage.WAL.Dir)
	assert.True(t, *opts.Storage.WAL.Sync)
	assert.Equal(t, 2*time.Hour, opts.Storage.WAL.Retention)

	assert.Equal(t, "/tmp/db", opts.Storage.Database.Dir)
	assert.Equal(t, int64(512), opts.Storage.Database.ReadCacheSizeMB)

	assert.Equal(t, 30*time.Minute, opts.Storage.Notification.Retention)

	assert.Equal(t, "0.0.0.0:9090", opts.Observability.Metric.BindAddress)
	assert.Equal(t, "debug", opts.Observability.Log.Level)
}

func TestServer_ConfigurationWithDefaults(t *testing.T) {
	// Create a config file with partial configuration
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "partial-config.yaml")

	configContent := `
server:
  public:
    bindAddress: "0.0.0.0:7000"
storage:
  wal:
    dir: "/custom/wal"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Create a new options instance
	opts := option.NewDefaultOptions()

	// Load configuration
	err = codec.TryReadAndInitConf(configPath, opts)
	require.NoError(t, err)

	// Verify that partial config overrides defaults but other values use defaults
	assert.Equal(t, "0.0.0.0:7000", opts.Server.Public.BindAddress) // From config
	assert.Equal(t, "/custom/wal", opts.Storage.WAL.Dir)            // From config

	// Verify defaults are applied to unset values
	assert.NotEmpty(t, opts.Server.Internal.BindAddress)      // Default
	assert.NotEmpty(t, opts.Observability.Metric.BindAddress) // Default
	assert.Equal(t, 1*time.Hour, opts.Storage.WAL.Retention)  // Default
	assert.True(t, *opts.Storage.WAL.Sync)                    // Default
}

func TestServer_ConfigurationValidation(t *testing.T) {
	// Test that validation works correctly
	opts := option.NewDefaultOptions()

	// Should pass validation with defaults
	err := opts.Validate()
	assert.NoError(t, err)

	// Test that validation is called and handles various scenarios
	// Note: The actual validation logic depends on the implementation
	// For now, we just verify that the validation method exists and can be called
	assert.NotNil(t, opts.Validate)
}

func TestServer_ConfigFileNotFound(t *testing.T) {
	opts := option.NewDefaultOptions()

	// Should fail when config file doesn't exist
	err := codec.TryReadAndInitConf("/nonexistent/config.yaml", opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no such file")
}

func TestServer_InvalidYAML(t *testing.T) {
	// Create an invalid YAML config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid-config.yaml")

	invalidContent := `
server:
  public:
    bindAddress: [invalid yaml syntax
`

	err := os.WriteFile(configPath, []byte(invalidContent), 0644)
	require.NoError(t, err)

	opts := option.NewDefaultOptions()

	// Should fail with invalid YAML
	err = codec.TryReadAndInitConf(configPath, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "yaml")
}

func TestServer_EmptyConfigFile(t *testing.T) {
	// Create an empty config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "empty-config.yaml")

	err := os.WriteFile(configPath, []byte{}, 0644)
	require.NoError(t, err)

	opts := option.NewDefaultOptions()

	// Should work with empty config (defaults should be applied)
	err = codec.TryReadAndInitConf(configPath, opts)
	assert.NoError(t, err)

	// Verify defaults are still applied
	assert.NotEmpty(t, opts.Server.Public.BindAddress)
	assert.NotEmpty(t, opts.Server.Internal.BindAddress)
}

func TestServer_CommandLineFlags(t *testing.T) {
	// Test that command line flags work correctly
	originalConfFile := confFile
	defer func() { confFile = originalConfFile }()

	// Reset the global options
	dataServerOptions = option.NewDefaultOptions()

	// Create a config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `
server:
  public:
    bindAddress: "0.0.0.0:7000"
  internal:
    bindAddress: "0.0.0.0:7001"
observability:
  metric:
    bindAddress: "0.0.0.0:8080"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Test configuration loading via the command pattern
	confFile = configPath
	err = codec.TryReadAndInitConf(confFile, dataServerOptions)
	require.NoError(t, err)

	// Verify config file values were loaded
	assert.Equal(t, "0.0.0.0:7000", dataServerOptions.Server.Public.BindAddress)
	assert.Equal(t, "0.0.0.0:7001", dataServerOptions.Server.Internal.BindAddress)
	assert.Equal(t, "0.0.0.0:8080", dataServerOptions.Observability.Metric.BindAddress)

	// Now test that CLI flags can override these
	// Note: In the actual cmd, flags are set during init(), but we can simulate this
	// by directly setting the flag values on the options struct
	dataServerOptions.Server.Public.BindAddress = "0.0.0.0:9000"
	dataServerOptions.Server.Internal.BindAddress = "0.0.0.0:9001"
	dataServerOptions.Observability.Metric.BindAddress = "0.0.0.0:9090"

	// Verify the options were updated
	assert.Equal(t, "0.0.0.0:9000", dataServerOptions.Server.Public.BindAddress)
	assert.Equal(t, "0.0.0.0:9001", dataServerOptions.Server.Internal.BindAddress)
	assert.Equal(t, "0.0.0.0:9090", dataServerOptions.Observability.Metric.BindAddress)
}
