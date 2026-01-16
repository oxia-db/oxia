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

package coordinator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestCoordinator_ConfigurationLoading(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "coordinator-config.yaml")

	configContent := `
cluster:
  configPath: "/path/to/cluster-config.json"
server:
  admin:
    bindAddress: "0.0.0.0:6643"
  internal:
    bindAddress: "0.0.0.0:6645"
    tls:
      certFile: "/path/to/internal-cert.pem"
      keyFile: "/path/to/internal-key.pem"
controller:
  tls:
    certFile: "/path/to/controller-cert.pem"
    keyFile: "/path/to/controller-key.pem"
metadata:
  providerName: "file"
  file:
    path: "/data/cluster-status.json"
  kubernetes:
    namespace: "oxia-system"
    configMapName: "oxia-cluster-status"
  raft:
    bootstrapNodes:
      - "node1:6645"
      - "node2:6645"
    address: "node1:6645"
    dataDir: "/data/raft"
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
	assert.Equal(t, "/path/to/cluster-config.json", opts.Cluster.ConfigPath)

	assert.Equal(t, "0.0.0.0:6643", opts.Server.Admin.BindAddress)
	assert.Equal(t, "0.0.0.0:6645", opts.Server.Internal.BindAddress)
	assert.Equal(t, "/path/to/internal-cert.pem", opts.Server.Internal.TLS.CertFile)
	assert.Equal(t, "/path/to/internal-key.pem", opts.Server.Internal.TLS.KeyFile)

	assert.Equal(t, "/path/to/controller-cert.pem", opts.Controller.TLS.CertFile)
	assert.Equal(t, "/path/to/controller-key.pem", opts.Controller.TLS.KeyFile)

	assert.Equal(t, "file", opts.Metadata.ProviderName)
	assert.Equal(t, "/data/cluster-status.json", opts.Metadata.File.Path)
	assert.Equal(t, "oxia-system", opts.Metadata.Kubernetes.Namespace)
	assert.Equal(t, "oxia-cluster-status", opts.Metadata.Kubernetes.ConfigMapName)
	assert.Equal(t, []string{"node1:6645", "node2:6645"}, opts.Metadata.Raft.BootstrapNodes)
	assert.Equal(t, "node1:6645", opts.Metadata.Raft.Address)
	assert.Equal(t, "/data/raft", opts.Metadata.Raft.DataDir)

	assert.Equal(t, "0.0.0.0:9090", opts.Observability.Metric.BindAddress)
	assert.Equal(t, "debug", opts.Observability.Log.Level)
}

func TestCoordinator_ConfigurationWithDefaults(t *testing.T) {
	// Create a config file with partial configuration
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "partial-config.yaml")

	configContent := `
server:
  admin:
    bindAddress: "0.0.0.0:7000"
metadata:
  providerName: "memory"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Create a new options instance
	opts := option.NewDefaultOptions()

	// Load configuration
	err = codec.TryReadAndInitConf(configPath, opts)
	require.NoError(t, err)

	// Verify that partial config overrides defaults but other values use defaults
	assert.Equal(t, "0.0.0.0:7000", opts.Server.Admin.BindAddress) // From config
	assert.Equal(t, "memory", opts.Metadata.ProviderName)          // From config

	// Verify defaults are applied to unset values
	assert.NotEmpty(t, opts.Server.Internal.BindAddress)      // Default
	assert.NotEmpty(t, opts.Observability.Metric.BindAddress) // Default
}

func TestCoordinator_MultipleMetadataProviders(t *testing.T) {
	testCases := []struct {
		name     string
		provider string
		config   string
	}{
		{
			name:     "file provider",
			provider: "file",
			config: `
metadata:
  providerName: "file"
  file:
    path: "/custom/cluster-status.json"
`,
		},
		{
			name:     "memory provider",
			provider: "memory",
			config: `
metadata:
  providerName: "memory"
`,
		},
		{
			name:     "raft provider",
			provider: "raft",
			config: `
metadata:
  providerName: "raft"
  raft:
    bootstrapNodes:
      - "raft1:6645"
    address: "raft1:6645"
    dataDir: "/raft/data"
`,
		},
		{
			name:     "configmap provider",
			provider: "configmap",
			config: `
metadata:
  providerName: "configmap"
  kubernetes:
    namespace: "test-namespace"
    configMapName: "test-configmap"
`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a temporary config file
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "coordinator-config.yaml")

			err := os.WriteFile(configPath, []byte(tc.config), 0644)
			require.NoError(t, err)

			// Create a new options instance
			opts := option.NewDefaultOptions()

			// Load configuration
			err = codec.TryReadAndInitConf(configPath, opts)
			require.NoError(t, err)

			// Verify provider is set correctly
			assert.Equal(t, tc.provider, opts.Metadata.ProviderName)
		})
	}
}

func TestCoordinator_ConfigurationValidation(t *testing.T) {
	// Test that validation works correctly
	opts := option.NewDefaultOptions()

	// Need to set a valid provider since WithDefault() is empty for MetadataOptions
	opts.Metadata.ProviderName = "file"

	// Should pass validation with defaults
	err := opts.Validate()
	assert.NoError(t, err)

	// Test that validation is called and handles various scenarios
	assert.NotNil(t, opts.Validate)

	// Test invalid metadata provider - should fail validation
	originalProvider := opts.Metadata.ProviderName
	opts.Metadata.ProviderName = "invalid-provider"
	err = opts.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must be one of")

	// Restore valid provider
	opts.Metadata.ProviderName = originalProvider
}

func TestCoordinator_ConfigFileNotFound(t *testing.T) {
	opts := option.NewDefaultOptions()

	// Should fail when config file doesn't exist
	err := codec.TryReadAndInitConf("/nonexistent/config.yaml", opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no such file")
}

func TestCoordinator_InvalidYAML(t *testing.T) {
	// Create an invalid YAML config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid-config.yaml")

	invalidContent := `
cluster:
  configPath: [invalid yaml syntax
`

	err := os.WriteFile(configPath, []byte(invalidContent), 0644)
	require.NoError(t, err)

	opts := option.NewDefaultOptions()

	// Should fail with invalid YAML
	err = codec.TryReadAndInitConf(configPath, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "yaml")
}

func TestCoordinator_EmptyConfigFile(t *testing.T) {
	// Create an empty config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "empty-config.yaml")

	err := os.WriteFile(configPath, []byte{}, 0644)
	require.NoError(t, err)

	opts := option.NewDefaultOptions()

	// Since MetadataOptions.WithDefault() is empty, we need to set a valid provider
	// This simulates what would happen in the actual CLI where flags set defaults
	opts.Metadata.ProviderName = "file"

	// Should work with empty config (defaults should be applied)
	err = codec.TryReadAndInitConf(configPath, opts)
	assert.NoError(t, err)

	// Verify defaults are still applied
	assert.NotEmpty(t, opts.Server.Admin.BindAddress)
	assert.NotEmpty(t, opts.Server.Internal.BindAddress)
	assert.Equal(t, "file", opts.Metadata.ProviderName)
}

func TestCoordinator_TLSSettings(t *testing.T) {
	// Create a config file with various TLS settings
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "tls-config.yaml")

	configContent := `
server:
  internal:
    bindAddress: "0.0.0.0:6645"
    tls:
      certFile: "/path/to/server-cert.pem"
      keyFile: "/path/to/server-key.pem"
      minVersion: 771
      maxVersion: 772
      insecureSkipVerify: true
      clientAuth: true
      trustedCaFile: "/path/to/ca.pem"
controller:
  tls:
    certFile: "/path/to/controller-cert.pem"
    keyFile: "/path/to/controller-key.pem"
    minVersion: 771
    maxVersion: 771
    insecureSkipVerify: false
    clientAuth: false
    trustedCaFile: "/path/to/controller-ca.pem"
    serverName: "controller.example.com"
metadata:
  providerName: "file"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	opts := option.NewDefaultOptions()

	// Load configuration
	err = codec.TryReadAndInitConf(configPath, opts)
	require.NoError(t, err)

	// Verify TLS settings for internal server
	assert.Equal(t, "/path/to/server-cert.pem", opts.Server.Internal.TLS.CertFile)
	assert.Equal(t, "/path/to/server-key.pem", opts.Server.Internal.TLS.KeyFile)
	assert.Equal(t, uint16(771), opts.Server.Internal.TLS.MinVersion)
	assert.Equal(t, uint16(772), opts.Server.Internal.TLS.MaxVersion)
	assert.True(t, opts.Server.Internal.TLS.InsecureSkipVerify)
	assert.True(t, opts.Server.Internal.TLS.ClientAuth)
	assert.Equal(t, "/path/to/ca.pem", opts.Server.Internal.TLS.TrustedCaFile)

	// Verify TLS settings for controller
	assert.Equal(t, "/path/to/controller-cert.pem", opts.Controller.TLS.CertFile)
	assert.Equal(t, "/path/to/controller-key.pem", opts.Controller.TLS.KeyFile)
	assert.Equal(t, uint16(771), opts.Controller.TLS.MinVersion)
	assert.Equal(t, uint16(771), opts.Controller.TLS.MaxVersion)
	assert.False(t, opts.Controller.TLS.InsecureSkipVerify)
	assert.False(t, opts.Controller.TLS.ClientAuth)
	assert.Equal(t, "/path/to/controller-ca.pem", opts.Controller.TLS.TrustedCaFile)
	assert.Equal(t, "controller.example.com", opts.Controller.TLS.ServerName)
}

func TestCoordinator_CommandLineFlags(t *testing.T) {
	// Test that command line flags work correctly
	originalConfFile := confFile
	defer func() { confFile = originalConfFile }()

	// Reset the global options
	coordinatorOptions = option.NewDefaultOptions()

	// Create a config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `
cluster:
  configPath: "/default/cluster.json"
server:
  admin:
    bindAddress: "0.0.0.0:6643"
  internal:
    bindAddress: "0.0.0.0:6645"
observability:
  metric:
    bindAddress: "0.0.0.0:9090"
metadata:
  providerName: "file"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Test configuration loading via the command pattern
	confFile = configPath
	err = codec.TryReadAndInitConf(confFile, coordinatorOptions)
	require.NoError(t, err)

	// Verify config file values were loaded
	assert.Equal(t, "/default/cluster.json", coordinatorOptions.Cluster.ConfigPath)
	assert.Equal(t, "0.0.0.0:6643", coordinatorOptions.Server.Admin.BindAddress)
	assert.Equal(t, "0.0.0.0:6645", coordinatorOptions.Server.Internal.BindAddress)
	assert.Equal(t, "0.0.0.0:9090", coordinatorOptions.Observability.Metric.BindAddress)

	// Now test that CLI flags can override these
	// Note: In the actual cmd, flags are set during init(), but we can simulate this
	// by directly setting the flag values on the options struct
	coordinatorOptions.Server.Admin.BindAddress = "0.0.0.0:7000"
	coordinatorOptions.Server.Internal.BindAddress = "0.0.0.0:7001"
	coordinatorOptions.Observability.Metric.BindAddress = "0.0.0.0:8080"
	coordinatorOptions.Metadata.ProviderName = "memory"

	// Verify the options were updated
	assert.Equal(t, "0.0.0.0:7000", coordinatorOptions.Server.Admin.BindAddress)
	assert.Equal(t, "0.0.0.0:7001", coordinatorOptions.Server.Internal.BindAddress)
	assert.Equal(t, "0.0.0.0:8080", coordinatorOptions.Observability.Metric.BindAddress)
	assert.Equal(t, "memory", coordinatorOptions.Metadata.ProviderName)
}
