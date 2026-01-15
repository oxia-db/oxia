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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfig is a mock implementation of ConfigurableOptions for testing.
type TestConfig struct {
	Name     string `yaml:"name"`
	Port     int    `yaml:"port"`
	Enabled  bool   `yaml:"enabled"`
	Required string `yaml:"required"`

	defaultsApplied bool
	validated       bool
}

func (tc *TestConfig) WithDefault() {
	if tc.Port == 0 {
		tc.Port = 8080
	}
	if tc.Name == "" {
		tc.Name = "default"
	}
	tc.defaultsApplied = true
}

func (tc *TestConfig) Validate() error {
	tc.validated = true
	if tc.Required == "" {
		return assert.AnError
	}
	return nil
}

func TestReadConf_Success(t *testing.T) {
	// Create a temporary config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `
name: "test-service"
port: 9000
enabled: true
required: "value"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Test reading the configuration
	var config TestConfig
	err = TryReadAndInitConf(configPath, &config)

	assert.NoError(t, err)
	assert.Equal(t, "test-service", config.Name)
	assert.Equal(t, 9000, config.Port)
	assert.True(t, config.Enabled)
	assert.Equal(t, "value", config.Required)
	assert.True(t, config.defaultsApplied)
	assert.True(t, config.validated)
}

func TestReadConf_WithDefaults(t *testing.T) {
	// Create a temporary config file with minimal values
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `
required: "value"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// Test reading the configuration
	var config TestConfig
	err = TryReadAndInitConf(configPath, &config)

	assert.NoError(t, err)
	assert.Equal(t, "default", config.Name) // Should get default value
	assert.Equal(t, 8080, config.Port)      // Should get default value
	assert.Equal(t, "value", config.Required)
	assert.True(t, config.defaultsApplied)
	assert.True(t, config.validated)
}

func TestReadConf_FileNotFound(t *testing.T) {
	var config TestConfig
	err := TryReadAndInitConf("/nonexistent/path/config.yaml", &config)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no such file or directory")
	assert.False(t, config.defaultsApplied)
	assert.False(t, config.validated)
}

func TestReadConf_InvalidYAML(t *testing.T) {
	// Create a temporary config file with invalid YAML
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	invalidContent := `
name: "test"
port: [invalid yaml syntax
`

	err := os.WriteFile(configPath, []byte(invalidContent), 0644)
	require.NoError(t, err)

	var config TestConfig
	err = TryReadAndInitConf(configPath, &config)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "yaml")
	assert.False(t, config.defaultsApplied)
	assert.False(t, config.validated)
}

func TestReadConf_ValidationError(t *testing.T) {
	// Create a temporary config file missing required field
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `
name: "test-service"
port: 9000
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	var config TestConfig
	err = TryReadAndInitConf(configPath, &config)

	assert.Error(t, err)
	assert.True(t, config.defaultsApplied) // Defaults should still be applied
	assert.True(t, config.validated)       // Validation should still be called
}

func TestConfigurableOptions_Interface(t *testing.T) {
	var _ ConfigurableOptions = (*TestConfig)(nil)
}

// TestReadConf_NilConfig tests that TryReadAndInitConf handles nil pointer gracefully.
func TestReadConf_NilConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	configContent := `
name: "test"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	err = TryReadAndInitConf(configPath, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "configuration options cannot be nil")
}
