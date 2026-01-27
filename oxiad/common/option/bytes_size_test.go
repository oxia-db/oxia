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

package option

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestBytesSizeUnmarshalYAML(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    BytesSize
		expectError bool
	}{
		{
			name:     "bytes",
			input:    "1024B",
			expected: 1024,
		},
		{
			name:     "kilobytes",
			input:    "1KB",
			expected: 1024,
		},
		{
			name:     "megabytes",
			input:    "1MB",
			expected: 1024 * 1024,
		},
		{
			name:     "gigabytes",
			input:    "1GB",
			expected: 1024 * 1024 * 1024,
		},
		{
			name:     "decimal bytes",
			input:    "1.5KB",
			expected: 1536,
		},
		{
			name:     "case insensitive",
			input:    "1mb",
			expected: 1024 * 1024,
		},
		{
			name:        "invalid format",
			input:       "invalid",
			expectError: true,
		},
		{
			name:        "empty string",
			input:       "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b BytesSize
			node := yaml.Node{}
			err := node.Encode(tt.input)
			assert.NoError(t, err)

			err = b.UnmarshalYAML(&node)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, b)
			}
		})
	}
}

func TestBytesSizeMarshalYAML(t *testing.T) {
	tests := []struct {
		name     string
		input    BytesSize
		expected string
	}{
		{
			name:     "bytes",
			input:    1024,
			expected: "1KiB",
		},
		{
			name:     "kilobytes",
			input:    1024 * 1024,
			expected: "1MiB",
		},
		{
			name:     "megabytes",
			input:    1024 * 1024 * 1024,
			expected: "1GiB",
		},
		{
			name:     "zero",
			input:    0,
			expected: "0B",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.input.MarshalYAML()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBytesSizeYAMLRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		input BytesSize
	}{
		{
			name:  "bytes",
			input: 512,
		},
		{
			name:  "kilobytes",
			input: 2048,
		},
		{
			name:  "megabytes",
			input: 5 * 1024 * 1024,
		},
		{
			name:  "gigabytes",
			input: 2 * 1024 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to YAML
			marshaled, err := tt.input.MarshalYAML()
			assert.NoError(t, err)

			// Unmarshal back
			var result BytesSize
			node := yaml.Node{}
			err = node.Encode(marshaled)
			assert.NoError(t, err)
			err = result.UnmarshalYAML(&node)
			assert.NoError(t, err)

			assert.Equal(t, tt.input, result)
		})
	}
}

func TestBytesSizeInStruct(t *testing.T) {
	type TestConfig struct {
		CacheSize BytesSize `yaml:"cacheSize"`
		MaxMemory BytesSize `yaml:"maxMemory"`
	}

	tests := []struct {
		name     string
		yaml     string
		expected TestConfig
	}{
		{
			name: "valid config",
			yaml: `
cacheSize: 100MB
maxMemory: 1GB
`,
			expected: TestConfig{
				CacheSize: 100 * 1024 * 1024,
				MaxMemory: 1024 * 1024 * 1024,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config TestConfig
			err := yaml.Unmarshal([]byte(tt.yaml), &config)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, config)
		})
	}
}
