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

	"github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestDatabaseOptionsWithDefault(t *testing.T) {
	tests := []struct {
		name     string
		input    DatabaseOptions
		expected DatabaseOptions
	}{
		{
			name:  "empty options",
			input: DatabaseOptions{},
			expected: DatabaseOptions{
				Dir:             "./data/db",
				ReadCacheSizeMB: 100,
				WriteCacheSize:  option.BytesSize(50 * 1024 * 1024), // 50MB
			},
		},
		{
			name: "partial options",
			input: DatabaseOptions{
				Dir: "/custom/path",
			},
			expected: DatabaseOptions{
				Dir:             "/custom/path",
				ReadCacheSizeMB: 100,
				WriteCacheSize:  option.BytesSize(50 * 1024 * 1024), // 50MB
			},
		},
		{
			name: "full options",
			input: DatabaseOptions{
				Dir:             "/custom/path",
				ReadCacheSizeMB: 200,
				WriteCacheSize:  option.BytesSize(100 * 1024 * 1024), // 100MB
			},
			expected: DatabaseOptions{
				Dir:             "/custom/path",
				ReadCacheSizeMB: 200,
				WriteCacheSize:  option.BytesSize(100 * 1024 * 1024), // 100MB
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.input
			opts.WithDefault()
			assert.Equal(t, tt.expected, opts)
		})
	}
}

func TestDatabaseOptionsYAML(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected DatabaseOptions
	}{
		{
			name: "full config",
			yaml: `
dir: "/data/db"
readCacheSizeMB: 200
writeCacheSize: "100MB"
`,
			expected: DatabaseOptions{
				Dir:             "/data/db",
				ReadCacheSizeMB: 200,
				WriteCacheSize:  option.BytesSize(100 * 1024 * 1024),
			},
		},
		{
			name: "minimal config",
			yaml: `
dir: "/data/db"
`,
			expected: DatabaseOptions{
				Dir:             "/data/db",
				ReadCacheSizeMB: 0,
				WriteCacheSize:  0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts DatabaseOptions
			err := yaml.Unmarshal([]byte(tt.yaml), &opts)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, opts)
		})
	}
}

func TestDatabaseOptionsYAMLMarshal(t *testing.T) {
	opts := DatabaseOptions{
		Dir:             "/data/db",
		ReadCacheSizeMB: 200,
		WriteCacheSize:  option.BytesSize(100 * 1024 * 1024),
	}

	data, err := yaml.Marshal(&opts)
	assert.NoError(t, err)
	assert.Contains(t, string(data), "dir: /data/db")
	assert.Contains(t, string(data), "readCacheSizeMB: 200")
	assert.Contains(t, string(data), "writeCacheSize:")
}

func TestDatabaseOptionsValidate(t *testing.T) {
	opts := DatabaseOptions{
		Dir:             "/data/db",
		ReadCacheSizeMB: 200,
		WriteCacheSize:  option.BytesSize(100 * 1024 * 1024),
	}

	err := opts.Validate()
	assert.NoError(t, err)
}
