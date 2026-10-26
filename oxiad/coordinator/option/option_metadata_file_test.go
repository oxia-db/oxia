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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestFileMetadataUnmarshalJSON(t *testing.T) {
	fm := FileMetadata{}
	require.NoError(t, json.Unmarshal([]byte(`{"path":"data/status.json","configName":"cluster.yaml"}`), &fm))
	require.Equal(t, FileMetadata{StatusName: "data/status.json", ConfigName: "cluster.yaml"}, fm)

	require.Error(t, json.Unmarshal([]byte(`{"dir":1}`), &fm))
}

func TestFileMetadataUnmarshalYAMLRejectsInvalidNode(t *testing.T) {
	fm := FileMetadata{}
	require.Error(t, yaml.Unmarshal([]byte("[a, b]"), &fm))
}

func TestFileMetadataApplyCompatPath(t *testing.T) {
	for _, tc := range []struct {
		name     string
		data     string
		expected FileMetadata
		err      string
	}{
		{
			name:     "dir and path",
			data:     "dir: data\npath: data/status.json\n",
			expected: FileMetadata{Dir: "data", StatusName: "status.json"},
		},
		{
			name:     "dir, path and matching status name",
			data:     "dir: data\npath: data/status.json\nstatusName: status.json\n",
			expected: FileMetadata{Dir: "data", StatusName: "status.json"},
		},
		{
			name: "dir conflicts with path",
			data: "dir: other\npath: data/status.json\n",
			err:  "file.dir and deprecated file.path",
		},
		{
			name: "status name conflicts with path in dir",
			data: "dir: data\npath: data/status.json\nstatusName: other.json\n",
			err:  "different file names",
		},
		{
			name: "status name conflicts with path",
			data: "path: data/status.json\nstatusName: other.json\n",
			err:  "different paths",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fm := FileMetadata{}
			err := yaml.Unmarshal([]byte(tc.data), &fm)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, fm)
		})
	}
}

func TestFileMetadataApplyLegacyConfigPath(t *testing.T) {
	for _, tc := range []struct {
		name       string
		file       FileMetadata
		configPath string
		expected   string
		err        string
	}{
		{name: "dir fills config name", file: FileMetadata{Dir: "configs"}, configPath: "configs/cluster.yaml",
			expected: "configs/cluster.yaml"},
		{name: "dir with matching config name", file: FileMetadata{Dir: "configs", ConfigName: "cluster.yaml"},
			configPath: "configs/cluster.yaml", expected: "configs/cluster.yaml"},
		{name: "dir with conflicting config name", file: FileMetadata{Dir: "configs", ConfigName: "other.yaml"},
			configPath: "configs/cluster.yaml", err: "different file names"},
		{name: "matching config path", file: FileMetadata{ConfigName: "configs/cluster.yaml"},
			configPath: "configs/cluster.yaml", expected: "configs/cluster.yaml"},
		{name: "conflicting config path", file: FileMetadata{ConfigName: "other.yaml"},
			configPath: "configs/cluster.yaml", err: "different paths"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mo := &MetadataOptions{ProviderOptions: ProviderOptions{ProviderName: ProviderFile, File: tc.file}}
			err := mo.ApplyLegacyClusterConfigPath(tc.configPath)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, mo.File.ConfigPath())
		})
	}
}
