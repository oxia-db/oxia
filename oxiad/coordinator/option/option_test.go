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

func TestMetadataOptionsNewFileDirDefaultsNames(t *testing.T) {
	opts := readOptions(t, `
metadata:
  providerName: file
  file:
    dir: metadata
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "metadata/cluster-status.json", opts.Metadata.File.StatusPath())
	require.Equal(t, "metadata/cluster.yaml", opts.Metadata.File.ConfigPath())
}

func TestMetadataOptionsName(t *testing.T) {
	opts := readOptions(t, `
metadata:
  name: coordinator-0
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "coordinator-0", opts.Metadata.Name)
}

func TestMetadataOptionsDefaultName(t *testing.T) {
	opts := readOptions(t, `
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.NotEmpty(t, opts.Metadata.Name)
}

func TestPublicServerOptionsBindAddress(t *testing.T) {
	opts := readOptions(t, `
server:
  public:
    bindAddress: coordinator.example.com:7000
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "coordinator.example.com:7000", opts.Server.Public.BindAddress)
}

func TestMetadataOptionsFileDefaultsStatusPathWithoutDir(t *testing.T) {
	opts := readOptions(t, `
metadata:
  providerName: file
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, DefaultFileStatusName, opts.Metadata.File.StatusPath())
	require.Equal(t, DefaultFileConfigName, opts.Metadata.File.ConfigPath())
}

func TestMetadataOptionsLegacyFileFields(t *testing.T) {
	opts := readOptions(t, `
cluster:
  configPath: configs/cluster.yaml
metadata:
  providerName: file
  file:
    path: data/cluster-status.json
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "data/cluster-status.json", opts.Metadata.File.StatusPath())
	require.Equal(t, "configs/cluster.yaml", opts.Metadata.File.ConfigPath())
}

func TestMetadataOptionsLegacyConfigMapFields(t *testing.T) {
	opts := readOptions(t, `
cluster:
  configPath: configmap:oxia/oxia-config
metadata:
  providerName: configmap
  kubernetes:
    namespace: oxia
    configMapName: oxia-status
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "oxia-status", opts.Metadata.Kubernetes.StatusNameOrDefault())
	require.Equal(t, "oxia-config", opts.Metadata.Kubernetes.ConfigNameOrDefault())
}

func TestMetadataOptionsConfigMapStatusWithLegacyFileClusterConfig(t *testing.T) {
	opts := readOptions(t, `
cluster:
  configPath: configs/cluster.yaml
metadata:
  providerName: configmap
  kubernetes:
    namespace: oxia
    configMapName: oxia-status
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "oxia-status", opts.Metadata.Kubernetes.StatusNameOrDefault())
	require.Empty(t, opts.Metadata.Kubernetes.ConfigName)
	require.Equal(t, "configs/cluster.yaml", opts.Metadata.File.ConfigPath())
}

func TestMetadataOptionsRejectLegacyClusterConfigConflict(t *testing.T) {
	opts := readOptions(t, `
cluster:
  configPath: configs/cluster.yaml
metadata:
  providerName: file
  file:
    dir: metadata
`)

	require.ErrorContains(t, opts.Validate(), "metadata.file.dir and deprecated cluster.configPath")
}

func TestPublicServerOptionsUnmarshalYAMLRejectsInvalidNode(t *testing.T) {
	opts := &Options{}
	err := yaml.Unmarshal([]byte("server:\n  public: [a, b]\n"), opts)
	require.Error(t, err)
}

func TestServerOptionsUnmarshalYAMLAcceptsDeprecatedAdmin(t *testing.T) {
	opts := readOptions(t, `
server:
  admin:
    bindAddress: 127.0.0.1:7000
    tls:
      certFile: /certs/admin.pem
  internal:
    bindAddress: 127.0.0.1:7001
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "127.0.0.1:7000", opts.Server.Public.BindAddress)
	require.Equal(t, "/certs/admin.pem", opts.Server.Public.TLS.CertFile)
	require.Equal(t, "127.0.0.1:7001", opts.Server.Internal.BindAddress)
}

func TestServerOptionsUnmarshalYAMLKeepsUnsetFields(t *testing.T) {
	opts := &Options{Server: ServerOptions{Internal: InternalServerOptions{BindAddress: "127.0.0.1:7001"}}}
	require.NoError(t, yaml.Unmarshal([]byte("server:\n  public:\n    bindAddress: 127.0.0.1:7000\n"), opts))

	require.Equal(t, "127.0.0.1:7000", opts.Server.Public.BindAddress)
	require.Equal(t, "127.0.0.1:7001", opts.Server.Internal.BindAddress)
}

func TestServerOptionsUnmarshalYAMLRejectsPublicAndAdmin(t *testing.T) {
	opts := &Options{}
	err := yaml.Unmarshal([]byte(`
server:
  public:
    bindAddress: 127.0.0.1:7000
  admin:
    bindAddress: 127.0.0.1:7002
`), opts)
	require.ErrorContains(t, err, "server.public and deprecated server.admin are both set")
}

func TestServerOptionsUnmarshalYAMLRejectsInvalidNode(t *testing.T) {
	opts := &Options{}
	require.Error(t, yaml.Unmarshal([]byte("server: [a, b]\n"), opts))
}

func TestMetadataOptionsValidateRejectsEmptyName(t *testing.T) {
	mo := &MetadataOptions{Name: "  ", ProviderOptions: ProviderOptions{ProviderName: ProviderMemory}}
	require.ErrorContains(t, mo.Validate(), "metadata.name must be set")
}

func TestMetadataOptionsGetStatusProviderOptions(t *testing.T) {
	mo := &MetadataOptions{
		Name: "coordinator-0",
		ProviderOptions: ProviderOptions{
			ProviderName: ProviderConfigMap,
			Kubernetes:   K8sMetadata{Namespace: "oxia"},
		},
	}
	require.Equal(t, mo.ProviderOptions, mo.GetStatusProviderOptions())
}

func TestMetadataOptionsApplyLegacyClusterConfigPathConfigMap(t *testing.T) {
	for _, tc := range []struct {
		name       string
		kubernetes K8sMetadata
		configPath string
		expected   K8sMetadata
		err        string
	}{
		{
			name:       "fills namespace and config name",
			configPath: "configmap:oxia/oxia-config",
			expected:   K8sMetadata{Namespace: "oxia", ConfigName: "oxia-config"},
		},
		{
			name:       "keeps matching values",
			kubernetes: K8sMetadata{Namespace: "oxia", ConfigName: "oxia-config"},
			configPath: "configmap:oxia/oxia-config",
			expected:   K8sMetadata{Namespace: "oxia", ConfigName: "oxia-config"},
		},
		{name: "missing name", configPath: "configmap:oxia", err: "invalid cluster.configPath"},
		{name: "empty namespace", configPath: "configmap:/oxia-config", err: "invalid cluster.configPath"},
		{name: "empty name", configPath: "configmap:oxia/", err: "invalid cluster.configPath"},
		{
			name:       "namespace conflict",
			kubernetes: K8sMetadata{Namespace: "other"},
			configPath: "configmap:oxia/oxia-config",
			err:        "metadata.kubernetes.namespace",
		},
		{
			name:       "config name conflict",
			kubernetes: K8sMetadata{ConfigName: "other"},
			configPath: "configmap:oxia/oxia-config",
			err:        "metadata.kubernetes.configName",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mo := &MetadataOptions{ProviderOptions: ProviderOptions{
				ProviderName: ProviderConfigMap,
				Kubernetes:   tc.kubernetes,
			}}
			err := mo.ApplyLegacyClusterConfigPath(tc.configPath)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, mo.Kubernetes)
		})
	}
}

func TestMetadataOptionsApplyLegacyClusterConfigPathFileRejectsConfigMap(t *testing.T) {
	mo := &MetadataOptions{ProviderOptions: ProviderOptions{ProviderName: ProviderFile}}
	require.ErrorContains(t, mo.ApplyLegacyClusterConfigPath("configmap:oxia/oxia-config"),
		"must be a file path when metadata provider is file")
}

func TestMetadataOptionsApplyLegacyClusterConfigPathOtherProviders(t *testing.T) {
	mo := &MetadataOptions{ProviderOptions: ProviderOptions{ProviderName: ProviderRaft}}
	require.NoError(t, mo.ApplyLegacyClusterConfigPath("configmap:oxia/oxia-config"))
	require.Empty(t, mo.File.ConfigName)
	require.Empty(t, mo.Kubernetes)

	require.NoError(t, mo.ApplyLegacyClusterConfigPath("configs/cluster.yaml"))
	require.Equal(t, "configs/cluster.yaml", mo.File.ConfigPath())
}

func TestProviderOptionsValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts ProviderOptions
		err  string
	}{
		{name: "memory", opts: ProviderOptions{ProviderName: ProviderMemory}},
		{name: "file", opts: ProviderOptions{ProviderName: ProviderFile}},
		{
			name: "configmap",
			opts: ProviderOptions{ProviderName: ProviderConfigMap, Kubernetes: K8sMetadata{Namespace: "oxia"}},
		},
		{name: "configmap without namespace", opts: ProviderOptions{ProviderName: ProviderConfigMap}, err: "k8s-namespace"},
		{name: "raft", opts: ProviderOptions{ProviderName: ProviderRaft, Raft: RaftMetadata{Address: "localhost:0"}}},
		{name: "raft without address", opts: ProviderOptions{ProviderName: ProviderRaft}, err: "raft-address"},
		{name: "unknown", opts: ProviderOptions{ProviderName: "etcd"}, err: "must be one of"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestK8sMetadataUnmarshalJSON(t *testing.T) {
	km := K8sMetadata{}
	require.NoError(t, json.Unmarshal([]byte(`{"namespace":"oxia","configMapName":"status","configName":"config"}`), &km))
	require.Equal(t, K8sMetadata{Namespace: "oxia", StatusName: "status", ConfigName: "config"}, km)

	require.Error(t, json.Unmarshal([]byte(`{"namespace":1}`), &km))
	require.ErrorContains(t, json.Unmarshal([]byte(`{"statusName":"a","configMapName":"b"}`), &km),
		"kubernetes.statusName and deprecated kubernetes.configMapName")
}

func TestK8sMetadataUnmarshalYAMLRejectsInvalidNode(t *testing.T) {
	km := K8sMetadata{}
	require.Error(t, yaml.Unmarshal([]byte("[a, b]"), &km))
}

func TestK8sMetadataNameOrDefault(t *testing.T) {
	km := K8sMetadata{}
	require.Equal(t, DefaultK8sStatusName, km.StatusNameOrDefault())
	require.Equal(t, DefaultK8sConfigName, km.ConfigNameOrDefault())
}

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

func readOptions(t *testing.T, data string) *Options {
	t.Helper()

	opts := &Options{}
	require.NoError(t, yaml.Unmarshal([]byte(data), opts))
	opts.WithDefault()
	return opts
}
