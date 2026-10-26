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

	"github.com/stretchr/testify/require"
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
