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
	require.Equal(t, "metadata/cluster.yaml", opts.Metadata.ClusterConfigPathOrLegacy(opts.Cluster.ConfigPath))
}

func TestMetadataOptionsFileDefaultsStatusPathWithoutDir(t *testing.T) {
	opts := readOptions(t, `
metadata:
  providerName: file
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, DefaultFileStatusName, opts.Metadata.File.StatusPath())
	require.Empty(t, opts.Metadata.ClusterConfigPathOrLegacy(opts.Cluster.ConfigPath))
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
	require.Equal(t, "configs/cluster.yaml", opts.Metadata.ClusterConfigPathOrLegacy(opts.Cluster.ConfigPath))
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
	require.Equal(t, "configmap:oxia/oxia-config", opts.Metadata.ClusterConfigPathOrLegacy(opts.Cluster.ConfigPath))
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
	require.Equal(t, "configs/cluster.yaml", opts.Metadata.ClusterConfigPathOrLegacy(opts.Cluster.ConfigPath))
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

func readOptions(t *testing.T, data string) *Options {
	t.Helper()

	opts := &Options{}
	require.NoError(t, yaml.Unmarshal([]byte(data), opts))
	opts.WithDefault()
	return opts
}
