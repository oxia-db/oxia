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
	"net"
	"os"
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

func TestMetadataOptionsIdentity(t *testing.T) {
	opts := readOptions(t, `
metadata:
  identity: coordinator-0
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "coordinator-0", opts.Metadata.Identity)
}

func TestMetadataOptionsDefaultIdentity(t *testing.T) {
	opts := readOptions(t, `
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.NotEmpty(t, opts.Metadata.Identity)
}

func TestPublicServerOptionsDefaultAdvertisedAddress(t *testing.T) {
	opts := readOptions(t, `
server:
  public:
    bindAddress: 0.0.0.0:7000
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, hostPort(t, "7000"), opts.Server.Public.AdvertisedAddress)
}

func TestPublicServerOptionsDefaultAdvertisedAddressUsesHostnameWithBindPort(t *testing.T) {
	opts := readOptions(t, `
server:
  public:
    bindAddress: coordinator.example.com:7000
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, hostPort(t, "7000"), opts.Server.Public.AdvertisedAddress)
}

func TestPublicServerOptionsExplicitAdvertisedAddress(t *testing.T) {
	opts := NewDefaultOptions()
	require.NoError(t, yaml.Unmarshal([]byte(`
server:
  public:
    bindAddress: 0.0.0.0:7000
    advertisedAddress: 0.0.0.0:6651
metadata:
  providerName: memory
`), opts))
	opts.WithDefault()

	require.NoError(t, opts.Validate())
	require.Equal(t, "0.0.0.0:6651", opts.Server.Public.AdvertisedAddress)
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

func readOptions(t *testing.T, data string) *Options {
	t.Helper()

	opts := &Options{}
	require.NoError(t, yaml.Unmarshal([]byte(data), opts))
	opts.WithDefault()
	return opts
}

func hostPort(t *testing.T, port string) string {
	t.Helper()

	hostname, err := os.Hostname()
	require.NoError(t, err)
	require.NotEmpty(t, hostname)
	return net.JoinHostPort(hostname, port)
}
