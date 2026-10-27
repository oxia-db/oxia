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

package metadata

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func memoryOptions() *option.Options {
	return &option.Options{
		Metadata: option.MetadataOptions{
			Name: "coordinator-test",
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameMemory,
			},
		},
	}
}

func newMemoryFactory(t *testing.T, initialConfig *commonproto.ClusterConfiguration) *Factory {
	t.Helper()

	factory, err := New(t.Context(), memoryOptions(), initialConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, factory.Close())
	})
	return factory
}

func newFileFactory(t *testing.T, dir string, initialConfig *commonproto.ClusterConfiguration) *Factory {
	t.Helper()

	factory, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			Name: "coordinator-test",
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameFile,
				File: option.FileMetadata{
					Dir: dir,
				},
			},
		},
	}, initialConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, factory.Close())
	})
	return factory
}

func newInitialConfig(namespace string) *commonproto.ClusterConfiguration {
	return &commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{{
			Name:              namespace,
			ReplicationFactor: 1,
			InitialShardCount: 1,
		}},
		Servers: []*commonproto.DataServerIdentity{{
			Public:   "localhost:6648",
			Internal: "localhost:6649",
		}},
	}
}

// takeOver creates a metadata of factory that takes the leadership.
func takeOver(t *testing.T, factory *Factory) Metadata {
	t.Helper()

	metadata := factory.CreateMetadata(t.Context())
	t.Cleanup(func() { assert.NoError(t, metadata.Close()) })
	_, err := metadata.WaitToBecomeLeader()
	require.NoError(t, err)
	return metadata
}

func loadConfig(t *testing.T, factory *Factory) *commonproto.ClusterConfiguration {
	t.Helper()

	current, err := factory.configProvider.Load()
	require.NoError(t, err)
	if current.Version == metadatacommon.NotExists {
		return nil
	}
	return current.Value
}

// The metadata that takes the leadership stores the initial configuration of
// the factory.
func TestNewInitialClusterConfig(t *testing.T) {
	factory := newMemoryFactory(t, newInitialConfig("test-namespace"))

	takeOver(t, factory)

	current := loadConfig(t, factory)
	require.NotNil(t, current)
	require.Len(t, current.Namespaces, 1)
	assert.Equal(t, "test-namespace", current.Namespaces[0].Name)
}

// A cluster restarted with an initial configuration keeps the configuration
// of the previous run, with the namespaces and data servers added to it
// through the admin API.
func TestNewInitialClusterConfigDoesNotOverwriteAfterRestart(t *testing.T) {
	dir := t.TempDir()

	first := newFileFactory(t, dir, newInitialConfig("first"))
	require.NoError(t, takeOver(t, first).Close())
	require.NoError(t, first.Close())

	restarted := newFileFactory(t, dir, newInitialConfig("second"))
	takeOver(t, restarted)

	current := loadConfig(t, restarted)
	require.NotNil(t, current)
	require.Len(t, current.Namespaces, 1)
	assert.Equal(t, "first", current.Namespaces[0].Name)
}

// An invalid initial configuration is rejected before anything is stored:
// once stored, it would never be replaced, and a coordinator would fail on it
// at every start.
func TestNewRejectsInvalidInitialClusterConfig(t *testing.T) {
	replicationAboveServers := newInitialConfig("test-namespace")
	replicationAboveServers.Namespaces[0].ReplicationFactor = 3

	noShards := newInitialConfig("test-namespace")
	noShards.Namespaces[0].InitialShardCount = 0

	for name, config := range map[string]*commonproto.ClusterConfiguration{
		"replication above servers":   replicationAboveServers,
		"no initial shards":           noShards,
		"coordinator without address": {Coordinators: []*commonproto.Coordinator{{Name: "c1"}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(t.Context(), memoryOptions(), config)
			require.ErrorContains(t, err, "invalid initial cluster configuration")
		})
	}
}

func TestNewFactoryFromOptionsLoadsFileClusterConfig(t *testing.T) {
	dir := t.TempDir()
	writeClusterConfig(t, filepath.Join(dir, option.DefaultFileConfigName))

	factory, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameFile,
				File: option.FileMetadata{
					Dir: dir,
				},
			},
		},
	}, nil)
	require.NoError(t, err)
	metadata := factory.CreateMetadata(t.Context())
	defer func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, factory.Close())
	}()

	borrowedConfig, err := metadata.GetConfig()
	require.NoError(t, err)
	config := borrowedConfig.UnsafeBorrow()
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
}

// The config provider watches its config map in the background, and the
// Kubernetes client logs while the status provider gets created.
func TestNewFactoryFromOptionsCreatesConfigMapProviders(t *testing.T) {
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:1
contexts:
- name: test
  context:
    cluster: test
current-context: test
`), 0o600))
	t.Setenv("KUBECONFIG", kubeconfig)

	factory, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			Name: "coordinator-test",
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameConfigMap,
				Kubernetes:   option.K8sMetadata{Namespace: "oxia"},
			},
		},
	}, nil)
	require.NoError(t, err)
	assert.NotNil(t, factory.configProvider)
	assert.NotNil(t, factory.statusProvider)
	assert.NoError(t, factory.Close())
}

func TestNewFactoryFromOptionsMergesLegacyClusterConfigPath(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "legacy-cluster.yaml")
	writeClusterConfig(t, configPath)

	factory, err := New(t.Context(), &option.Options{
		Cluster: option.ClusterOptions{
			ConfigPath: configPath,
		},
		Metadata: option.MetadataOptions{
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameFile,
				File: option.FileMetadata{
					StatusName: filepath.Join(dir, option.DefaultFileStatusName),
				},
			},
		},
	}, nil)
	require.NoError(t, err)
	metadata := factory.CreateMetadata(t.Context())
	defer func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, factory.Close())
	}()

	borrowedConfig, err := metadata.GetConfig()
	require.NoError(t, err)
	config := borrowedConfig.UnsafeBorrow()
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
}

func TestFactoryOnAppliedForwardsToInterceptor(t *testing.T) {
	factory := &Factory{}
	// Without an interceptor the entries are dropped
	factory.OnApplied("key", []byte("data"), 1)

	interceptor := &recordingInterceptor{}
	factory.raftInterceptor = interceptor
	factory.OnApplied("key", []byte("data"), 2)
	assert.Equal(t, []string{"key"}, interceptor.keys)
	assert.Equal(t, []int64{2}, interceptor.versions)
}

func TestNewRejectsNilOptions(t *testing.T) {
	_, err := New(t.Context(), nil, nil)
	require.ErrorContains(t, err, "options must not be nil")
}

func TestNewRejectsInvalidLegacyClusterConfigPath(t *testing.T) {
	_, err := New(t.Context(), &option.Options{
		Cluster: option.ClusterOptions{ConfigPath: "configmap:oxia/oxia-config"},
		Metadata: option.MetadataOptions{
			Name:            "coordinator-test",
			ProviderOptions: option.ProviderOptions{ProviderName: metadatacommon.NameFile},
		},
	}, nil)
	require.ErrorContains(t, err, "must be a file path")
}

func TestNewRejectsUnknownProvider(t *testing.T) {
	_, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			Name:            "coordinator-test",
			ProviderOptions: option.ProviderOptions{ProviderName: "etcd"},
		},
	}, nil)
	require.ErrorContains(t, err, "must be one of")
}

func TestNewFileProviderFailures(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))

	for name, fileOptions := range map[string]option.FileMetadata{
		"status": {StatusName: filepath.Join(file, "dir", "status.json")},
		"config": {
			StatusName: filepath.Join(dir, "status.json"),
			ConfigName: filepath.Join(file, "dir", "cluster.yaml"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(t.Context(), &option.Options{
				Metadata: option.MetadataOptions{
					Name: "coordinator-test",
					ProviderOptions: option.ProviderOptions{
						ProviderName: metadatacommon.NameFile,
						File:         fileOptions,
					},
				},
			}, nil)
			require.Error(t, err)
		})
	}
}

func TestNewConfigMapProviderRejectsEmptyNameOnLeadership(t *testing.T) {
	writeKubeconfig(t, `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:1
contexts:
- name: test
  context:
    cluster: test
current-context: test
`)

	factory, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameConfigMap,
				Kubernetes:   option.K8sMetadata{Namespace: "oxia"},
			},
		},
	}, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, factory.Close()) }()

	_, err = factory.statusProvider.WaitToBecomeLeader()
	require.ErrorContains(t, err, "failed to create the leader elector")
}

func TestNewConfigMapProviderFailsWithoutKubeconfig(t *testing.T) {
	writeKubeconfig(t, "apiVersion: v1\nkind: Config\n")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	_, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			Name: "coordinator-test",
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameConfigMap,
				Kubernetes:   option.K8sMetadata{Namespace: "oxia"},
			},
		},
	}, nil)
	require.Error(t, err)
}

func TestNewRaftProvider(t *testing.T) {
	options := &option.Options{
		Metadata: option.MetadataOptions{
			Name: "coordinator-test",
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameRaft,
				Raft: option.RaftMetadata{
					Address:        "127.0.0.1:0",
					BootstrapNodes: []string{"127.0.0.1:0"},
					DataDir:        t.TempDir(),
				},
			},
		},
	}

	factory, err := New(t.Context(), options, nil)
	require.NoError(t, err)
	assert.NotNil(t, factory.raft)
	assert.NotNil(t, factory.raftInterceptor, "the config provider follows the applied entries")
	require.NoError(t, factory.Close())

	options.Metadata.Raft.Address = "invalid-address"
	_, err = New(t.Context(), options, nil)
	require.ErrorContains(t, err, "failed to create raft metadata provider")
}

type recordingInterceptor struct {
	keys     []string
	versions []int64
}

func (r *recordingInterceptor) OnApplied(key string, _ []byte, version int64) {
	r.keys = append(r.keys, key)
	r.versions = append(r.versions, version)
}

func writeKubeconfig(t *testing.T, content string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	t.Setenv("KUBECONFIG", path)
}

func writeClusterConfig(t *testing.T, path string) {
	t.Helper()

	require.NoError(t, os.WriteFile(path, []byte(`
namespaces:
  - name: default
    replicationFactor: 1
    initialShardCount: 1
servers:
  - public: s1:9091
    internal: s1:8191
`), 0600))
}
