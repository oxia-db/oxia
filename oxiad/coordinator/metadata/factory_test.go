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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func newMemoryFactory(t *testing.T) *Factory {
	t.Helper()

	factory, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			Name: "coordinator-test",
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameMemory,
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, factory.Close())
	})
	return factory
}

func newFileFactory(t *testing.T, dir string) *Factory {
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
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, factory.Close())
	})
	return factory
}

func newSeedConfig(namespace string) *commonproto.ClusterConfiguration {
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

func loadConfig(t *testing.T, factory *Factory) *commonproto.ClusterConfiguration {
	t.Helper()

	current, err := factory.configProvider.Load()
	require.NoError(t, err)
	if current.Version == metadatacommon.NotExists {
		return nil
	}
	return current.Value
}

func TestSeedClusterConfig(t *testing.T) {
	factory := newMemoryFactory(t)

	require.NoError(t, factory.SeedClusterConfig(newSeedConfig("test-namespace")))

	seeded := loadConfig(t, factory)
	require.NotNil(t, seeded)
	require.Len(t, seeded.Namespaces, 1)
	assert.Equal(t, "test-namespace", seeded.Namespaces[0].Name)
}

func TestSeedClusterConfigDoesNotOverwrite(t *testing.T) {
	factory := newMemoryFactory(t)

	require.NoError(t, factory.SeedClusterConfig(newSeedConfig("first")))
	require.NoError(t, factory.SeedClusterConfig(newSeedConfig("second")))

	current := loadConfig(t, factory)
	require.NotNil(t, current)
	require.Len(t, current.Namespaces, 1)
	assert.Equal(t, "first", current.Namespaces[0].Name)
}

// An embedded coordinator seeds its configuration at every start. A restart
// must keep the configuration of the previous run, with the namespaces and
// data servers added to it through the admin API.
func TestFactorySeedClusterConfigDoesNotOverwriteAfterRestart(t *testing.T) {
	dir := t.TempDir()

	first := newFileFactory(t, dir)
	require.NoError(t, first.SeedClusterConfig(newSeedConfig("first")))
	require.NoError(t, first.Close())

	restarted := newFileFactory(t, dir)
	require.NoError(t, restarted.SeedClusterConfig(newSeedConfig("second")))

	current := loadConfig(t, restarted)
	require.NotNil(t, current)
	require.Len(t, current.Namespaces, 1)
	assert.Equal(t, "first", current.Namespaces[0].Name)
}

// An invalid configuration is rejected before anything is stored: once stored,
// it would never be replaced by a later seed, and a coordinator would fail on
// it at every start.
func TestSeedClusterConfigRejectsInvalid(t *testing.T) {
	replicationAboveServers := newSeedConfig("test-namespace")
	replicationAboveServers.Namespaces[0].ReplicationFactor = 3

	noShards := newSeedConfig("test-namespace")
	noShards.Namespaces[0].InitialShardCount = 0

	for name, config := range map[string]*commonproto.ClusterConfiguration{
		"nil":                         nil,
		"replication above servers":   replicationAboveServers,
		"no initial shards":           noShards,
		"coordinator without address": {Coordinators: []*commonproto.Coordinator{{Name: "c1"}}},
	} {
		t.Run(name, func(t *testing.T) {
			factory := newMemoryFactory(t)

			require.Error(t, factory.SeedClusterConfig(config))
			assert.Nil(t, loadConfig(t, factory))
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
	})
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
	})
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
	})
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
	_, err := New(t.Context(), nil)
	require.ErrorContains(t, err, "options must not be nil")
}

func TestNewRejectsInvalidLegacyClusterConfigPath(t *testing.T) {
	_, err := New(t.Context(), &option.Options{
		Cluster: option.ClusterOptions{ConfigPath: "configmap:oxia/oxia-config"},
		Metadata: option.MetadataOptions{
			Name:            "coordinator-test",
			ProviderOptions: option.ProviderOptions{ProviderName: metadatacommon.NameFile},
		},
	})
	require.ErrorContains(t, err, "must be a file path")
}

func TestNewRejectsUnknownProvider(t *testing.T) {
	_, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			Name:            "coordinator-test",
			ProviderOptions: option.ProviderOptions{ProviderName: "etcd"},
		},
	})
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
			})
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
	})
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
	})
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

	factory, err := New(t.Context(), options)
	require.NoError(t, err)
	assert.NotNil(t, factory.raft)
	assert.NotNil(t, factory.raftInterceptor, "the config provider follows the applied entries")
	require.NoError(t, factory.Close())

	options.Metadata.Raft.Address = "invalid-address"
	_, err = New(t.Context(), options)
	require.ErrorContains(t, err, "failed to create raft metadata provider")
}

func TestFactorySeedClusterConfigReturnsLoadError(t *testing.T) {
	factory := newMemoryFactory(t)
	factory.configProvider = fakeProvider[*commonproto.ClusterConfiguration]{
		Provider: factory.configProvider,
		load:     failLoad[*commonproto.ClusterConfiguration]("load failure"),
	}

	require.ErrorContains(t, factory.SeedClusterConfig(newSeedConfig("test-namespace")), "load failure")
}

func TestFactorySeedClusterConfigStoreFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		storeErr error
		expected string
	}{
		// Another coordinator seeded its configuration first
		"bad version": {storeErr: metadatacommon.ErrBadVersion},
		"other":       {storeErr: errors.New("store failure"), expected: "store failure"},
	} {
		t.Run(name, func(t *testing.T) {
			factory := newMemoryFactory(t)
			factory.configProvider = fakeProvider[*commonproto.ClusterConfiguration]{
				Provider: factory.configProvider,
				store: func(provider.Versioned[*commonproto.ClusterConfiguration]) (metadatacommon.Version, error) {
					return metadatacommon.NotExists, tc.storeErr
				},
			}

			err := factory.SeedClusterConfig(newSeedConfig("test-namespace"))
			if tc.expected == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.expected)
		})
	}
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
