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

	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestNewFactoryFromOptionsCreatesMemoryProviders(t *testing.T) {
	factory, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			Name: "coordinator-test",
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadatacommon.NameMemory,
			},
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, factory.statusProvider)
	assert.NotNil(t, factory.configProvider)
	require.NoError(t, factory.Close())
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
