// Copyright 2023-2025 The Oxia Authors
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

	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestNewFactoryFromOptionsLoadsFileClusterConfig(t *testing.T) {
	dir := t.TempDir()
	writeClusterConfig(t, filepath.Join(dir, option.DefaultFileConfigName))

	factory, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			ProviderOptions: option.ProviderOptions{
				ProviderName: provider.NameFile,
				File: option.FileMetadata{
					Dir: dir,
				},
			},
		},
	})
	require.NoError(t, err)
	metadata, err := factory.CreateMetadata(t.Context())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, factory.Close())
	}()

	config := metadata.GetConfig()
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
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
				ProviderName: provider.NameFile,
				File: option.FileMetadata{
					StatusName: filepath.Join(dir, option.DefaultFileStatusName),
				},
			},
		},
	})
	require.NoError(t, err)
	metadata, err := factory.CreateMetadata(t.Context())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, factory.Close())
	}()

	config := metadata.GetConfig()
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
}

func TestComputeConfigUpdatesCachedConfig(t *testing.T) {
	statusProvider := memory.NewProvider(provider.ClusterStatusCodec)
	configProvider := memory.NewProvider(provider.ClusterConfigCodec)
	_, err := configProvider.Store(&commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{{
			Name:              "default",
			ReplicationFactor: 1,
			InitialShardCount: 1,
		}},
		Servers: []*commonproto.DataServerIdentity{{
			Public:   "s1:9091",
			Internal: "s1:8191",
		}},
	}, provider.NotExists)
	require.NoError(t, err)

	metadata := newMetadata(t.Context(), statusProvider, configProvider)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
	})

	err = metadata.ComputeConfig(func(config *commonproto.ClusterConfiguration) error {
		config.AllowExtraAuthorities = append(config.AllowExtraAuthorities, "127.0.0.1:6648")
		return nil
	})
	require.NoError(t, err)

	config := metadata.GetConfig()
	require.Equal(t, []string{"127.0.0.1:6648"}, config.GetAllowExtraAuthorities())
}

func TestComputeConfigRetriesOnBadVersion(t *testing.T) {
	statusProvider := memory.NewProvider(provider.ClusterStatusCodec)
	configProvider := &retryConfigProvider{
		config: &commonproto.ClusterConfiguration{
			Namespaces: []*commonproto.Namespace{{
				Name:              "default",
				ReplicationFactor: 1,
				InitialShardCount: 1,
			}},
			Servers: []*commonproto.DataServerIdentity{{
				Public:   "s1:9091",
				Internal: "s1:8191",
			}},
		},
		version: provider.NotExists,
	}

	metadata := newMetadata(t.Context(), statusProvider, configProvider)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
	})

	callCount := 0
	err := metadata.ComputeConfig(func(config *commonproto.ClusterConfiguration) error {
		callCount++
		config.Namespaces[0].InitialShardCount++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, callCount)
	require.EqualValues(t, 6, metadata.GetConfig().GetNamespaces()[0].GetInitialShardCount())
}

type retryConfigProvider struct {
	config      *commonproto.ClusterConfiguration
	version     provider.Version
	storeCalled bool
}

func (*retryConfigProvider) Close() error { return nil }

func (*retryConfigProvider) WaitToBecomeLeader() error { return nil }

func (*retryConfigProvider) Watch() (*commonwatch.Receiver[*commonproto.ClusterConfiguration], error) {
	return nil, provider.ErrWatchUnsupported
}

func (p *retryConfigProvider) Get() (*commonproto.ClusterConfiguration, provider.Version, error) {
	return gproto.Clone(p.config).(*commonproto.ClusterConfiguration), p.version, nil
}

func (p *retryConfigProvider) Store(
	value *commonproto.ClusterConfiguration,
	expectedVersion provider.Version,
) (provider.Version, error) {
	if expectedVersion != p.version {
		panic(provider.ErrBadVersion)
	}

	if !p.storeCalled {
		p.storeCalled = true
		p.config = &commonproto.ClusterConfiguration{
			Namespaces: []*commonproto.Namespace{{
				Name:              "default",
				ReplicationFactor: 1,
				InitialShardCount: 5,
			}},
			Servers: []*commonproto.DataServerIdentity{{
				Public:   "s1:9091",
				Internal: "s1:8191",
			}},
		}
		p.version = provider.NextVersion(p.version)
		panic(provider.ErrBadVersion)
	}

	p.config = gproto.Clone(value).(*commonproto.ClusterConfiguration)
	p.version = provider.NextVersion(p.version)
	return p.version, nil
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
