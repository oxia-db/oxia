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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadataconstant "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
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
				ProviderName: metadataconstant.NameFile,
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

	config := metadata.GetConfig().UnsafeBorrow()
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
				ProviderName: metadataconstant.NameFile,
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

	config := metadata.GetConfig().UnsafeBorrow()
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
}

func TestMetadataGetSelfReturnsConfiguredCoordinator(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, option.DefaultFileConfigName), []byte(`
coordinators:
  - name: coordinator-0
    publicAddress: coordinator-0.example.com:6651
`), 0600))

	factory, err := New(t.Context(), &option.Options{
		Metadata: option.MetadataOptions{
			Name: "coordinator-0",
			ProviderOptions: option.ProviderOptions{
				ProviderName: metadataconstant.NameFile,
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

	self, err := metadata.GetSelf()
	require.NoError(t, err)
	require.Equal(t, "coordinator-0", self.GetName())
	require.Equal(t, "coordinator-0.example.com:6651", self.GetPublicAddress())

	self.Name = "changed"
	nextSelf, err := metadata.GetSelf()
	require.NoError(t, err)
	require.Equal(t, "coordinator-0", nextSelf.GetName())
}

// storeFailingProvider fails every write, like a store the coordinator lost
// access to (e.g. after losing the leadership).
type storeFailingProvider struct {
	provider.Provider[*commonproto.ClusterStatus]
}

func (storeFailingProvider) Store(provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
	return metadataconstant.NotExists, errors.New("store failed")
}

func TestMetadataStatusWritersGiveUpOnceCanceled(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, "")
	_, err := statusProvider.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value: &commonproto.ClusterStatus{
			Namespaces: map[string]*commonproto.NamespaceStatus{
				"default": {Shards: map[int64]*commonproto.ShardMetadata{0: {Term: 1}}},
			},
		},
		Version: metadataconstant.NotExists,
	})
	require.NoError(t, err)
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")

	ctx, cancel := context.WithCancel(t.Context())
	metadata := newMetadata(ctx, storeFailingProvider{statusProvider}, configProvider, "")
	cancel()

	// Every status writer gives up, and reports that nothing was persisted
	_, err = metadata.ReserveShardIDs(1)
	require.Error(t, err)
	require.False(t, metadata.CreateNamespaceStatus("other", &commonproto.NamespaceStatus{}))
	require.Error(t, metadata.UpdateNamespaceStatus("default", &commonproto.NamespaceStatus{}))
	require.Nil(t, metadata.DeleteNamespaceStatus("default").UnsafeBorrow())
	require.Error(t, metadata.UpdateShardStatus("default", 0, &commonproto.ShardMetadata{Term: 2}))
	require.Error(t, metadata.DeleteShardStatus("default", 0))

	status := statusProvider.Watch().Load().Value
	require.Len(t, status.GetNamespaces(), 1)
	require.EqualValues(t, 1, status.GetNamespaces()["default"].GetShards()[0].GetTerm())
	require.NoError(t, metadata.Close())
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
