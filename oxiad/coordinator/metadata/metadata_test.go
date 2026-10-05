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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

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
	_, err = metadata.AllocateShardIDs(1)
	require.Error(t, err)
	require.Error(t, metadata.CreateNamespaceStatus("other", &commonproto.NamespaceStatus{}))
	require.Nil(t, metadata.DeleteNamespaceStatus("default").UnsafeBorrow())
	require.Error(t, metadata.UpdateShardStatus("default", 0, &commonproto.ShardMetadata{Term: 2}))
	require.Error(t, metadata.UpdateShardStatuses("default", func(shards map[int64]*commonproto.ShardMetadata) bool {
		shards[0].Term = 2
		return true
	}))
	require.Error(t, metadata.DeleteShardStatus("default", 0))

	status := loaded(t, statusProvider).Value
	require.Len(t, status.GetNamespaces(), 1)
	require.EqualValues(t, 1, status.GetNamespaces()["default"].GetShards()[0].GetTerm())
	require.NoError(t, metadata.Close())
}

func TestMetadataStatusUpdatesFailWhenTargetIsGone(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	require.NoError(t, metadata.CreateNamespaceStatus("default", &commonproto.NamespaceStatus{
		Shards: map[int64]*commonproto.ShardMetadata{0: {Term: 1}},
	}))

	require.ErrorIs(t, metadata.UpdateShardStatus("other", 0, &commonproto.ShardMetadata{Term: 2}), metadataconstant.ErrNotFound)

	// A deleted shard must not be re-created
	require.ErrorIs(t, metadata.UpdateShardStatus("default", 1, &commonproto.ShardMetadata{Term: 2}), metadataconstant.ErrNotFound)
	_, exists := metadata.GetShardStatus("default", 1)
	require.False(t, exists)

	require.NoError(t, metadata.UpdateShardStatus("default", 0, &commonproto.ShardMetadata{Term: 2}))
	shard, exists := metadata.GetShardStatus("default", 0)
	require.True(t, exists)
	require.EqualValues(t, 2, shard.UnsafeBorrow().GetTerm())

	// Several shards are updated in a namespace that exists, and nothing is
	// stored when the update returns false
	require.ErrorIs(t, metadata.UpdateShardStatuses("other", func(map[int64]*commonproto.ShardMetadata) bool {
		return true
	}), metadataconstant.ErrNotFound)
	require.NoError(t, metadata.UpdateShardStatuses("default", func(shards map[int64]*commonproto.ShardMetadata) bool {
		shards[0].Term = 3
		return false
	}))
	shard, _ = metadata.GetShardStatus("default", 0)
	require.EqualValues(t, 2, shard.UnsafeBorrow().GetTerm())
	_, exists = metadata.GetShardStatus("default", 1)
	require.False(t, exists)
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

// UpdateShardStatuses deletes a namespace left without shards, as
// DeleteShardStatus does, so that it can be created again.
func TestMetadataUpdateShardStatusesDeletesEmptiedNamespace(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	defer func() {
		require.NoError(t, metadata.Close())
	}()
	newNamespaceStatus := func() *commonproto.NamespaceStatus {
		return &commonproto.NamespaceStatus{Shards: map[int64]*commonproto.ShardMetadata{0: {}, 1: {}}}
	}
	deleteShard := func(shard int64) func(map[int64]*commonproto.ShardMetadata) bool {
		return func(shards map[int64]*commonproto.ShardMetadata) bool {
			delete(shards, shard)
			return true
		}
	}
	require.NoError(t, metadata.CreateNamespaceStatus("default", newNamespaceStatus()))

	require.NoError(t, metadata.UpdateShardStatuses("default", deleteShard(0)))
	_, exists := metadata.GetNamespaceStatus("default")
	require.True(t, exists)

	require.NoError(t, metadata.UpdateShardStatuses("default", deleteShard(1)))
	_, exists = metadata.GetNamespaceStatus("default")
	require.False(t, exists)
	require.NoError(t, metadata.CreateNamespaceStatus("default", newNamespaceStatus()))
}

// reloadRecordingProvider counts the reloads, and only accepts writes after
// one, like a provider whose snapshot is outdated until reloaded.
type reloadRecordingProvider struct {
	provider.Provider[*commonproto.ClusterStatus]
	reloads *atomic.Int32
}

func (p reloadRecordingProvider) Reload() error {
	p.reloads.Add(1)
	return p.Provider.Reload()
}

func (p reloadRecordingProvider) Store(snapshot provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
	if p.reloads.Load() == 0 {
		return metadataconstant.NotExists, metadataconstant.ErrBadVersion
	}
	return p.Provider.Store(snapshot)
}

// A coordinator taking over reloads the snapshots it loaded before the
// leadership, before its first write.
func TestMetadataReloadsOnLeadership(t *testing.T) {
	var statusReloads, configReloads atomic.Int32
	statusProvider := reloadRecordingProvider{
		Provider: memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, ""),
		reloads:  &statusReloads,
	}
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	countingConfig := reloadRecordingConfig{Provider: configProvider, reloads: &configReloads}
	metadata := newMetadata(t.Context(), statusProvider, countingConfig, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	_, err := metadata.WaitToBecomeLeader()
	require.NoError(t, err)
	require.EqualValues(t, 1, statusReloads.Load())
	require.EqualValues(t, 1, configReloads.Load())
	// The status recovery, the first write, came after the reload.
	instanceID, err := metadata.GetInstanceID()
	require.NoError(t, err)
	require.NotEmpty(t, instanceID)
}

// lostLeadershipProvider reports a leadership already lost when it is acquired.
type lostLeadershipProvider struct {
	provider.Provider[*commonproto.ClusterStatus]
}

func (lostLeadershipProvider) WaitToBecomeLeader() (<-chan struct{}, error) {
	lost := make(chan struct{})
	close(lost)
	return lost, nil
}

// A coordinator that loses the leadership while it reloads the metadata does
// not write the status recovery.
func TestMetadataStopsTakeoverWhenLeadershipLost(t *testing.T) {
	statusProvider := lostLeadershipProvider{
		Provider: memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, ""),
	}
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	_, err := metadata.WaitToBecomeLeader()
	require.Error(t, err)
	instanceID, err := metadata.GetInstanceID()
	require.NoError(t, err)
	require.Empty(t, instanceID)
}

type reloadRecordingConfig struct {
	provider.Provider[*commonproto.ClusterConfiguration]
	reloads *atomic.Int32
}

func (p reloadRecordingConfig) Reload() error {
	p.reloads.Add(1)
	return p.Provider.Reload()
}

// loaded returns the stored snapshot, failing the test if it cannot be loaded.
func loaded[T gproto.Message](t *testing.T, p provider.Provider[T]) *provider.Versioned[T] {
	t.Helper()
	snapshot, err := p.Load()
	require.NoError(t, err)
	return snapshot
}

// writeFailingStatusProvider fails every status write.
type writeFailingStatusProvider struct {
	provider.Provider[*commonproto.ClusterStatus]
}

func (writeFailingStatusProvider) Store(provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
	return metadataconstant.NotExists, errors.New("store unavailable")
}

// A takeover whose status recovery fails reports the failure, instead of
// starting without an instance id.
func TestMetadataTakeoverFailsWhenRecoveryFails(t *testing.T) {
	statusProvider := writeFailingStatusProvider{
		Provider: memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, ""),
	}
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	_, err := metadata.WaitToBecomeLeader()
	require.ErrorContains(t, err, "store unavailable")
}

// loadFailingConfigProvider fails every load of the configuration.
type loadFailingConfigProvider struct {
	provider.Provider[*commonproto.ClusterConfiguration]
}

func (loadFailingConfigProvider) Load() (*provider.Versioned[*commonproto.ClusterConfiguration], error) {
	return nil, errors.New("configuration unavailable")
}

// GetSelf reports a configuration that cannot be loaded, without waiting for
// it.
func TestMetadataGetSelfReturnsLoadError(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, "")
	configProvider := loadFailingConfigProvider{
		Provider: memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, ""),
	}
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "coordinator")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	_, err := metadata.GetSelf()
	require.ErrorContains(t, err, "configuration unavailable")
}

// GetLeader reports a configuration that cannot be loaded, without waiting
// for it.
func TestMetadataGetLeaderReturnsLoadError(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, "coordinator")
	configProvider := loadFailingConfigProvider{
		Provider: memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "coordinator"),
	}
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "coordinator")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	_, err := metadata.GetLeader()
	require.ErrorContains(t, err, "configuration unavailable")
}

// loadFailingStatusProvider fails every load of the status.
type loadFailingStatusProvider struct {
	provider.Provider[*commonproto.ClusterStatus]
}

func (loadFailingStatusProvider) Load() (*provider.Versioned[*commonproto.ClusterStatus], error) {
	return nil, errors.New("status unavailable")
}

// GetInstanceID reports a status that cannot be loaded, without waiting for
// it.
func TestMetadataGetInstanceIDReturnsLoadError(t *testing.T) {
	statusProvider := loadFailingStatusProvider{
		Provider: memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, ""),
	}
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	_, err := metadata.GetInstanceID()
	require.ErrorContains(t, err, "status unavailable")
}

func TestMetadataListNamespaceStatusReturnsLoadError(t *testing.T) {
	statusProvider := loadFailingStatusProvider{
		Provider: memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, ""),
	}
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	_, err := metadata.ListNamespaceStatus()
	require.ErrorContains(t, err, "status unavailable")
}

// countingFailingStatusProvider counts the status writes, and fails them.
type countingFailingStatusProvider struct {
	provider.Provider[*commonproto.ClusterStatus]
	writes *atomic.Int32
}

func (p countingFailingStatusProvider) Store(provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
	p.writes.Add(1)
	return metadataconstant.NotExists, errors.New("store unavailable")
}

// AllocateShardIDs reports a failed write to its caller, which retries,
// instead of retrying it.
func TestMetadataAllocateShardIDsReturnsWriteError(t *testing.T) {
	var writes atomic.Int32
	statusProvider := countingFailingStatusProvider{
		Provider: memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, ""),
		writes:   &writes,
	}
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })

	_, err := metadata.AllocateShardIDs(2)
	require.ErrorContains(t, err, "store unavailable")
	require.EqualValues(t, 1, writes.Load())
}
