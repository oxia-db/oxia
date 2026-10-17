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

// --- Leadership ---

// A coordinator taking over reloads the snapshots it loaded before the
// leadership, before its first write.
func TestMetadataWaitToBecomeLeaderReloadsMetadata(t *testing.T) {
	var statusReloads, configReloads atomic.Int32
	statusProvider := newStatusProvider()
	configProvider := newConfigProvider()
	metadata := newTestMetadata(t, fakeProvider[*commonproto.ClusterStatus]{
		Provider: statusProvider,
		reload: func() error {
			statusReloads.Add(1)
			return statusProvider.Reload()
		},
		// The snapshot is outdated until reloaded
		store: func(snapshot provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
			if statusReloads.Load() == 0 {
				return metadataconstant.NotExists, metadataconstant.ErrBadVersion
			}
			return statusProvider.Store(snapshot)
		},
	}, fakeProvider[*commonproto.ClusterConfiguration]{
		Provider: configProvider,
		reload: func() error {
			configReloads.Add(1)
			return configProvider.Reload()
		},
	}, "")

	_, err := metadata.WaitToBecomeLeader()
	require.NoError(t, err)
	require.EqualValues(t, 1, statusReloads.Load())
	require.EqualValues(t, 1, configReloads.Load())
	// The status recovery, the first write, came after the reload.
	instanceID, err := metadata.GetInstanceID()
	require.NoError(t, err)
	require.NotEmpty(t, instanceID)
}

// A coordinator that loses the leadership while it reloads the metadata does
// not write the status recovery.
func TestMetadataWaitToBecomeLeaderStopsWhenLeadershipLost(t *testing.T) {
	metadata := newTestMetadata(t, fakeProvider[*commonproto.ClusterStatus]{
		Provider: newStatusProvider(),
		waitToBecomeLeader: func() (<-chan struct{}, error) {
			lost := make(chan struct{})
			close(lost)
			return lost, nil
		},
	}, newConfigProvider(), "")

	_, err := metadata.WaitToBecomeLeader()
	require.Error(t, err)
	instanceID, err := metadata.GetInstanceID()
	require.NoError(t, err)
	require.Empty(t, instanceID)
}

// A takeover whose status recovery fails reports the failure, instead of
// starting without an instance id.
func TestMetadataWaitToBecomeLeaderFailsWhenRecoveryFails(t *testing.T) {
	metadata := newTestMetadata(t, fakeProvider[*commonproto.ClusterStatus]{
		Provider: newStatusProvider(),
		store:    failStore("store unavailable"),
	}, newConfigProvider(), "")

	_, err := metadata.WaitToBecomeLeader()
	require.ErrorContains(t, err, "store unavailable")
}

// A takeover with a missing configuration and a status with namespaces fails,
// and writes nothing: the configuration was deleted, and reconciling it as
// empty would delete every namespace.
func TestMetadataWaitToBecomeLeaderRejectsDeletedConfiguration(t *testing.T) {
	statusProvider := newStoredStatusProvider(t)
	version := loaded(t, statusProvider).Version
	metadata := newTestMetadata(t, statusProvider, newConfigProvider(), "")

	_, err := metadata.WaitToBecomeLeader()
	require.ErrorContains(t, err, "the cluster configuration is missing")
	require.Equal(t, version, loaded(t, statusProvider).Version)
}

// A cluster that was never configured, e.g. one set up through the admin API,
// restarts with an instance id and no configuration: an empty configuration
// deletes nothing, so the takeover succeeds.
func TestMetadataWaitToBecomeLeaderAcceptsUnconfiguredCluster(t *testing.T) {
	statusProvider := newStatusProvider()
	_, err := statusProvider.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   &commonproto.ClusterStatus{InstanceId: "instance"},
		Version: metadataconstant.NotExists,
	})
	require.NoError(t, err)
	metadata := newTestMetadata(t, statusProvider, newConfigProvider(), "")

	_, err = metadata.WaitToBecomeLeader()
	require.NoError(t, err)
}

// newLeaderMetadata returns a metadata that holds the leadership. Closing lost
// loses it, and once superseded is set, every status write is refused on the
// version, as when another coordinator took over and wrote the status.
func newLeaderMetadata(t *testing.T) (metadata Metadata, lost chan struct{}, superseded *atomic.Bool) {
	t.Helper()

	lost = make(chan struct{})
	superseded = &atomic.Bool{}
	statusProvider := newStatusProvider()
	metadata = newTestMetadata(t, fakeProvider[*commonproto.ClusterStatus]{
		Provider: statusProvider,
		waitToBecomeLeader: func() (<-chan struct{}, error) {
			return lost, nil
		},
		store: func(snapshot provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
			if superseded.Load() {
				return metadataconstant.NotExists, metadataconstant.ErrBadVersion
			}
			return statusProvider.Store(snapshot)
		},
	}, newConfigProvider(), "")

	_, err := metadata.WaitToBecomeLeader()
	require.NoError(t, err)
	return metadata, lost, superseded
}

// A coordinator that lost the leadership can still have a status write in
// flight. The new leader's writes refuse it on the version: the write fails
// instead of taking the whole process down.
func TestMetadataStatusWriteAfterLeadershipLossFails(t *testing.T) {
	metadata, lost, superseded := newLeaderMetadata(t)

	close(lost)
	superseded.Store(true)

	_, err := metadata.AllocateShardIDs(1)
	require.ErrorIs(t, err, metadataconstant.ErrBadVersion)
}

// A new leader can write the status before this coordinator learns that it
// lost the leadership. The version check already refused the stale write: it
// fails, and the process keeps running.
func TestMetadataStatusVersionConflictWhileLeadingFails(t *testing.T) {
	metadata, _, superseded := newLeaderMetadata(t)

	superseded.Store(true)

	require.NotPanics(t, func() {
		_, err := metadata.AllocateShardIDs(1)
		require.ErrorIs(t, err, metadataconstant.ErrBadVersion)
	})
}

// --- Coordinators ---

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

// --- Namespace status ---

func TestMetadataCreateNamespaceStatusAlreadyExists(t *testing.T) {
	statusProvider := newStatusProvider()
	metadata := newTestMetadata(t, statusProvider, newConfigProvider(), "")

	require.NoError(t, metadata.CreateNamespaceStatus("default", &commonproto.NamespaceStatus{ReplicationFactor: 3}))
	version := loaded(t, statusProvider).Version
	err := metadata.CreateNamespaceStatus("default", &commonproto.NamespaceStatus{ReplicationFactor: 1})
	require.ErrorIs(t, err, metadataconstant.ErrAlreadyExists)
	require.Contains(t, err.Error(), "default")
	require.Equal(t, version, loaded(t, statusProvider).Version)
	status, exists, err := metadata.GetNamespaceStatus("default")
	require.NoError(t, err)
	require.True(t, exists)
	require.EqualValues(t, 3, status.UnsafeBorrow().GetReplicationFactor())
}

func TestMetadataCreateNamespaceStatusReturnsWriteError(t *testing.T) {
	statusProvider := newStatusProvider()
	storeErr := errors.New("status store unavailable")
	writes := 0
	metadata := newTestMetadata(t, fakeProvider[*commonproto.ClusterStatus]{
		Provider: statusProvider,
		store: func(snapshot provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
			writes++
			if writes > 1 {
				return statusProvider.Store(snapshot)
			}
			return metadataconstant.NotExists, storeErr
		},
	}, newConfigProvider(), "")

	err := metadata.CreateNamespaceStatus("default", &commonproto.NamespaceStatus{ReplicationFactor: 3})
	require.ErrorIs(t, err, storeErr)
	require.NotErrorIs(t, err, metadataconstant.ErrAlreadyExists)
	require.Equal(t, 1, writes)
	_, exists, err := metadata.GetNamespaceStatus("default")
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, metadataconstant.NotExists, loaded(t, statusProvider).Version)
}

func TestMetadataCreateNamespaceStatusCanBeRetriedByCaller(t *testing.T) {
	statusProvider := newStatusProvider()
	writes := 0
	storeErr := errors.New("temporary status store failure")
	metadata := newTestMetadata(t, fakeProvider[*commonproto.ClusterStatus]{
		Provider: statusProvider,
		store: func(snapshot provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
			writes++
			if writes == 1 {
				return metadataconstant.NotExists, storeErr
			}
			return statusProvider.Store(snapshot)
		},
	}, newConfigProvider(), "")

	namespaceStatus := &commonproto.NamespaceStatus{ReplicationFactor: 3}
	err := metadata.CreateNamespaceStatus("default", namespaceStatus)
	require.ErrorIs(t, err, storeErr)
	require.Equal(t, 1, writes)
	_, exists, err := metadata.GetNamespaceStatus("default")
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, metadata.CreateNamespaceStatus("default", namespaceStatus))
	require.Equal(t, 2, writes)
	status, exists, err := metadata.GetNamespaceStatus("default")
	require.NoError(t, err)
	require.True(t, exists)
	require.EqualValues(t, 3, status.UnsafeBorrow().GetReplicationFactor())
}

func TestMetadataCreateNamespaceStatusReturnsErrorAfterCommit(t *testing.T) {
	statusProvider := newStatusProvider()
	storeErr := errors.New("metadata write timed out after committing")
	writes := 0
	metadata := newTestMetadata(t, fakeProvider[*commonproto.ClusterStatus]{
		Provider: statusProvider,
		store: func(snapshot provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
			writes++
			version, err := statusProvider.Store(snapshot)
			if err != nil {
				return version, err
			}
			return version, storeErr
		},
	}, newConfigProvider(), "")

	err := metadata.CreateNamespaceStatus("default", &commonproto.NamespaceStatus{ReplicationFactor: 3})
	require.ErrorIs(t, err, storeErr)
	saved, exists, err := metadata.GetNamespaceStatus("default")
	require.NoError(t, err)
	require.True(t, exists)
	require.EqualValues(t, 3, saved.UnsafeBorrow().ReplicationFactor)
	require.NotEqual(t, metadataconstant.NotExists, loaded(t, statusProvider).Version)

	require.ErrorIs(t, metadata.CreateNamespaceStatus("default", &commonproto.NamespaceStatus{}), metadataconstant.ErrAlreadyExists)
	require.Equal(t, 1, writes)
}

// PatchNamespaceStatus deletes a namespace left without shards, as
// DeleteShardStatus does, so that it can be created again.
func TestMetadataPatchNamespaceStatusDeletesEmptiedNamespace(t *testing.T) {
	metadata := newTestMetadata(t, newStatusProvider(), newConfigProvider(), "")
	newNamespaceStatus := func() *commonproto.NamespaceStatus {
		return &commonproto.NamespaceStatus{Shards: map[int64]*commonproto.ShardMetadata{0: {}, 1: {}}}
	}
	deleteShard := func(shard int64) func(*commonproto.NamespaceStatus) bool {
		return func(status *commonproto.NamespaceStatus) bool {
			delete(status.Shards, shard)
			return true
		}
	}
	require.NoError(t, metadata.CreateNamespaceStatus("default", newNamespaceStatus()))

	require.NoError(t, metadata.PatchNamespaceStatus("default", deleteShard(0)))
	_, exists, err := metadata.GetNamespaceStatus("default")
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, metadata.PatchNamespaceStatus("default", deleteShard(1)))
	_, exists, err = metadata.GetNamespaceStatus("default")
	require.NoError(t, err)
	require.False(t, exists)
	require.NoError(t, metadata.CreateNamespaceStatus("default", newNamespaceStatus()))
}

// --- Shard status ---

func TestMetadataStatusUpdatesFailWhenTargetIsGone(t *testing.T) {
	metadata := newTestMetadata(t, newStatusProvider(), newConfigProvider(), "")
	require.NoError(t, metadata.CreateNamespaceStatus("default", &commonproto.NamespaceStatus{
		Shards: map[int64]*commonproto.ShardMetadata{0: {Term: 1}},
	}))

	require.ErrorIs(t, metadata.UpdateShardStatus("other", 0, &commonproto.ShardMetadata{Term: 2}), metadataconstant.ErrNotFound)

	// A deleted shard must not be re-created
	require.ErrorIs(t, metadata.UpdateShardStatus("default", 1, &commonproto.ShardMetadata{Term: 2}), metadataconstant.ErrNotFound)
	_, exists, err := metadata.GetShardStatus("default", 1)
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, metadata.UpdateShardStatus("default", 0, &commonproto.ShardMetadata{Term: 2}))
	shard, exists, err := metadata.GetShardStatus("default", 0)
	require.NoError(t, err)
	require.True(t, exists)
	require.EqualValues(t, 2, shard.UnsafeBorrow().GetTerm())

	// Several shards are updated in a namespace that exists, and nothing is
	// stored when the update returns false
	require.ErrorIs(t, metadata.PatchNamespaceStatus("other", func(*commonproto.NamespaceStatus) bool {
		return true
	}), metadataconstant.ErrNotFound)
	require.NoError(t, metadata.PatchNamespaceStatus("default", func(status *commonproto.NamespaceStatus) bool {
		status.Shards[0].Term = 3
		return false
	}))
	shard, _, err = metadata.GetShardStatus("default", 0)
	require.NoError(t, err)
	require.EqualValues(t, 2, shard.UnsafeBorrow().GetTerm())
	_, exists, err = metadata.GetShardStatus("default", 1)
	require.NoError(t, err)
	require.False(t, exists)
}

// --- Errors ---

// The reads of the configuration report a configuration that cannot be
// loaded, without waiting for it.
func TestMetadataReadsReturnConfigurationLoadError(t *testing.T) {
	for _, test := range []struct {
		name string
		read func(t *testing.T, metadata Metadata) error
	}{
		{"GetSelf", func(_ *testing.T, metadata Metadata) error {
			_, err := metadata.GetSelf()
			return err
		}},
		{"GetLeader", func(_ *testing.T, metadata Metadata) error {
			_, err := metadata.GetLeader()
			return err
		}},
		{"GetConfig", func(_ *testing.T, metadata Metadata) error {
			_, err := metadata.GetConfig()
			return err
		}},
		{"GetLoadBalancerConfig", func(_ *testing.T, metadata Metadata) error {
			_, err := metadata.GetLoadBalancerConfig()
			return err
		}},
		{"ListNamespace", func(_ *testing.T, metadata Metadata) error {
			_, err := metadata.ListNamespace()
			return err
		}},
		{"GetNamespace", func(t *testing.T, metadata Metadata) error {
			t.Helper()
			_, exists, err := metadata.GetNamespace("default")
			require.False(t, exists)
			return err
		}},
		{"GetNamespaceTermOptions", func(_ *testing.T, metadata Metadata) error {
			_, err := metadata.GetNamespaceTermOptions("default")
			return err
		}},
		{"ListDataServer", func(_ *testing.T, metadata Metadata) error {
			_, err := metadata.ListDataServer()
			return err
		}},
		{"GetDataServer", func(t *testing.T, metadata Metadata) error {
			t.Helper()
			_, exists, err := metadata.GetDataServer("s1")
			require.False(t, exists)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := newTestMetadata(t,
				memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, "coordinator"),
				fakeProvider[*commonproto.ClusterConfiguration]{
					Provider: memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "coordinator"),
					load:     failLoad[*commonproto.ClusterConfiguration]("configuration unavailable"),
				}, "coordinator")

			require.ErrorContains(t, test.read(t, metadata), "configuration unavailable")
		})
	}
}

// The reads of the status report a status that cannot be loaded, without
// waiting for it.
func TestMetadataReadsReturnStatusLoadError(t *testing.T) {
	for _, test := range []struct {
		name string
		read func(t *testing.T, metadata Metadata) error
	}{
		{"GetInstanceID", func(_ *testing.T, metadata Metadata) error {
			_, err := metadata.GetInstanceID()
			return err
		}},
		{"ListNamespaceStatus", func(_ *testing.T, metadata Metadata) error {
			_, err := metadata.ListNamespaceStatus()
			return err
		}},
		{"GetNamespaceStatus", func(t *testing.T, metadata Metadata) error {
			t.Helper()
			_, exists, err := metadata.GetNamespaceStatus("default")
			require.False(t, exists)
			return err
		}},
		{"GetShardStatus", func(t *testing.T, metadata Metadata) error {
			t.Helper()
			_, exists, err := metadata.GetShardStatus("default", 0)
			require.False(t, exists)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := newTestMetadata(t, fakeProvider[*commonproto.ClusterStatus]{
				Provider: newStatusProvider(),
				load:     failLoad[*commonproto.ClusterStatus]("status unavailable"),
			}, newConfigProvider(), "")

			require.ErrorContains(t, test.read(t, metadata), "status unavailable")
		})
	}
}

// The writes of the status report a failed write to their caller, which
// decides how to retry, after a single attempt.
func TestMetadataStatusWritesReturnWriteError(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(metadata Metadata) error
	}{
		{"UpdateShardStatus", func(metadata Metadata) error {
			return metadata.UpdateShardStatus("default", 0, &commonproto.ShardMetadata{Term: 2})
		}},
		{"PatchNamespaceStatus", func(metadata Metadata) error {
			return metadata.PatchNamespaceStatus("default", func(status *commonproto.NamespaceStatus) bool {
				status.Shards[0].Term = 2
				return true
			})
		}},
		{"DeleteShardStatus", func(metadata Metadata) error {
			return metadata.DeleteShardStatus("default", 0)
		}},
		{"AllocateShardIDs", func(metadata Metadata) error {
			_, err := metadata.AllocateShardIDs(2)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var writes atomic.Int32
			failing := failStore("store unavailable")
			metadata := newTestMetadata(t, fakeProvider[*commonproto.ClusterStatus]{
				Provider: newStoredStatusProvider(t),
				store: func(snapshot provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
					writes.Add(1)
					return failing(snapshot)
				},
			}, newConfigProvider(), "")

			require.ErrorContains(t, test.write(metadata), "store unavailable")
			require.EqualValues(t, 1, writes.Load())
		})
	}
}

// Every status writer gives up once the metadata is closing, and reports that
// nothing was persisted.
func TestMetadataStatusWritersGiveUpOnceCanceled(t *testing.T) {
	statusProvider := newStoredStatusProvider(t)
	ctx, cancel := context.WithCancel(t.Context())
	metadata := newDefaultMetadata(ctx, fakeProvider[*commonproto.ClusterStatus]{
		Provider: statusProvider,
		store:    failStore("store failed"),
	}, newConfigProvider(), "")
	cancel()

	_, err := metadata.AllocateShardIDs(1)
	require.Error(t, err)
	require.Error(t, metadata.CreateNamespaceStatus("other", &commonproto.NamespaceStatus{}))
	require.Nil(t, metadata.DeleteNamespaceStatus("default").UnsafeBorrow())
	require.Error(t, metadata.UpdateShardStatus("default", 0, &commonproto.ShardMetadata{Term: 2}))
	require.Error(t, metadata.PatchNamespaceStatus("default", func(status *commonproto.NamespaceStatus) bool {
		status.Shards[0].Term = 2
		return true
	}))
	require.Error(t, metadata.DeleteShardStatus("default", 0))

	status := loaded(t, statusProvider).Value
	require.Len(t, status.GetNamespaces(), 1)
	require.EqualValues(t, 1, status.GetNamespaces()["default"].GetShards()[0].GetTerm())
	require.NoError(t, metadata.Close())
}

// --- Helpers ---

// newTestMetadata returns a metadata on the providers, closed with the test.
func newTestMetadata(
	t *testing.T,
	statusProvider provider.Provider[*commonproto.ClusterStatus],
	configProvider provider.Provider[*commonproto.ClusterConfiguration],
	name string,
) Metadata {
	t.Helper()
	metadata := newDefaultMetadata(t.Context(), statusProvider, configProvider, name)
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })
	return metadata
}

func newStatusProvider() provider.Provider[*commonproto.ClusterStatus] {
	return memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, "")
}

func newConfigProvider() provider.Provider[*commonproto.ClusterConfiguration] {
	return memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
}

// newStoredStatusProvider returns a status provider that stores the namespace
// default, with the shard 0 in the term 1.
func newStoredStatusProvider(t *testing.T) provider.Provider[*commonproto.ClusterStatus] {
	t.Helper()
	statusProvider := newStatusProvider()
	_, err := statusProvider.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value: &commonproto.ClusterStatus{
			Namespaces: map[string]*commonproto.NamespaceStatus{
				"default": {Shards: map[int64]*commonproto.ShardMetadata{0: {Term: 1}}},
			},
		},
		Version: metadataconstant.NotExists,
	})
	require.NoError(t, err)
	return statusProvider
}

// loaded returns the stored snapshot, failing the test if it cannot be loaded.
func loaded[T gproto.Message](t *testing.T, p provider.Provider[T]) *provider.Versioned[T] {
	t.Helper()
	snapshot, err := p.Load()
	require.NoError(t, err)
	return snapshot
}

// fakeProvider wraps a provider: each of its functions, when set, replaces the
// method of the wrapped provider.
type fakeProvider[T gproto.Message] struct {
	provider.Provider[T]
	load               func() (*provider.Versioned[T], error)
	store              func(provider.Versioned[T]) (metadataconstant.Version, error)
	reload             func() error
	waitToBecomeLeader func() (<-chan struct{}, error)
}

func (p fakeProvider[T]) Load() (*provider.Versioned[T], error) {
	if p.load != nil {
		return p.load()
	}
	return p.Provider.Load()
}

func (p fakeProvider[T]) Store(snapshot provider.Versioned[T]) (metadataconstant.Version, error) {
	if p.store != nil {
		return p.store(snapshot)
	}
	return p.Provider.Store(snapshot)
}

func (p fakeProvider[T]) Reload() error {
	if p.reload != nil {
		return p.reload()
	}
	return p.Provider.Reload()
}

func (p fakeProvider[T]) WaitToBecomeLeader() (<-chan struct{}, error) {
	if p.waitToBecomeLeader != nil {
		return p.waitToBecomeLeader()
	}
	return p.Provider.WaitToBecomeLeader()
}

// failLoad returns a load that fails with message.
func failLoad[T gproto.Message](message string) func() (*provider.Versioned[T], error) {
	return func() (*provider.Versioned[T], error) {
		return nil, errors.New(message)
	}
}

// failStore returns a status store that fails with message.
func failStore(message string) func(provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
	return func(provider.Versioned[*commonproto.ClusterStatus]) (metadataconstant.Version, error) {
		return metadataconstant.NotExists, errors.New(message)
	}
}
