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

func TestMetadataStatusUpdatesFailWhenTargetIsGone(t *testing.T) {
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	require.True(t, metadata.CreateNamespaceStatus("default", &commonproto.NamespaceStatus{
		Shards: map[int64]*commonproto.ShardMetadata{0: {Term: 1}},
	}))

	require.ErrorIs(t, metadata.UpdateNamespaceStatus("other", &commonproto.NamespaceStatus{}), metadataconstant.ErrNotFound)
	require.ErrorIs(t, metadata.UpdateShardStatus("other", 0, &commonproto.ShardMetadata{Term: 2}), metadataconstant.ErrNotFound)

	// A deleted shard must not be re-created
	require.ErrorIs(t, metadata.UpdateShardStatus("default", 1, &commonproto.ShardMetadata{Term: 2}), metadataconstant.ErrNotFound)
	_, exists := metadata.GetShardStatus("default", 1)
	require.False(t, exists)

	require.NoError(t, metadata.UpdateShardStatus("default", 0, &commonproto.ShardMetadata{Term: 2}))
	shard, exists := metadata.GetShardStatus("default", 0)
	require.True(t, exists)
	require.EqualValues(t, 2, shard.UnsafeBorrow().GetTerm())
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

func newSplitTestMetadata(t *testing.T, parent *commonproto.ShardMetadata) Metadata {
	t.Helper()

	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadataconstant.WatchDisabled, "")
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadataconstant.WatchEnabled, "")
	metadata := newMetadata(t.Context(), statusProvider, configProvider, "")
	require.True(t, metadata.CreateNamespaceStatus("default", &commonproto.NamespaceStatus{
		Shards: map[int64]*commonproto.ShardMetadata{0: parent},
	}))
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })
	return metadata
}

func splittableShard() *commonproto.ShardMetadata {
	return &commonproto.ShardMetadata{
		Status:         commonproto.ShardStatusSteadyState,
		Term:           3,
		Ensemble:       []*commonproto.DataServerIdentity{{Internal: "s1:8191"}},
		Int32HashRange: &commonproto.HashRange{Min: 0, Max: 100},
	}
}

func splitEnsembles() ([]*commonproto.DataServerIdentity, []*commonproto.DataServerIdentity) {
	return []*commonproto.DataServerIdentity{{Internal: "s1:8191"}},
		[]*commonproto.DataServerIdentity{{Internal: "s2:8191"}}
}

func TestMetadataInitShardSplitCreatesParentAndChildrenTogether(t *testing.T) {
	metadata := newSplitTestMetadata(t, splittableShard())
	left, right := splitEnsembles()

	require.NoError(t, metadata.InitShardSplit("default", 0, 1, 2, 40, left, right))

	parent, exists := metadata.GetShardStatus("default", 0)
	require.True(t, exists)
	split := parent.UnsafeBorrow().GetSplit()
	require.Equal(t, commonproto.SplitPhaseBootstrap, split.GetPhaseOrDefault())
	require.Equal(t, []int64{1, 2}, split.GetChildShardIds())
	require.EqualValues(t, 40, split.GetSplitPoint())
	// The parent keeps serving its whole range until the split completes
	require.EqualValues(t, 3, parent.UnsafeBorrow().GetTerm())
	require.EqualValues(t, 0, parent.UnsafeBorrow().GetInt32HashRange().GetMin())
	require.EqualValues(t, 100, parent.UnsafeBorrow().GetInt32HashRange().GetMax())

	// The children partition the parent's range at the split point
	leftChild, exists := metadata.GetShardStatus("default", 1)
	require.True(t, exists)
	require.EqualValues(t, 0, leftChild.UnsafeBorrow().GetInt32HashRange().GetMin())
	require.EqualValues(t, 40, leftChild.UnsafeBorrow().GetInt32HashRange().GetMax())

	rightChild, exists := metadata.GetShardStatus("default", 2)
	require.True(t, exists)
	require.EqualValues(t, 41, rightChild.UnsafeBorrow().GetInt32HashRange().GetMin())
	require.EqualValues(t, 100, rightChild.UnsafeBorrow().GetInt32HashRange().GetMax())

	// Each child starts at term 0, on its own ensemble, and points back at the parent
	for shard, internal := range map[int64]string{1: "s1:8191", 2: "s2:8191"} {
		child, exists := metadata.GetShardStatus("default", shard)
		require.True(t, exists)
		metadata := child.UnsafeBorrow()
		require.EqualValues(t, 0, metadata.GetTerm())
		require.Equal(t, commonproto.ShardStatusSteadyState, metadata.GetStatusOrDefault())
		require.Len(t, metadata.GetEnsemble(), 1)
		require.Equal(t, internal, metadata.GetEnsemble()[0].GetInternal())
		require.EqualValues(t, 0, metadata.GetSplit().GetParentShardId())
		require.EqualValues(t, 40, metadata.GetSplit().GetSplitPoint())
		require.Empty(t, metadata.GetSplit().GetChildShardIds())
	}
}

func TestMetadataInitShardSplitRejectsUnsplittableParent(t *testing.T) {
	left, right := splitEnsembles()

	for name, testCase := range map[string]struct {
		parent      *commonproto.ShardMetadata
		splitPoint  uint32
		leftChild   int64
		rightChild  int64
		leftEnsemb  []*commonproto.DataServerIdentity
		rightEnsemb []*commonproto.DataServerIdentity
	}{
		"not in steady state": {parent: func() *commonproto.ShardMetadata {
			shard := splittableShard()
			shard.Status = commonproto.ShardStatusDeleting
			return shard
		}(), splitPoint: 40, leftChild: 1, rightChild: 2, leftEnsemb: left, rightEnsemb: right},
		"split already active": {parent: func() *commonproto.ShardMetadata {
			shard := splittableShard()
			shard.Split = &commonproto.SplitMetadata{ChildShardIds: []int64{7, 8}}
			return shard
		}(), splitPoint: 40, leftChild: 1, rightChild: 2, leftEnsemb: left, rightEnsemb: right},
		"pending ensemble changes": {parent: func() *commonproto.ShardMetadata {
			shard := splittableShard()
			shard.PendingDeleteShardNodes = []*commonproto.DataServerIdentity{{Internal: "s9:8191"}}
			return shard
		}(), splitPoint: 40, leftChild: 1, rightChild: 2, leftEnsemb: left, rightEnsemb: right},
		"hash range too small": {parent: func() *commonproto.ShardMetadata {
			shard := splittableShard()
			shard.Int32HashRange = &commonproto.HashRange{Min: 7, Max: 7}
			return shard
		}(), splitPoint: 7, leftChild: 1, rightChild: 2, leftEnsemb: left, rightEnsemb: right},
		"split point below the range": {parent: func() *commonproto.ShardMetadata {
			shard := splittableShard()
			shard.Int32HashRange = &commonproto.HashRange{Min: 10, Max: 100}
			return shard
		}(), splitPoint: 5, leftChild: 1, rightChild: 2, leftEnsemb: left, rightEnsemb: right},
		"split point at the range end": {parent: splittableShard(), splitPoint: 100, leftChild: 1, rightChild: 2, leftEnsemb: left, rightEnsemb: right},
		"child id already in use":      {parent: splittableShard(), splitPoint: 40, leftChild: 0, rightChild: 2, leftEnsemb: left, rightEnsemb: right},
		"children share an id":         {parent: splittableShard(), splitPoint: 40, leftChild: 1, rightChild: 1, leftEnsemb: left, rightEnsemb: right},
		"child without an ensemble":    {parent: splittableShard(), splitPoint: 40, leftChild: 1, rightChild: 2, leftEnsemb: left, rightEnsemb: nil},
	} {
		t.Run(name, func(t *testing.T) {
			metadata := newSplitTestMetadata(t, testCase.parent)

			err := metadata.InitShardSplit("default", 0, testCase.leftChild, testCase.rightChild,
				testCase.splitPoint, testCase.leftEnsemb, testCase.rightEnsemb)
			require.ErrorIs(t, err, metadataconstant.ErrFailedPrecondition)

			// Nothing was persisted: no child shards, and the parent is untouched
			for _, shard := range []int64{1, 2} {
				_, exists := metadata.GetShardStatus("default", shard)
				require.False(t, exists)
			}
			parent, exists := metadata.GetShardStatus("default", 0)
			require.True(t, exists)
			require.Equal(t, testCase.parent.GetSplit(), parent.UnsafeBorrow().GetSplit())
		})
	}
}

func TestMetadataInitShardSplitFailsWhenTargetIsGone(t *testing.T) {
	metadata := newSplitTestMetadata(t, splittableShard())
	left, right := splitEnsembles()

	require.ErrorIs(t, metadata.InitShardSplit("other", 0, 1, 2, 40, left, right), metadataconstant.ErrNotFound)
	require.ErrorIs(t, metadata.InitShardSplit("default", 7, 1, 2, 40, left, right), metadataconstant.ErrNotFound)
}

func TestMetadataInitShardSplitKeepsCallerEnsemblesIsolated(t *testing.T) {
	metadata := newSplitTestMetadata(t, splittableShard())
	left, right := splitEnsembles()

	require.NoError(t, metadata.InitShardSplit("default", 0, 1, 2, 40, left, right))

	// A caller that reuses its ensemble slices must not reach into the status
	left[0].Internal = "mutated:8191"

	child, exists := metadata.GetShardStatus("default", 1)
	require.True(t, exists)
	require.Equal(t, "s1:8191", child.UnsafeBorrow().GetEnsemble()[0].GetInternal())
}
