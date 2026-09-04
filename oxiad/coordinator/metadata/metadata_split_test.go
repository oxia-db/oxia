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
	"testing"

	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
)

const (
	splitTestNamespace = "default"
	splitTestParent    = int64(1)
	splitTestLeft      = int64(2)
	splitTestRight     = int64(3)
)

func TestUpdateSplitShardStatusMergesLatestMetadataAndClonesInputs(t *testing.T) {
	metadata := newSplitMetadataTestStore(t)
	latestParent := splitTestParentMetadata()
	latestParent.Term = 8
	latestParent.Leader = &commonproto.DataServerIdentity{Internal: "new-leader:8191"}
	metadata.UpdateShardStatus(splitTestNamespace, splitTestParent, latestParent)

	unrelated := &commonproto.ShardMetadata{Status: commonproto.ShardStatusSteadyState, Term: 12}
	metadata.UpdateShardStatus(splitTestNamespace, 9, unrelated)
	split, children := splitTestMetadata()
	expectedSplit := gproto.Clone(split).(*commonproto.SplitMetadata)
	expectedLeft := gproto.Clone(children[splitTestLeft]).(*commonproto.ShardMetadata)
	expectedRight := gproto.Clone(children[splitTestRight]).(*commonproto.ShardMetadata)

	require.NoError(t, metadata.UpdateSplitShardStatus(splitTestNamespace, splitTestParent, split, children))

	split.SplitPoint = 99
	children[splitTestLeft].Term = 99
	delete(children, splitTestRight)

	status := requireSplitNamespaceStatus(t, metadata)
	require.Len(t, status.Shards, 4)
	require.Equal(t, int64(8), status.Shards[splitTestParent].Term)
	require.Equal(t, "new-leader:8191", status.Shards[splitTestParent].GetLeader().GetInternal())
	require.True(t, gproto.Equal(expectedSplit, status.Shards[splitTestParent].Split))
	require.True(t, gproto.Equal(expectedLeft, status.Shards[splitTestLeft]))
	require.True(t, gproto.Equal(expectedRight, status.Shards[splitTestRight]))
	require.True(t, gproto.Equal(unrelated, status.Shards[9]))
}

func TestUpdateSplitShardStatusIsIdempotent(t *testing.T) {
	metadata := newSplitMetadataTestStore(t)
	split, children := splitTestMetadata()
	require.NoError(t, metadata.UpdateSplitShardStatus(splitTestNamespace, splitTestParent, split, children))
	first := requireSplitNamespaceStatus(t, metadata)

	require.NoError(t, metadata.UpdateSplitShardStatus(splitTestNamespace, splitTestParent, split, children))
	require.True(t, gproto.Equal(first, requireSplitNamespaceStatus(t, metadata)))
}

func TestUpdateSplitShardStatusRejectsConflicts(t *testing.T) {
	t.Run("active split", func(t *testing.T) {
		metadata := newSplitMetadataTestStore(t)
		split, children := splitTestMetadata()
		require.NoError(t, metadata.UpdateSplitShardStatus(splitTestNamespace, splitTestParent, split, children))
		split.SplitPoint++

		err := metadata.UpdateSplitShardStatus(splitTestNamespace, splitTestParent, split, children)
		require.ErrorContains(t, err, "already has an active split")
	})

	t.Run("existing child", func(t *testing.T) {
		metadata := newSplitMetadataTestStore(t)
		metadata.UpdateShardStatus(splitTestNamespace, splitTestLeft, &commonproto.ShardMetadata{Term: 1})
		split, children := splitTestMetadata()

		err := metadata.UpdateSplitShardStatus(splitTestNamespace, splitTestParent, split, children)
		require.ErrorContains(t, err, "child shard 2 already exists")
	})

	t.Run("changed persisted child", func(t *testing.T) {
		metadata := newSplitMetadataTestStore(t)
		split, children := splitTestMetadata()
		require.NoError(t, metadata.UpdateSplitShardStatus(splitTestNamespace, splitTestParent, split, children))
		changedChild := gproto.Clone(children[splitTestLeft]).(*commonproto.ShardMetadata)
		changedChild.Term++
		metadata.UpdateShardStatus(splitTestNamespace, splitTestLeft, changedChild)

		err := metadata.UpdateSplitShardStatus(splitTestNamespace, splitTestParent, split, children)
		require.ErrorContains(t, err, "does not match the split")
	})
}

func TestUpdateSplitShardStatusValidatesInput(t *testing.T) {
	metadata := newSplitMetadataTestStore(t)
	split, children := splitTestMetadata()

	tests := []struct {
		name     string
		split    *commonproto.SplitMetadata
		children map[int64]*commonproto.ShardMetadata
		error    string
	}{
		{name: "missing split", children: children, error: "split metadata is required"},
		{
			name:     "duplicate child IDs",
			split:    &commonproto.SplitMetadata{ChildShardIds: []int64{splitTestLeft, splitTestLeft}},
			children: children,
			error:    "requires two distinct children",
		},
		{
			name:     "missing child",
			split:    split,
			children: map[int64]*commonproto.ShardMetadata{splitTestLeft: children[splitTestLeft]},
			error:    "requires two distinct children",
		},
		{
			name:  "unexpected child",
			split: split,
			children: map[int64]*commonproto.ShardMetadata{
				splitTestLeft: children[splitTestLeft],
				4:             children[splitTestRight],
			},
			error: "shard 4 is not listed",
		},
		{
			name:  "missing child metadata",
			split: split,
			children: map[int64]*commonproto.ShardMetadata{
				splitTestLeft:  nil,
				splitTestRight: children[splitTestRight],
			},
			error: "metadata for child shard 2 is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := metadata.UpdateSplitShardStatus(splitTestNamespace, splitTestParent, test.split, test.children)
			require.ErrorContains(t, err, test.error)
		})
	}
}

func newSplitMetadataTestStore(t *testing.T) Metadata {
	t.Helper()

	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "test")
	_, err := statusProvider.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value: &commonproto.ClusterStatus{
			Namespaces: map[string]*commonproto.NamespaceStatus{
				splitTestNamespace: {
					Shards: map[int64]*commonproto.ShardMetadata{
						splitTestParent: splitTestParentMetadata(),
					},
				},
			},
		},
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)

	metadata := newMetadata(t.Context(), statusProvider, nil, "test")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })
	return metadata
}

func splitTestParentMetadata() *commonproto.ShardMetadata {
	return &commonproto.ShardMetadata{
		Status:         commonproto.ShardStatusSteadyState,
		Term:           7,
		Leader:         &commonproto.DataServerIdentity{Internal: "parent:8191"},
		Int32HashRange: &commonproto.HashRange{Min: 0, Max: 99},
	}
}

func splitTestMetadata() (*commonproto.SplitMetadata, map[int64]*commonproto.ShardMetadata) {
	split := &commonproto.SplitMetadata{
		Phase:         commonproto.SplitPhaseBootstrap,
		ChildShardIds: []int64{splitTestLeft, splitTestRight},
		SplitPoint:    49,
	}
	return split, map[int64]*commonproto.ShardMetadata{
		splitTestLeft: {
			Status:         commonproto.ShardStatusSteadyState,
			Int32HashRange: &commonproto.HashRange{Min: 0, Max: 49},
			Split:          &commonproto.SplitMetadata{ParentShardId: splitTestParent, SplitPoint: 49},
		},
		splitTestRight: {
			Status:         commonproto.ShardStatusSteadyState,
			Int32HashRange: &commonproto.HashRange{Min: 50, Max: 99},
			Split:          &commonproto.SplitMetadata{ParentShardId: splitTestParent, SplitPoint: 49},
		},
	}
}

func requireSplitNamespaceStatus(t *testing.T, metadata Metadata) *commonproto.NamespaceStatus {
	t.Helper()

	status, exists := metadata.GetNamespaceStatus(splitTestNamespace)
	require.True(t, exists)
	return gproto.Clone(status.UnsafeBorrow()).(*commonproto.NamespaceStatus)
}
