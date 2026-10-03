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

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
)

type refreshedConflictProvider struct {
	provider.Provider[*commonproto.ClusterStatus]
	writes int
}

func (p *refreshedConflictProvider) Store(snapshot provider.Versioned[*commonproto.ClusterStatus]) (metadatacommon.Version, error) {
	p.writes++
	if p.writes == 1 {
		// Another write committed after metadata read the old snapshot. The
		// provider refreshes its watch before reporting the version conflict.
		snapshot.Value = &commonproto.ClusterStatus{ShardIdGenerator: 10}
		if _, err := p.Provider.Store(snapshot); err != nil {
			return metadatacommon.NotExists, err
		}
		return metadatacommon.NotExists, metadatacommon.ErrBadVersion
	}
	return p.Provider.Store(snapshot)
}

func TestReserveShardIDsRetriesRefreshedVersionConflict(t *testing.T) {
	status := &refreshedConflictProvider{Provider: memory.NewProvider(
		metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "review")}
	config := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchDisabled, "review")
	metadata := newMetadata(t.Context(), status, config, "review")
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })
	base, err := metadata.ReserveShardIDs(2)
	require.NoError(t, err)
	require.Equal(t, int64(10), base)
	require.Equal(t, int64(12), status.Watch().Load().Value.ShardIdGenerator)
	require.Equal(t, 2, status.writes)
}
