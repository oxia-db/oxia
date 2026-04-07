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

package resource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

func newTestStatusResource() (StatusResource, metadata.Provider) {
	provider := metadata.NewMetadataProviderMemory()
	return NewStatusResource(provider), provider
}

func initClusterStatus(t *testing.T, sr StatusResource) *Versioned[*model.ClusterStatus] {
	t.Helper()
	state := sr.Get()
	cs := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"default": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {Status: model.ShardStatusSteadyState, Term: 1},
					1: {Status: model.ShardStatusSteadyState, Term: 1},
				},
			},
		},
	}
	err := sr.Update(&Versioned[*model.ClusterStatus]{Data: cs, Version: state.Version})
	require.NoError(t, err)
	return sr.Get()
}

func TestStatusResource_Update_RefreshesOnProviderVersionMismatch(t *testing.T) {
	sr, provider := newTestStatusResource()
	state := initClusterStatus(t, sr)

	// Simulate external write (e.g., configmap annotation bump) by writing
	// directly to the provider, which advances its version without the
	// status resource knowing.
	externalCS := state.Data.Clone()
	_, err := provider.Store(externalCS, state.Version)
	require.NoError(t, err)

	// Now the status resource's cached version is stale.
	// An Update should fail with ErrMetadataBadVersion.
	cs := state.Data.Clone()
	err = sr.Update(&Versioned[*model.ClusterStatus]{Data: cs, Version: state.Version})
	assert.ErrorIs(t, err, metadata.ErrMetadataBadVersion)

	// After the failed write, storeOrRefresh should have refreshed the cache.
	// A subsequent read should return the updated version.
	refreshed := sr.Get()
	assert.NotEqual(t, state.Version, refreshed.Version)

	// A retry with the refreshed version should succeed.
	cs2 := refreshed.Data.Clone()
	err = sr.Update(&Versioned[*model.ClusterStatus]{Data: cs2, Version: refreshed.Version})
	assert.NoError(t, err)
}

func TestStatusResource_UpdateShard_RefreshesOnProviderVersionMismatch(t *testing.T) {
	sr, provider := newTestStatusResource()
	state := initClusterStatus(t, sr)

	// External write bumps provider version
	externalCS := state.Data.Clone()
	_, err := provider.Store(externalCS, state.Version)
	require.NoError(t, err)

	// UpdateShard with stale version should fail
	shardMeta := model.ShardMetadata{Status: model.ShardStatusElection, Term: 2}
	_, err = sr.UpdateShard("default", 0, &Versioned[*model.ShardMetadata]{Data: &shardMeta, Version: state.Version})
	assert.ErrorIs(t, err, metadata.ErrMetadataBadVersion)

	// Cache should be refreshed — retry succeeds
	refreshed := sr.Get()
	_, err = sr.UpdateShard("default", 0, &Versioned[*model.ShardMetadata]{Data: &shardMeta, Version: refreshed.Version})
	assert.NoError(t, err)

	// Verify the shard was updated
	v := sr.GetShard("default", 0)
	assert.True(t, v.IsSome())
	assert.Equal(t, model.ShardStatusElection, v.Unwrap().Data.Status)
	assert.EqualValues(t, 2, v.Unwrap().Data.Term)
}

func TestStatusResource_DeleteShard_RefreshesOnProviderVersionMismatch(t *testing.T) {
	sr, provider := newTestStatusResource()
	state := initClusterStatus(t, sr)

	// External write bumps provider version
	externalCS := state.Data.Clone()
	_, err := provider.Store(externalCS, state.Version)
	require.NoError(t, err)

	// DeleteShard with stale version should fail
	err = sr.DeleteShard("default", 0, state.Version)
	assert.ErrorIs(t, err, metadata.ErrMetadataBadVersion)

	// Cache should be refreshed — retry succeeds
	refreshed := sr.Get()
	err = sr.DeleteShard("default", 0, refreshed.Version)
	assert.NoError(t, err)

	// Verify the shard was deleted
	v := sr.GetShard("default", 0)
	assert.True(t, v.IsNone())
}

func TestStatusResource_Swap_DoesNotRefreshOnMismatch(t *testing.T) {
	sr, provider := newTestStatusResource()
	state := initClusterStatus(t, sr)

	// External write bumps provider version
	externalCS := state.Data.Clone()
	_, err := provider.Store(externalCS, state.Version)
	require.NoError(t, err)

	// Swap with stale version should fail
	cs := state.Data.Clone()
	err = sr.Swap(&Versioned[*model.ClusterStatus]{Data: cs, Version: state.Version})
	assert.ErrorIs(t, err, metadata.ErrMetadataBadVersion)

	// Swap does NOT refresh — the cached version should still be stale
	afterSwap := sr.Get()
	assert.Equal(t, state.Version, afterSwap.Version)
}

func TestStatusResource_MultipleExternalBumps_RecoverEachTime(t *testing.T) {
	sr, provider := newTestStatusResource()
	state := initClusterStatus(t, sr)

	for i := 0; i < 10; i++ {
		// External write bumps provider version
		current := sr.Get()
		externalCS := current.Data.Clone()
		_, err := provider.Store(externalCS, current.Version)
		require.NoError(t, err)

		// Update fails (stale)
		cs := current.Data.Clone()
		err = sr.Update(&Versioned[*model.ClusterStatus]{Data: cs, Version: current.Version})
		assert.ErrorIs(t, err, metadata.ErrMetadataBadVersion)

		// Retry succeeds (refreshed)
		refreshed := sr.Get()
		assert.NotEqual(t, current.Version, refreshed.Version)
		err = sr.Update(&Versioned[*model.ClusterStatus]{Data: refreshed.Data.Clone(), Version: refreshed.Version})
		assert.NoError(t, err, "round %d", i)
	}

	_ = state
}
