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

package coordinator

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/tests/mock"
)

// TestCoordinator_AutoSplit drives the full auto-split pipeline end to end:
// a cluster is configured with auto-split enabled and a low size threshold,
// enough data is written to exceed it, and the test asserts that the
// coordinator splits the shard on its own (no manual InitiateSplit call).
func TestCoordinator_AutoSplit(t *testing.T) {
	// Servers run under t.Context() and are torn down at test end; we only need
	// their addresses here.
	_, sa1 := newServer(t)
	_, sa2 := newServer(t)
	_, sa3 := newServer(t)

	metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              constant.DefaultNamespace,
		ReplicationFactor: 3,
		InitialShardCount: 1,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	// Enable auto-split with a low size threshold and a fast poll interval so
	// the test triggers quickly. Stabilization is 0 (split on the first
	// over-threshold sample). MaxShardsPerNamespace caps the namespace at two
	// shards, so the oversized shard splits exactly once and the guard-rail
	// then prevents the (still-large) children from cascading further — giving
	// a deterministic end state and exercising the guard-rail end to end.
	clusterConfig.AutoSplit = &proto.AutoSplitConfig{
		Enabled:               true,
		MaxShardSizeMb:        1,
		MaxShardsPerNamespace: 2,
		StabilizationPeriod:   "0s",
		CooldownPeriod:        "1s",
		CollectionInterval:    "200ms",
	}

	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   clusterConfig,
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)

	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))
	metadata := coordinatorInstance.Metadata()

	// Wait for the initial single shard to reach steady state.
	require.Eventually(t, func() bool {
		shard := mock.StatusSnapshot(t, metadata).Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.GetStatusOrDefault() == proto.ShardStatusSteadyState
	}, 30*time.Second, 100*time.Millisecond)

	client, err := oxia.NewSyncClient(sa1.Public)
	require.NoError(t, err)
	ctx := context.Background()

	// Write enough incompressible data to push the shard's on-disk size well
	// past the 1 MiB threshold. Random values defeat Pebble compression so the
	// size registers regardless of flush timing.
	rng := rand.New(rand.NewSource(1))
	const (
		numKeys   = 512
		valueSize = 8 * 1024 // 8 KiB each -> ~4 MiB total, 4x the threshold
	)
	writtenKeys := make(map[string][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := make([]byte, valueSize)
		rng.Read(value)
		_, _, err := client.Put(ctx, key, value)
		require.NoError(t, err)
		writtenKeys[key] = value
	}
	require.NoError(t, client.Close())

	// The coordinator's auto-split monitor should detect the oversized shard
	// and split it on its own. Wait until the original shard 0 is gone and the
	// shard set has settled (a large shard may split more than once): every
	// shard steady-state with a leader and no split metadata, and the set
	// unchanged across several consecutive polls.
	var settledIDs []int64
	stablePolls := 0
	require.Eventually(t, func() bool {
		ns, ok := mock.StatusSnapshot(t, metadata).Namespaces[constant.DefaultNamespace]
		if !ok {
			return false
		}
		if _, parentExists := ns.Shards[0]; parentExists {
			stablePolls = 0
			return false
		}
		ids := make([]int64, 0, len(ns.Shards))
		for id, meta := range ns.Shards {
			if meta.GetStatusOrDefault() != proto.ShardStatusSteadyState || meta.Leader == nil || meta.Split != nil {
				stablePolls = 0
				return false
			}
			ids = append(ids, id)
		}
		slices.Sort(ids)
		if slices.Equal(ids, settledIDs) {
			stablePolls++
		} else {
			settledIDs = ids
			stablePolls = 0
		}
		// Require the parent to have split into at least two shards and the set
		// to hold steady for ~1.5s so any cascade has finished.
		return len(settledIDs) >= 2 && stablePolls >= 3
	}, 3*time.Minute, 500*time.Millisecond)

	// The resulting shards must tile the entire original hash range
	// [0, MaxUint32] contiguously with no gaps or overlaps.
	status := mock.StatusSnapshot(t, metadata)
	ns := status.Namespaces[constant.DefaultNamespace]
	ranges := make([]*proto.HashRange, 0, len(settledIDs))
	for _, id := range settledIDs {
		ranges = append(ranges, ns.Shards[id].GetInt32HashRange())
	}
	slices.SortFunc(ranges, func(a, b *proto.HashRange) int {
		return cmp.Compare(a.GetMin(), b.GetMin())
	})
	assert.EqualValues(t, 0, ranges[0].GetMin(), "coverage must start at 0")
	assert.EqualValues(t, math.MaxUint32, ranges[len(ranges)-1].GetMax(), "coverage must end at MaxUint32")
	for i := 1; i < len(ranges); i++ {
		assert.EqualValues(t, ranges[i-1].GetMax()+1, ranges[i].GetMin(),
			"shard ranges must be contiguous between index %d and %d", i-1, i)
	}

	// All previously written data must still be readable through a fresh
	// client that picks up the new (child) shard assignments.
	var readClient oxia.SyncClient
	require.Eventually(t, func() bool {
		readClient, err = oxia.NewSyncClient(sa1.Public)
		if err != nil {
			return false
		}
		if _, _, _, err = readClient.Get(ctx, "key-0000"); err != nil {
			_ = readClient.Close()
			return false
		}
		return true
	}, 30*time.Second, 500*time.Millisecond)
	defer func() { assert.NoError(t, readClient.Close()) }()

	for key, expected := range writtenKeys {
		_, value, _, err := readClient.Get(ctx, key)
		if !assert.NoError(t, err, "get key %s", key) {
			continue
		}
		assert.Equal(t, expected, value, "value mismatch for key %s", key)
	}
}
