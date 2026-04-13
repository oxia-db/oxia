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

//revive:disable-next-line:var-naming
package util

import (
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

// loadAwareEnsembleSupplier picks the `nc.ReplicationFactor` servers that
// currently hold the fewest ensemble slots across all already-placed shards in
// `cs`. It is used to verify that ApplyClusterChanges publishes intermediate
// shard placements into status during the init loop, so that successive
// per-shard ensemble selections within the same init cycle observe each
// other's choices. Without that publication every call sees an empty cluster
// and returns the same servers, producing wildly uneven initial placement.
func loadAwareEnsembleSupplier(servers []model.Server) func(*model.NamespaceConfig, *model.ClusterStatus) ([]model.Server, error) {
	return func(nc *model.NamespaceConfig, cs *model.ClusterStatus) ([]model.Server, error) {
		load := make(map[string]int, len(servers))
		for _, s := range servers {
			load[s.Internal] = 0
		}
		for _, ns := range cs.Namespaces {
			for _, sh := range ns.Shards {
				for _, m := range sh.Ensemble {
					load[m.Internal]++
				}
			}
		}
		ranked := make([]model.Server, len(servers))
		copy(ranked, servers)
		sort.SliceStable(ranked, func(i, j int) bool {
			return load[ranked[i].Internal] < load[ranked[j].Internal]
		})
		return ranked[:nc.ReplicationFactor], nil
	}
}

// TestClientUpdates_InitialPlacementSeesPriorShards verifies that the
// per-shard ensembleSupplier called by ApplyClusterChanges during a new
// namespace's bootstrap can observe shards that were placed earlier in the
// same call. Regression for an upstream bug where the new NamespaceStatus was
// only assigned into status.Namespaces *after* the GenerateShards loop, so
// every shard was selected against an empty cluster snapshot, leading to
// pseudo-random initial placement that immediately tripped the count-based
// load-balancer once the cluster came up.
func TestClientUpdates_InitialPlacementSeesPriorShards(t *testing.T) {
	servers := []model.Server{s1, s2, s3, s4}
	const shardCount = 16
	const rf = 3

	status := model.NewClusterStatus()
	_, _ = ApplyClusterChanges(&model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "ns-1",
			InitialShardCount: shardCount,
			ReplicationFactor: rf,
		}},
		Servers: servers,
	}, status, loadAwareEnsembleSupplier(servers))

	slots := map[string]int{}
	for _, s := range servers {
		slots[s.Internal] = 0
	}
	for _, sh := range status.Namespaces["ns-1"].Shards {
		assert.Len(t, sh.Ensemble, rf)
		for _, m := range sh.Ensemble {
			slots[m.Internal]++
		}
	}

	// shardCount * rf == 48 ensemble slots distributed across 4 servers ⇒
	// exactly 12 slots per server when each ensembleSupplier call sees prior
	// placements.
	expected := shardCount * rf / len(servers)
	for _, s := range servers {
		assert.Equal(t, expected, slots[s.Internal],
			"server %s should hold exactly %d ensemble slots", s.Internal, expected)
	}
}

var (
	s1 = model.Server{Public: "s1:6648", Internal: "s1:6649"}
	s2 = model.Server{Public: "s2:6648", Internal: "s2:6649"}
	s3 = model.Server{Public: "s3:6648", Internal: "s3:6649"}
	s4 = model.Server{Public: "s4:6648", Internal: "s4:6649"}
)

func TestClientUpdates_ClusterInit(t *testing.T) {
	servers := []model.Server{s1, s2, s3, s4}
	status := model.NewClusterStatus()
	shardsAdded, shardsToRemove := ApplyClusterChanges(&model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}, {
			Name:              "ns-2",
			InitialShardCount: 2,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	}, status, func(namespaceConfig *model.NamespaceConfig, status *model.ClusterStatus) ([]model.Server, error) {
		return SimpleEnsembleSupplier(servers, namespaceConfig, status), nil
	})

	assert.Equal(t, &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s1, s2, s3},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32,
						},
					},
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					1: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s4, s1, s2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s3, s4, s1},
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}, status)

	assert.Equal(t, []int64{}, shardsToRemove)
	assert.Equal(t, map[int64]string{
		0: "ns-1",
		1: "ns-2",
		2: "ns-2"}, shardsAdded)
}

func TestClientUpdates_NamespaceAdded(t *testing.T) {
	servers := []model.Server{s1, s2, s3, s4}
	status := &model.ClusterStatus{Namespaces: map[string]model.NamespaceStatus{
		"ns-1": {
			ReplicationFactor: 3,
			Shards: map[int64]model.ShardMetadata{
				0: {
					Status:   model.ShardStatusUnknown,
					Term:     -1,
					Leader:   nil,
					Ensemble: []model.Server{s1, s2, s3},
					Int32HashRange: model.Int32HashRange{
						Min: 0,
						Max: math.MaxUint32,
					},
				},
			},
		},
	}, ShardIdGenerator: 1,
		ServerIdx: 3}
	shardsAdded, shardsToRemove := ApplyClusterChanges(&model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}, {
			Name:              "ns-2",
			InitialShardCount: 2,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	}, status, func(namespaceConfig *model.NamespaceConfig, status *model.ClusterStatus) ([]model.Server, error) {
		return SimpleEnsembleSupplier(servers, namespaceConfig, status), nil
	})

	assert.Equal(t, &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:                  model.ShardStatusUnknown,
						Term:                    -1,
						Leader:                  nil,
						Ensemble:                []model.Server{s1, s2, s3},
						RemovedNodes:            nil,
						PendingDeleteShardNodes: nil,
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32,
						},
					},
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					1: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s4, s1, s2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s3, s4, s1},
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}, status)

	assert.Equal(t, []int64{}, shardsToRemove)
	assert.Equal(t, map[int64]string{
		1: "ns-2",
		2: "ns-2"}, shardsAdded)
}

func TestClientUpdates_NamespaceRemoved(t *testing.T) {
	servers := []model.Server{s1, s2, s3, s4}
	status := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s1, s2, s3},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32,
						},
					},
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					1: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s4, s1, s2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:   model.ShardStatusUnknown,
						Term:     -1,
						Leader:   nil,
						Ensemble: []model.Server{s3, s4, s1},
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}
	shardsAdded, shardsToRemove := ApplyClusterChanges(&model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	}, status, func(namespaceConfig *model.NamespaceConfig, status *model.ClusterStatus) ([]model.Server, error) {
		return SimpleEnsembleSupplier(servers, namespaceConfig, status), nil
	})

	assert.Equal(t, &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:                  model.ShardStatusUnknown,
						Term:                    -1,
						Leader:                  nil,
						Ensemble:                []model.Server{s1, s2, s3},
						RemovedNodes:            nil,
						PendingDeleteShardNodes: nil,
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32,
						},
					},
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					1: {
						Status:                  model.ShardStatusDeleting,
						Term:                    -1,
						Leader:                  nil,
						Ensemble:                []model.Server{s4, s1, s2},
						RemovedNodes:            nil,
						PendingDeleteShardNodes: nil,
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: math.MaxUint32 / 2,
						},
					},
					2: {
						Status:                  model.ShardStatusDeleting,
						Term:                    -1,
						Leader:                  nil,
						Ensemble:                []model.Server{s3, s4, s1},
						RemovedNodes:            nil,
						PendingDeleteShardNodes: nil,
						Int32HashRange: model.Int32HashRange{
							Min: math.MaxUint32/2 + 1,
							Max: math.MaxUint32,
						},
					},
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}, status)

	sort.Slice(shardsToRemove, func(i, j int) bool { return shardsToRemove[i] < shardsToRemove[j] })
	assert.Equal(t, []int64{1, 2}, shardsToRemove)
	assert.Equal(t, map[int64]string{}, shardsAdded)
}
