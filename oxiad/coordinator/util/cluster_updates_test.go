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
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
)

func hashRange(start, end uint32) *proto.HashRange {
	return &proto.HashRange{Min: start, Max: end}
}

func shardMetadata(status string, ensemble []*proto.DataServerIdentity, start, end uint32) *proto.ShardMetadata {
	return &proto.ShardMetadata{
		Status:         status,
		Term:           -1,
		Ensemble:       ensemble,
		Int32HashRange: hashRange(start, end),
	}
}

func loadAwareEnsembleSupplier(servers []*proto.DataServerIdentity) func(*proto.Namespace, *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
	return func(nc *proto.Namespace, cs *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
		load := make(map[string]int, len(servers))
		for _, s := range servers {
			load[s.GetInternal()] = 0
		}
		for _, ns := range cs.GetNamespaces() {
			for _, sh := range ns.GetShards() {
				for _, m := range sh.GetEnsemble() {
					load[m.GetInternal()]++
				}
			}
		}
		ranked := make([]*proto.DataServerIdentity, len(servers))
		copy(ranked, servers)
		sort.SliceStable(ranked, func(i, j int) bool {
			return load[ranked[i].GetInternal()] < load[ranked[j].GetInternal()]
		})
		return ranked[:nc.GetReplicationFactor()], nil
	}
}

func assertStatusEqual(t *testing.T, expected, actual *proto.ClusterStatus) {
	t.Helper()
	assert.True(t, gproto.Equal(expected, actual), "expected status %v, got %v", expected, actual)
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
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	const shardCount = 16
	const rf = 3

	status := proto.NewClusterStatus()
	_, _ = ApplyClusterChanges(&proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: shardCount,
			ReplicationFactor: rf,
		}},
		Servers: servers,
	}, status, loadAwareEnsembleSupplier(servers))

	slots := map[string]int{}
	for _, s := range servers {
		slots[s.GetInternal()] = 0
	}
	for _, sh := range status.GetNamespaces()["ns-1"].GetShards() {
		assert.Len(t, sh.GetEnsemble(), rf)
		for _, m := range sh.GetEnsemble() {
			slots[m.GetInternal()]++
		}
	}

	expected := shardCount * rf / len(servers)
	for _, s := range servers {
		assert.Equal(t, expected, slots[s.GetInternal()],
			"server %s should hold exactly %d ensemble slots", s.GetInternal(), expected)
	}
}

var (
	s1 = &proto.DataServerIdentity{Public: "s1:6648", Internal: "s1:6649"}
	s2 = &proto.DataServerIdentity{Public: "s2:6648", Internal: "s2:6649"}
	s3 = &proto.DataServerIdentity{Public: "s3:6648", Internal: "s3:6649"}
	s4 = &proto.DataServerIdentity{Public: "s4:6648", Internal: "s4:6649"}
)

func TestClientUpdates_ClusterInit(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	status := proto.NewClusterStatus()
	shardsAdded, shardsToRemove := ApplyClusterChanges(&proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}, {
			Name:              "ns-2",
			InitialShardCount: 2,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	}, status, func(namespaceConfig *proto.Namespace, status *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
		return SimpleEnsembleSupplier(servers, namespaceConfig, status), nil
	})

	assertStatusEqual(t, &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s1, s2, s3}, 0, math.MaxUint32),
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					1: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s4, s1, s2}, 0, math.MaxUint32/2),
					2: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s3, s4, s1}, math.MaxUint32/2+1, math.MaxUint32),
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
		2: "ns-2",
	}, shardsAdded)
}

func TestClientUpdates_NamespaceAdded(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s1, s2, s3}, 0, math.MaxUint32),
				},
			},
		},
		ShardIdGenerator: 1,
		ServerIdx:        3,
	}
	shardsAdded, shardsToRemove := ApplyClusterChanges(&proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}, {
			Name:              "ns-2",
			InitialShardCount: 2,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	}, status, func(namespaceConfig *proto.Namespace, status *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
		return SimpleEnsembleSupplier(servers, namespaceConfig, status), nil
	})

	assertStatusEqual(t, &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s1, s2, s3}, 0, math.MaxUint32),
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					1: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s4, s1, s2}, 0, math.MaxUint32/2),
					2: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s3, s4, s1}, math.MaxUint32/2+1, math.MaxUint32),
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}, status)

	assert.Equal(t, []int64{}, shardsToRemove)
	assert.Equal(t, map[int64]string{
		1: "ns-2",
		2: "ns-2",
	}, shardsAdded)
}

func TestClientUpdates_NamespaceRemoved(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s1, s2, s3}, 0, math.MaxUint32),
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					1: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s4, s1, s2}, 0, math.MaxUint32/2),
					2: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s3, s4, s1}, math.MaxUint32/2+1, math.MaxUint32),
				},
			},
		},
		ShardIdGenerator: 3,
		ServerIdx:        1,
	}
	shardsAdded, shardsToRemove := ApplyClusterChanges(&proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	}, status, func(namespaceConfig *proto.Namespace, status *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
		return SimpleEnsembleSupplier(servers, namespaceConfig, status), nil
	})

	assertStatusEqual(t, &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{
			"ns-1": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					0: shardMetadata(proto.ShardStatusUnknown, []*proto.DataServerIdentity{s1, s2, s3}, 0, math.MaxUint32),
				},
			},
			"ns-2": {
				ReplicationFactor: 3,
				Shards: map[int64]*proto.ShardMetadata{
					1: shardMetadata(proto.ShardStatusDeleting, []*proto.DataServerIdentity{s4, s1, s2}, 0, math.MaxUint32/2),
					2: shardMetadata(proto.ShardStatusDeleting, []*proto.DataServerIdentity{s3, s4, s1}, math.MaxUint32/2+1, math.MaxUint32),
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
