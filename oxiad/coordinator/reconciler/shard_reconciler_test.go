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

package reconciler

import (
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/sharding"
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

func simpleEnsembleSupplier(candidates []*proto.DataServerIdentity, nc *proto.Namespace, cs *proto.ClusterStatus) []*proto.DataServerIdentity {
	n := len(candidates)
	res := make([]*proto.DataServerIdentity, nc.GetReplicationFactor())
	for i := uint32(0); i < nc.GetReplicationFactor(); i++ {
		res[i] = candidates[int(cs.ServerIdx+i)%n]
	}
	return res
}

func applyClusterChangesForTest(
	config *proto.ClusterConfiguration,
	status *proto.ClusterStatus,
	ensembleSupplier func(namespaceConfig *proto.Namespace, status *proto.ClusterStatus) ([]*proto.DataServerIdentity, error),
) (map[int64]string, []int64) {
	shardsToAdd := map[int64]string{}
	shardsToDelete := []int64{}

	namespaceConfigs := make(map[string]*proto.Namespace, len(config.GetNamespaces()))
	for _, namespaceConfig := range config.GetNamespaces() {
		namespaceConfigs[namespaceConfig.GetName()] = namespaceConfig
	}

	if status.Namespaces == nil {
		status.Namespaces = map[string]*proto.NamespaceStatus{}
	}

	for _, namespaceConfig := range config.GetNamespaces() {
		if _, exists := status.Namespaces[namespaceConfig.GetName()]; exists {
			continue
		}

		namespaceStatus := &proto.NamespaceStatus{
			Shards:            map[int64]*proto.ShardMetadata{},
			ReplicationFactor: namespaceConfig.GetReplicationFactor(),
		}
		status.Namespaces[namespaceConfig.GetName()] = namespaceStatus

		for _, shard := range sharding.GenerateShards(status.ShardIdGenerator, namespaceConfig.GetInitialShardCount()) {
			ensemble, err := ensembleSupplier(namespaceConfig, status)
			if err != nil {
				continue
			}

			namespaceStatus.Shards[shard.Id] = &proto.ShardMetadata{
				Status:   proto.ShardStatusUnknown,
				Term:     -1,
				Leader:   nil,
				Ensemble: ensemble,
				Int32HashRange: &proto.HashRange{
					Min: shard.Min,
					Max: shard.Max,
				},
			}
			status.ServerIdx = (status.ServerIdx + namespaceConfig.GetReplicationFactor()) % uint32(len(config.GetServers()))
			shardsToAdd[shard.Id] = namespaceConfig.GetName()
		}
		status.ShardIdGenerator += int64(namespaceConfig.GetInitialShardCount())
	}

	for namespace, namespaceStatus := range status.Namespaces {
		if _, exists := namespaceConfigs[namespace]; exists {
			continue
		}
		for shardID, shardMetadata := range namespaceStatus.Shards {
			shardMetadata.Status = proto.ShardStatusDeleting
			namespaceStatus.Shards[shardID] = shardMetadata
			shardsToDelete = append(shardsToDelete, shardID)
		}
	}

	return shardsToAdd, shardsToDelete
}

func assertStatusEqual(t *testing.T, expected, actual *proto.ClusterStatus) {
	t.Helper()
	assert.True(t, gproto.Equal(expected, actual), "expected status %v, got %v", expected, actual)
}

var (
	s1 = &proto.DataServerIdentity{Public: "s1:6648", Internal: "s1:6649"}
	s2 = &proto.DataServerIdentity{Public: "s2:6648", Internal: "s2:6649"}
	s3 = &proto.DataServerIdentity{Public: "s3:6648", Internal: "s3:6649"}
	s4 = &proto.DataServerIdentity{Public: "s4:6648", Internal: "s4:6649"}
)

func TestApplyClusterChangesInitialPlacementSeesPriorShards(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	const shardCount = 16
	const replicationFactor = 3

	status := proto.NewClusterStatus()
	_, _ = applyClusterChangesForTest(&proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: shardCount,
			ReplicationFactor: replicationFactor,
		}},
		Servers: servers,
	}, status, loadAwareEnsembleSupplier(servers))

	slots := map[string]int{}
	for _, s := range servers {
		slots[s.GetInternal()] = 0
	}
	for _, shard := range status.GetNamespaces()["ns-1"].GetShards() {
		assert.Len(t, shard.GetEnsemble(), replicationFactor)
		for _, member := range shard.GetEnsemble() {
			slots[member.GetInternal()]++
		}
	}

	expected := shardCount * replicationFactor / len(servers)
	for _, s := range servers {
		assert.Equal(t, expected, slots[s.GetInternal()],
			"server %s should hold exactly %d ensemble slots", s.GetInternal(), expected)
	}
}

func TestApplyClusterChangesClusterInit(t *testing.T) {
	servers := []*proto.DataServerIdentity{s1, s2, s3, s4}
	status := proto.NewClusterStatus()
	shardsAdded, shardsToRemove := applyClusterChangesForTest(&proto.ClusterConfiguration{
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
		return simpleEnsembleSupplier(servers, namespaceConfig, status), nil
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

func TestApplyClusterChangesNamespaceAdded(t *testing.T) {
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
	shardsAdded, shardsToRemove := applyClusterChangesForTest(&proto.ClusterConfiguration{
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
		return simpleEnsembleSupplier(servers, namespaceConfig, status), nil
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

func TestApplyClusterChangesNamespaceRemoved(t *testing.T) {
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
	shardsAdded, shardsToRemove := applyClusterChangesForTest(&proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "ns-1",
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	}, status, func(namespaceConfig *proto.Namespace, status *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
		return simpleEnsembleSupplier(servers, namespaceConfig, status), nil
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
