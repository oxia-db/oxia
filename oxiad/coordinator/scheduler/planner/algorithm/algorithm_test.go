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

package algorithm

import (
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

func TestForEachCandidateYieldsEveryOpOfAKind(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"
	collect := func(kind model.Kind) []string {
		var ops []string
		forEachCandidate(s, []string{"n0", "n1", "n2"}, kind, func(op model.Candidate, _ *stats.ShardStats) {
			ops = append(ops, op.From+">"+op.To)
		})
		return ops
	}

	assert.Equal(t, []string{"n0>n0", "n0>n1", "n0>n2", "n1>n0", "n1>n1", "n1>n2"}, collect(model.Replace))
	assert.Equal(t, []string{"n0>n1"}, collect(model.TransferLeader))
	assert.Equal(t, []string{">"}, collect(model.Split))
}

// cluster returns a snapshot of count data servers, n0 to n<count-1>.
func cluster(count int) *stats.ClusterStats {
	s := &stats.ClusterStats{DataServers: make(map[string]*stats.DataServerStats),
		Namespaces: make(map[string]*stats.NamespaceStats)}
	for i := range count {
		name := fmt.Sprintf("n%d", i)
		s.DataServers[name] = &stats.DataServerStats{DataServer: &proto.DataServer{
			Identity: &proto.DataServerIdentity{Internal: name}}}
	}
	return s
}

func removed(s *stats.ClusterStats, name string) {
	s.DataServers[name] = &stats.DataServerStats{DataServer: &proto.DataServer{
		Identity: &proto.DataServerIdentity{Internal: name}}, Removed: true}
}

func addShard(s *stats.ClusterStats, namespace string, id int64, ensemble ...string) *stats.ShardStats {
	ns, ok := s.Namespaces[namespace]
	if !ok {
		ns = &stats.NamespaceStats{Shards: make(map[int64]*stats.ShardStats)}
		s.Namespaces[namespace] = ns
	}
	shard := &stats.ShardStats{Ensemble: ensemble}
	ns.Shards[id] = shard
	return shard
}

// names returns the names of the data servers of s, in order.
func names(s *stats.ClusterStats) []string {
	return slices.Sorted(maps.Keys(s.DataServers))
}

// allowAll lets every op through, as a plan without limits.
func allowAll(model.Candidate) bool {
	return true
}

// zonedCluster returns zones zones of perZone data servers each, z<zone>-<i>,
// and shards shards spread over namespaces namespaces, each with an
// anti-affinity on the zone: each shard has a replica in each zone, on one of
// its first used data servers, and the member of the first zone leads it. The
// other data servers are empty.
func zonedCluster(zones, perZone, used, namespaces, shards int) *stats.ClusterStats {
	s := &stats.ClusterStats{DataServers: make(map[string]*stats.DataServerStats),
		Namespaces: make(map[string]*stats.NamespaceStats)}
	name := func(zone, i int) string { return fmt.Sprintf("z%d-%02d", zone, i) }
	for zone := range zones {
		for i := range perZone {
			s.DataServers[name(zone, i)] = &stats.DataServerStats{DataServer: &proto.DataServer{
				Identity: &proto.DataServerIdentity{Internal: name(zone, i)},
				Metadata: &proto.DataServerMetadata{Labels: map[string]string{"zone": fmt.Sprint(zone)}}}}
		}
	}
	for n := range namespaces {
		s.Namespaces[fmt.Sprintf("ns-%03d", n)] = &stats.NamespaceStats{Shards: make(map[int64]*stats.ShardStats),
			AntiAffinities: []*proto.AntiAffinity{{Labels: []string{"zone"}, Mode: "strict"}}}
	}
	for id := range shards {
		ensemble := make([]string, 0, zones)
		for zone := range zones {
			ensemble = append(ensemble, name(zone, id%used))
		}
		ns := s.Namespaces[fmt.Sprintf("ns-%03d", id%namespaces)]
		ns.Shards[int64(id)] = &stats.ShardStats{Ensemble: ensemble, Leader: ensemble[0]}
	}
	return s
}
