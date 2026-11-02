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
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

// n0 and n1 hold two replicas each and n2 none: a replica moves to n2, from a
// follower rather than from the leader.
func TestShardGreedyFindBestPicksTheBestOp(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"
	addShard(s, "a", 2, "n0", "n1").Leader = "n0"

	c, found := replaceGreedy(s).FindBest(tier.NewLoad(s), allowAll)

	require.True(t, found)
	assert.Equal(t, model.Candidate{Kind: model.Replace, Namespace: "a", Shard: 1, From: "n1", To: "n2",
		Reason: "namespace-replicas"}, c)
}

// Once an op targets n2, the next one targets n3, on another shard.
func TestShardGreedyFindBestSkipsTheOpsThatTheLimitsBlock(t *testing.T) {
	s := cluster(4)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"
	addShard(s, "a", 2, "n0", "n1").Leader = "n0"
	g, load := replaceGreedy(s), tier.NewLoad(s)
	shards, targets := map[int64]bool{}, map[string]bool{}
	allows := func(op model.Candidate) bool { return !shards[op.Shard] && !targets[op.To] }

	first, found := g.FindBest(load, allows)
	require.True(t, found)
	assert.Equal(t, "n2", first.To)
	shards[first.Shard], targets[first.To] = true, true
	load.Move(s, first)

	second, found := g.FindBest(load, allows)
	require.True(t, found)
	assert.Equal(t, "n3", second.To)
	assert.NotEqual(t, first.Shard, second.Shard)
}

// n2 shares the zone of n1: it can only take the replica of n1, although n1
// leads, since the anti-affinity rejects it in place of n0.
func TestShardGreedyFindBestSkipsTheOpsThatAFilterRejects(t *testing.T) {
	s := cluster(3)
	for name, zone := range map[string]string{"n0": "z0", "n1": "z1", "n2": "z1"} {
		s.DataServers[name].Metadata = &proto.DataServerMetadata{Labels: map[string]string{"zone": zone}}
	}
	addShard(s, "a", 1, "n0", "n1").Leader = "n1"
	addShard(s, "a", 2, "n0", "n1").Leader = "n1"
	s.Namespaces["a"].AntiAffinities = []*proto.AntiAffinity{{Labels: []string{"zone"}}}

	c, found := replaceGreedy(s).FindBest(tier.NewLoad(s), allowAll)

	require.True(t, found)
	assert.Equal(t, "n1", c.From)
	assert.Equal(t, "n2", c.To)
}

func TestShardGreedyFindBestFindsNothingOnceBalanced(t *testing.T) {
	s := cluster(2)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"

	_, found := replaceGreedy(s).FindBest(tier.NewLoad(s), allowAll)

	assert.False(t, found)
}

// The search finds the op that the reference, which runs the filters before it
// scores an op, finds, on random clusters and limits.
func TestShardGreedyFindBestMatchesTheReference(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	for i := range 500 {
		s := randomCluster(random)
		load, dataServers := tier.NewLoad(s), names(s)
		planned := make(map[string]bool)
		for _, name := range dataServers {
			planned[name] = random.IntN(4) == 0
		}
		allows := func(op model.Candidate) bool { return !planned[op.To] }
		// Some searches only see some of the data servers
		if random.IntN(4) == 0 {
			dataServers = slices.DeleteFunc(dataServers, func(string) bool { return random.IntN(3) == 0 })
		}
		for _, kind := range []model.Kind{model.Replace, model.TransferLeader, model.Split} {
			want, wantFound := referenceFindBest(s, load, dataServers, kind, allows)
			got, found := NewShardGreedy(s, dataServers, kind, tiersOf(kind), filter.Defaults()).FindBest(load, allows)
			require.Equal(t, wantFound, found, "cluster %d, %s", i, kind)
			require.Equal(t, want, got, "cluster %d, %s", i, kind)
		}
	}
}

// Data servers join each zone: the replaces move replicas onto them.
func BenchmarkShardGreedyFindBest(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(size.name, func(b *testing.B) {
			s := zonedCluster(size.zones, size.perZone, size.used, size.namespaces, size.shards)
			g, load := replaceGreedy(s), tier.NewLoad(s)
			for b.Loop() {
				g.FindBest(load, allowAll)
			}
		})
	}
}

var benchSizes = []struct {
	name                                     string
	zones, perZone, used, namespaces, shards int
}{
	{"9-servers-99-shards", 3, 3, 2, 1, 99},
	{"51-servers-1000-shards", 3, 17, 12, 1, 1000},
	{"51-servers-10000-shards", 3, 17, 12, 1, 10000},
	{"51-servers-100000-shards", 3, 17, 12, 1, 100000},
	{"51-servers-10-namespaces-10000-shards", 3, 17, 12, 10, 10000},
	{"51-servers-100-namespaces-10000-shards", 3, 17, 12, 100, 10000},
}

// replaceGreedy returns the greedy search of the replaces on s, with the default
// filters and tiers.
func replaceGreedy(s *stats.ClusterStats) *ShardGreedy {
	return NewShardGreedy(s, names(s), model.Replace, tiersOf(model.Replace), filter.Defaults())
}

// tiersOf returns the default tiers that score the ops of kind.
func tiersOf(kind model.Kind) []tier.Tier {
	var tiers []tier.Tier
	for _, t := range tier.Defaults() {
		if t.GetKind() == kind {
			tiers = append(tiers, t)
		}
	}
	return tiers
}

// referenceFindBest is the expected answer of FindBest: the best op of kind by
// the definition, every op checked, then scored.
func referenceFindBest(s *stats.ClusterStats, load *tier.Load, dataServers []string, kind model.Kind,
	allows func(model.Candidate) bool) (model.Candidate, bool) {
	tiers, filters := tiersOf(kind), filter.Defaults()
	var bestOp model.Candidate
	var bestScore tier.Score
	bestLeader, found := 0, false
	forEachCandidate(s, dataServers, kind, func(op model.Candidate, shard *stats.ShardStats) {
		if !allows(op) || filter.Check(filters, s, op) != nil {
			return
		}
		sc := tier.Evaluate(tiers, s, load, op)
		if sc.Improved < 0 {
			return
		}
		leader := 0
		if op.From == shard.Leader {
			leader = 1
		}
		if !found || cmp.Or(sc.Compare(bestScore), cmp.Compare(leader, bestLeader)) < 0 {
			bestOp, bestScore, bestLeader, found = op, sc, leader, true
		}
	})
	if found {
		bestOp.Reason = tiers[bestScore.Improved].GetName()
	}
	return bestOp, found
}

// randomCluster returns up to 3 zones of up to 4 data servers, some removed or
// down, and up to 3 namespaces, some with an anti-affinity on the zone, of up
// to 20 shards on random members, some changing, splitting or over their split
// threshold.
func randomCluster(random *rand.Rand) *stats.ClusterStats {
	s := &stats.ClusterStats{DataServers: make(map[string]*stats.DataServerStats),
		Namespaces: make(map[string]*stats.NamespaceStats)}
	for zone := range 1 + random.IntN(3) {
		for i := range 1 + random.IntN(4) {
			name := fmt.Sprintf("z%d-%d", zone, i)
			s.DataServers[name] = &stats.DataServerStats{
				DataServer: &proto.DataServer{Identity: &proto.DataServerIdentity{Internal: name},
					Metadata: &proto.DataServerMetadata{Labels: map[string]string{"zone": fmt.Sprint(zone)}}},
				Removed: random.IntN(10) == 0, Down: random.IntN(10) == 0}
		}
	}
	dataServers := names(s)
	id := int64(0)
	for n := range 1 + random.IntN(3) {
		ns := &stats.NamespaceStats{Shards: make(map[int64]*stats.ShardStats)}
		if random.IntN(2) == 0 {
			ns.AntiAffinities = []*proto.AntiAffinity{{Labels: []string{"zone"}, Mode: "strict"}}
		}
		for range random.IntN(21) {
			members := slices.Clone(dataServers)
			random.Shuffle(len(members), func(a, b int) { members[a], members[b] = members[b], members[a] })
			ensemble := members[:1+random.IntN(min(3, len(members)))]
			shard := &stats.ShardStats{Ensemble: ensemble, Changing: random.IntN(10) == 0,
				Splitting: random.IntN(20) == 0, Splittable: random.IntN(2) == 0, Overshoot: 3 * random.Float64()}
			if random.IntN(5) > 0 {
				shard.Leader = ensemble[random.IntN(len(ensemble))]
			}
			ns.Shards[id] = shard
			id++
		}
		s.Namespaces[fmt.Sprintf("ns-%d", n)] = ns
	}
	return s
}
