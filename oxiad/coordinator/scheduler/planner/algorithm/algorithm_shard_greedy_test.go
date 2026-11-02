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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

// n0 and n1 hold two replicas each and n2 none: a replica moves to n2, from a
// follower rather than from the leader.
func TestShardGreedyFindBestPicksTheBestOp(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"
	addShard(s, "a", 2, "n0", "n1").Leader = "n0"

	c, found := replaceGreedy().FindBest(s, tier.NewLoad(s), names(s), allowAll)

	require.True(t, found)
	assert.Equal(t, model.Candidate{Kind: model.Replace, Namespace: "a", Shard: 1, From: "n1", To: "n2",
		Reason: "namespace-replicas"}, c)
}

// Once an op targets n2, the next one targets n3, on another shard.
func TestShardGreedyFindBestSkipsTheOpsThatTheLimitsBlock(t *testing.T) {
	s := cluster(4)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"
	addShard(s, "a", 2, "n0", "n1").Leader = "n0"
	g, load := replaceGreedy(), tier.NewLoad(s)
	shards, targets := map[int64]bool{}, map[string]bool{}
	allows := func(op model.Candidate) bool { return !shards[op.Shard] && !targets[op.To] }

	first, found := g.FindBest(s, load, names(s), allows)
	require.True(t, found)
	assert.Equal(t, "n2", first.To)
	shards[first.Shard], targets[first.To] = true, true
	load.Move(s, first)

	second, found := g.FindBest(s, load, names(s), allows)
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

	c, found := replaceGreedy().FindBest(s, tier.NewLoad(s), names(s), allowAll)

	require.True(t, found)
	assert.Equal(t, "n1", c.From)
	assert.Equal(t, "n2", c.To)
}

func TestShardGreedyFindBestFindsNothingOnceBalanced(t *testing.T) {
	s := cluster(2)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"

	_, found := replaceGreedy().FindBest(s, tier.NewLoad(s), names(s), allowAll)

	assert.False(t, found)
}

// replaceGreedy returns the greedy search of the replaces, with the default
// filters and tiers.
func replaceGreedy() *ShardGreedy {
	var tiers []tier.Tier
	for _, t := range tier.Defaults() {
		if t.GetKind() == model.Replace {
			tiers = append(tiers, t)
		}
	}
	return NewShardGreedy(model.Replace, tiers, filter.Defaults())
}
