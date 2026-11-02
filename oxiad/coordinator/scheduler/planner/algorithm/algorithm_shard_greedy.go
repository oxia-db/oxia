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
	"maps"
	"slices"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

// ShardGreedy plans the ops of a kind one at a time: each time, the single op
// that ranks first among those that the limits of the plan allow and the
// filters pass.
//
// A ShardGreedy keeps buffers between the searches of a plan: one goroutine
// uses it at a time.
type ShardGreedy struct {
	kind    model.Kind
	tiers   []tier.Tier
	filters []filter.Filter

	// shards caches the shards of shardsOf, in the order of their namespaces
	// and ids: a plan searches the same snapshot once per op.
	shards   []shardRef
	shardsOf *stats.ClusterStats
	// pairs holds the scores of the ops of a namespace by their source and
	// target, reused from one search to the next.
	pairs []tier.Score
}

type shardRef struct {
	namespace string
	id        int64
	shard     *stats.ShardStats
}

// NewShardGreedy returns the greedy search of the ops of kind, scored by tiers,
// the tiers of that kind in priority order, and passed by filters.
func NewShardGreedy(kind model.Kind, tiers []tier.Tier, filters []filter.Filter) *ShardGreedy {
	return &ShardGreedy{kind: kind, tiers: tiers, filters: filters}
}

// FindBest returns the op of the kind that allows lets through, under the
// limits of the plan, that the filters pass and that scores best against load,
// the load that the ops of the plan leave: by its score, then moving a
// follower before the leader, then the first in the order of the names, so
// that a plan is the same every time. dataServers holds the names of every
// data server of s, in order.
func (g *ShardGreedy) FindBest(s *stats.ClusterStats, load *tier.Load, dataServers []string,
	allows func(model.Candidate) bool) (model.Candidate, bool) {
	var best bestOp
	if g.kind == model.Split {
		var score tier.Score
		for _, ref := range g.sortedShards(s) {
			op := model.Candidate{Kind: g.kind, Namespace: ref.namespace, Shard: ref.id}
			tier.EvaluateInto(&score, g.tiers, s, load, op)
			best.offer(g.filters, allows, s, op, ref.shard, &score)
		}
	} else {
		g.findBestMove(s, load, dataServers, allows, &best)
	}
	if best.found {
		best.op.Reason = g.tiers[best.score.Improved].GetName()
	}
	return best.op, best.found
}

// findBestMove searches the replaces or the transfers. The tiers score them by
// their namespace and their data servers only, not their shard: it scores each
// pair of data servers of a namespace once, and visits only the targets that
// improve a tier.
func (g *ShardGreedy) findBestMove(s *stats.ClusterStats, load *tier.Load, dataServers []string,
	allows func(model.Candidate) bool, best *bestOp) {
	n := len(dataServers)
	index := make(map[string]int, n)
	for i, name := range dataServers {
		index[name] = i
	}
	if len(g.pairs) != n*n {
		g.pairs = make([]tier.Score, n*n)
	}
	namespace := ""
	for _, ref := range g.sortedShards(s) {
		if ref.namespace != namespace {
			namespace = ref.namespace
			g.scorePairs(s, load, dataServers, namespace)
		}
		op := model.Candidate{Kind: g.kind, Namespace: ref.namespace, Shard: ref.id}
		if g.kind == model.TransferLeader {
			g.offerTransfers(s, op, ref.shard, index, allows, best)
		} else {
			g.offerReplaces(s, op, ref.shard, index, dataServers, allows, best)
		}
	}
}

// offerTransfers offers best the transfers of shard, of op, to each other
// member, by the scores of their pairs.
func (g *ShardGreedy) offerTransfers(s *stats.ClusterStats, op model.Candidate, shard *stats.ShardStats,
	index map[string]int, allows func(model.Candidate) bool, best *bestOp) {
	if shard.Leader == "" {
		return
	}
	n := len(index)
	op.From = shard.Leader
	for _, to := range shard.Ensemble {
		score := &g.pairs[index[op.From]*n+index[to]]
		if to == shard.Leader || score.Improved < 0 {
			continue
		}
		op.To = to
		best.offer(g.filters, allows, s, op, shard, score)
	}
}

// offerReplaces offers best the replaces of each member of shard, of op, with
// each data server whose pair improves a tier.
func (g *ShardGreedy) offerReplaces(s *stats.ClusterStats, op model.Candidate, shard *stats.ShardStats,
	index map[string]int, dataServers []string, allows func(model.Candidate) bool, best *bestOp) {
	n := len(dataServers)
	for _, from := range shard.Ensemble {
		op.From = from
		row := g.pairs[index[from]*n : (index[from]+1)*n]
		for to := range row {
			if row[to].Improved < 0 {
				continue
			}
			op.To = dataServers[to]
			best.offer(g.filters, allows, s, op, shard, &row[to])
		}
	}
}

// scorePairs scores the op of the kind of namespace from each data server to
// each other one, into pairs.
func (g *ShardGreedy) scorePairs(s *stats.ClusterStats, load *tier.Load, dataServers []string, namespace string) {
	n := len(dataServers)
	op := model.Candidate{Kind: g.kind, Namespace: namespace}
	for from := range n {
		op.From = dataServers[from]
		for to := range n {
			op.To = dataServers[to]
			tier.EvaluateInto(&g.pairs[from*n+to], g.tiers, s, load, op)
		}
	}
}

// sortedShards returns the shards of s in the order of their namespaces and
// ids, computed once per snapshot.
func (g *ShardGreedy) sortedShards(s *stats.ClusterStats) []shardRef {
	if g.shardsOf == s {
		return g.shards
	}
	g.shards, g.shardsOf = g.shards[:0], s
	for _, name := range slices.Sorted(maps.Keys(s.Namespaces)) {
		ns := s.Namespaces[name]
		for _, id := range slices.Sorted(maps.Keys(ns.Shards)) {
			g.shards = append(g.shards, shardRef{namespace: name, id: id, shard: ns.Shards[id]})
		}
	}
	return g.shards
}

// bestOp is the op that ranks first among those offered so far.
type bestOp struct {
	op     model.Candidate
	score  tier.Score
	leader int
	found  bool
}

// offer makes op, of shard, the best op if it improves a tier, ranks first,
// and passes the limits that allows checks and filters. The limits and the
// filters cost the most: only an op that would rank first runs them, which
// finds the same op as checking every op first.
func (b *bestOp) offer(filters []filter.Filter, allows func(model.Candidate) bool, s *stats.ClusterStats,
	op model.Candidate, shard *stats.ShardStats, score *tier.Score) {
	if score.Improved < 0 {
		return
	}
	leader := 0
	if op.From == shard.Leader {
		leader = 1
	}
	if b.found && cmp.Or(score.Compare(b.score), cmp.Compare(leader, b.leader)) >= 0 {
		return
	}
	if !allows(op) || filter.Check(filters, s, op) != nil {
		return
	}
	b.op, b.leader, b.found = op, leader, true
	b.score.Improved = score.Improved
	b.score.Deltas = append(b.score.Deltas[:0], score.Deltas...)
}
