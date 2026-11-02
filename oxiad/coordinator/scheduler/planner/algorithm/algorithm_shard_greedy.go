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
	s           *stats.ClusterStats
	dataServers []string
	kind        model.Kind
	tiers       []tier.Tier
	filters     []filter.Filter

	// index holds the position of each data server in dataServers, and shards
	// the shards of s in the order of their namespaces and ids.
	index  map[string]int
	shards []shardRef
	// pairs holds the scores of the ops of a namespace by their source and
	// target, and scratch the score of an op outside dataServers, reused from
	// one search to the next.
	pairs   []tier.Score
	scratch tier.Score
}

type shardRef struct {
	namespace string
	id        int64
	shard     *stats.ShardStats
}

// NewShardGreedy returns the greedy search of the ops of kind on s, between
// dataServers, the names of the data servers in order, scored by tiers, the
// tiers of that kind in priority order, and passed by filters.
func NewShardGreedy(s *stats.ClusterStats, dataServers []string, kind model.Kind, tiers []tier.Tier,
	filters []filter.Filter) *ShardGreedy {
	g := &ShardGreedy{s: s, dataServers: dataServers, kind: kind, tiers: tiers, filters: filters,
		index: make(map[string]int, len(dataServers))}
	for i, name := range dataServers {
		g.index[name] = i
	}
	for _, name := range slices.Sorted(maps.Keys(s.Namespaces)) {
		ns := s.Namespaces[name]
		for _, id := range slices.Sorted(maps.Keys(ns.Shards)) {
			g.shards = append(g.shards, shardRef{namespace: name, id: id, shard: ns.Shards[id]})
		}
	}
	return g
}

// FindBest returns the op of the kind that allows lets through, under the
// limits of the plan, that the filters pass and that scores best against load,
// the load that the ops of the plan leave: by its score, then moving a
// follower before the leader, then the first in the order of the names, so
// that a plan is the same every time.
func (g *ShardGreedy) FindBest(load *tier.Load, allows func(model.Candidate) bool) (model.Candidate, bool) {
	var best bestOp
	if g.kind == model.Split {
		var score tier.Score
		for _, ref := range g.shards {
			op := model.Candidate{Kind: g.kind, Namespace: ref.namespace, Shard: ref.id}
			tier.EvaluateInto(&score, g.tiers, g.s, load, op)
			best.offer(g.filters, allows, g.s, op, ref.shard, &score)
		}
	} else {
		g.findBestMove(load, allows, &best)
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
func (g *ShardGreedy) findBestMove(load *tier.Load, allows func(model.Candidate) bool, best *bestOp) {
	n := len(g.dataServers)
	if len(g.pairs) != n*n {
		g.pairs = make([]tier.Score, n*n)
	}
	for i, ref := range g.shards {
		if i == 0 || ref.namespace != g.shards[i-1].namespace {
			g.scorePairs(load, ref.namespace)
		}
		op := model.Candidate{Kind: g.kind, Namespace: ref.namespace, Shard: ref.id}
		if g.kind == model.TransferLeader {
			g.offerTransfers(load, op, ref.shard, allows, best)
		} else {
			g.offerReplaces(load, op, ref.shard, allows, best)
		}
	}
}

// offerTransfers offers best the transfers of shard, of op, to each other
// member.
func (g *ShardGreedy) offerTransfers(load *tier.Load, op model.Candidate, shard *stats.ShardStats,
	allows func(model.Candidate) bool, best *bestOp) {
	if shard.Leader == "" {
		return
	}
	op.From = shard.Leader
	for _, to := range shard.Ensemble {
		if to == shard.Leader {
			continue
		}
		op.To = to
		best.offer(g.filters, allows, g.s, op, shard, g.scoreOf(load, op))
	}
}

// offerReplaces offers best the replaces of each member of shard, of op, with
// each data server whose pair improves a tier.
func (g *ShardGreedy) offerReplaces(load *tier.Load, op model.Candidate, shard *stats.ShardStats,
	allows func(model.Candidate) bool, best *bestOp) {
	n := len(g.dataServers)
	for _, from := range shard.Ensemble {
		op.From = from
		i, ok := g.index[from]
		if !ok {
			for _, to := range g.dataServers {
				op.To = to
				best.offer(g.filters, allows, g.s, op, shard, g.scoreOf(load, op))
			}
			continue
		}
		row := g.pairs[i*n : (i+1)*n]
		for to := range row {
			if row[to].Improved < 0 {
				continue
			}
			op.To = g.dataServers[to]
			best.offer(g.filters, allows, g.s, op, shard, &row[to])
		}
	}
}

// scoreOf returns the score of op: the score of its pair, or its own score if
// one of its data servers isn't in dataServers.
func (g *ShardGreedy) scoreOf(load *tier.Load, op model.Candidate) *tier.Score {
	from, okFrom := g.index[op.From]
	to, okTo := g.index[op.To]
	if okFrom && okTo {
		return &g.pairs[from*len(g.dataServers)+to]
	}
	tier.EvaluateInto(&g.scratch, g.tiers, g.s, load, op)
	return &g.scratch
}

// scorePairs scores the op of the kind of namespace from each data server to
// each other one, into pairs.
func (g *ShardGreedy) scorePairs(load *tier.Load, namespace string) {
	n := len(g.dataServers)
	op := model.Candidate{Kind: g.kind, Namespace: namespace}
	for from := range n {
		op.From = g.dataServers[from]
		for to := range n {
			op.To = g.dataServers[to]
			tier.EvaluateInto(&g.pairs[from*n+to], g.tiers, g.s, load, op)
		}
	}
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
