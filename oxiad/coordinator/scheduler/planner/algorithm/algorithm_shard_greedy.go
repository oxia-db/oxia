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
// A ShardGreedy keeps the scores of its searches: one goroutine uses it at a
// time.
type ShardGreedy struct {
	s       *stats.ClusterStats
	kind    model.Kind
	tiers   []tier.Tier
	filters []filter.Filter

	// dataServers holds the names of the data servers of s in order, index
	// their positions, and shards the shards of s in the order of their
	// namespaces and ids.
	dataServers []string
	index       map[string]int
	shards      []shardRef
	// deltas and improved hold the score of the op of the namespace being
	// searched from each data server to each other one, scored the first time
	// that the search reads it, or reads its row: stamps and rowStamps hold the
	// search of each, and stamp the search of the namespace.
	deltas    []int
	improved  []int
	stamps    []uint64
	rowStamps []uint64
	stamp     uint64
}

type shardRef struct {
	namespace string
	id        int64
	shard     *stats.ShardStats
}

// NewShardGreedy returns the greedy search of the ops of kind on s, scored by
// tiers, the tiers of that kind in priority order, and passed by filters.
func NewShardGreedy(s *stats.ClusterStats, kind model.Kind, tiers []tier.Tier, filters []filter.Filter) *ShardGreedy {
	g := &ShardGreedy{s: s, kind: kind, tiers: tiers, filters: filters,
		dataServers: slices.Sorted(maps.Keys(s.DataServers))}
	g.index = make(map[string]int, len(g.dataServers))
	for i, name := range g.dataServers {
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
			g.offer(&best, allows, op, ref.shard, score)
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
// pair of data servers of a namespace once, the first time that it reads it.
func (g *ShardGreedy) findBestMove(load *tier.Load, allows func(model.Candidate) bool, best *bestOp) {
	n, t := len(g.dataServers), len(g.tiers)
	if len(g.stamps) != n*n {
		g.deltas, g.improved = make([]int, n*n*t), make([]int, n*n)
		g.stamps, g.rowStamps = make([]uint64, n*n), make([]uint64, n)
	}
	for i, ref := range g.shards {
		if i == 0 || ref.namespace != g.shards[i-1].namespace {
			g.stamp++
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
// member. A leader that isn't a data server of s transfers nothing: the tiers
// can't score it.
func (g *ShardGreedy) offerTransfers(load *tier.Load, op model.Candidate, shard *stats.ShardStats,
	allows func(model.Candidate) bool, best *bestOp) {
	from, ok := g.index[shard.Leader]
	if !ok {
		return
	}
	op.From = shard.Leader
	for _, to := range shard.Ensemble {
		if to == shard.Leader {
			continue
		}
		op.To = to
		g.offer(best, allows, op, shard, g.scoreOf(load, op, from, g.index[to]))
	}
}

// offerReplaces offers best the replaces of each member of shard, of op, with
// each data server whose pair improves a tier.
func (g *ShardGreedy) offerReplaces(load *tier.Load, op model.Candidate, shard *stats.ShardStats,
	allows func(model.Candidate) bool, best *bestOp) {
	n, t := len(g.dataServers), len(g.tiers)
	for _, member := range shard.Ensemble {
		op.From = member
		from := g.index[member]
		g.scoreRow(load, op, from)
		for to := range n {
			pair := from*n + to
			if g.improved[pair] < 0 {
				continue
			}
			op.To = g.dataServers[to]
			g.offer(best, allows, op, shard, tier.Score{Deltas: g.deltas[pair*t : (pair+1)*t], Improved: g.improved[pair]})
		}
	}
}

// scoreRow scores op from the data server at from to each data server, the
// first time that the search of the namespace reads them.
func (g *ShardGreedy) scoreRow(load *tier.Load, op model.Candidate, from int) {
	if g.rowStamps[from] == g.stamp {
		return
	}
	g.rowStamps[from] = g.stamp
	for to, name := range g.dataServers {
		op.To = name
		g.scoreOf(load, op, from, to)
	}
}

// scoreOf returns the score of op, from the data server at from to the one at
// to, scored the first time that the search of the namespace reads it.
func (g *ShardGreedy) scoreOf(load *tier.Load, op model.Candidate, from int, to int) tier.Score {
	pair, t := from*len(g.dataServers)+to, len(g.tiers)
	deltas := g.deltas[pair*t : (pair+1)*t : (pair+1)*t]
	if g.stamps[pair] != g.stamp {
		score := tier.Score{Deltas: deltas}
		tier.EvaluateInto(&score, g.tiers, g.s, load, op)
		g.improved[pair], g.stamps[pair] = score.Improved, g.stamp
	}
	return tier.Score{Deltas: deltas, Improved: g.improved[pair]}
}

// bestOp is the op that ranks first among those offered so far.
type bestOp struct {
	op     model.Candidate
	score  tier.Score
	leader int
	found  bool
}

// offer makes op, of shard, the best op if it improves a tier, ranks first,
// and passes the limits that allows checks and the filters. The limits and the
// filters cost the most: only an op that would rank first runs them, which
// finds the same op as checking every op first.
func (g *ShardGreedy) offer(best *bestOp, allows func(model.Candidate) bool, op model.Candidate,
	shard *stats.ShardStats, score tier.Score) {
	if score.Improved < 0 {
		return
	}
	leader := 0
	if op.From == shard.Leader {
		leader = 1
	}
	if best.found && cmp.Or(score.Compare(best.score), cmp.Compare(leader, best.leader)) >= 0 {
		return
	}
	if !allows(op) || filter.Check(g.filters, g.s, op) != nil {
		return
	}
	best.op, best.leader, best.found = op, leader, true
	best.score.Improved = score.Improved
	best.score.Deltas = append(best.score.Deltas[:0], score.Deltas...)
}
