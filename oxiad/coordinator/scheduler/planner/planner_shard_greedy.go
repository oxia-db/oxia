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

package planner

import (
	"cmp"
	"maps"
	"slices"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

// shardGreedy plans the ops of a phase one at a time: each time, the single op
// that ranks first among those that the limits of the plan allow and the
// filters pass.
type shardGreedy struct {
	phase   phase
	filters []filter.Filter
}

func newShardGreedy(ph phase, filters []filter.Filter) *shardGreedy {
	return &shardGreedy{phase: ph, filters: filters}
}

// findBest returns the op of the phase that the limits of pl allow, that the
// filters pass and that ranks first: by its score, then moving a follower
// before the leader, then the first in the order of the names, so that a plan
// is the same every time.
func (g *shardGreedy) findBest(pl *plan) (model.Candidate, bool) {
	ph := g.phase
	var bestOp model.Candidate
	var bestScore tier.Score
	bestLeader, found := 0, false
	forEachCandidate(pl.clusterStats, pl.dataServerNames, ph.kind, func(op model.Candidate, shard *stats.ShardStats) {
		if !pl.allows(op) || filter.Check(g.filters, pl.clusterStats, op) != nil {
			return
		}
		sc := tier.Evaluate(ph.tiers, pl.clusterStats, pl.dataServerLoad, op)
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
		bestOp.Reason = ph.tiers[bestScore.Improved].GetName()
	}
	return bestOp, found
}

// forEachCandidate yields every op of kind with its shard: for a replace,
// every member of a shard replaced with every data server; for a transfer,
// the leadership of a shard moved to every other member; for a split, every
// shard.
func forEachCandidate(s *stats.ClusterStats, dataServers []string, kind model.Kind,
	yield func(model.Candidate, *stats.ShardStats)) {
	for _, name := range slices.Sorted(maps.Keys(s.Namespaces)) {
		ns := s.Namespaces[name]
		for _, id := range slices.Sorted(maps.Keys(ns.Shards)) {
			shard := ns.Shards[id]
			op := model.Candidate{Kind: kind, Namespace: name, Shard: id}
			switch kind {
			case model.Split:
				yield(op, shard)
			case model.TransferLeader:
				op.From = shard.Leader
				for _, to := range shard.Ensemble {
					if shard.Leader != "" && to != shard.Leader {
						op.To = to
						yield(op, shard)
					}
				}
			default:
				for _, from := range shard.Ensemble {
					for _, to := range dataServers {
						op.From, op.To = from, to
						yield(op, shard)
					}
				}
			}
		}
	}
}
