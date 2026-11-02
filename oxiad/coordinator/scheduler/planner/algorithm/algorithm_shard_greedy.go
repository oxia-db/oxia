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

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

// ShardGreedy plans the ops of a kind one at a time: each time, the single op
// that ranks first among those that the limits of the plan allow and the
// filters pass.
type ShardGreedy struct {
	kind    model.Kind
	tiers   []tier.Tier
	filters []filter.Filter
}

// NewShardGreedy returns the greedy search of the ops of kind, scored by tiers,
// the tiers of that kind in priority order, and passed by filters.
func NewShardGreedy(kind model.Kind, tiers []tier.Tier, filters []filter.Filter) *ShardGreedy {
	return &ShardGreedy{kind: kind, tiers: tiers, filters: filters}
}

// FindBest returns the op of the kind that allows lets through, under the
// limits of the plan, that the filters pass and that scores best against load,
// the load that the ops of the plan leave: by its score, then moving a follower
// before the leader, then the first in the order of the names, so that a plan
// is the same every time.
func (g *ShardGreedy) FindBest(s *stats.ClusterStats, load *tier.Load, dataServers []string,
	allows func(model.Candidate) bool) (model.Candidate, bool) {
	var bestOp model.Candidate
	var bestScore tier.Score
	bestLeader, found := 0, false
	forEachCandidate(s, dataServers, g.kind, func(op model.Candidate, shard *stats.ShardStats) {
		if !allows(op) || filter.Check(g.filters, s, op) != nil {
			return
		}
		sc := tier.Evaluate(g.tiers, s, load, op)
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
		bestOp.Reason = g.tiers[bestScore.Improved].GetName()
	}
	return bestOp, found
}
