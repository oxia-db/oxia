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

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/internal/candidate"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

// leaderGraph holds the leader transfers of a plan that the filters allow, by
// their source, to pass a leader on through other data servers when no
// transfer evens out the leaders alone. E.g. with an anti-affinity on the
// zone, two data servers of a zone never share a shard: a leader moves from
// one to the other through a data server of another zone.
//
// The filters only read the snapshot, so the graph is built once per plan;
// the limits of the plan, which its ops change, are checked during each
// search.
type leaderGraph struct {
	out        map[string][]candidate.Candidate
	namespaces []string
	// dataServers holds the data servers that can lead, in the order of their
	// names.
	dataServers []string
}

func (p *planner) newLeaderGraph(pl *plan) *leaderGraph {
	g := &leaderGraph{
		out:        make(map[string][]candidate.Candidate),
		namespaces: slices.Sorted(maps.Keys(pl.clusterStats.Namespaces)),
		dataServers: slices.DeleteFunc(slices.Clone(pl.dataServerNames), func(name string) bool {
			return pl.clusterStats.DataServers[name].Removed
		}),
	}
	forEachCandidate(pl.clusterStats, pl.dataServerNames, candidate.TransferLeader,
		func(op candidate.Candidate, _ *stats.ShardStats) {
			// The load doesn't count the leaders of a removed data server
			if pl.clusterStats.DataServers[op.From].Removed || p.doFilter(pl.clusterStats, op) != nil {
				return
			}
			g.out[op.From] = append(g.out[op.From], op)
		})
	return g
}

// findPath returns the transfers of the shortest path that moves a leader from
// a data server to one with at least two fewer, through data servers whose
// counts it leaves as they are, or nil. It evens out the leaders of each
// namespace first, then of all of them, in the order of the tiers.
//
// Each transfer of a path, alone, leaves the leaders of its namespace as even
// as they are, and doesn't give its target more leaders than its source had:
// a path that only partly completes leaves the leaders no less even. Together
// they even out the leaders of the tier, and leave every namespace at least as
// even: a run of k transfers of one namespace moves one of its leaders from a
// data server to one that has k fewer.
func (g *leaderGraph) findPath(pl *plan) []candidate.Candidate {
	l := pl.dataServerLoad
	for _, namespace := range g.namespaces {
		counts := l.GetLeaders(namespace)
		path := g.search(pl, counts, func(op candidate.Candidate) bool {
			return op.Namespace == namespace && counts[op.From] == counts[op.To]+1
		})
		if path != nil {
			return withReason(path, tier.NamespaceLeaders().GetName())
		}
	}
	total := l.GetTotalLeaders()
	path := g.search(pl, total, func(op candidate.Candidate) bool {
		counts := l.GetLeaders(op.Namespace)
		return counts[op.From] == counts[op.To]+1 && total[op.From] <= total[op.To]+1
	})
	return withReason(path, tier.TotalLeaders().GetName())
}

// search returns the shortest path of transfers that allowed lets through,
// from the data servers with the most counts to one with at least two fewer,
// or from the next ones if none is reachable, or nil.
func (g *leaderGraph) search(pl *plan, counts map[string]int, allowed func(candidate.Candidate) bool) []candidate.Candidate {
	levels := make([]int, 0, len(g.dataServers))
	for _, name := range g.dataServers {
		levels = append(levels, counts[name])
	}
	slices.SortFunc(levels, func(a, b int) int { return cmp.Compare(b, a) })
	levels = slices.Compact(levels)
	for _, top := range levels {
		// No data server has two fewer than top: the counts are even
		if top-levels[len(levels)-1] < 2 {
			break
		}
		if path := g.bfs(pl, counts, top, allowed); path != nil {
			return path
		}
	}
	return nil
}

// bfs returns the shortest path of transfers that allowed lets through, from a
// data server with top counts to one with at most top-2, from the target
// back to the source, or nil.
func (g *leaderGraph) bfs(pl *plan, counts map[string]int, top int, allowed func(candidate.Candidate) bool) []candidate.Candidate {
	parents := make(map[string]candidate.Candidate)
	visited := make(map[string]bool)
	var queue []string
	for _, name := range g.dataServers {
		if counts[name] == top {
			visited[name] = true
			queue = append(queue, name)
		}
	}
	for len(queue) > 0 {
		from := queue[0]
		queue = queue[1:]
		for _, op := range g.out[from] {
			if visited[op.To] || !pl.allows(op) || !allowed(op) {
				continue
			}
			visited[op.To] = true
			parents[op.To] = op
			if counts[op.To] > top-2 {
				queue = append(queue, op.To)
				continue
			}
			var path []candidate.Candidate
			for op, ok := parents[op.To]; ok; op, ok = parents[op.From] {
				path = append(path, op)
			}
			return path
		}
	}
	return nil
}

func withReason(path []candidate.Candidate, reason string) []candidate.Candidate {
	for i := range path {
		path[i].Reason = reason
	}
	return path
}
