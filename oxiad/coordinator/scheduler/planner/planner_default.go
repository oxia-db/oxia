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
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

var _ Planner = &planner{}

type planner struct {
	logger  *slog.Logger
	filters []filter.Filter
	// phases plan the ops of each kind, in the order of their first tier.
	phases []phase
}

// phase plans the ops of one kind, scored by the tiers of that kind in
// priority order.
type phase struct {
	kind  model.Kind
	tiers []tier.Tier
}

func New(filters []filter.Filter, tiers []tier.Tier) Planner {
	p := &planner{logger: slog.With(slog.String("component", "planner")), filters: filters}
	for _, t := range tiers {
		i := slices.IndexFunc(p.phases, func(ph phase) bool { return ph.kind == t.GetKind() })
		if i < 0 {
			p.phases = append(p.phases, phase{kind: t.GetKind()})
			i = len(p.phases) - 1
		}
		p.phases[i].tiers = append(p.phases[i].tiers, t)
	}
	return p
}

func (p *planner) Plan(s *stats.ClusterStats) ([]action.Action, []model.Blocked) {
	pl := newPlan(s)
	var actions []action.Action
	add := func(c model.Candidate) {
		pl.apply(c)
		p.logger.Info("Planned an op", slog.String("kind", c.Kind.String()), slog.String("namespace", c.Namespace),
			slog.Int64("shard", c.Shard), slog.String("from", c.From), slog.String("to", c.To),
			slog.String("reason", c.Reason))
		switch c.Kind {
		case model.TransferLeader:
			actions = append(actions, action.NewLeaderTransferAction(c.Shard, c.To))
		case model.Split:
			actions = append(actions, action.NewSplitAction(c.Namespace, c.Shard))
		default:
			actions = append(actions, action.NewChangeEnsembleAction(c.Shard, s.DataServers[c.From].GetIdentity(),
				s.DataServers[c.To].GetIdentity()))
		}
	}
	for i, ph := range p.phases {
		planned := p.planPhase(pl, ph, add)
		if planned > 0 || i == len(p.phases)-1 {
			break
		}
		// The next phase plans only once the ops of this one no longer change
		// the cluster: none to plan, and no change of an ensemble pending for a
		// replace. A leader is one of the replicas, and every replace runs an
		// election that may move it.
		if ph.kind == model.Replace && pl.ensembleChanging {
			break
		}
	}
	return actions, p.findBlocked(s, pl.dataServerNames)
}

// planPhase plans the ops of ph with add, and returns how many: the best op,
// one after the other, applying each to the load, until no op is left within
// the limits.
func (p *planner) planPhase(pl *plan, ph phase, add func(model.Candidate)) int {
	planned := 0
	var leaders *leaderGraph
	for {
		if c, found := p.findBest(pl, ph); found {
			add(c)
			planned++
			continue
		}
		// No transfer evens out the leaders alone: a path may still pass one
		// on through other data servers
		if ph.kind != model.TransferLeader {
			return planned
		}
		if leaders == nil {
			leaders = p.newLeaderGraph(pl)
		}
		path := leaders.findPath(pl)
		if path == nil {
			return planned
		}
		for _, c := range path {
			add(c)
		}
		planned += len(path)
	}
}

func (*planner) Place(s *stats.ClusterStats, namespace string, replicas int, antiAffinities []*proto.AntiAffinity,
	eligible func(*stats.DataServerStats) bool) ([]string, error) {
	l := tier.NewLoad(s)
	counts := l.GetReplicas(namespace)
	candidates := slices.DeleteFunc(slices.Collect(maps.Keys(s.DataServers)), func(name string) bool {
		dataServer := s.DataServers[name]
		return dataServer.Removed || (eligible != nil && !eligible(dataServer))
	})
	slices.SortFunc(candidates, func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[a], counts[b]), cmp.Compare(l.GetTotalReplicas()[a], l.GetTotalReplicas()[b]), strings.Compare(a, b))
	})
	ensemble := make([]string, 0, replicas)
	for _, name := range candidates {
		if len(ensemble) == replicas {
			break
		}
		if !filter.Conflicts(s, antiAffinities, ensemble, "", name) {
			ensemble = append(ensemble, name)
		}
	}
	if len(ensemble) < replicas {
		return nil, fmt.Errorf("%w: only %d of %d data servers keep the anti-affinities",
			errs.ErrNotFound, len(ensemble), replicas)
	}
	return ensemble, nil
}

// doFilter returns the error of the first filter that rejects op.
func (p *planner) doFilter(s *stats.ClusterStats, op model.Candidate) error {
	for _, f := range p.filters {
		if err := f.Filter(s, op); err != nil {
			return fmt.Errorf("%s: %w", f.GetName(), err)
		}
	}
	return nil
}

// findBest returns the op of ph that the limits of pl allow, that the filters
// pass and that ranks first: by its score, then moving a follower before the leader,
// then the first in the order of the names, so that a plan is the same every
// time.
func (p *planner) findBest(pl *plan, ph phase) (model.Candidate, bool) {
	var bestOp model.Candidate
	var bestScore tier.Score
	bestLeader, found := 0, false
	forEachCandidate(pl.clusterStats, pl.dataServerNames, ph.kind, func(op model.Candidate, shard *stats.ShardStats) {
		if !pl.allows(op) || p.doFilter(pl.clusterStats, op) != nil {
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

// findBlocked returns the replicas on removed data servers that no op can move,
// whatever the budget. A drain always improves a tier, so the filters decide:
// the filter that rejects the first data server that could otherwise take the
// replica says why.
func (p *planner) findBlocked(s *stats.ClusterStats, dataServers []string) []model.Blocked {
	var blocked []model.Blocked
	for _, name := range slices.Sorted(maps.Keys(s.Namespaces)) {
		ns := s.Namespaces[name]
		for _, id := range slices.Sorted(maps.Keys(ns.Shards)) {
			shard := ns.Shards[id]
		replicas:
			for _, from := range shard.Ensemble {
				if shard.Changing || shard.Splitting || !s.DataServers[from].Removed {
					continue
				}
				op := model.Candidate{Namespace: name, Shard: id, From: from}
				reason := ""
				for _, to := range dataServers {
					op.To = to
					switch err := p.doFilter(s, op); {
					case err == nil:
						continue replicas
					case reason == "" && !s.DataServers[to].Removed && !slices.Contains(shard.Ensemble, to):
						reason = err.Error()
					default:
					}
				}
				blocked = append(blocked, model.Blocked{Namespace: name, Shard: id, DataServer: from,
					Reason: cmp.Or(reason, "no data server can take a replica")})
			}
		}
	}
	return blocked
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
