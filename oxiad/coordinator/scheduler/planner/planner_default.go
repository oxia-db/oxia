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
	"maps"
	"slices"
	"strings"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/load"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

var _ Planner = &planner{}

type planner struct {
	filters []filter.Filter
	// tiers are in priority order.
	tiers []tier.Tier
}

func New(filters []filter.Filter, tiers []tier.Tier) Planner {
	return &planner{filters: filters, tiers: tiers}
}

func (p *planner) Plan(s *model.Snapshot, pol policy.Policy, inFlight []model.Op) ([]model.Op, []model.Stuck) {
	l := load.New(s)
	dataServers := slices.Sorted(maps.Keys(s.DataServers))
	moves := pol.MaxMovesFor(s.ConfiguredDataServers()) - len(inFlight)
	perDataServer := make(map[string]int)
	perNamespace := make(map[string]int)
	planned := make(map[int64]bool)
	for _, op := range inFlight {
		if op.Kind == model.Replace {
			perDataServer[op.To]++
		}
		perNamespace[op.Namespace]++
		// The metadata shows an op only once its election persists it: plan
		// on the load it will leave, or the next ops overshoot and undo it
		if pending(s, op) {
			l.Move(s, op)
		}
	}
	settled := p.replicasSettled(s, l, dataServers, pol.Tolerance, inFlight)

	var ops []model.Op
	for len(ops) < moves {
		op, found := p.best(s, l, dataServers, pol.Tolerance, func(op model.Op) bool {
			if planned[op.Shard] || perNamespace[op.Namespace] >= pol.MaxMovesPerNamespace {
				return false
			}
			if op.Kind == model.TransferLeader {
				return settled
			}
			// Only a replace copies a replica onto its target
			return perDataServer[op.To] < pol.MaxMovesPerDataServer
		})
		if !found {
			break
		}
		ops = append(ops, op)
		planned[op.Shard] = true
		if op.Kind == model.Replace {
			perDataServer[op.To]++
		}
		perNamespace[op.Namespace]++
		l.Move(s, op)
	}
	return ops, p.stuck(s, dataServers)
}

// pending reports whether s doesn't show op in flight yet.
func pending(s *model.Snapshot, op model.Op) bool {
	_, shard := s.Shard(op)
	if shard == nil {
		return false
	}
	if op.Kind == model.TransferLeader {
		return shard.Leader == op.From
	}
	return slices.Contains(shard.Ensemble, op.From) && !slices.Contains(shard.Ensemble, op.To)
}

// replicasSettled reports whether the replicas no longer move: no replace in
// flight, no change of an ensemble pending, and no replace left to plan. The
// leaders move only then: a leader is one of the replicas, and every replace
// runs an election that may move it.
func (p *planner) replicasSettled(s *model.Snapshot, l *load.Load, dataServers []string, tolerance float64,
	inFlight []model.Op) bool {
	if slices.ContainsFunc(inFlight, func(op model.Op) bool { return op.Kind == model.Replace }) {
		return false
	}
	for _, ns := range s.Namespaces {
		for _, shard := range ns.Shards {
			if shard.Changing {
				return false
			}
		}
	}
	_, found := p.best(s, l, dataServers, tolerance, func(op model.Op) bool { return op.Kind == model.Replace })
	return !found
}

func (p *planner) Validate(s *model.Snapshot, op model.Op) error {
	if _, shard := s.Shard(op); shard == nil {
		return fmt.Errorf("%w: shard %d of namespace %q", errs.ErrNotFound, op.Shard, op.Namespace)
	}
	if err := p.filter(s, op); err != nil {
		return err
	}
	if tier.Evaluate(p.tiers, s, load.New(s), op, 0).Improved < 0 {
		return fmt.Errorf("%w: moving shard %d from %q to %q doesn't improve a tier",
			errs.ErrFailedPrecondition, op.Shard, op.From, op.To)
	}
	return nil
}

func (*planner) Place(s *model.Snapshot, namespace string, replicas int, antiAffinities []*proto.AntiAffinity,
	eligible func(*model.DataServer) bool) ([]string, error) {
	l := load.New(s)
	counts := l.Replicas(namespace)
	candidates := slices.DeleteFunc(slices.Collect(maps.Keys(s.DataServers)), func(name string) bool {
		dataServer := s.DataServers[name]
		return dataServer.Removed || (eligible != nil && !eligible(dataServer))
	})
	slices.SortFunc(candidates, func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[a], counts[b]), cmp.Compare(l.TotalReplicas()[a], l.TotalReplicas()[b]), strings.Compare(a, b))
	})
	ensemble := make([]string, 0, replicas)
	for _, candidate := range candidates {
		if len(ensemble) == replicas {
			break
		}
		if !filter.Conflicts(s, antiAffinities, ensemble, "", candidate) {
			ensemble = append(ensemble, candidate)
		}
	}
	if len(ensemble) < replicas {
		return nil, fmt.Errorf("%w: only %d of %d data servers keep the anti-affinities",
			errs.ErrNotFound, len(ensemble), replicas)
	}
	return ensemble, nil
}

// filter returns the error of the first filter that rejects op.
func (p *planner) filter(s *model.Snapshot, op model.Op) error {
	for _, f := range p.filters {
		if err := f.Filter(s, op); err != nil {
			return fmt.Errorf("%s: %w", f.Name(), err)
		}
	}
	return nil
}

// best returns the candidate that allow accepts, that the filters pass and
// that ranks first: by its score, then moving a follower before the leader,
// then the first in the order of the names, so that a plan is the same every
// time.
func (p *planner) best(s *model.Snapshot, l *load.Load, dataServers []string, tolerance float64,
	allow func(model.Op) bool) (model.Op, bool) {
	var bestOp model.Op
	var bestScore tier.Score
	bestLeader, found := 0, false
	forEachCandidate(s, dataServers, func(op model.Op, shard *model.Shard) {
		if !allow(op) || p.filter(s, op) != nil {
			return
		}
		sc := tier.Evaluate(p.tiers, s, l, op, tolerance)
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
		bestOp.Reason = p.tiers[bestScore.Improved].Name()
	}
	return bestOp, found
}

// stuck returns the replicas on removed data servers that no op can move,
// whatever the budget. A drain always improves a tier, so the filters decide.
func (p *planner) stuck(s *model.Snapshot, dataServers []string) []model.Stuck {
	var stuck []model.Stuck
	forEachShard(s, func(op model.Op, shard *model.Shard) {
		if shard.Frozen {
			return
		}
		for _, from := range shard.Ensemble {
			if !s.DataServers[from].Removed {
				continue
			}
			op.From = from
			if reason, ok := p.stuckReason(s, dataServers, op, shard); ok {
				stuck = append(stuck, model.Stuck{Namespace: op.Namespace, Shard: op.Shard, DataServer: from, Reason: reason})
			}
		}
	})
	return stuck
}

// stuckReason returns why no data server can take the replica of op.From:
// the filter that rejects the first data server that could otherwise take it.
// It returns false if a data server can take it.
func (p *planner) stuckReason(s *model.Snapshot, dataServers []string, op model.Op, shard *model.Shard) (string, bool) {
	reason, found := "no data server can take a replica", false
	for _, to := range dataServers {
		op.To = to
		err := p.filter(s, op)
		if err == nil {
			return "", false
		}
		if dataServer := s.DataServers[to]; !found && !dataServer.Removed && dataServer.Stable && !slices.Contains(shard.Ensemble, to) {
			reason, found = err.Error(), true
		}
	}
	return reason, true
}

// forEachCandidate yields every op that replaces a member of a shard with a
// data server, and every op that transfers its leadership to another member,
// with the shard.
func forEachCandidate(s *model.Snapshot, dataServers []string, yield func(model.Op, *model.Shard)) {
	forEachShard(s, func(op model.Op, shard *model.Shard) {
		for _, from := range shard.Ensemble {
			for _, to := range dataServers {
				op.From, op.To = from, to
				yield(op, shard)
			}
		}
		if shard.Leader == "" {
			return
		}
		op.Kind, op.From = model.TransferLeader, shard.Leader
		for _, to := range shard.Ensemble {
			if to != shard.Leader {
				op.To = to
				yield(op, shard)
			}
		}
	})
}

// forEachShard yields every shard, by namespace then ID, with an op that
// names it.
func forEachShard(s *model.Snapshot, yield func(model.Op, *model.Shard)) {
	for _, name := range slices.Sorted(maps.Keys(s.Namespaces)) {
		ns := s.Namespaces[name]
		for _, id := range slices.Sorted(maps.Keys(ns.Shards)) {
			yield(model.Op{Namespace: name, Shard: id}, ns.Shards[id])
		}
	}
}
