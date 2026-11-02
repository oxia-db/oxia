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
	"log/slog"
	"maps"
	"slices"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

// plan is the state of one plan: its actions, and the load and the limits
// left, as its ops leave them. A plan makes at most one op on each shard and
// onto each data server, so that a round never piles copies or leaders onto
// one data server, and the cluster splits one shard at a time. A replace also
// waits while its target has MaxCopies replicas still catching up, from the
// changes of the rounds before. A namespace waits in the phases after one that
// hasn't settled it. It enumerates the data servers in the order of their
// names, so that a plan is the same every time.
type plan struct {
	logger          *slog.Logger
	clusterStats    *stats.ClusterStats
	actions         []action.Action
	dataServerLoad  *tier.Load
	dataServerNames []string
	plannedShards   map[int64]bool
	plannedTargets  map[string]bool
	splitActive     bool
	// changing holds the namespaces with a change of an ensemble pending,
	// phasePlanned those that the current phase planned an op of, and waiting
	// those that a phase before it hasn't settled.
	changing     map[string]bool
	phasePlanned map[string]bool
	waiting      map[string]bool
}

// newPlan returns an empty plan of s, with the splits and the changes of
// ensembles that are already in progress.
func newPlan(s *stats.ClusterStats) *plan {
	pl := &plan{
		logger:          slog.With(slog.String("component", "planner")),
		clusterStats:    s,
		dataServerLoad:  tier.NewLoad(s),
		dataServerNames: slices.Sorted(maps.Keys(s.DataServers)),
		plannedShards:   make(map[int64]bool),
		plannedTargets:  make(map[string]bool),
		changing:        make(map[string]bool),
		phasePlanned:    make(map[string]bool),
		waiting:         make(map[string]bool),
	}
	for name, ns := range s.Namespaces {
		for _, shard := range ns.Shards {
			pl.splitActive = pl.splitActive || shard.Splitting
			if shard.Changing {
				pl.changing[name] = true
			}
		}
	}
	return pl
}

// endPhase closes the phase of kind: the namespaces that it planned an op of
// wait in the next phases, and after the replaces, those with a change of an
// ensemble pending, since its election and its catch-up may still move a
// leader.
func (pl *plan) endPhase(kind model.Kind) {
	for name := range pl.phasePlanned {
		pl.waiting[name] = true
	}
	clear(pl.phasePlanned)
	if kind == model.Replace {
		for name := range pl.changing {
			pl.waiting[name] = true
		}
	}
}

// allWaiting reports whether every namespace waits: the next phases have
// nothing to plan.
func (pl *plan) allWaiting() bool {
	return len(pl.waiting) == len(pl.clusterStats.Namespaces)
}

// copies reports whether the data server to has room for one more copy of a
// replica.
func (pl *plan) copies(to string) bool {
	return pl.clusterStats.MaxCopies == 0 || pl.clusterStats.DataServers[to].Copying < pl.clusterStats.MaxCopies
}

// add adds the action that makes op to the plan, and charges op to the limits
// and to the load of the plan.
func (pl *plan) add(op model.Candidate) {
	pl.count(op)
	pl.dataServerLoad.Move(pl.clusterStats, op)
	pl.logger.Info("Planned an op", slog.String("kind", op.Kind.String()), slog.String("namespace", op.Namespace),
		slog.Int64("shard", op.Shard), slog.String("from", op.From), slog.String("to", op.To),
		slog.String("reason", op.Reason))
	switch op.Kind {
	case model.TransferLeader:
		pl.actions = append(pl.actions, action.NewLeaderTransferAction(op.Shard, op.To))
	case model.Split:
		pl.actions = append(pl.actions, action.NewSplitAction(op.Namespace, op.Shard))
	default:
		pl.actions = append(pl.actions, action.NewChangeEnsembleAction(op.Shard,
			pl.clusterStats.DataServers[op.From].GetIdentity(), pl.clusterStats.DataServers[op.To].GetIdentity()))
	}
}

// count charges op to the limits of the plan.
func (pl *plan) count(op model.Candidate) {
	pl.plannedShards[op.Shard] = true
	pl.phasePlanned[op.Namespace] = true
	if op.Kind == model.Split {
		pl.splitActive = true
		return
	}
	pl.plannedTargets[op.To] = true
}

// allows reports whether the limits of the plan leave room for op.
func (pl *plan) allows(op model.Candidate) bool {
	if pl.plannedShards[op.Shard] || pl.waiting[op.Namespace] {
		return false
	}
	if op.Kind == model.Split {
		return !pl.splitActive
	}
	if op.Kind == model.Replace && !pl.copies(op.To) {
		return false
	}
	return !pl.plannedTargets[op.To]
}
