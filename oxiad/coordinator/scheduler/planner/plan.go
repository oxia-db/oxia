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
	"maps"
	"slices"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/internal/candidate"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/load"
)

// plan is the state of one plan: the load and the limits left, as its ops
// leave them. A plan makes at most one op on each shard and onto each data
// server, so that a round never piles copies or leaders onto one data server.
type plan struct {
	clusterStats *stats.ClusterStats
	// dataServerLoad is the load of the data servers, as the ops of the plan
	// leave it.
	dataServerLoad *load.Load
	// dataServerNames holds the names of every data server, sorted, so that
	// the ops are enumerated in the same order every time.
	dataServerNames []string
	// plannedShards holds the shards that an op of the plan already changes.
	plannedShards map[int64]bool
	// plannedTargets holds the data servers that an op of the plan already
	// copies a replica, or moves a leadership, onto.
	plannedTargets map[string]bool
	// splitActive is set once a split is planned or in progress: the
	// cluster splits one shard at a time.
	splitActive bool
	// ensembleChanging is set if a change of an ensemble is pending.
	ensembleChanging bool
}

// newPlan returns an empty plan of s, with the splits and the changes of
// ensembles that are already in progress.
func newPlan(s *stats.ClusterStats) *plan {
	pl := &plan{
		clusterStats:    s,
		dataServerLoad:  load.New(s),
		dataServerNames: slices.Sorted(maps.Keys(s.DataServers)),
		plannedShards:   make(map[int64]bool),
		plannedTargets:  make(map[string]bool),
	}
	for _, ns := range s.Namespaces {
		for _, shard := range ns.Shards {
			pl.splitActive = pl.splitActive || shard.Splitting
			pl.ensembleChanging = pl.ensembleChanging || shard.Changing
		}
	}
	return pl
}

// apply charges op to the limits and to the load of the plan.
func (pl *plan) apply(op candidate.Candidate) {
	pl.count(op)
	pl.dataServerLoad.Move(pl.clusterStats, op)
}

// count charges op to the limits of the plan.
func (pl *plan) count(op candidate.Candidate) {
	pl.plannedShards[op.Shard] = true
	if op.Kind == candidate.Split {
		pl.splitActive = true
		return
	}
	pl.plannedTargets[op.To] = true
}

// allows reports whether the limits of the plan leave room for op.
func (pl *plan) allows(op candidate.Candidate) bool {
	if pl.plannedShards[op.Shard] {
		return false
	}
	if op.Kind == candidate.Split {
		return !pl.splitActive
	}
	return !pl.plannedTargets[op.To]
}
