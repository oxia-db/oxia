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

// Package planner decides how to place and move the replicas of the shards. It
// is pure: it reads a stats.ClusterStats and returns ops, and never changes the
// cluster or the snapshot.
package planner

import (
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// Planner plans the ops of a round with a fixed algorithm over two kinds of
// strategies, the filters (package filter) and the tiers (package tier).
//
// # Phases
//
// A plan runs a phase for each kind of op, in the order of the tiers: the
// replaces, then the transfers of the leadership, then the splits. A phase
// plans the ops of a namespace only once the phases before it are quiet for
// that namespace: no op of theirs planned, no change of an ensemble pending,
// and none left to plan. A leader is one of the replicas, and every replace
// runs an election that may move it, so the leaders of a namespace wait for
// its replicas, not for those of the other namespaces. The tiers that count
// every namespace together, e.g. the total of the leaders, score the ops of
// the namespaces that are quiet: the next rounds even out what the others
// still move. A split places new replicas, and elects their leaders; the
// cluster splits one shard at a time.
//
// # Phase
//
//	          +---------------------+
//	phase --->| 1. enumerate        |    replace: every shard x member x data
//	          |    candidates       |    server; transfer: every shard x member;
//	          +---------------------+    split: every shard
//	                     |
//	          +---------------------+
//	          | 2. filter           |--x a filter rejects it: a hard constraint,
//	          |    every filter, in |    e.g. a frozen shard, a removed target,
//	          |    order            |    an anti-affinity; or a limit is reached:
//	          |                     |    one op per shard and per target
//	          +---------------------+
//	                     |
//	          +---------------------+
//	          | 3. score            |--x it raises a tier before the one it
//	          |    the change of    |    lowers, or lowers none: drain,
//	          |    each tier of the |    then the namespace, then the total
//	          |    phase            |
//	          +---------------------+
//	                     |
//	          +---------------------+
//	          | 4. select           |    the lowest changes, compared tier by
//	          |    the best         |    tier, then a follower before a leader,
//	          |                     |    then the first by name
//	          +---------------------+
//	                     |
//	          apply it to the load of the round, and back to 1 until no
//	          candidate is left
//
// The potential of a tier is the sum of the squared loads of its data
// servers. An op must lower the potential of a tier, by a gap of at least 2,
// without raising a tier before it: the loads end within 1 of each other.
// The potentials can't go below zero, so the rounds settle: when Plan returns
// no op, the cluster is balanced, as far as the filters allow.
//
// The planner reads only the snapshot, which says which data servers are
// down. A Planner holds no state, and any goroutine may call it.
type Planner interface {
	// Plan returns the ops to start in this round, best first, at most one
	// per shard and one onto each data server. The ops of the previous round
	// have finished, so s shows them. It also returns the replicas on removed
	// data servers that no op can move, whatever the limits, with the filter
	// that keeps them.
	Plan(s *stats.ClusterStats) ([]action.Action, []model.Blocked)

	// Place returns the ensemble of a new shard of namespace: the data servers
	// with the fewest replicas of the namespace, then in total, then the first
	// by name, that keep the anti-affinities. It considers only the data
	// servers that eligible accepts, unless eligible is nil, and that aren't
	// removed; a data server doesn't need to be running, since the election
	// of the shard waits for it. It returns errs.ErrNotFound if fewer than
	// replicas data servers fit.
	Place(s *stats.ClusterStats, namespace string, replicas int, antiAffinities []*proto.AntiAffinity,
		eligible func(*stats.DataServerStats) bool) ([]string, error)
}
