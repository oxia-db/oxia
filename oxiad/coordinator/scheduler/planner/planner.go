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
// is pure: it reads a model.Snapshot and returns ops, and never changes the
// cluster or the snapshot.
package planner

import (
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

// Planner plans the ops of a round with a fixed algorithm over two kinds of
// strategies, the filters (package filter) and the tiers (package tier).
//
// # Plan
//
//	          +---------------------+
//	Plan ---->| 1. enumerate        |    every shard x member x data server:
//	          |    candidates       |    replace the member with the data server
//	          +---------------------+
//	                     |
//	          +---------------------+
//	          | 2. filter           |--x a filter rejects it: a hard constraint,
//	          |    every filter, in |    e.g. a frozen shard, a member down, an
//	          |    order            |    anti-affinity, or a limit is reached
//	          +---------------------+
//	                     |
//	          +---------------------+
//	          | 3. score            |--x it raises a tier before the one it
//	          |    the change of    |    lowers, or lowers none enough: drain,
//	          |    each tier        |    then the namespace, then the total
//	          +---------------------+
//	                     |
//	          +---------------------+
//	          | 4. select           |    the lowest changes, compared tier by
//	          |    the best         |    tier, then a follower before a leader,
//	          |                     |    then the first by name
//	          +---------------------+
//	                     |
//	          apply it to the load of the round, and back to 1 until the
//	          moves of the policy run out or no candidate is left
//
// The potential of a tier is the sum of the squared loads of its data
// servers. An op must lower the potential of a tier, by a gap of at least 2
// replicas or the tolerance times the mean, without raising a tier before it.
// The potentials can't go below zero, so the rounds settle: when Plan returns
// no op, the cluster is balanced, as far as the filters allow.
//
// A Planner holds no state, and any goroutine may call it.
type Planner interface {
	// Plan returns the ops to start in this round, best first, at most one
	// per shard, within the limits of p net of the ops in flight, and with
	// the tolerance of p. It also returns the replicas on removed data
	// servers that no op can move, whatever the limits, with the filter that
	// keeps them.
	Plan(s *model.Snapshot, p policy.Policy, inFlight []model.Op) ([]model.Op, []model.Stuck)

	// Validate checks an op, whoever proposed it, as Plan checks its
	// candidates without the limits and the tolerance: it returns
	// errs.ErrNotFound for an unknown namespace or shard, the error of the
	// first filter that rejects it, or errs.ErrFailedPrecondition if it
	// improves no tier.
	Validate(s *model.Snapshot, op model.Op) error

	// Place returns the ensemble of a new shard of namespace: the data servers
	// with the fewest replicas of the namespace, then in total, then the first
	// by name, that keep the anti-affinities. It considers only the data
	// servers that eligible accepts, unless eligible is nil, and that aren't
	// removed; a data server doesn't need to be running, since the election
	// of the shard waits for it. It returns errs.ErrNotFound if fewer than
	// replicas data servers fit.
	Place(s *model.Snapshot, namespace string, replicas int, antiAffinities []*proto.AntiAffinity,
		eligible func(*model.DataServer) bool) ([]string, error)
}
