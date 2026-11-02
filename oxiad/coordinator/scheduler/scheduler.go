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

// Package scheduler places and moves the replicas of the shards, under the
// load balancer and the auto-split of the cluster configuration.
package scheduler

import (
	"io"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
)

// Scheduler balances the shards in three steps, in this order, until no valid
// op is left:
//
//  1. Replicas: it drains the removed data servers, then evens out the
//     replicas of each namespace, then of all of them.
//  2. Leaders: once no replica of a namespace moves, it evens out the leaders
//     of the namespace, by its own counts and by the total, since every
//     replace runs an election that may move a leader.
//  3. Shard load: once nothing else of its namespace moves, it splits a shard
//     over the size or the throughput threshold of the auto-split, one at a
//     time, and the next rounds balance its children.
//
// It balances counts, not traffic: a busy shard is split, never moved for its
// load. It drives itself: nothing has to tell it that the cluster changed.
//
// # Round
//
//	start, a change of the        +---------------------+
//	configuration, or the ------->| 1. read the policy  |--x unreadable: the
//	schedule interval             |    and the snapshot |    next round retries
//	                              +---------------------+
//	                                         |
//	                              +---------------------+
//	                              | 2. plan             |    the planner, phase
//	                              |                     |    by phase, one op per
//	                              |                     |    shard and per target
//	                              +---------------------+
//	                                         |
//	                              +---------------------+
//	                              | 3. dispatch         |--x a data server of the
//	                              |    each op          |    op went down since
//	                              |                     |    the snapshot: that op
//	                              +---------------------+    fails, the others go on
//	                                         |
//	                              +---------------------+
//	                              | 4. wait             |    until every op
//	                              |    every op         |    completes
//	                              +---------------------+
//
// A round reads the shard status and the configuration from the metadata, so
// the scheduler keeps no copy of the cluster between rounds, and which data
// servers are down: the plan moves nothing that needs one, so that a data
// server down doesn't hold the other shards, and the dispatch checks them
// again before it sends an op. A single goroutine runs the rounds. A round
// waits for its ops, which the metadata shows once they are done, so the next
// round plans on the cluster they leave; it runs right away. The shard of a
// failed op backs off for the schedule interval: the next rounds plan around
// it, so that a failure holds neither the other shards nor a busy loop. A
// shard controller completes or fails every action it takes, so the wait has
// no timeout.
//
// Nobody calls the scheduler: New starts it, its owner closes it, and its only
// output is GetActions. It runs the first round right away, then a round on
// each change of the configuration or schedule interval, until Close.
type Scheduler interface {
	io.Closer

	// GetActions delivers the actions of the ops, for the runtime to dispatch to
	// the controllers of their shards. An action reports its outcome back to
	// the scheduler. The scheduler stops sending when it closes.
	GetActions() <-chan action.Action
}
