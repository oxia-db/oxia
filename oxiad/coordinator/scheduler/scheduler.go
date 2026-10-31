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
// scheduler policy of the cluster configuration.
package scheduler

import (
	"io"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
)

// Scheduler moves the replicas and the leaders of the shards until no valid op
// is left: it drains the removed data servers, balances the replicas of each
// namespace, then of all of them, and once the replicas settle, balances the
// leaders the same way. It drives itself: nothing has to tell it that the
// cluster changed.
//
// # Round
//
//	start, a change of the        +---------------------+
//	configuration, a finished --->| 1. read the policy  |--x unreadable: the
//	op, or the policy interval    |    and the snapshot |    next round retries
//	                              +---------------------+
//	                                         |
//	                              +---------------------+
//	                              | 2. plan             |    the planner, within
//	                              |                     |    the budget left by
//	                              |                     |    the ops in flight
//	                              +---------------------+
//	                                         |
//	                              +---------------------+
//	                              | 3. dispatch         |    the dispatcher
//	                              |    each op          |    validates it again,
//	                              |                     |    and sends its action
//	                              +---------------------+    on Actions
//	                                         |
//	          the runtime takes the action and dispatches it to the controller
//	          of its shard; the outcome comes back to the scheduler and starts
//	          the next round
//
// A round reads the shard status and the configuration from the metadata,
// and the state of the data servers from the health callback, so the
// scheduler keeps no copy of the cluster between rounds. A single goroutine
// runs the rounds, and owns the ops in flight.
//
// Nobody calls the scheduler: its owner starts and closes it, and its only
// output is Actions.
type Scheduler interface {
	io.Closer

	// Start runs the first round, then a round on each change of the
	// configuration, finished op, or interval of the policy, until Close.
	Start()

	// Actions delivers the actions of the ops, for the runtime to dispatch to
	// the controllers of their shards. An action reports its outcome back to
	// the scheduler. The scheduler stops sending when it closes.
	Actions() <-chan action.Action
}
