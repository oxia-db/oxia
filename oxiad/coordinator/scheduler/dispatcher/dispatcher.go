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

// Package dispatcher runs the actions of a round: it checks that the data
// servers of each one are running, sends it to the runtime, which hands it to
// the controller of its shard, or runs it, for a split, and waits until every
// one completes.
package dispatcher

import (
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// Dispatcher is the gate between the plan, made on a snapshot, and the
// cluster as it is now.
//
// # Life of an action
//
//	          +-------------------+
//	action -->| 1. precheck       |--x a data server of the action is down:
//	          |    the data       |    the target, or a member but a removed
//	          |    servers        |    source; the other actions go on
//	          +-------------------+
//	                    |
//	          +-------------------+
//	          | 2. send           |    the send waits for the runtime to take
//	          |                   |    the action, until the scheduler closes
//	          +-------------------+
//	                    |
//	          +-------------------+
//	          | 3. wait           |    until the action completes, or the
//	          |                   |    scheduler closes
//	          +-------------------+
//
// A shard controller completes an action once the metadata shows it, so that
// the next round plans on the cluster that the actions leave, and fails the
// actions left in its queue when it closes: the wait needs no timeout. A transfer fails
// when the election elects another member: it elects the target only if the
// target has every committed entry.
//
// A Dispatcher holds no state between calls.
type Dispatcher interface {
	// Dispatch sends actions, which the planner returned for s, in order, and
	// waits until each one it sent completes, or the scheduler closes. It
	// returns the result of each action, in the order of actions. An action
	// whose data servers aren't all running fails with errs.ErrUnavailable,
	// and isn't sent: a data server went down since the snapshot. The others
	// are sent all the same: the planner leaves each op of a plan valid on its
	// own, e.g. a path of leader transfers that only partly completes leaves
	// the leaders no less even. An action still running when the scheduler
	// closes fails with context.Canceled. It returns nothing if the scheduler
	// closes before it sends every action.
	Dispatch(s *stats.ClusterStats, actions []action.Action) []model.Result
}
