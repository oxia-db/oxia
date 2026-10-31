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

// Package dispatcher runs the ops of the scheduler: it validates each op,
// turns it into an action for the controller of its shard (a change of the
// ensemble for a replace, an election that prefers the target for a
// transfer), tracks it until it
// finishes, and makes the shard wait after a failure.
package dispatcher

import (
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

// Dispatcher is the safety gate between a proposer of ops (the planner today,
// an operator or a model later) and the cluster.
//
// # Life of an op
//
//	          +-------------------+
//	Start --->| 1. validate       |--x the planner rejects it: a filter fails,
//	          |    on the round's |    or it improves no tier; the shard is
//	          |    snapshot       |    left as it is
//	          +-------------------+
//	                    |
//	          +-------------------+
//	          | 2. in flight      |    the shard is Busy, and the op counts in
//	          |    send the       |    the next plan; the send waits for the
//	          |    action         |    runtime to take it, until the scheduler
//	          +-------------------+    closes
//	                    |
//	        +-----------+-----------+
//	        |                       |
//	+---------------+     +-------------------+
//	| 3a. outcome   |     | 3b. moveTimeout   |  Expire drops it; the shard
//	|  on Results,  |     |     passed        |  waits, and an outcome that
//	|  to Finish    |     +-------------------+  arrives later is ignored
//	+---------------+
//	        |
//	   done: the shard is free; failed: it waits moveBackoff, doubled after
//	   each failure up to moveTimeout, before an op may move it again. A
//	   transfer fails when the election elects another member: it elects the
//	   target only if the target has every committed entry
//
// A shard has at most one op in flight. A Dispatcher is not safe for
// concurrent use: the scheduler loop owns it, and calls it from its goroutine
// only.
type Dispatcher interface {
	// Results delivers each flight once its op finishes, done or failed. The
	// owner passes it to Finish.
	Results() <-chan *Flight

	// Busy reports whether shard has an op in flight, or waits after a
	// failure: the snapshot marks it frozen, so that no op moves it.
	Busy(shard int64) bool

	// InFlight returns the ops in flight, which count against the limits of
	// the policy in the next plan.
	InFlight() []model.Op

	// Start validates op on the snapshot it was planned on and, if it passes,
	// sends its action. It returns the reason of a rejection; every proposer
	// goes through it.
	Start(s *model.Snapshot, op model.Op) error

	// Finish records the outcome of a flight from Results. A flight that
	// timed out is no longer in flight, and its outcome is ignored.
	Finish(f *Flight, p policy.Policy)

	// Expire drops the ops that ran longer than p.MoveTimeout, as failures,
	// and forgets the failures that ended as long ago.
	Expire(p policy.Policy)
}
