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

package election

import (
	"github.com/oxia-db/oxia/common/proto"
)

// FeaturesSupplier returns the features that each data server supports, by
// name, as their handshakes reported them.
type FeaturesSupplier = func(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature

// Election elects the leader of a shard for a new term. An election is used
// once: it starts from the shard status it was created with, runs a single
// time, and updates the shard status as it goes. The shard controller creates
// a new one for each election, and cancels the current one before it starts
// the next:
//
//	Start
//	  | 1. persist a new term, with the status Election and the current
//	  |    addresses of the ensemble
//	  | 2. change the ensemble, if asked: the replaced member becomes a removed
//	  |    member, and the added one joins the ensemble
//	  | 3. pin, in the term options, the features that the whole ensemble
//	  |    supports
//	  | 4. fence: send NewTerm to the ensemble and the removed members, wait for
//	  |    a majority of them and of the ensemble, then for the others during a
//	  |    grace period. The fenced members of the ensemble are the candidates
//	  | 5. check that the removed members don't have entries that may be
//	  |    committed beyond every candidate, and that the candidates support
//	  |    the pinned features and the ones already enabled on the shard
//	  | 6. send BecomeLeader to a candidate with the highest head entry, with
//	  |    the other candidates as its followers
//	  | 7. persist the leader, with the status SteadyState; the removed members
//	  v    are pending deletion
//	leader elected, and in the background until Cancel:
//	  - fence the members that missed the fence, and add them to the leader
//	  - wait for the followers to catch up with the head entry of the leader
//
// A step that fails starts over from step 1, with a new term, until the
// election is cancelled. If the removed members may have committed entries
// that no candidate has, the election gives up the change of the ensemble,
// unless an attempt sent BecomeLeader already, and the next attempts elect a
// leader of the current ensemble. Features that the candidates enable, or that
// the ensemble supports, beyond the pinned ones are pinned by the next attempt.
// The election stops at step 1 if the shard is being deleted, is the parent of
// a split past the point of no return, or is part of a split while the
// ensemble changes.
type Election interface {
	// Start runs the election until it elects a leader, and returns it. It
	// fails if the election stopped before, e.g. because it was cancelled,
	// and if it gave up the change of the ensemble, after electing a leader
	// of the current ensemble. It panics if it is called twice.
	Start() (*proto.DataServerIdentity, error)

	// Cancel stops the election, and waits for its background tasks.
	Cancel()

	// IsFollowerCaughtUp reports whether the followers caught up with the
	// elected leader. It can be called from any goroutine.
	IsFollowerCaughtUp() bool
}
