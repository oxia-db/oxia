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

// Election elects the leader of a shard in a new term. An election runs once:
// it starts from the shard status it was created with, updates the shard
// status as it goes, and either elects a leader or fails. The shard controller
// creates a new one for each election, and cancels the current one before it
// starts the next.
//
//	Start
//	  | 1. load the options of the term and the current addresses of the
//	  |    ensemble, then persist a new term with the status Election. The
//	  |    term is persisted before fencing with it, so that no later election
//	  |    reuses it. The shard must not be deleting, the parent of a split
//	  |    past the point of no return, or part of a split while its ensemble
//	  |    changes
//	  | 2. change the ensemble, if asked: the replaced member becomes a removed
//	  |    member, which is fenced but is not a candidate, and the added one
//	  |    joins the ensemble. If an earlier election persisted the new
//	  |    ensemble already, the election completes the change as is
//	  | 3. pin, in the options of the term, the features that every member of
//	  |    the ensemble supports, and the ones that an earlier election found
//	  |    already enabled on the shard
//	  | 4. fence: send NewTerm to the ensemble and the removed members, and
//	  |    wait for a majority of them and for a majority of the ensemble
//	  |    among them, the new leader needing the acks of its followers; then
//	  |    wait for the others during a grace period. The fenced members of the
//	  |    ensemble are the candidates
//	  | 5. check that enough candidates have every entry of the removed members
//	  |    that answered, which may be committed, for any later fence quorum of
//	  |    the new ensemble to include one of them; and that the candidates
//	  |    support the pinned features and the ones already enabled on the shard
//	  | 6. persist a candidate with the highest head entry as the leader, with
//	  |    the status SteadyState and the new ensemble. It is persisted before
//	  |    the leader is told, so that if BecomeLeader fails, the next election
//	  |    starts from the new ensemble. The removed members are pending
//	  |    deletion
//	  | 7. send BecomeLeader to the leader, with the other candidates as its
//	  v    followers
//	leader elected; in the background, until Cancel:
//	  - fence the members of the ensemble that missed the fence, and add them
//	    to the leader as followers
//	  - wait for the followers to catch up with the head entry of the leader
//	    (see IsFollowerCaughtUp)
//
// A failed step fails the election, and the caller retries with a new election
// from the current shard status. The election fails with
// errs.ErrShardNotElectable, which must not be retried, at step 1 if the shard
// can't be elected. It fails with a *feature.EnabledError at step 5 if the
// candidates have features enabled that the term didn't pin: the next election
// pins them.
type Election interface {
	// Start runs the election, and returns the elected leader. It fails if
	// called twice.
	Start() (*proto.DataServerIdentity, error)

	// Cancel stops the election, and waits for its background tasks.
	Cancel()

	// IsFollowerCaughtUp reports whether the followers caught up with the
	// leader. Any goroutine can call it.
	IsFollowerCaughtUp() bool
}
