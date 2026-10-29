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
// # Election
//
//	           +----------------------+
//	Start ---->| 1. new term          |--x errs.ErrShardNotElectable: the shard is
//	           |    persist term+1,   |    deleting, the parent of a split past the
//	           |    status Election   |    point of no return, or in a split while
//	           +----------------------+    its ensemble changes
//	                      |
//	           +----------------------+
//	           | 2. change ensemble   |    only for a change, see below
//	           +----------------------+
//	                      |
//	           +----------------------+
//	           | 3. pin features      |    the ones the whole ensemble supports, and
//	           |                      |    the ones an earlier election found enabled
//	           +----------------------+
//	                      |
//	           +----------------------+
//	           | 4. fence             |--x no majority of the ensemble and the
//	           |    NewTerm to the    |    removed members, or of the ensemble
//	           |    ensemble and the  |
//	           |    removed members   |
//	           +----------------------+
//	                      |
//	           +----------------------+
//	           | 5. check features    |--x *feature.EnabledError: an enabled feature
//	           |                      |    isn't pinned; the next election pins it
//	           +----------------------+
//	                      |
//	           +----------------------+
//	           | 6. choose leader     |    the fenced member with the highest head
//	           |                      |    entry, which has every committed entry
//	           +----------------------+
//	                      |
//	           +----------------------+
//	           | 7. persist           |--x a store error
//	           |    SteadyState, the  |
//	           |    leader, ensemble  |    before the leader is told: if 8 fails, the
//	           |    and removed       |    next election starts from them
//	           +----------------------+
//	                      |
//	           +----------------------+
//	           | 8. BecomeLeader      |--x an rpc error
//	           |    the other fenced  |
//	           |    members of the    |
//	           |    ensemble follow   |
//	           +----------------------+
//	                      |
//	                      v
//	background, until Cancel:
//	  - fence the members of the ensemble that missed the fence, and add them as
//	    followers
//	  - wait for a majority of the ensemble to commit the head entry of the
//	    leader (see IsQuorumCaughtUp)
//
// A failed step (x) fails the election, and the caller retries with a new
// election from the current shard status, except after
// errs.ErrShardNotElectable.
//
// # Change of the ensemble
//
// A change replaces one member of the ensemble: "from" becomes a removed member
// and "to" joins the ensemble. A committed entry is on a majority of the
// ensemble before or after the change, which differ by one member, so a fence
// quorum of the ensemble and the removed members includes one of its holders.
// The removed members are thus fenced and are candidates, until enough members
// of the ensemble have every entry:
//
//	ensemble {A,B,C}         ensemble {A,C,D}              ensemble {A,C,D}
//	removed  {}      ---->   removed  {B}          ---->   removed  {}
//	                 change                       release  pending deletion {B}
//	                 election                     (n+1)/2 members of the
//	                    |                         ensemble committed the head
//	                    |                         entry of the leader: any later
//	                    |                         fence quorum includes one of them
//	                    v
//	           6. choose leader among the fenced members of the ensemble and the
//	              removed members; a member of the ensemble wins a tie
//	                    |
//	       +------------+---------------------------+
//	       |                                        |
//	 a member of the ensemble                 a removed member, ahead of every
//	 has the highest head entry               member of the ensemble
//	       |                                        |
//	 it leads, the change is done             it goes back into the ensemble and
//	                                          leads, in place of the member with
//	                                          the lowest head entry, which is
//	                                          removed instead: the change is undone
//
// Every election fences the removed members until they're released, and can
// choose them. If the release fails, the next election releases them. The shard
// controller refuses another change until then: with two changes, the
// ensembles would differ by two members, and a fence quorum could miss a
// committed entry.
type Election interface {
	// Start runs the election, and returns the elected leader. It fails if
	// called twice.
	Start() (*proto.DataServerIdentity, error)

	// Cancel stops the election, and waits for its background tasks.
	Cancel()

	// IsQuorumCaughtUp reports whether a majority of the ensemble, the leader
	// included, committed the head entry of the leader at the election: any
	// fence quorum of a later election includes one of them. It stays false
	// once the election is canceled. Any goroutine can call it.
	IsQuorumCaughtUp() bool
}
