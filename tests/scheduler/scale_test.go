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

package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The cluster grows from one data server in each of the three zones to three,
// then shrinks back, a zone at a time and then a data server at a time, while
// a third of the replica moves and of the leader transfers fail. After each
// step, the scheduler balances the replicas and the leaders, and moves
// replicas onto the new data servers, or off the removed ones, only.
func TestSchedulerScalesZoneByZone(t *testing.T) {
	s := newSimulation(t, 0.3)

	// The leaders start at random: only the leaders move
	s.waitBalanced()
	s.requireMovedOnlyTo()

	// zone-1 grows to two data servers
	s.addDataServer("ds-3", "zone-1")
	s.waitBalanced()
	s.requireMovedOnlyTo("ds-3")
	assert.Positive(t, s.failures)

	// zone-2 and zone-3 grow to two data servers
	s.addDataServer("ds-4", "zone-2")
	s.addDataServer("ds-5", "zone-3")
	s.waitBalanced()
	s.requireMovedOnlyTo("ds-4", "ds-5")

	// Every zone grows to three data servers
	s.addDataServer("ds-6", "zone-1")
	s.addDataServer("ds-7", "zone-2")
	s.addDataServer("ds-8", "zone-3")
	s.waitBalanced()
	s.requireMovedOnlyTo("ds-6", "ds-7", "ds-8")

	// Every zone shrinks back to two data servers at once: a shard can have
	// two replicas to drain, each still in its zone
	s.removeDataServer("ds-6")
	s.removeDataServer("ds-7")
	s.removeDataServer("ds-8")
	s.waitBalanced()
	s.requireDrained("ds-6", "ds-7", "ds-8")

	// Then back to one data server in each zone, one data server at a time
	for _, name := range []string{"ds-5", "ds-4", "ds-3"} {
		s.removeDataServer(name)
		s.waitBalanced()
		s.requireDrained(name)
	}
}
