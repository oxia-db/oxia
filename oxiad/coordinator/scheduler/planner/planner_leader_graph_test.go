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

package planner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
)

// n0 has two leaders and n3 none, and each other data server one: only a path
// of three transfers, each of another namespace, reaches n3.
func TestLeaderGraphFindPathPassesALeaderAlongThreeDataServers(t *testing.T) {
	s := cluster(4)
	addShard(s, "w", 0, "n0").Leader = "n0"
	addShard(s, "x", 1, "n0", "n1").Leader = "n0"
	addShard(s, "y", 2, "n1", "n2").Leader = "n1"
	addShard(s, "z", 3, "n2", "n3").Leader = "n2"
	pl := newPlan(s)

	path := newLeaderGraph(pl, filter.Defaults()).findPath(pl)

	assert.Equal(t, []string{"3 n2->n3 total-leaders", "2 n1->n2 total-leaders", "1 n0->n1 total-leaders"},
		describePath(path))
}

// n0 has the most leaders but transfers none: a path starts from n1, which has
// the most after it.
func TestLeaderGraphFindPathStartsFromTheNextDataServers(t *testing.T) {
	s := cluster(4)
	for id := range int64(3) {
		addShard(s, "w", id, "n0").Leader = "n0"
	}
	addShard(s, "w", 3, "n1").Leader = "n1"
	addShard(s, "y", 4, "n1", "n3").Leader = "n1"
	addShard(s, "z", 5, "n3", "n2").Leader = "n3"
	pl := newPlan(s)

	path := newLeaderGraph(pl, filter.Defaults()).findPath(pl)

	assert.Equal(t, []string{"5 n3->n2 total-leaders", "4 n1->n3 total-leaders"}, describePath(path))
}

// The only transfer out of n0 would leave n1 with two leaders of x and n0 with
// none: it unbalances x, so no path evens out the leaders of n0 and n2.
func TestLeaderGraphFindPathKeepsTheNamespacesBalanced(t *testing.T) {
	s := cluster(3)
	addShard(s, "w", 0, "n0").Leader = "n0"
	addShard(s, "x", 1, "n0", "n1").Leader = "n0"
	addShard(s, "x", 2, "n1").Leader = "n1"
	addShard(s, "y", 3, "n1", "n2").Leader = "n2"
	pl := newPlan(s)

	assert.Nil(t, newLeaderGraph(pl, filter.Defaults()).findPath(pl))
}

// A removed data server leads no counted leader: no path starts from it.
func TestLeaderGraphSkipsTheTransfersOfARemovedDataServer(t *testing.T) {
	s := cluster(2)
	removed(s, "r")
	addShard(s, "x", 1, "r", "n0").Leader = "r"
	pl := newPlan(s)

	assert.Empty(t, newLeaderGraph(pl, filter.Defaults()).out)
}

// describePath names each transfer of path by its shard, its source and
// target, and its reason.
func describePath(path []model.Candidate) []string {
	described := make([]string, 0, len(path))
	for _, op := range path {
		described = append(described, fmt.Sprintf("%d %s->%s %s", op.Shard, op.From, op.To, op.Reason))
	}
	return described
}
