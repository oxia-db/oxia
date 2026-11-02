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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

func TestNewPlanTakesTheSplitsAndTheChangesInProgress(t *testing.T) {
	s := cluster(2)
	addShard(s, "a", 1, "n0", "n1")
	assert.Empty(t, newPlan(s).changing)
	assert.True(t, newPlan(s).allows(model.Candidate{Kind: model.Split, Shard: 1}))

	addShard(s, "a", 2, "n0", "n1").Changing = true
	addShard(s, "a", 3, "n0", "n1").Splitting = true
	pl := newPlan(s)

	assert.Equal(t, map[string]bool{"a": true}, pl.changing)
	assert.False(t, pl.allows(model.Candidate{Kind: model.Split, Shard: 1}), "a split is in progress")
}

func TestPlanAllowsOneOpPerShardAndPerTarget(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1")
	addShard(s, "a", 2, "n0", "n1")
	pl := newPlan(s)

	pl.add(model.Candidate{Kind: model.Replace, Namespace: "a", Shard: 1, From: "n0", To: "n2"})

	assert.False(t, pl.allows(model.Candidate{Kind: model.Replace, Shard: 1, From: "n1", To: "n0"}), "same shard")
	assert.False(t, pl.allows(model.Candidate{Kind: model.TransferLeader, Shard: 2, From: "n0", To: "n2"}), "same target")
	assert.True(t, pl.allows(model.Candidate{Kind: model.TransferLeader, Shard: 2, From: "n0", To: "n1"}))
}

func TestPlanAllowsTheCopiesUpToTheLimit(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1")
	s.DataServers["n2"].Copying = 2
	replace := model.Candidate{Kind: model.Replace, Namespace: "a", Shard: 1, From: "n0", To: "n2"}
	transfer := model.Candidate{Kind: model.TransferLeader, Namespace: "a", Shard: 1, From: "n0", To: "n2"}

	assert.True(t, newPlan(s).allows(replace), "no limit")

	s.MaxCopies = 2
	assert.False(t, newPlan(s).allows(replace))
	assert.True(t, newPlan(s).allows(transfer), "a transfer copies no replica")

	s.MaxCopies = 3
	assert.True(t, newPlan(s).allows(replace))
}

func TestPlanAllowsOneSplitAtATime(t *testing.T) {
	s := cluster(2)
	addShard(s, "a", 1, "n0", "n1")
	addShard(s, "a", 2, "n0", "n1")
	pl := newPlan(s)

	pl.add(model.Candidate{Kind: model.Split, Namespace: "a", Shard: 1})

	assert.False(t, pl.allows(model.Candidate{Kind: model.Split, Namespace: "a", Shard: 2}))
}

func TestPlanEndPhaseHoldsTheNamespacesNotSettled(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1")
	addShard(s, "b", 2, "n0", "n1").Changing = true
	addShard(s, "c", 3, "n0", "n1")
	pl := newPlan(s)

	pl.add(model.Candidate{Kind: model.Replace, Namespace: "a", Shard: 1, From: "n0", To: "n2"})
	assert.Empty(t, pl.waiting, "a phase holds its namespaces only once it ends")
	pl.endPhase(model.Replace)

	assert.Equal(t, map[string]bool{"a": true, "b": true}, pl.waiting)
	assert.False(t, pl.allWaiting())
	assert.False(t, pl.allows(model.Candidate{Kind: model.TransferLeader, Namespace: "a", Shard: 4, To: "n1"}))
	assert.True(t, pl.allows(model.Candidate{Kind: model.TransferLeader, Namespace: "c", Shard: 3, To: "n1"}))
	assert.False(t, pl.allows(model.Candidate{Kind: model.Split, Namespace: "a", Shard: 1}))
	assert.True(t, pl.allows(model.Candidate{Kind: model.Split, Namespace: "c", Shard: 3}))

	pl.add(model.Candidate{Kind: model.TransferLeader, Namespace: "c", Shard: 3, From: "n0", To: "n1"})
	pl.endPhase(model.TransferLeader)
	assert.True(t, pl.allWaiting())
}

func TestPlanEndPhaseHoldsTheChangesOnlyAfterTheReplaces(t *testing.T) {
	s := cluster(2)
	addShard(s, "a", 1, "n0", "n1").Changing = true
	pl := newPlan(s)

	pl.endPhase(model.TransferLeader)

	assert.Empty(t, pl.waiting)
}

func TestPlanAddReturnsTheActionOfEachKind(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"
	addShard(s, "a", 2, "n0", "n1").Leader = "n0"
	addShard(s, "a", 3, "n0", "n1").Leader = "n0"
	pl := newPlan(s)

	pl.add(model.Candidate{Kind: model.Replace, Namespace: "a", Shard: 1, From: "n1", To: "n2"})
	pl.add(model.Candidate{Kind: model.TransferLeader, Namespace: "a", Shard: 2, From: "n0", To: "n1"})
	pl.add(model.Candidate{Kind: model.Split, Namespace: "a", Shard: 3})

	actions := pl.actions
	assert.Len(t, actions, 3)
	change := actions[0].(*action.ChangeEnsembleAction)
	assert.Equal(t, int64(1), change.Shard)
	assert.Equal(t, "n1", change.From.GetInternal())
	assert.Equal(t, "n2", change.To.GetInternal())
	assert.Equal(t, "n1", actions[1].(*action.ElectionAction).PreferredLeader)
	split := actions[2].(*action.SplitAction)
	assert.Equal(t, "a", split.Namespace)
	assert.Equal(t, int64(3), split.Shard)
	assert.Equal(t, map[string]int{"n0": 3, "n1": 2, "n2": 1}, pl.dataServerLoad.GetTotalReplicas(),
		"the replace moved a replica of n1 onto n2")
}
