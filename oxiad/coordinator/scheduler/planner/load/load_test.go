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

package load

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

func TestLoadCountsTheReplicasAndTheLeaders(t *testing.T) {
	s := snapshot()

	l := New(s)

	assert.Equal(t, map[string]int{"n0": 1}, l.Replicas("a"))
	assert.Equal(t, map[string]int{"n0": 1}, l.TotalReplicas())
	assert.Equal(t, map[string]int{"n0": 1}, l.Leaders("a"))
	assert.Equal(t, map[string]int{"n0": 1}, l.TotalLeaders())
	assert.InDelta(t, 0.5, l.Mean(l.TotalReplicas()), 0)
}

func TestLoadMoveReplaces(t *testing.T) {
	s := snapshot()
	l := New(s)

	l.Move(s, model.Op{Namespace: "a", From: "r", To: "n1"})
	l.Move(s, model.Op{Namespace: "a", From: "n0", To: "n1"})
	l.Move(s, model.Op{Namespace: "b", From: "r", To: "n0"})

	assert.Equal(t, map[string]int{"n0": 0, "n1": 2}, l.Replicas("a"))
	assert.Equal(t, map[string]int{"n0": 1}, l.Replicas("b"))
	assert.Equal(t, map[string]int{"n0": 1, "n1": 2}, l.TotalReplicas())
	assert.Equal(t, map[string]int{"n0": 1}, l.TotalLeaders())
}

func TestLoadMoveTransfersTheLeader(t *testing.T) {
	s := snapshot()
	l := New(s)

	l.Move(s, model.Op{Kind: model.TransferLeader, Namespace: "a", From: "n0", To: "n1"})

	assert.Equal(t, map[string]int{"n0": 0, "n1": 1}, l.Leaders("a"))
	assert.Equal(t, map[string]int{"n0": 0, "n1": 1}, l.TotalLeaders())
	assert.Equal(t, map[string]int{"n0": 1}, l.TotalReplicas())
}

// snapshot has a shard of namespace a on the removed data server r and on n0,
// its leader, and a shard of namespace c led by r.
func snapshot() *model.Snapshot {
	return &model.Snapshot{
		DataServers: map[string]*model.DataServer{"n0": {}, "n1": {}, "r": {Removed: true}},
		Namespaces: map[string]*model.Namespace{
			"a": {Shards: map[int64]*model.Shard{1: {Ensemble: []string{"r", "n0"}, Leader: "n0"}}},
			"c": {Shards: map[int64]*model.Shard{2: {Ensemble: []string{"r"}, Leader: "r"}}},
		},
	}
}
