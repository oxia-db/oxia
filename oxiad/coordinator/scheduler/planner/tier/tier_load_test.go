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

package tier

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

func TestLoadCountsTheReplicasAndTheLeaders(t *testing.T) {
	s := newLoadStats()

	l := NewLoad(s)

	assert.Equal(t, map[string]int{"n0": 1}, l.GetReplicas("a"))
	assert.Equal(t, map[string]int{"n0": 1}, l.GetTotalReplicas())
	assert.Equal(t, map[string]int{"n0": 1}, l.GetLeaders("a"))
	assert.Equal(t, map[string]int{"n0": 1}, l.GetTotalLeaders())
}

func TestLoadMoveReplaces(t *testing.T) {
	s := newLoadStats()
	l := NewLoad(s)

	l.Move(s, model.Candidate{Namespace: "a", From: "r", To: "n1"})
	l.Move(s, model.Candidate{Namespace: "a", From: "n0", To: "n1"})
	l.Move(s, model.Candidate{Namespace: "b", From: "r", To: "n0"})

	assert.Equal(t, map[string]int{"n0": 0, "n1": 2}, l.GetReplicas("a"))
	assert.Equal(t, map[string]int{"n0": 1}, l.GetReplicas("b"))
	assert.Equal(t, map[string]int{"n0": 1, "n1": 2}, l.GetTotalReplicas())
	assert.Equal(t, map[string]int{"n0": 1}, l.GetTotalLeaders())
}

func TestLoadMoveTransfersTheLeader(t *testing.T) {
	s := newLoadStats()
	l := NewLoad(s)

	l.Move(s, model.Candidate{Kind: model.TransferLeader, Namespace: "a", From: "n0", To: "n1"})

	assert.Equal(t, map[string]int{"n0": 0, "n1": 1}, l.GetLeaders("a"))
	assert.Equal(t, map[string]int{"n0": 0, "n1": 1}, l.GetTotalLeaders())
	assert.Equal(t, map[string]int{"n0": 1}, l.GetTotalReplicas())
}

func TestLoadMoveLeavesTheLoadOfASplit(t *testing.T) {
	s := newLoadStats()
	l := NewLoad(s)

	l.Move(s, model.Candidate{Kind: model.Split, Namespace: "a", Shard: 1})

	assert.Equal(t, NewLoad(s), l)
}

// snapshot has a shard of namespace a on the removed data server r and on n0,
// its leader, and a shard of namespace c led by r.
func newLoadStats() *stats.ClusterStats {
	return &stats.ClusterStats{
		DataServers: map[string]*stats.DataServerStats{"n0": {}, "n1": {}, "r": {Removed: true}},
		Namespaces: map[string]*stats.NamespaceStats{
			"a": {Shards: map[int64]*stats.ShardStats{1: {Ensemble: []string{"r", "n0"}, Leader: "n0"}}},
			"c": {Shards: map[int64]*stats.ShardStats{2: {Ensemble: []string{"r"}, Leader: "r"}}},
		},
	}
}
