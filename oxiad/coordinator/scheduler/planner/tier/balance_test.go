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
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/load"
)

func TestBalanceNeedsAGapOfTwo(t *testing.T) {
	// a: n0 has 2, n1 1, n2 0
	s := snapshot(map[string][][]string{"a": {{"n0", "n1"}, {"n0", "r"}}})
	l := load.New(s)

	delta, enough := NamespaceReplicas().Delta(s, l, model.Op{Namespace: "a", From: "n0", To: "n2"}, 0)
	assert.Equal(t, -2, delta)
	assert.True(t, enough)

	delta, enough = NamespaceReplicas().Delta(s, l, model.Op{Namespace: "a", From: "n1", To: "n2"}, 0)
	assert.Zero(t, delta)
	assert.False(t, enough)
}

func TestBalanceCountsTheTotal(t *testing.T) {
	s := snapshot(map[string][][]string{"a": {{"n0", "n2"}}, "b": {{"n0", "n2"}}})
	l := load.New(s)

	delta, enough := TotalReplicas().Delta(s, l, model.Op{Namespace: "a", From: "n0", To: "n1"}, 0)

	assert.Equal(t, -2, delta)
	assert.True(t, enough)
}

func TestBalanceAddsTheReplicaOfADrain(t *testing.T) {
	s := snapshot(map[string][][]string{"a": {{"r", "n0"}}})

	delta, enough := NamespaceReplicas().Delta(s, load.New(s), model.Op{Namespace: "a", From: "r", To: "n0"}, 0)

	assert.Equal(t, 3, delta)
	assert.False(t, enough)
}

func TestBalanceMovesLeadersWithoutTolerance(t *testing.T) {
	// a: n0 leads 2 of 3 shards, n1 none; a tolerance of 0.9 would need 3
	s := snapshot(map[string][][]string{"a": {{"n0", "n1"}, {"n0", "n1"}, {"n2", "n1"}}})
	for _, shard := range s.Namespaces["a"].Shards {
		shard.Leader = shard.Ensemble[0]
	}
	l := load.New(s)
	transfer := model.Op{Kind: model.TransferLeader, Namespace: "a", From: "n0", To: "n1"}

	delta, enough := NamespaceLeaders().Delta(s, l, transfer, 0.9)
	assert.Equal(t, -2, delta)
	assert.True(t, enough)
	delta, enough = TotalLeaders().Delta(s, l, transfer, 0.9)
	assert.Equal(t, -2, delta)
	assert.True(t, enough)

	// The replicas don't move
	delta, enough = NamespaceReplicas().Delta(s, l, transfer, 0)
	assert.Zero(t, delta)
	assert.False(t, enough)
}

func TestThresholdIsAtLeastTwo(t *testing.T) {
	assert.Equal(t, 2, threshold(10, 0.05))
	assert.Equal(t, 5, threshold(100, 0.05))
}
