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
)

func TestBalanceNeedsAGapOfTwo(t *testing.T) {
	// a: n0 has 2, n1 1, n2 0
	s := newStats(map[string][][]string{"a": {{"n0", "n1"}, {"n0", "r"}}})
	l := NewLoad(s)

	delta := NamespaceReplicas().Delta(s, l, model.Candidate{Namespace: "a", From: "n0", To: "n2"})
	assert.Equal(t, -2, delta)

	delta = NamespaceReplicas().Delta(s, l, model.Candidate{Namespace: "a", From: "n1", To: "n2"})
	assert.Zero(t, delta)
}

func TestBalanceCountsTheTotal(t *testing.T) {
	s := newStats(map[string][][]string{"a": {{"n0", "n2"}}, "b": {{"n0", "n2"}}})
	l := NewLoad(s)

	delta := TotalReplicas().Delta(s, l, model.Candidate{Namespace: "a", From: "n0", To: "n1"})

	assert.Equal(t, -2, delta)
}

func TestBalanceAddsTheReplicaOfADrain(t *testing.T) {
	s := newStats(map[string][][]string{"a": {{"r", "n0"}}})

	delta := NamespaceReplicas().Delta(s, NewLoad(s), model.Candidate{Namespace: "a", From: "r", To: "n0"})

	assert.Equal(t, 3, delta)
}

func TestBalanceMovesTheLeaders(t *testing.T) {
	// a: n0 leads 2 of 3 shards, n1 none
	s := newStats(map[string][][]string{"a": {{"n0", "n1"}, {"n0", "n1"}, {"n2", "n1"}}})
	for _, shard := range s.Namespaces["a"].Shards {
		shard.Leader = shard.Ensemble[0]
	}
	l := NewLoad(s)
	transfer := model.Candidate{Kind: model.TransferLeader, Namespace: "a", From: "n0", To: "n1"}

	assert.Equal(t, -2, NamespaceLeaders().Delta(s, l, transfer))
	assert.Equal(t, -2, TotalLeaders().Delta(s, l, transfer))
}
