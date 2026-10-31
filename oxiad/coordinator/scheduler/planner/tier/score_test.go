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

func TestEvaluateImprovesTheTotalOnANamespaceTie(t *testing.T) {
	s := snapshot(map[string][][]string{"a": {{"n0", "n2"}}, "b": {{"n0", "n2"}}})
	l := load.New(s)
	op := model.Op{Namespace: "a", From: "n0", To: "n1"}

	// a has a gap of 1, the total a gap of 2
	assert.Equal(t, Score{Deltas: []int{0, 0, -2, 0, 0}, Improved: 2}, Evaluate(Defaults(), s, l, op, 0))

	// A gap of 1 in the total doesn't improve it
	l.Move(s, model.Op{Namespace: "b", From: "n2", To: "n1"})
	assert.Equal(t, -1, Evaluate(Defaults(), s, l, op, 0).Improved)
}

func TestEvaluateStopsAtAnEarlierTierItRaises(t *testing.T) {
	// a: n0 1, n1 2, n2 1; the total of n0 is the highest
	s := snapshot(map[string][][]string{"a": {{"n0", "n1"}, {"n2", "n1"}}, "b": {{"n0", "n1"}, {"n0", "n1"}}})

	sc := Evaluate(Defaults(), s, load.New(s), model.Op{Namespace: "a", From: "n0", To: "n2"}, 0)

	assert.Equal(t, Score{Deltas: []int{0, 2, 0, 0, 0}, Improved: -1}, sc)
}

func TestEvaluateDrainsFirst(t *testing.T) {
	s := snapshot(map[string][][]string{"a": {{"r", "n0"}}})

	sc := Evaluate(Defaults(), s, load.New(s), model.Op{Namespace: "a", From: "r", To: "n0"}, 0)

	assert.Equal(t, Score{Deltas: []int{-1, 3, 3, 0, 0}, Improved: 0}, sc)
}

func TestScoreCompareRanksTheTiersInOrder(t *testing.T) {
	drain := Score{Deltas: []int{-1, 3, 3}}
	balance := Score{Deltas: []int{0, -2, -2}}

	assert.Negative(t, drain.Compare(balance))
	assert.Positive(t, balance.Compare(drain))
	assert.Zero(t, balance.Compare(Score{Deltas: []int{0, -2, -2}}))
}
