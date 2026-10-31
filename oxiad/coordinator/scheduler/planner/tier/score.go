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
	"slices"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/load"
)

// Score is the change of the potential of each tier that an op makes, in the
// order of the tiers, and the index of the tier that it improves, or -1.
type Score struct {
	Deltas   []int
	Improved int
}

// Evaluate scores op against tiers, in priority order. An op improves a tier
// when it lowers its potential enough without raising the potential of a tier
// before it: every op then lowers the potentials, in that order, and since
// they can't go below zero, the plans settle. It stops at the first tier that
// op raises before improving one.
func Evaluate(tiers []Tier, s *model.Snapshot, l *load.Load, op model.Op, tolerance float64) Score {
	sc := Score{Deltas: make([]int, len(tiers)), Improved: -1}
	for i, t := range tiers {
		delta, enough := t.Delta(s, l, op, tolerance)
		sc.Deltas[i] = delta
		if sc.Improved >= 0 {
			continue
		}
		if delta > 0 {
			return sc
		}
		if delta < 0 && enough {
			sc.Improved = i
		}
	}
	return sc
}

// Compare ranks two scores tier by tier: the lower, the better.
func (sc Score) Compare(other Score) int {
	return slices.Compare(sc.Deltas, other.Deltas)
}
