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

// Package tier holds the objectives of the planner. Each tier has a potential,
// the sum of the squared loads of its data servers, that goes down as the load
// evens out, and scores one kind of op: the phase of the planner for that kind
// applies it.
package tier

import (
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/internal/candidate"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/load"
)

// Tier is an objective of the planner.
type Tier interface {
	GetName() string
	// GetKind is the kind of the ops that the tier scores.
	GetKind() candidate.Kind
	// Delta returns the change of the potential that op, of the tier's kind,
	// makes: op improves the tier if it is negative.
	Delta(s *stats.ClusterStats, l *load.Load, op candidate.Candidate) int
}

// Defaults returns the tiers of the scheduler, in priority order: the
// replicas before the leaders, since a leader is one of the replicas, and the
// leaders before the splits, which place new replicas and elect their leaders.
func Defaults() []Tier {
	return []Tier{Drain{}, NamespaceReplicas(), TotalReplicas(), NamespaceLeaders(), TotalLeaders(), Overshoot{}}
}
