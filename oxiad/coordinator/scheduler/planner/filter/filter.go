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

// Package filter holds the hard constraints of the planner: each filter
// rejects the candidates that break one, whatever their score, while the tiers
// only rank the candidates that every filter passes. The filters read only the
// stats of the round, which tell the data servers that are down: the dispatch
// checks them again before it sends an action.
// Each filter has its own file, and Defaults runs them in order: the cheapest
// checks first.
package filter

import (
	"fmt"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// Filter rejects a candidate that breaks a hard constraint, with the reason.
// A filter is pure: it reads s and op only, and gives the same answer for the
// same input, so that a plan is the same every time.
type Filter interface {
	// GetName prefixes the error of the filter, and so the reason of a blocked
	// replica.
	GetName() string
	// Filter returns nil if op keeps the constraint, or an error that wraps an
	// errs sentinel and says why not. A filter passes the kinds of op that its
	// constraint doesn't apply to.
	Filter(s *stats.ClusterStats, op model.Candidate) error
}

// Check returns the error of the first of filters that rejects op, prefixed
// with its name, or nil.
func Check(filters []Filter, s *stats.ClusterStats, op model.Candidate) error {
	for _, f := range filters {
		if err := f.Filter(s, op); err != nil {
			return fmt.Errorf("%s: %w", f.GetName(), err)
		}
	}
	return nil
}

// Defaults returns the filters of the scheduler, in the order of their steps:
// the cheapest first, since the planner stops at the first one that rejects a
// candidate and runs them on every candidate of a round.
func Defaults() []Filter {
	return []Filter{Frozen{}, Replicated{}, Membership{}, Target{}, Running{},
		AntiAffinity{}, Splittable{}}
}
