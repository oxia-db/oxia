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
// rejects the candidates that break one.
package filter

import (
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

// Filter rejects a candidate that breaks a hard constraint, with the reason.
type Filter interface {
	Name() string
	Filter(s *model.Snapshot, op model.Op) error
}

// Defaults returns the filters of the scheduler, the cheapest first.
func Defaults() []Filter {
	return []Filter{Frozen{}, Replicated{}, Membership{}, Target{}, Source{}, Members{}, AntiAffinity{}}
}
