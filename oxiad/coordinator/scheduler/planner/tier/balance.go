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
	"math"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/load"
)

// Balance evens out what counts returns for the namespace of an op, on the
// data servers in the configuration, with the ops of kind.
type Balance struct {
	name string
	kind model.OpKind
	// tolerant balances apply the tolerance, for the ops that copy a replica.
	tolerant bool
	counts   func(l *load.Load, namespace string) map[string]int
}

// NamespaceReplicas evens out the replicas of each namespace.
func NamespaceReplicas() Balance {
	return Balance{name: "namespace-replicas", kind: model.Replace, tolerant: true, counts: (*load.Load).Replicas}
}

// TotalReplicas evens out the replicas of every namespace together.
func TotalReplicas() Balance {
	return Balance{name: "total-replicas", kind: model.Replace, tolerant: true,
		counts: func(l *load.Load, _ string) map[string]int { return l.TotalReplicas() }}
}

// NamespaceLeaders evens out the leaders of each namespace.
func NamespaceLeaders() Balance {
	return Balance{name: "namespace-leaders", kind: model.TransferLeader, counts: (*load.Load).Leaders}
}

// TotalLeaders evens out the leaders of every namespace together.
func TotalLeaders() Balance {
	return Balance{name: "total-leaders", kind: model.TransferLeader,
		counts: func(l *load.Load, _ string) map[string]int { return l.TotalLeaders() }}
}

func (b Balance) Name() string {
	return b.name
}

// Delta acts on a gap of at least 2 between the source and the target, the
// least that lowers the potential. A tolerant balance acts on tolerance times
// the mean if that is more, so that a small skew doesn't copy a whole shard;
// moving a leader is only an election.
func (b Balance) Delta(s *model.Snapshot, l *load.Load, op model.Op, tolerance float64) (int, bool) {
	if op.Kind != b.kind {
		return 0, false
	}
	counts := b.counts(l, op.Namespace)
	delta := 2*counts[op.To] + 1
	if s.DataServers[op.From].Removed {
		return delta, false
	}
	delta -= 2*counts[op.From] - 1
	if !b.tolerant {
		tolerance = 0
	}
	return delta, counts[op.From]-counts[op.To] >= threshold(l.Mean(counts), tolerance)
}

func threshold(mean float64, tolerance float64) int {
	return max(2, int(math.Ceil(tolerance*mean)))
}
