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
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/internal/candidate"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/load"
)

// Balance evens out what counts returns for the namespace of an op, on the
// data servers in the configuration, with the ops of kind.
type Balance struct {
	name   string
	kind   candidate.Kind
	counts func(l *load.Load, namespace string) map[string]int
}

// NamespaceReplicas evens out the replicas of each namespace.
func NamespaceReplicas() Balance {
	return Balance{name: "namespace-replicas", kind: candidate.Replace, counts: (*load.Load).GetReplicas}
}

// TotalReplicas evens out the replicas of every namespace together.
func TotalReplicas() Balance {
	return Balance{name: "total-replicas", kind: candidate.Replace,
		counts: func(l *load.Load, _ string) map[string]int { return l.GetTotalReplicas() }}
}

// NamespaceLeaders evens out the leaders of each namespace.
func NamespaceLeaders() Balance {
	return Balance{name: "namespace-leaders", kind: candidate.TransferLeader, counts: (*load.Load).GetLeaders}
}

// TotalLeaders evens out the leaders of every namespace together.
func TotalLeaders() Balance {
	return Balance{name: "total-leaders", kind: candidate.TransferLeader,
		counts: func(l *load.Load, _ string) map[string]int { return l.GetTotalLeaders() }}
}

func (b Balance) GetName() string {
	return b.name
}

func (b Balance) GetKind() candidate.Kind {
	return b.kind
}

// Delta is the change of the sum of the squares of the counts: it is negative
// when the source has at least 2 more than the target, so the counts end
// within 1 of each other. A replica on a removed data server isn't counted, so
// draining it only adds to the target.
func (b Balance) Delta(s *stats.ClusterStats, l *load.Load, op candidate.Candidate) int {
	counts := b.counts(l, op.Namespace)
	delta := 2*counts[op.To] + 1
	if s.DataServers[op.From].Removed {
		return delta
	}
	return delta - (2*counts[op.From] - 1)
}
