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

// Package candidate is the change of a shard that the planner enumerates,
// scores and picks, before it turns the ones it picks into actions.
package candidate

import "github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"

// Kind is what a candidate changes.
type Kind int

const (
	// Replace replaces the member From of the ensemble of a shard with To,
	// which copies a whole replica onto To and runs an election.
	Replace Kind = iota
	// TransferLeader moves the leadership of a shard from its leader From to
	// the member To, with an election that prefers To.
	TransferLeader
	// Split splits a shard in two children at the middle of its hash range,
	// placed as new shards. It has no From and no To.
	Split
)

func (k Kind) String() string {
	switch k {
	case TransferLeader:
		return "transfer-leader"
	case Split:
		return "split"
	default:
		return "replace"
	}
}

// Candidate is a change of a shard that the planner considers.
type Candidate struct {
	Kind      Kind
	Namespace string
	Shard     int64
	From      string
	To        string
	// Reason says which objective the candidate improves, for the decision
	// log.
	Reason string
}

// GetShard returns the namespace and the shard of c in s, or nil if s doesn't
// have them.
func (c Candidate) GetShard(s *stats.ClusterStats) (*stats.NamespaceStats, *stats.ShardStats) {
	ns, ok := s.Namespaces[c.Namespace]
	if !ok {
		return nil, nil
	}
	return ns, ns.Shards[c.Shard]
}
