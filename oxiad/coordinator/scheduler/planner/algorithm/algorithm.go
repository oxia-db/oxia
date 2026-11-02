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

// Package algorithm holds the searches of the planner: the greedy search of the
// single best op, and the paths of leader transfers. They read the snapshot,
// the load that the ops planned so far leave, and the limits of the plan.
package algorithm

import (
	"maps"
	"slices"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// forEachCandidate yields every op of kind with its shard: for a replace,
// every member of a shard replaced with every data server; for a transfer,
// the leadership of a shard moved to every other member; for a split, every
// shard.
func forEachCandidate(s *stats.ClusterStats, dataServers []string, kind model.Kind,
	yield func(model.Candidate, *stats.ShardStats)) {
	for _, name := range slices.Sorted(maps.Keys(s.Namespaces)) {
		ns := s.Namespaces[name]
		for _, id := range slices.Sorted(maps.Keys(ns.Shards)) {
			shard := ns.Shards[id]
			op := model.Candidate{Kind: kind, Namespace: name, Shard: id}
			switch kind {
			case model.Split:
				yield(op, shard)
			case model.TransferLeader:
				op.From = shard.Leader
				for _, to := range shard.Ensemble {
					if shard.Leader != "" && to != shard.Leader {
						op.To = to
						yield(op, shard)
					}
				}
			default:
				for _, from := range shard.Ensemble {
					for _, to := range dataServers {
						op.From, op.To = from, to
						yield(op, shard)
					}
				}
			}
		}
	}
}
