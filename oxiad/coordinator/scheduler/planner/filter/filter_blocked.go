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

package filter

import (
	"cmp"
	"maps"
	"slices"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// Blocked returns the replicas on removed data servers that filters reject
// whatever their target, so that their drain can't complete: e.g. the only
// data server of a zone, with an anti-affinity on the zone. The reason is the
// error of the filter that rejects the first data server that could otherwise
// take the replica. It ignores the limits of a plan, and skips the shards that
// a change, a split or a backoff holds.
func Blocked(filters []Filter, s *stats.ClusterStats) []model.Blocked {
	dataServers := slices.Sorted(maps.Keys(s.DataServers))
	var blocked []model.Blocked
	for _, name := range slices.Sorted(maps.Keys(s.Namespaces)) {
		ns := s.Namespaces[name]
		for _, id := range slices.Sorted(maps.Keys(ns.Shards)) {
			shard := ns.Shards[id]
		replicas:
			for _, from := range shard.Ensemble {
				if shard.Changing || shard.Splitting || shard.BackingOff || !s.DataServers[from].Removed {
					continue
				}
				op := model.Candidate{Namespace: name, Shard: id, From: from}
				reason := ""
				for _, to := range dataServers {
					op.To = to
					switch err := Check(filters, s, op); {
					case err == nil:
						continue replicas
					case reason == "" && !s.DataServers[to].Removed && !slices.Contains(shard.Ensemble, to):
						reason = err.Error()
					default:
					}
				}
				blocked = append(blocked, model.Blocked{Namespace: name, Shard: id, DataServer: from,
					Reason: cmp.Or(reason, "no data server can take a replica")})
			}
		}
	}
	return blocked
}
