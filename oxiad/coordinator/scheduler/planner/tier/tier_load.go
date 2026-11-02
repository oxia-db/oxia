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
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// Load counts the replicas and the leaders of each data server in the
// configuration, by namespace and in total, for the tiers. A plan moves them as its ops would,
// one op after the other.
type Load struct {
	replicas     map[string]map[string]int
	totalReplica map[string]int
	leaders      map[string]map[string]int
	totalLeader  map[string]int
}

func NewLoad(s *stats.ClusterStats) *Load {
	l := &Load{replicas: make(map[string]map[string]int), totalReplica: make(map[string]int),
		leaders: make(map[string]map[string]int), totalLeader: make(map[string]int)}
	for name, ns := range s.Namespaces {
		for _, shard := range ns.Shards {
			for _, member := range shard.Ensemble {
				if !s.DataServers[member].Removed {
					add(l.replicas, l.totalReplica, name, member, 1)
				}
			}
			if leader, ok := s.DataServers[shard.Leader]; ok && !leader.Removed {
				add(l.leaders, l.totalLeader, name, shard.Leader, 1)
			}
		}
	}
	return l
}

// GetReplicas returns the replicas of namespace, by data server.
func (l *Load) GetReplicas(namespace string) map[string]int {
	return l.replicas[namespace]
}

// GetTotalReplicas returns the replicas of every namespace, by data server.
func (l *Load) GetTotalReplicas() map[string]int {
	return l.totalReplica
}

// GetLeaders returns the leaders of namespace, by data server.
func (l *Load) GetLeaders(namespace string) map[string]int {
	return l.leaders[namespace]
}

// GetTotalLeaders returns the leaders of every namespace, by data server.
func (l *Load) GetTotalLeaders() map[string]int {
	return l.totalLeader
}

// Move applies op. The leader that the election of a replace elects is left
// where it is, and a split leaves the load as it is until its children are
// placed: the plan can't know them.
func (l *Load) Move(s *stats.ClusterStats, op model.Candidate) {
	if op.Kind == model.Split {
		return
	}
	counts, total := l.replicas, l.totalReplica
	if op.Kind == model.TransferLeader {
		counts, total = l.leaders, l.totalLeader
	}
	if !s.DataServers[op.From].Removed {
		add(counts, total, op.Namespace, op.From, -1)
	}
	add(counts, total, op.Namespace, op.To, 1)
}

func add(counts map[string]map[string]int, total map[string]int, namespace string, dataServer string, n int) {
	if counts[namespace] == nil {
		counts[namespace] = make(map[string]int)
	}
	counts[namespace][dataServer] += n
	total[dataServer] += n
}
