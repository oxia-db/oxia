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

package stats

import (
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

type ClusterStats struct {
	DataServers map[string]*DataServerStats
	Namespaces  map[string]*NamespaceStats
}

func (s *ClusterStats) GetShard(id int64) *ShardStats {
	for _, ns := range s.Namespaces {
		if shard, ok := ns.Shards[id]; ok {
			return shard
		}
	}
	return nil
}

func New(dataServers map[string]commonobject.Borrowed[*proto.DataServer],
	namespaces map[string]commonobject.Borrowed[*proto.Namespace],
	statuses map[string]commonobject.Borrowed[*proto.NamespaceStatus]) *ClusterStats {
	s := &ClusterStats{
		DataServers: make(map[string]*DataServerStats, len(dataServers)),
		Namespaces:  make(map[string]*NamespaceStats, len(statuses)),
	}
	for name, borrowed := range dataServers {
		s.DataServers[name] = &DataServerStats{DataServer: borrowed.UnsafeBorrow()}
	}
	for name, borrowed := range statuses {
		ns := &NamespaceStats{Shards: make(map[int64]*ShardStats)}
		if config, ok := namespaces[name]; ok {
			ns.AntiAffinities = config.UnsafeBorrow().GetAntiAffinities()
		}
		for id, shard := range borrowed.UnsafeBorrow().GetShards() {
			if shard.GetStatusOrDefault() == proto.ShardStatusDeleting {
				ns.DeletingShards++
				continue
			}
			ensemble := make([]string, 0, len(shard.GetEnsemble()))
			for _, member := range shard.GetEnsemble() {
				memberName := member.GetNameOrDefault()
				if _, ok := s.DataServers[memberName]; !ok {
					s.DataServers[memberName] = &DataServerStats{DataServer: &proto.DataServer{Identity: member}, Removed: true}
				}
				ensemble = append(ensemble, memberName)
			}
			hashRange := shard.GetInt32HashRange()
			ns.Shards[id] = &ShardStats{
				Ensemble:  ensemble,
				Leader:    shard.GetLeader().GetNameOrDefault(),
				Changing:  shard.GetEnsembleChange() != nil,
				Splitting: shard.GetSplit() != nil,
				Splittable: shard.GetStatusOrDefault() == proto.ShardStatusSteadyState &&
					hashRange.GetMax()-hashRange.GetMin() >= 1 && len(shard.GetPendingDeleteShardNodes()) == 0,
			}
		}
		s.Namespaces[name] = ns
	}
	return s
}
