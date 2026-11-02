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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

func TestBlockedReportsTheReplicasThatNoDataServerCanTake(t *testing.T) {
	s := drainStats("n0", "n1")
	s.Namespaces = map[string]*stats.NamespaceStats{
		"single": {Shards: map[int64]*stats.ShardStats{1: {Ensemble: []string{"r"}}}},
		"zones": {AntiAffinities: []*proto.AntiAffinity{{Labels: []string{"zone"}}},
			Shards: map[int64]*stats.ShardStats{2: {Ensemble: []string{"r", "n0"}}}},
		"frozen": {Shards: map[int64]*stats.ShardStats{3: {Ensemble: []string{"r", "n0"}, Changing: true}}},
		"free":   {Shards: map[int64]*stats.ShardStats{4: {Ensemble: []string{"r", "n0"}}}},
	}

	assert.Equal(t, []model.Blocked{
		{Namespace: "single", Shard: 1, DataServer: "r", Reason: "replicated: failed precondition: shard 1 has a single replica"},
		{Namespace: "zones", Shard: 2, DataServer: "r", Reason: `anti-affinity: failed precondition: "n1" breaks the anti-affinity of shard 2`},
	}, Blocked(Defaults(), s))
}

func TestBlockedReportsAReplicaWithoutTarget(t *testing.T) {
	s := drainStats("n0")
	s.Namespaces = map[string]*stats.NamespaceStats{
		"a": {Shards: map[int64]*stats.ShardStats{1: {Ensemble: []string{"r", "n0"}}}},
	}

	assert.Equal(t, []model.Blocked{{Namespace: "a", Shard: 1, DataServer: "r", Reason: "no data server can take a replica"}},
		Blocked(Defaults(), s))
}

// drainStats has the data servers names, without labels, and the removed
// one r.
func drainStats(names ...string) *stats.ClusterStats {
	s := &stats.ClusterStats{DataServers: map[string]*stats.DataServerStats{"r": {Removed: true}}}
	for _, name := range names {
		s.DataServers[name] = &stats.DataServerStats{DataServer: &proto.DataServer{Identity: &proto.DataServerIdentity{Internal: name}}}
	}
	return s
}
