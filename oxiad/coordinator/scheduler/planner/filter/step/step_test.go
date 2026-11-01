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

package step

import (
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/internal/candidate"
)

// newStats has the data servers n0 to n2 in zones z0 to z2,
// n3 without a zone, and a shard 1 of namespace a on n0 and n1.
func newStats() *stats.ClusterStats {
	node := func(zone string) *stats.DataServerStats {
		n := &stats.DataServerStats{DataServer: &proto.DataServer{Metadata: &proto.DataServerMetadata{}}}
		if zone != "" {
			n.Metadata.Labels = map[string]string{"zone": zone}
		}
		return n
	}
	return &stats.ClusterStats{
		DataServers: map[string]*stats.DataServerStats{"n0": node("z0"), "n1": node("z1"), "n2": node("z2"), "n3": node("")},
		Namespaces: map[string]*stats.NamespaceStats{"a": {Shards: map[int64]*stats.ShardStats{
			1: {Ensemble: []string{"n0", "n1"}},
		}}},
	}
}

// split splits shard 1.
var split = candidate.Candidate{Kind: candidate.Split, Namespace: "a", Shard: 1}

// move moves the replica of shard 1 from from to to.
func move(from string, to string) candidate.Candidate {
	return candidate.Candidate{Namespace: "a", Shard: 1, From: from, To: to}
}
