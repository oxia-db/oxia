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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/internal/candidate"
)

func TestDefaults(t *testing.T) {
	var names []string
	var kinds []candidate.Kind
	for _, tier := range Defaults() {
		names = append(names, tier.GetName())
		kinds = append(kinds, tier.GetKind())
	}

	assert.Equal(t, []string{"drain", "namespace-replicas", "total-replicas", "namespace-leaders", "total-leaders",
		"split"}, names)
	assert.Equal(t, []candidate.Kind{candidate.Replace, candidate.Replace, candidate.Replace, candidate.TransferLeader,
		candidate.TransferLeader, candidate.Split}, kinds)
}

// snapshot has the data servers n0 to n2 and the removed one r, and the
// shards of namespace a and b.
func newStats(shards map[string][][]string) *stats.ClusterStats {
	s := &stats.ClusterStats{
		DataServers: map[string]*stats.DataServerStats{"n0": {}, "n1": {}, "n2": {}, "r": {Removed: true}},
		Namespaces:  make(map[string]*stats.NamespaceStats),
	}
	id := int64(0)
	for name, ensembles := range shards {
		ns := &stats.NamespaceStats{Shards: make(map[int64]*stats.ShardStats)}
		for _, ensemble := range ensembles {
			id++
			ns.Shards[id] = &stats.ShardStats{Ensemble: ensemble}
		}
		s.Namespaces[name] = ns
	}
	return s
}
