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

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

func TestDefaults(t *testing.T) {
	var names []string
	for _, tier := range Defaults() {
		names = append(names, tier.Name())
	}

	assert.Equal(t, []string{"drain", "namespace-replicas", "total-replicas", "namespace-leaders", "total-leaders"}, names)
}

// snapshot has the data servers n0 to n2 and the removed one r, and the
// shards of namespace a and b.
func snapshot(shards map[string][][]string) *model.Snapshot {
	s := &model.Snapshot{
		DataServers: map[string]*model.DataServer{"n0": {}, "n1": {}, "n2": {}, "r": {Removed: true}},
		Namespaces:  make(map[string]*model.Namespace),
	}
	id := int64(0)
	for name, ensembles := range shards {
		ns := &model.Namespace{Shards: make(map[int64]*model.Shard)}
		for _, ensemble := range ensembles {
			id++
			ns.Shards[id] = &model.Shard{Ensemble: ensemble}
		}
		s.Namespaces[name] = ns
	}
	return s
}
