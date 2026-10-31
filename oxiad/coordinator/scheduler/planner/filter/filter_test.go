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

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

func TestDefaults(t *testing.T) {
	var names []string
	for _, f := range Defaults() {
		names = append(names, f.Name())
	}

	assert.Equal(t, []string{"frozen", "replicated", "membership", "target", "source", "members", "anti-affinity"},
		names)
}

// snapshot has the running, stable data servers n0 to n2 in zones z0 to z2,
// n3 without a zone, and a shard 1 of namespace a on n0 and n1.
func snapshot() *model.Snapshot {
	node := func(zone string) *model.DataServer {
		n := &model.DataServer{Running: true, Stable: true}
		if zone != "" {
			n.Labels = map[string]string{"zone": zone}
		}
		return n
	}
	return &model.Snapshot{
		DataServers: map[string]*model.DataServer{"n0": node("z0"), "n1": node("z1"), "n2": node("z2"), "n3": node("")},
		Namespaces: map[string]*model.Namespace{"a": {Shards: map[int64]*model.Shard{
			1: {Ensemble: []string{"n0", "n1"}},
		}}},
	}
}

// move moves the replica of shard 1 from from to to.
func move(from string, to string) model.Op {
	return model.Op{Namespace: "a", Shard: 1, From: from, To: to}
}
