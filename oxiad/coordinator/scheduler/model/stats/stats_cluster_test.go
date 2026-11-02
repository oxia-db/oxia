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
	"testing"

	"github.com/stretchr/testify/assert"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

func TestClusterStatsShard(t *testing.T) {
	shard := &ShardStats{}
	s := &ClusterStats{Namespaces: map[string]*NamespaceStats{"a": {Shards: map[int64]*ShardStats{1: shard}}, "b": {}}}

	assert.Same(t, shard, s.GetShard(1))
	assert.Nil(t, s.GetShard(2))
}

func TestNew(t *testing.T) {
	n0, r := &proto.DataServerIdentity{Internal: "n0"}, &proto.DataServerIdentity{Internal: "r"}
	zones := []*proto.AntiAffinity{{Labels: []string{"zone"}}}
	dataServer := &proto.DataServer{Identity: n0, Metadata: &proto.DataServerMetadata{Labels: map[string]string{"zone": "z0"}}}

	s := New(
		map[string]commonobject.Borrowed[*proto.DataServer]{
			"n0": commonobject.Borrow(dataServer),
		},
		map[string]commonobject.Borrowed[*proto.Namespace]{"a": commonobject.Borrow(&proto.Namespace{AntiAffinities: zones})},
		map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
			"a": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
				1: {Ensemble: []*proto.DataServerIdentity{n0, r}, Leader: n0, Status: proto.ShardStatusSteadyState,
					Int32HashRange: &proto.HashRange{Min: 0, Max: 9}},
				2: {Ensemble: []*proto.DataServerIdentity{n0}, Split: &proto.SplitMetadata{}},
				3: {Ensemble: []*proto.DataServerIdentity{n0}, EnsembleChange: &proto.EnsembleChangeMetadata{From: r, To: n0}},
				4: {Ensemble: []*proto.DataServerIdentity{n0}, Status: proto.ShardStatusDeleting},
				5: {Ensemble: []*proto.DataServerIdentity{n0}, Status: proto.ShardStatusSteadyState,
					Int32HashRange: &proto.HashRange{Min: 3, Max: 3}},
				6: {Ensemble: []*proto.DataServerIdentity{n0}, Status: proto.ShardStatusSteadyState,
					Int32HashRange: &proto.HashRange{Min: 0, Max: 9}, PendingDeleteShardNodes: []*proto.DataServerIdentity{r}},
			}}),
			"b": commonobject.Borrow(&proto.NamespaceStatus{}),
		})

	assert.Equal(t, map[string]*DataServerStats{
		"n0": {DataServer: dataServer, Copying: 1},
		"r":  {DataServer: &proto.DataServer{Identity: r}, Removed: true},
	}, s.DataServers)
	assert.Equal(t, &NamespaceStats{AntiAffinities: zones, Shards: map[int64]*ShardStats{
		1: {Ensemble: []string{"n0", "r"}, Leader: "n0", Splittable: true},
		2: {Ensemble: []string{"n0"}, Splitting: true},
		3: {Ensemble: []string{"n0"}, Changing: true},
		5: {Ensemble: []string{"n0"}},
		6: {Ensemble: []string{"n0"}},
	}, DeletingShards: 1}, s.Namespaces["a"])
	assert.Equal(t, &NamespaceStats{Shards: map[int64]*ShardStats{}}, s.Namespaces["b"])
}
