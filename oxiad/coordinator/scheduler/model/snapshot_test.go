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

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

func TestSnapshotConfiguredDataServers(t *testing.T) {
	s := &Snapshot{DataServers: map[string]*DataServer{"n0": {}, "n1": {}, "r": {Removed: true}}}

	assert.Equal(t, 2, s.ConfiguredDataServers())
}

func TestSnapshotShard(t *testing.T) {
	shard := &Shard{}
	ns := &Namespace{Shards: map[int64]*Shard{1: shard}}
	s := &Snapshot{Namespaces: map[string]*Namespace{"a": ns}}

	gotNamespace, gotShard := s.Shard(Op{Namespace: "a", Shard: 1})
	assert.Same(t, ns, gotNamespace)
	assert.Same(t, shard, gotShard)

	gotNamespace, gotShard = s.Shard(Op{Namespace: "a", Shard: 2})
	assert.Same(t, ns, gotNamespace)
	assert.Nil(t, gotShard)

	gotNamespace, gotShard = s.Shard(Op{Namespace: "b", Shard: 1})
	assert.Nil(t, gotNamespace)
	assert.Nil(t, gotShard)
}

func TestNewSnapshot(t *testing.T) {
	n0, r := &proto.DataServerIdentity{Internal: "n0"}, &proto.DataServerIdentity{Internal: "r"}
	zones := []*proto.AntiAffinity{{Labels: []string{"zone"}}}

	s := NewSnapshot(
		map[string]commonobject.Borrowed[*proto.DataServer]{
			"n0": commonobject.Borrow(&proto.DataServer{Identity: n0,
				Metadata: &proto.DataServerMetadata{Labels: map[string]string{"zone": "z0"}}}),
		},
		map[string]commonobject.Borrowed[*proto.Namespace]{"a": commonobject.Borrow(&proto.Namespace{AntiAffinities: zones})},
		map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
			"a": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
				1: {Ensemble: []*proto.DataServerIdentity{n0, r}, Leader: n0},
				2: {Ensemble: []*proto.DataServerIdentity{n0}, Split: &proto.SplitMetadata{}},
				3: {Ensemble: []*proto.DataServerIdentity{n0}, EnsembleChange: &proto.EnsembleChangeMetadata{}},
				4: {Ensemble: []*proto.DataServerIdentity{n0}, Status: proto.ShardStatusDeleting},
			}}),
			"b": commonobject.Borrow(&proto.NamespaceStatus{}),
		})

	assert.Equal(t, map[string]*DataServer{
		"n0": {Identity: n0, Labels: map[string]string{"zone": "z0"}},
		"r":  {Identity: r, Removed: true},
	}, s.DataServers)
	assert.Equal(t, &Namespace{AntiAffinities: zones, Shards: map[int64]*Shard{
		1: {Ensemble: []string{"n0", "r"}, Leader: "n0"},
		2: {Ensemble: []string{"n0"}, Frozen: true},
		3: {Ensemble: []string{"n0"}, Frozen: true, Changing: true},
	}}, s.Namespaces["a"])
	assert.Equal(t, &Namespace{Shards: map[int64]*Shard{}}, s.Namespaces["b"])
}
