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

package election

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
)

func TestFewestLeadersFailsWithoutCandidates(t *testing.T) {
	_, err := fewestLeaders(nil, nil)

	assert.ErrorIs(t, err, errs.ErrNotFound)
}

func TestFewestLeadersCountsTheLeadersOfEveryNamespace(t *testing.T) {
	s1, s2, s3 := server("s1"), server("s2"), server("s3")
	namespaces := map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
		"a": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
			0: {Leader: s1},
			1: {Leader: s2},
		}}),
		"b": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
			2: {Leader: s1},
			3: {},
		}}),
	}

	leader, err := fewestLeaders([]*proto.DataServerIdentity{s1, s2, s3}, namespaces)
	require.NoError(t, err)
	assert.Same(t, s3, leader)

	leader, err = fewestLeaders([]*proto.DataServerIdentity{s1, s2}, namespaces)
	require.NoError(t, err)
	assert.Same(t, s2, leader)
}

func TestFewestLeadersSkipsTheShardsBeingDeletedOrSplit(t *testing.T) {
	s1, s2 := server("s1"), server("s2")
	namespaces := map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
		"a": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
			0: {Leader: s1, Status: proto.ShardStatusDeleting},
			1: {Leader: s1, Split: &proto.SplitMetadata{}},
			2: {Leader: s2},
		}}),
	}

	leader, err := fewestLeaders([]*proto.DataServerIdentity{s2, s1}, namespaces)

	require.NoError(t, err)
	assert.Same(t, s1, leader)
}

func TestFewestLeadersPicksTheFirstOnATie(t *testing.T) {
	s1, s2 := server("s1"), server("s2")

	leader, err := fewestLeaders([]*proto.DataServerIdentity{s2, s1}, nil)

	require.NoError(t, err)
	assert.Same(t, s2, leader)
}

func server(name string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{Internal: name, Public: name}
}
