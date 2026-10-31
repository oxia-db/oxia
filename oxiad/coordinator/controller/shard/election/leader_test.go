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

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

func TestFewestLeadersCountsTheNamespaceFirst(t *testing.T) {
	s1, s2, s3 := testDataServer("s1"), testDataServer("s2"), testDataServer("s3")
	namespaces := map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
		// In a, s1 leads 1 shard and s2 none; in total s2 leads 2
		"a": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
			1: {Leader: s1},
			2: {Leader: s3, Status: proto.ShardStatusDeleting},
			3: {},
		}}),
		"b": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
			4: {Leader: s2},
			5: {Leader: s2},
		}}),
	}

	assert.Equal(t, s2, fewestLeaders([]*proto.DataServerIdentity{s1, s2}, namespaces, "a"))
	assert.Equal(t, s1, fewestLeaders([]*proto.DataServerIdentity{s1, s2}, namespaces, "b"))
	assert.Equal(t, s3, fewestLeaders([]*proto.DataServerIdentity{s3, s1}, namespaces, "c"))
	assert.Nil(t, fewestLeaders(nil, namespaces, "a"))
}
