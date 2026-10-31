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

package dataserver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestMatchDataServerMatchesByName(t *testing.T) {
	name := "s1"
	s1 := &proto.DataServerIdentity{Name: &name, Public: "s1:9091", Internal: "s1:8191"}
	moved := &proto.DataServerIdentity{Name: &name, Public: "s1-new:9091", Internal: "s1-new:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}

	assert.True(t, MatchDataServer(s1)(moved))
	assert.False(t, MatchDataServer(s1)(s2))
}

func TestAddDataServerSkipsTheSameDataServer(t *testing.T) {
	name := "s1"
	s1 := &proto.DataServerIdentity{Name: &name, Public: "s1:9091", Internal: "s1:8191"}
	moved := &proto.DataServerIdentity{Name: &name, Public: "s1-new:9091", Internal: "s1-new:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}

	servers := AddDataServer(nil, s1)
	servers = AddDataServer(servers, moved)
	servers = AddDataServer(servers, s2)
	assert.Equal(t, []*proto.DataServerIdentity{s1, s2}, servers)
}

func TestShardDataServersYieldsEveryReference(t *testing.T) {
	server := func(name string) *proto.DataServerIdentity { return &proto.DataServerIdentity{Name: &name} }
	shard := &proto.ShardMetadata{
		Leader:                  server("leader"),
		Ensemble:                []*proto.DataServerIdentity{server("e1"), server("e2")},
		EnsembleChange:          &proto.EnsembleChangeMetadata{From: server("from"), To: server("to")},
		PendingDeleteShardNodes: []*proto.DataServerIdentity{server("pending")},
	}

	var names []string
	for s := range ShardDataServers(shard) {
		names = append(names, s.GetNameOrDefault())
	}
	assert.ElementsMatch(t, []string{"leader", "from", "to", "e1", "e2", "pending"}, names)

	for s := range ShardDataServers(shard) {
		assert.Equal(t, "leader", s.GetNameOrDefault())
		break
	}
}

func TestShardDataServersSkipsMissingReferences(t *testing.T) {
	for range ShardDataServers(&proto.ShardMetadata{}) {
		assert.Fail(t, "a shard without data servers yields none")
	}
}
