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

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

// The data servers of a shard are its leader, the members of its ensemble,
// and the ones being removed from it.
func TestGetStatusDataServersIncludesAllShardMembers(t *testing.T) {
	leader := &proto.DataServerIdentity{Public: "leader:9091", Internal: "leader:8191"}
	member := &proto.DataServerIdentity{Public: "member:9091", Internal: "member:8191"}
	removed := &proto.DataServerIdentity{Public: "removed:9091", Internal: "removed:8191"}
	pendingDelete := &proto.DataServerIdentity{Public: "pending:9091", Internal: "pending:8191"}
	status := map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
		"ns": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
			0: {
				Leader:                  leader,
				Ensemble:                []*proto.DataServerIdentity{leader, member},
				EnsembleChange:          &proto.EnsembleChangeMetadata{From: removed, To: member},
				PendingDeleteShardNodes: []*proto.DataServerIdentity{pendingDelete},
			},
			// Without a leader
			1: {Ensemble: []*proto.DataServerIdentity{member}},
		}}),
	}

	var names []string
	for _, server := range GetStatusDataServers(status, nil) {
		names = append(names, server.GetNameOrDefault())
	}
	assert.ElementsMatch(t, []string{
		leader.GetNameOrDefault(), member.GetNameOrDefault(),
		removed.GetNameOrDefault(), pendingDelete.GetNameOrDefault(),
	}, names)
}

// A data server stored DRAINING has to be stored DRAINED by its controller,
// even once no shard references it: only its name is known then. The other
// stored data servers are left out.
func TestGetStatusDataServersIncludesDrainingDataServers(t *testing.T) {
	member := &proto.DataServerIdentity{Name: new("member"), Public: "member:9091", Internal: "member:8191"}
	status := map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
		"ns": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
			0: {Ensemble: []*proto.DataServerIdentity{member}},
		}}),
	}
	stored := func(state proto.DataServerState) commonobject.Borrowed[*proto.DataServerStatus] {
		return commonobject.Borrow(&proto.DataServerStatus{State: state})
	}
	dataServerStatus := map[string]commonobject.Borrowed[*proto.DataServerStatus]{
		"member":   stored(proto.DataServerState_DATA_SERVER_STATE_DRAINING),
		"draining": stored(proto.DataServerState_DATA_SERVER_STATE_DRAINING),
		"drained":  stored(proto.DataServerState_DATA_SERVER_STATE_DRAINED),
		"running":  stored(proto.DataServerState_DATA_SERVER_STATE_RUNNING),
	}

	servers := map[string]*proto.DataServerIdentity{}
	for _, server := range GetStatusDataServers(status, dataServerStatus) {
		servers[server.GetNameOrDefault()] = server
	}
	assert.Len(t, servers, 2)
	assert.Same(t, member, servers["member"])
	assert.Equal(t, &proto.DataServerIdentity{Name: new("draining")}, servers["draining"])
}

func TestIsDrained(t *testing.T) {
	drained := &proto.DataServerIdentity{Name: new("drained"), Public: "drained:9190", Internal: "drained:8190"}
	other := &proto.DataServerIdentity{Public: "other:9190", Internal: "other:8190"}
	for _, test := range []struct {
		name     string
		shard    *proto.ShardMetadata
		expected bool
	}{
		{"caught up without it", &proto.ShardMetadata{Ensemble: []*proto.DataServerIdentity{other}, EnsembleCaughtUp: true}, true},
		{"not caught up", &proto.ShardMetadata{Ensemble: []*proto.DataServerIdentity{other}}, false},
		{"deleting", &proto.ShardMetadata{Ensemble: []*proto.DataServerIdentity{other}, Status: proto.ShardStatusDeleting}, true},
		{"ensemble", &proto.ShardMetadata{Ensemble: []*proto.DataServerIdentity{other, drained}, EnsembleCaughtUp: true}, false},
		{"removed by a change", &proto.ShardMetadata{
			EnsembleChange: &proto.EnsembleChangeMetadata{From: drained, To: other}, EnsembleCaughtUp: true}, false},
		{"added by a change", &proto.ShardMetadata{
			EnsembleChange: &proto.EnsembleChangeMetadata{From: other, To: drained}, EnsembleCaughtUp: true}, false},
		{"pending delete", &proto.ShardMetadata{
			PendingDeleteShardNodes: []*proto.DataServerIdentity{drained}, EnsembleCaughtUp: true}, true},
		{"deleting with it", &proto.ShardMetadata{Ensemble: []*proto.DataServerIdentity{drained}, Status: proto.ShardStatusDeleting}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			namespaces := map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
				"ns": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{0: test.shard}}),
			}
			assert.Equal(t, test.expected, IsDrained("drained", namespaces))
		})
	}
	assert.True(t, IsDrained("drained", nil))
}
