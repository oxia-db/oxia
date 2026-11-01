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
	"slices"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

// FromStatus returns the data servers that the shards reference,
// and the ones stored as DRAINING: once no shard references a draining data
// server, only its stored status tells that it still has to be stored
// DRAINED, and its controller does it. The configuration isn't read: the
// reconciler drains the data servers that it doesn't have.
func FromStatus(status map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	dataServerStatus map[string]commonobject.Borrowed[*proto.DataServerStatus]) []*proto.DataServerIdentity {
	servers := make(map[string]*proto.DataServerIdentity)
	for _, borrowedNamespace := range status {
		for _, shard := range borrowedNamespace.UnsafeBorrow().GetShards() {
			for server := range ShardDataServers(shard) {
				servers[server.GetNameOrDefault()] = server
			}
		}
	}

	for name, borrowedStatus := range dataServerStatus {
		if _, exists := servers[name]; exists ||
			borrowedStatus.UnsafeBorrow().GetState() != proto.DataServerState_DATA_SERVER_STATE_DRAINING {
			continue
		}
		servers[name] = &proto.DataServerIdentity{Name: &name}
	}

	result := make([]*proto.DataServerIdentity, 0, len(servers))
	for _, server := range servers {
		result = append(result, server)
	}
	return result
}

// IsDrained reports whether no ensemble, nor change of an ensemble, has the
// data server, and every shard caught up: a change of the ensemble opens a new
// term, which resets the flag of the shard, so the shards that the data server
// left hold a full copy on every member of their new ensembles. A pending
// delete of its copy doesn't count: it only frees the data server's disk, and
// never completes if the data server is dead. The shards being deleted are
// left out of the second check: they don't catch up again, and their copies
// don't matter any more.
func IsDrained(name string, namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus]) bool {
	for _, namespace := range namespaces {
		for _, shard := range namespace.UnsafeBorrow().GetShards() {
			change := shard.GetEnsembleChange()
			if change.GetFrom().GetNameOrDefault() == name || change.GetTo().GetNameOrDefault() == name ||
				slices.ContainsFunc(shard.GetEnsemble(), func(server *proto.DataServerIdentity) bool {
					return server.GetNameOrDefault() == name
				}) {
				return false
			}
			if shard.GetStatusOrDefault() != proto.ShardStatusDeleting && !shard.GetEnsembleCaughtUp() {
				return false
			}
		}
	}
	return true
}
