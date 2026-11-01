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
	"iter"
	"slices"

	"github.com/oxia-db/oxia/common/proto"
)

// MatchDataServer returns a predicate matching a data server by name, the same
// data server whatever its addresses, e.g. for slices.ContainsFunc.
func MatchDataServer(server *proto.DataServerIdentity) func(*proto.DataServerIdentity) bool {
	name := server.GetNameOrDefault()
	return func(other *proto.DataServerIdentity) bool {
		return other.GetNameOrDefault() == name
	}
}

// AddDataServer appends the data server to the list, unless the list has it
// already.
func AddDataServer(servers []*proto.DataServerIdentity, server *proto.DataServerIdentity) []*proto.DataServerIdentity {
	if slices.ContainsFunc(servers, MatchDataServer(server)) {
		return servers
	}
	return append(servers, server)
}

// GetShardDataServers yields the data servers that the shard references: its
// leader, its ensemble, both sides of a change of the ensemble, and the data
// servers that still have to delete their copy.
func GetShardDataServers(shard *proto.ShardMetadata) iter.Seq[*proto.DataServerIdentity] {
	return func(yield func(*proto.DataServerIdentity) bool) {
		servers := [][]*proto.DataServerIdentity{
			{shard.GetLeader(), shard.GetEnsembleChange().GetFrom(), shard.GetEnsembleChange().GetTo()},
			shard.GetEnsemble(),
			shard.GetPendingDeleteShardNodes(),
		}
		for _, server := range slices.Concat(servers...) {
			if server != nil && !yield(server) {
				return
			}
		}
	}
}
