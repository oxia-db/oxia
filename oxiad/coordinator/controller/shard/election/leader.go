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
	"cmp"
	"slices"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
)

// fewestLeaders returns the candidate that leads the fewest shards of every
// namespace, or the first of them on a tie. The shards being deleted or split
// don't count. It returns errs.ErrNotFound without a candidate.
func fewestLeaders(candidates []*proto.DataServerIdentity,
	namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus]) (*proto.DataServerIdentity, error) {
	if len(candidates) == 0 {
		return nil, errs.ErrNotFound
	}
	leaders := make(map[string]int)
	for _, status := range namespaces {
		for _, shard := range status.UnsafeBorrow().GetShards() {
			if shard.GetStatusOrDefault() == proto.ShardStatusDeleting || shard.GetSplit() != nil ||
				shard.GetLeader() == nil {
				continue
			}
			leaders[shard.GetLeader().GetNameOrDefault()]++
		}
	}
	return slices.MinFunc(candidates, func(a, b *proto.DataServerIdentity) int {
		return cmp.Compare(leaders[a.GetNameOrDefault()], leaders[b.GetNameOrDefault()])
	}), nil
}
