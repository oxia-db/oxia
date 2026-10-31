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
	"strings"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

// fewestLeaders returns the candidate that leads the fewest shards of
// namespace, then of every namespace, then the first by name, so that a
// failover spreads the leaders as the scheduler does. It returns nil without a
// candidate.
func fewestLeaders(candidates []*proto.DataServerIdentity,
	namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus], namespace string) *proto.DataServerIdentity {
	inNamespace := make(map[string]int)
	total := make(map[string]int)
	for name, status := range namespaces {
		for _, shard := range status.UnsafeBorrow().GetShards() {
			if shard.GetStatusOrDefault() == proto.ShardStatusDeleting || shard.GetLeader() == nil {
				continue
			}
			leader := shard.GetLeader().GetNameOrDefault()
			total[leader]++
			if name == namespace {
				inNamespace[leader]++
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return slices.MinFunc(candidates, func(a, b *proto.DataServerIdentity) int {
		x, y := a.GetNameOrDefault(), b.GetNameOrDefault()
		return cmp.Or(cmp.Compare(inNamespace[x], inNamespace[y]), cmp.Compare(total[x], total[y]),
			strings.Compare(x, y))
	})
}
