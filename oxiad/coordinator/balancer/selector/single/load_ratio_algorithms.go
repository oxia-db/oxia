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

package single

import (
	"cmp"

	"github.com/emirpasic/gods/v2/lists/arraylist"

	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/model"
)

func DefaultShardsRank(params *model.RatioParams) *model.Ratio {
	// Loads are replica counts, which float64 compares exactly. Fractions of the
	// total don't: 10/48 - 9/48 > 1/48, so the balancer would never settle.
	const shardLoadRatio = 1.0

	nodeLoadRatios := arraylist.New[*model.NodeLoadRatio]()

	first := true
	maxNodeLoadRatio := 0.0
	minNodeLoadRatio := 0.0

	for nodeID, shards := range params.NodeShardsInfos {
		if params.QuarantineNodes != nil {
			if params.QuarantineNodes.Contains(nodeID) {
				continue
			}
		}

		shardRatios := arraylist.New[*model.ShardLoadRatio]()
		for _, info := range shards {
			shardRatios.Add(&model.ShardLoadRatio{
				ShardInfo: &info,
				Ratio:     shardLoadRatio,
			})
		}
		nodeLoadRatio := float64(len(shards))

		if first {
			maxNodeLoadRatio = nodeLoadRatio
			minNodeLoadRatio = nodeLoadRatio
			first = false
		} else {
			if nodeLoadRatio > maxNodeLoadRatio {
				maxNodeLoadRatio = nodeLoadRatio
			} else if nodeLoadRatio < minNodeLoadRatio {
				minNodeLoadRatio = nodeLoadRatio
			}
		}

		shardRatios.Sort(func(x, y *model.ShardLoadRatio) int {
			return cmp.Compare(x.Ratio, y.Ratio)
		})
		nodeLoadRatios.Add(&model.NodeLoadRatio{
			NodeID:      nodeID,
			Node:        params.HistoryNodes[nodeID],
			Ratio:       nodeLoadRatio,
			ShardRatios: shardRatios,
		})
	}
	nodeLoadRatios.Sort(func(x, y *model.NodeLoadRatio) int {
		return cmp.Compare(x.Ratio, y.Ratio)
	})
	return model.NewRatio(maxNodeLoadRatio, minNodeLoadRatio, shardLoadRatio, nodeLoadRatios)
}
