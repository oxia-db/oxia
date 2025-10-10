// Copyright 2023-2025 The Oxia Authors
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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/coordinator/model"
)

func TestDefaultShardsRank(t *testing.T) {
	params := &model.RatioParams{
		NodeShardsInfos: map[string][]model.ShardInfo{
			"sv-1": {
				{Namespace: "ns-1", ShardID: 1, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-2", ShardID: 2, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-3", ShardID: 3, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-4", ShardID: 4, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-3"}, {Internal: "sv-4"}}},
				{Namespace: "ns-5", ShardID: 5, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-4"}, {Internal: "sv-5"}}},
			},
			"sv-2": {
				{Namespace: "ns-1", ShardID: 1, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-2", ShardID: 2, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-3", ShardID: 3, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
			},
			"sv-3": {
				{Namespace: "ns-1", ShardID: 1, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-2", ShardID: 2, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-3", ShardID: 3, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-2"}, {Internal: "sv-3"}}},
				{Namespace: "ns-4", ShardID: 4, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-3"}, {Internal: "sv-4"}}},
			},
			"sv-4": {
				{Namespace: "ns-4", ShardID: 4, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-3"}, {Internal: "sv-4"}}},
				{Namespace: "ns-5", ShardID: 5, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-4"}, {Internal: "sv-5"}}},
			},
			"sv-5": {
				{Namespace: "ns-5", ShardID: 5, Ensemble: []model.Server{{Internal: "sv-1"}, {Internal: "sv-4"}, {Internal: "sv-5"}}},
			},
		},
	}
	ratioSnapshot := DefaultShardsRank(params)
	nodeIter := ratioSnapshot.NodeIterator()

	assert.True(t, nodeIter.Last())
	nodeRatio := nodeIter.Value()
	assert.Equal(t, "sv-1", nodeRatio.NodeID)
	assert.Equal(t, 0.3333333333333333, nodeRatio.Ratio)
	shardRatios := nodeRatio.ShardRatios
	assert.Equal(t, 5, shardRatios.Size())

	assert.True(t, nodeIter.Prev())
	nodeRatio = nodeIter.Value()
	assert.Equal(t, "sv-3", nodeRatio.NodeID)
	assert.Equal(t, 0.26666666666666666, nodeRatio.Ratio)
	shardRatios = nodeRatio.ShardRatios
	assert.Equal(t, 4, shardRatios.Size())

	assert.True(t, nodeIter.Prev())
	nodeRatio = nodeIter.Value()
	assert.Equal(t, "sv-2", nodeRatio.NodeID)
	assert.Equal(t, 0.2, nodeRatio.Ratio)
	shardRatios = nodeRatio.ShardRatios
	assert.Equal(t, 3, shardRatios.Size())

	assert.True(t, nodeIter.Prev())
	nodeRatio = nodeIter.Value()
	assert.Equal(t, "sv-4", nodeRatio.NodeID)
	assert.Equal(t, 0.13333333333333333, nodeRatio.Ratio)
	shardRatios = nodeRatio.ShardRatios
	assert.Equal(t, 2, shardRatios.Size())

	assert.True(t, nodeIter.Prev())
	nodeRatio = nodeIter.Value()
	assert.Equal(t, "sv-5", nodeRatio.NodeID)
	assert.Equal(t, 0.06666666666666667, nodeRatio.Ratio)
	shardRatios = nodeRatio.ShardRatios
	assert.Equal(t, 1, shardRatios.Size())
}
