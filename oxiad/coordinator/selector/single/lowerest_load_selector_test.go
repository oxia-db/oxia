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

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/selector"
)

func TestSelectLowerestLoadSelector(t *testing.T) {
	llSelector := &lowerestLoadSelector{}
	_, err := llSelector.Select(&Context{
		LoadRatioSupplier: nil,
	})
	assert.ErrorIs(t, err, selector.ErrNoFunctioning)
	ratioSnapshot := DefaultShardsRank(&model.RatioParams{
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
	})
	selected := linkedhashset.New[string]()
	context := &Context{
		Candidates: linkedhashset.New("sv-1", "sv-2", "sv-3", "sv-4", "sv-5"),
		LoadRatioSupplier: func() *model.Ratio {
			return ratioSnapshot
		},
	}
	id, err := llSelector.Select(context)
	assert.NoError(t, err)
	assert.EqualValues(t, "sv-5", id)

	selected.Add(id)
	context.SetSelected(selected)

	id, err = llSelector.Select(context)
	assert.NoError(t, err)
	assert.EqualValues(t, "sv-4", id)

	selected.Add(id)
	context.SetSelected(selected)

	id, err = llSelector.Select(context)
	assert.NoError(t, err)
	assert.EqualValues(t, "sv-2", id)

	selected.Add(id)
	context.SetSelected(selected)

	id, err = llSelector.Select(context)
	assert.NoError(t, err)
	assert.EqualValues(t, "sv-3", id)

	selected.Add(id)
	context.SetSelected(selected)

	id, err = llSelector.Select(context)
	assert.NoError(t, err)
	assert.EqualValues(t, "sv-1", id)
}
