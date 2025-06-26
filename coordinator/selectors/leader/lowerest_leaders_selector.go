// Copyright 2025 StreamNative, Inc.
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

package leader

import (
	"math/rand"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/selectors"
	"github.com/oxia-db/oxia/coordinator/utils"
)

var _ selectors.Selector[*Context, model.Server] = &leader{}

type leader struct{}

func (*leader) Select(context *Context) (model.Server, error) {
	status := context.Status
	candidates := linkedhashset.New[string]()
	for _, candidate := range context.Candidates {
		candidates.Add(candidate.GetIdentifier())
	}
	_, _, leaders := utils.NodeShardLeaders(candidates, status)

	minLeaders := -1
	var minLeadersNode model.Server

	for idx, candidate := range context.Candidates {
		if shards, exist := leaders[candidate.GetIdentifier()]; exist {
			leaderNum := shards.Size()
			if minLeaders == -1 || leaderNum < minLeaders {
				minLeaders = leaderNum
				minLeadersNode = context.Candidates[idx]
			}
		}
	}
	if minLeaders == -1 {
		return context.Candidates[rand.Intn(len(context.Candidates))], nil //nolint:gosec
	}
	return minLeadersNode, nil
}

func NewSelector() selectors.Selector[*Context, model.Server] {
	return &leader{}
}
