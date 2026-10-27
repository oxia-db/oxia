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

package leader

import (
	"math"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"

	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/state"
)

var _ selector.Selector[*Context, *commonproto.DataServerIdentity] = &leader{}

type leader struct{}

func (*leader) Select(context *Context) (*commonproto.DataServerIdentity, error) {
	if len(context.Candidates) == 0 {
		return nil, errs.ErrBalancerNoCandidates
	}

	candidates := linkedhashset.New[string]()
	for _, candidate := range context.Candidates {
		candidates.Add(candidate.GetNameOrDefault())
	}
	_, _, leaders := state.NodeShardLeaders(candidates, context.Namespaces)

	minLeaders := math.MaxInt
	var minLeadersNode *commonproto.DataServerIdentity

	for _, candidate := range context.Candidates {
		leaderNum := 0
		if shards, exist := leaders[candidate.GetNameOrDefault()]; exist {
			leaderNum = shards.Size()
		}
		if leaderNum < minLeaders {
			minLeaders = leaderNum
			minLeadersNode = candidate
		}
	}
	return minLeadersNode, nil
}

func NewSelector() selector.Selector[*Context, *commonproto.DataServerIdentity] {
	return &leader{}
}
