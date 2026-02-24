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
	"github.com/oxia-db/oxia/oxiad/coordinator/selector"
)

var _ selector.Selector[*Context, string] = &lowerestLoadSelector{}

type lowerestLoadSelector struct{}

func (*lowerestLoadSelector) Select(ssContext *Context) (string, error) {
	if ssContext.LoadRatioSupplier == nil {
		return "", selector.ErrNoFunctioning
	}
	loadRatios := ssContext.LoadRatioSupplier()
	if loadRatios == nil {
		return "", selector.ErrNoFunctioning
	}
	for iter := loadRatios.NodeIterator(); iter.Next(); {
		nodeRatio := iter.Value()
		lowerLoad := nodeRatio.NodeID
		if ssContext.Candidates.Contains(lowerLoad) {
			return lowerLoad, nil
		}
	}
	return "", selector.ErrNoFunctioning
}
