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
	"sync"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/policies"
	"github.com/oxia-db/oxia/oxiad/coordinator/utils"
)

type Context struct {
	Candidates         *linkedhashset.Set[string]
	CandidatesMetadata map[string]model.ServerMetadata
	Policies           *policies.Policies
	Status             *model.ClusterStatus

	LoadRatioSupplier func() *model.Ratio

	selected *linkedhashset.Set[string]

	candidateOnce               sync.Once
	labelValueGroupedCandidates map[string]map[string]*linkedhashset.Set[string]

	selectedOnce                    sync.Once
	labelGroupedSelectedLabelValues map[string]*linkedhashset.Set[string]
}

func (so *Context) LabelValueGroupedCandidates() map[string]map[string]*linkedhashset.Set[string] {
	so.maybeGrouping()
	return so.labelValueGroupedCandidates
}

func (so *Context) LabelGroupedSelectedLabelValues() map[string]*linkedhashset.Set[string] {
	so.maybeGrouping()
	return so.labelGroupedSelectedLabelValues
}

func (so *Context) SetSelected(selected *linkedhashset.Set[string]) {
	so.selected = selected
	so.selectedOnce = sync.Once{}
	so.Candidates = so.Candidates.Difference(so.selected)
}

func (so *Context) maybeGrouping() {
	so.candidateOnce.Do(func() {
		so.labelValueGroupedCandidates = utils.GroupingCandidatesWithLabelValue(so.Candidates, so.CandidatesMetadata)
	})
	so.selectedOnce.Do(func() {
		so.labelGroupedSelectedLabelValues = utils.GroupingValueWithLabel(so.selected, so.CandidatesMetadata)
	})
}
