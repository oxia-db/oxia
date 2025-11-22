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

package ensemble

import (
	"github.com/emirpasic/gods/v2/sets/linkedhashset"

	"github.com/oxia-db/oxia/oxiad/coordinator/selector"
	"github.com/oxia-db/oxia/oxiad/coordinator/selector/single"
)

var _ selector.Selector[*Context, []string] = &ensemble{}

type ensemble struct {
	singleServerSelector selector.Selector[*single.Context, string]
}

func (ensemble *ensemble) Select(context *Context) ([]string, error) {
	esm := make([]string, context.Replicas)

	sServerContext := &single.Context{
		Candidates:         context.Candidates,
		CandidatesMetadata: context.CandidatesMetadata,
		Status:             context.Status,
		Policies:           context.Policies,
		LoadRatioSupplier:  context.LoadRatioSupplier,
	}
	selected := linkedhashset.New[string]()
	sServerContext.SetSelected(selected)

	for idx := range context.Replicas {
		var selectedIdPtr string
		var err error
		if selectedIdPtr, err = ensemble.singleServerSelector.Select(sServerContext); err != nil {
			return nil, err
		}
		if selectedIdPtr != "" {
			selectedId := selectedIdPtr
			esm[idx] = selectedId
			selected.Add(selectedId)
			sServerContext.SetSelected(selected)
		}
	}
	if selected.Size() != context.Replicas {
		return nil, selector.ErrUnsatisfiedEnsembleReplicas
	}
	return esm, nil
}

func NewSelector() selector.Selector[*Context, []string] {
	selector := single.NewSelector()
	return &ensemble{
		singleServerSelector: selector,
	}
}
