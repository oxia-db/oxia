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
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/oxia-db/oxia/oxiad/coordinator/policies"
	"github.com/oxia-db/oxia/oxiad/coordinator/selectors"
)

var _ selectors.Selector[*Context, string] = &serverAntiAffinitiesSelector{}

type serverAntiAffinitiesSelector struct{}

func (*serverAntiAffinitiesSelector) Select(ssContext *Context) (string, error) { //nolint:revive
	nsPolicies := ssContext.Policies
	if nsPolicies == nil || len(nsPolicies.AntiAffinities) == 0 {
		return "", selectors.ErrNoFunctioning
	}
	if ssContext.selected == nil {
		ssContext.selected = linkedhashset.New[string]()
	}
	selectedLabelValues := ssContext.LabelGroupedSelectedLabelValues()
	candidates := linkedhashset.New[string]()
	for affinityIdx, affinity := range nsPolicies.AntiAffinities {
		for _, label := range affinity.Labels {
			labelSatisfiedCandidates := linkedhashset.New[string]()
			labelGroupedCandidates := ssContext.LabelValueGroupedCandidates()[label]
			for candidatesLabelValue, servers := range labelGroupedCandidates {
				if len(selectedLabelValues) > 0 {
					if selectedLabelValueSet, exist := selectedLabelValues[label]; exist {
						if selectedLabelValueSet.Contains(candidatesLabelValue) {
							continue
						}
					}
				}
				for iter := servers.Iterator(); iter.Next(); {
					labelSatisfiedCandidates.Add(iter.Value())
				}
			}
			if affinityIdx > 0 {
				labelSatisfiedCandidates = labelSatisfiedCandidates.Intersection(candidates)
			}
			if labelSatisfiedCandidates.Size() < 1 {
				switch affinity.Mode {
				case policies.Strict:
					return "", selectors.ErrUnsatisfiedAntiAffinity
				// case p.Relaxed:
				default:
					return "", selectors.ErrUnsupportedAntiAffinityMode
				}
			}
			if affinityIdx == 0 {
				candidates.Add(labelSatisfiedCandidates.Values()...)
				continue
			}
			candidates = labelSatisfiedCandidates
		}
	}
	if candidates.Size() == 1 {
		_, value := candidates.Find(func(_ int, _ string) bool { return true })
		return value, nil
	}

	ssContext.Candidates = candidates
	return "", selectors.ErrMultipleResult
}
