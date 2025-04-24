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

package ensemble

import (
	"testing"

	"github.com/emirpasic/gods/sets/linkedhashset"
	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/policies"
)

func TestSelectMultipleAntiAffinitiesSatisfied(t *testing.T) {
	ensembleSelector := NewSelector()
	candidatesMetadata := map[string]model.ServerMetadata{
		"s1": {Labels: map[string]string{"region": "us-east", "type": "compute1"}},
		"s2": {Labels: map[string]string{"region": "us-north", "type": "compute1"}},
		"s3": {Labels: map[string]string{"region": "us-south", "type": "storage1"}},
		"s4": {Labels: map[string]string{"region": "us-west", "type": "storage2"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{Labels: []string{"region"}, Mode: policies.Strict},
			{Labels: []string{"type"}, Mode: policies.Strict},
		},
	}

	context := &Context{
		CandidatesMetadata: candidatesMetadata,
		Candidates:         linkedhashset.New("s1", "s2", "s3", "s4"),
		Status: &model.ClusterStatus{
			ServerIdx: 0,
		},
		Policies: nsPolicies,
		Replicas: 3,
	}

	esm, err := ensembleSelector.Select(context)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(esm))

	assert.Contains(t, esm, "s1")
	assert.Contains(t, esm, "s3")
	assert.Contains(t, esm, "s4")
}
