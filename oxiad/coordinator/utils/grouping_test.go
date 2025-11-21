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

//revive:disable-next-line:var-naming
package utils

import (
	"testing"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/stretchr/testify/assert"
)

func TestGroupingCandidatesNormalCase(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {
			Labels: map[string]string{
				"region": "us-east",
				"type":   "compute",
			},
		},
		"server2": {
			Labels: map[string]string{
				"region": "us-west",
				"type":   "compute",
			},
		},
	}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.NotNil(t, result)

	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.True(t, regionGroup["us-east"].Contains("server1"))
	assert.True(t, regionGroup["us-west"].Contains("server2"))

	typeGroup, exists := result["type"]
	assert.True(t, exists)
	assert.True(t, typeGroup["compute"].Contains("server1"))
	assert.True(t, typeGroup["compute"].Contains("server2"))
}

func TestGroupingCandidatesNoCandidates(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New[string](), candidatesMetadata)
	assert.Empty(t, result)
}

func TestGroupingCandidatesNoMetadata(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.Empty(t, result)
}

func TestGroupingCandidatesPartialMetadataMissing(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
	}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.True(t, regionGroup["us-east"].Contains("server1"))
	assert.False(t, regionGroup["us-east"].Contains("server2"))
}

func TestGroupingCandidatesAllSameLabelValue(t *testing.T) {
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-east"}},
	}
	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.True(t, regionGroup["us-east"].Contains("server1"))
	assert.True(t, regionGroup["us-east"].Contains("server2"))
}
