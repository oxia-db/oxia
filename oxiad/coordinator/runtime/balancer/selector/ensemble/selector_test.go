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
	"testing"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"

	"github.com/oxia-db/oxia/common/proto"
)

func TestSelectMultipleAntiAffinitiesSatisfied(t *testing.T) {
	ensembleSelector := NewSelector()
	candidatesMetadata := map[string]*proto.DataServerMetadata{
		"s1": {Labels: map[string]string{"region": "us-east", "type": "compute1"}},
		"s2": {Labels: map[string]string{"region": "us-north", "type": "compute1"}},
		"s3": {Labels: map[string]string{"region": "us-south", "type": "storage1"}},
		"s4": {Labels: map[string]string{"region": "us-west", "type": "storage2"}},
	}
	antiAffinities := []*proto.AntiAffinity{
		{Labels: []string{"region"}, Mode: proto.AntiAffinityModeStrict},
		{Labels: []string{"type"}, Mode: proto.AntiAffinityModeStrict},
	}

	context := &Context{
		CandidatesMetadata: candidatesMetadata,
		Candidates:         linkedhashset.New("s1", "s2", "s3", "s4"),
		Namespace:          "ns-1",
		Shard:              7,
		AntiAffinities:     antiAffinities,
		Replicas:           3,
	}

	esm, err := ensembleSelector.Select(context)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(esm))

	assert.True(t, slices.Contains(esm, "s1") || slices.Contains(esm, "s2"))
	assert.Contains(t, esm, "s3")
	assert.Contains(t, esm, "s4")
}
