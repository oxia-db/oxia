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

package single

import (
	"testing"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector"
)

func TestServerSelectReturnsUnsatisfiedAntiAffinity(t *testing.T) {
	context := &Context{
		Candidates: linkedhashset.New("s1", "s2"),
		CandidatesMetadata: map[string]*proto.DataServerMetadata{
			"s1": {Labels: map[string]string{"region": "us-east"}},
			"s2": {Labels: map[string]string{"region": "us-east"}},
		},
		AntiAffinities: []*proto.AntiAffinity{{Labels: []string{"region"}, Mode: proto.AntiAffinityModeStrict}},
	}
	context.SetSelected(linkedhashset.New("s1"))

	_, err := NewSelector().Select(context)

	assert.ErrorIs(t, err, selector.ErrUnsatisfiedAntiAffinity)
}

func TestSelectorSelectNoCandidates(t *testing.T) {
	_, err := NewSelector().Select(&Context{
		Candidates: linkedhashset.New[string](),
		Namespace:  "ns-1",
	})
	assert.ErrorIs(t, err, selector.ErrNoCandidates)
}
