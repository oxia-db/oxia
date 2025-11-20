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
	"fmt"
	"testing"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/policies"
	"github.com/oxia-db/oxia/coordinator/selectors"
)

func TestSelectNoAntiAffinities(t *testing.T) {
	selector := &serverAntiAffinitiesSelector{}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-west"}},
		"server3": {Labels: map[string]string{"region": "eu-east"}},
		"server4": {Labels: map[string]string{"region": "eu-west"}},
		"server5": {Labels: map[string]string{"region": "ap-east"}},
		"server6": {Labels: map[string]string{"region": "ap-west"}},
	}
	var nsPolicies *policies.Policies

	context := &Context{
		CandidatesMetadata: candidatesMetadata,
		Candidates:         linkedhashset.New[string]("server1", "server2", "server3", "server4", "server5", "server6"),
		Policies:           nsPolicies,
	}
	context.SetSelected(linkedhashset.New[string]())

	_, err := selector.Select(context)
	assert.ErrorIs(t, err, selectors.ErrNoFunctioning)
}

func TestSelectSatisfiedAntiAffinities(t *testing.T) {
	selector := &serverAntiAffinitiesSelector{}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-west"}},
		"server3": {Labels: map[string]string{"region": "eu-east"}},
		"server4": {Labels: map[string]string{"region": "eu-west"}},
		"server5": {Labels: map[string]string{"region": "ap-east"}},
		"server6": {Labels: map[string]string{"region": "ap-west"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{
				Labels: []string{"region"},
				Mode:   policies.Strict,
			},
		},
	}
	selected := linkedhashset.New[string]()

	context := &Context{
		CandidatesMetadata: candidatesMetadata,
		Candidates:         linkedhashset.New("server1", "server2", "server3", "server4", "server5", "server6"),
		Policies:           nsPolicies,
	}
	context.SetSelected(selected)

	replicas := 6
	for idx := range replicas {
		selectedServer, err := selector.Select(context)
		if idx == replicas-1 { // last select
			assert.Nil(t, err)
			assert.NotNil(t, selectedServer)
			selected.Add(selectedServer)
			context.SetSelected(selected)
			continue
		}
		assert.ErrorIs(t, err, selectors.ErrMultipleResult)
		assert.Empty(t, selectedServer)
		assert.Equal(t, replicas-idx, context.Candidates.Size())

		// assume we will use the first as selected servers
		_, v := context.Candidates.Find(func(index int, _ string) bool {
			return index == 0
		})
		selected.Add(v)
		context.SetSelected(selected)
	}
	assert.Equal(t, 6, selected.Size())
	for idx := range replicas {
		id := fmt.Sprintf("dataserver%d", idx+1)
		assert.True(t, selected.Contains(id))
	}
}

func TestSelectUnsatisfiedAntiAffinitiesStrict(t *testing.T) {
	selector := &serverAntiAffinitiesSelector{}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-east"}},
		"server3": {Labels: map[string]string{"region": "us-east"}},
		"server4": {Labels: map[string]string{"region": "us-east"}},
		"server5": {Labels: map[string]string{"region": "us-east"}},
		"server6": {Labels: map[string]string{"region": "us-east"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{
				Labels: []string{"region"},
				Mode:   policies.Strict,
			},
		},
	}
	selected := linkedhashset.New[string]()
	context := &Context{
		CandidatesMetadata: candidatesMetadata,
		Candidates:         linkedhashset.New("server1", "server2", "server3", "server4", "server5", "server6"),
		Policies:           nsPolicies,
	}
	result, err := selector.Select(context)
	assert.ErrorIs(t, err, selectors.ErrMultipleResult)
	assert.Empty(t, result)
	assert.Equal(t, 6, context.Candidates.Size())

	// choose the first one
	_, v := context.Candidates.Find(func(index int, _ string) bool {
		return index == 0
	})
	selected.Add(v)
	context.SetSelected(selected)

	result, err = selector.Select(context)
	assert.ErrorIs(t, err, selectors.ErrUnsatisfiedAntiAffinity)
	assert.Empty(t, result)
}

func TestSelectUnsatisfiedAntiAffinitiesRelax(t *testing.T) {
	selector := &serverAntiAffinitiesSelector{}
	candidatesMetadata := map[string]model.ServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-east"}},
		"server3": {Labels: map[string]string{"region": "us-east"}},
		"server4": {Labels: map[string]string{"region": "us-east"}},
		"server5": {Labels: map[string]string{"region": "us-east"}},
		"server6": {Labels: map[string]string{"region": "us-east"}},
	}
	nsPolicies := &policies.Policies{
		AntiAffinities: []policies.AntiAffinity{
			{
				Labels: []string{"region"},
				Mode:   policies.Relaxed,
			},
		},
	}
	selectedServers := linkedhashset.New[string]()
	context := &Context{
		CandidatesMetadata: candidatesMetadata,
		Candidates:         linkedhashset.New("server1", "server2", "server3", "server4", "server5", "server6"),
		Policies:           nsPolicies,
	}
	context.SetSelected(selectedServers)
	result, err := selector.Select(context)
	assert.ErrorIs(t, err, selectors.ErrMultipleResult)
	assert.Empty(t, result)
	assert.Equal(t, 6, context.Candidates.Size())

	// choose the first one
	_, v := context.Candidates.Find(func(index int, value string) bool {
		return index == 0
	})
	selectedServers.Add(v)
	context.SetSelected(selectedServers)

	result, err = selector.Select(context)
	assert.ErrorIs(t, err, selectors.ErrUnsupportedAntiAffinityMode)
	assert.Empty(t, result)
}

func TestSelectMultipleAntiAffinitiesSatisfied(t *testing.T) {
	selector := &serverAntiAffinitiesSelector{}
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

	selectedServers := linkedhashset.New[string]()
	context := &Context{
		CandidatesMetadata: candidatesMetadata,
		Candidates:         linkedhashset.New("s1", "s2", "s3", "s4"),
		Policies:           nsPolicies,
	}
	context.SetSelected(selectedServers)

	selectTimes(t, selector, context, 3)

	assert.Equal(t, 3, selectedServers.Size())
	assert.True(t, selectedServers.Contains("s1") || selectedServers.Contains("s2"))
	assert.True(t, selectedServers.Contains("s3"))
	assert.True(t, selectedServers.Contains("s4"))

	result, err := selector.Select(context)
	assert.ErrorIs(t, err, selectors.ErrUnsatisfiedAntiAffinity)
	assert.Empty(t, result)
}

func selectTimes(t *testing.T, selector *serverAntiAffinitiesSelector, context *Context, times int) {
	t.Helper()
	for i := 0; i < times; i++ {
		if selectedServer, err := selector.Select(context); err != nil {
			if errors.Is(err, selectors.ErrMultipleResult) || errors.Is(err, selectors.ErrNoFunctioning) {
				// assume we will use the first as selected servers
				_, v := context.Candidates.Find(func(index int, value string) bool {
					return index == 0
				})
				context.selected.Add(v)
				context.SetSelected(context.selected)
				continue
			}
			assert.NoError(t, err)
		} else {
			context.selected.Add(selectedServer)
			context.SetSelected(context.selected)
		}
	}
}
