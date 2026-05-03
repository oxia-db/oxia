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
	"testing"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFinalSelectorIgnoresCandidateInsertionOrder(t *testing.T) {
	selector := &finalSelector{}
	selected := linkedhashset.New[string]()

	firstContext := &Context{
		Candidates: linkedhashset.New("s3", "s1", "s2"),
		Namespace:  "ns-1",
		Shard:      7,
	}
	firstContext.SetSelected(selected)

	secondContext := &Context{
		Candidates: linkedhashset.New("s2", "s3", "s1"),
		Namespace:  "ns-1",
		Shard:      7,
	}
	secondContext.SetSelected(selected)

	first, err := selector.Select(firstContext)
	require.NoError(t, err)

	second, err := selector.Select(secondContext)
	require.NoError(t, err)

	assert.Equal(t, first, second)
}

func TestFinalSelectorAdvancesWithReplicaOrdinal(t *testing.T) {
	selector := &finalSelector{}
	selected := linkedhashset.New[string]()
	context := &Context{
		Candidates: linkedhashset.New("s4", "s1", "s3", "s2"),
		Namespace:  "ns-1",
		Shard:      11,
	}
	context.SetSelected(selected)

	first, err := selector.Select(context)
	require.NoError(t, err)

	selected.Add(first)
	context.SetSelected(selected)

	second, err := selector.Select(context)
	require.NoError(t, err)

	assert.NotEqual(t, first, second)
}
