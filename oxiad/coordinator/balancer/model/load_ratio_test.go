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

package model

import (
	"testing"

	"github.com/emirpasic/gods/v2/lists/arraylist"
	"github.com/stretchr/testify/assert"
)

func TestRatioMaxNodeLoadRatioReturnsMax(t *testing.T) {
	ratio := NewRatio(0.6, 0.1, 0.05, arraylist.New[*NodeLoadRatio]())

	assert.InDelta(t, 0.6, ratio.MaxNodeLoadRatio(), 1e-9)
	assert.InDelta(t, 0.1, ratio.MinNodeLoadRatio(), 1e-9)
	assert.InDelta(t, 0.5, ratio.RatioGap(), 1e-9)
}

func TestRatioReCalculateRatiosKeepsBoundsWhenEmpty(t *testing.T) {
	ratio := NewRatio(0.6, 0.1, 0.05, arraylist.New[*NodeLoadRatio]())

	ratio.ReCalculateRatios()

	assert.InDelta(t, 0.6, ratio.MaxNodeLoadRatio(), 1e-9)
	assert.InDelta(t, 0.1, ratio.MinNodeLoadRatio(), 1e-9)
}

func TestRatioRemoveDeletedNodeRejectsNonEmptyNode(t *testing.T) {
	ratio := NewRatio(0.5, 0.5, 0.5, arraylist.New(newNodeLoadRatio("sv-1", 0.5)))

	assert.ErrorContains(t, ratio.RemoveDeletedNode("sv-1"), "cannot remove non-empty node")
	assert.Equal(t, []string{"sv-1"}, nodeIDs(ratio))
}

func TestRatioRemoveDeletedNodeRemovesEmptyNode(t *testing.T) {
	ratio := NewRatio(0.5, 0, 0.5, arraylist.New(newNodeLoadRatio("sv-1", 0), newNodeLoadRatio("sv-2", 0.5)))

	assert.NoError(t, ratio.RemoveDeletedNode("sv-1"))
	assert.Equal(t, []string{"sv-2"}, nodeIDs(ratio))
}

func TestRatioRemoveDeletedNodeIgnoresUnknownNode(t *testing.T) {
	ratio := NewRatio(0.5, 0.5, 0.5, arraylist.New(newNodeLoadRatio("sv-1", 0.5)))

	assert.NoError(t, ratio.RemoveDeletedNode("sv-2"))
	assert.Equal(t, []string{"sv-1"}, nodeIDs(ratio))
}

func newNodeLoadRatio(nodeID string, ratio float64) *NodeLoadRatio {
	return &NodeLoadRatio{NodeID: nodeID, Ratio: ratio, ShardRatios: arraylist.New[*ShardLoadRatio]()}
}

func nodeIDs(ratio *Ratio) []string {
	var ids []string
	for iter := ratio.NodeIterator(); iter.Next(); {
		ids = append(ids, iter.Value().NodeID)
	}
	return ids
}
