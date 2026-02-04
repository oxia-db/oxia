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

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestNegotiate_EmptyInput(t *testing.T) {
	result := negotiate(nil)
	assert.Nil(t, result)

	result = negotiate(map[string][]proto.Feature{})
	assert.Nil(t, result)
}

func TestNegotiate_SingleNode(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
	}

	result := negotiate(nodeFeatures)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}, result)
}

func TestNegotiate_AllNodesSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_FINGERPRINT},
		"node3": {proto.Feature_FEATURE_FINGERPRINT},
	}

	result := negotiate(nodeFeatures)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}, result)
}

func TestNegotiate_PartialSupport(t *testing.T) {
	// Only node1 and node2 support FINGERPRINT, node3 doesn't
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_FINGERPRINT},
		"node3": {},
	}

	result := negotiate(nodeFeatures)
	assert.Empty(t, result)
}

func TestNegotiate_IgnoresUnknownFeature(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_FINGERPRINT},
	}

	result := negotiate(nodeFeatures)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}, result)
	assert.NotContains(t, result, proto.Feature_FEATURE_UNKNOWN)
}

func TestNegotiate_HandlesDuplicates(t *testing.T) {
	// Node1 has FINGERPRINT listed twice
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT, proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_FINGERPRINT},
	}

	result := negotiate(nodeFeatures)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}, result)
}

func TestNegotiate_NoCommonFeatures(t *testing.T) {
	// Simulate a scenario where nodes have no features in common
	// (using empty lists to represent old nodes)
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
		"node2": {},
		"node3": {},
	}

	result := negotiate(nodeFeatures)
	assert.Empty(t, result)
}

func TestNegotiate_OldNodeWithNoFeatures(t *testing.T) {
	// Simulates rolling upgrade scenario where some nodes are old
	// and report no features (or empty features)
	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": {proto.Feature_FEATURE_FINGERPRINT},
		"new-node-2": {proto.Feature_FEATURE_FINGERPRINT},
		"old-node":   nil, // Old node doesn't report any features
	}

	result := negotiate(nodeFeatures)
	assert.Empty(t, result, "should not enable features when old nodes are present")
}

func TestNoOpSupportedFeaturesSupplier(t *testing.T) {
	result := NoOpSupportedFeaturesSupplier(nil)
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

func TestNegotiate_MixedVersions_RollingUpgrade(t *testing.T) {
	// Simulate a rolling upgrade scenario:
	// - 2 new nodes support FINGERPRINT
	// - 1 old node supports nothing

	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": {proto.Feature_FEATURE_FINGERPRINT},
		"new-node-2": {proto.Feature_FEATURE_FINGERPRINT},
		"old-node":   {}, // Old node reports empty features
	}

	result := negotiate(nodeFeatures)
	assert.Empty(t, result, "features should not be enabled until all nodes are upgraded")

	// After upgrading the old node
	nodeFeatures["old-node"] = []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}

	result = negotiate(nodeFeatures)
	assert.Contains(t, result, proto.Feature_FEATURE_FINGERPRINT, "feature should be enabled after all nodes are upgraded")
}
