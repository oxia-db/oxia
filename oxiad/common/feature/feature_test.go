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

package feature

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestIsSupported(t *testing.T) {
	assert.True(t, IsSupported(proto.Feature_FEATURE_DB_CHECKSUM))
	assert.False(t, IsSupported(proto.Feature_FEATURE_UNKNOWN))
	assert.False(t, IsSupported(proto.Feature(999)))
}

func TestUnsupported(t *testing.T) {
	assert.Empty(t, Unsupported(nil))
	assert.Empty(t, Unsupported([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}))
	assert.Equal(t, []proto.Feature{proto.Feature(999)},
		Unsupported([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM, proto.Feature(999)}))
}

func TestMissing(t *testing.T) {
	checksum := proto.Feature_FEATURE_DB_CHECKSUM
	other := proto.Feature(999)

	assert.Empty(t, Missing(nil, nil))
	assert.Empty(t, Missing(nil, []proto.Feature{checksum}))
	assert.Empty(t, Missing([]proto.Feature{checksum}, []proto.Feature{checksum, other}))
	assert.Equal(t, []proto.Feature{checksum}, Missing([]proto.Feature{checksum}, nil))
	assert.Equal(t, []proto.Feature{other}, Missing([]proto.Feature{checksum, other}, []proto.Feature{checksum}))
}

func TestNegotiateEmptyInput(t *testing.T) {
	result := Negotiate(nil, 0)
	assert.Nil(t, result)

	result = Negotiate(map[string][]proto.Feature{}, 0)
	assert.Nil(t, result)
}

func TestNegotiateSingleNode(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := Negotiate(nodeFeatures, 1)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
}

func TestNegotiateAllNodesSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node3": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := Negotiate(nodeFeatures, 3)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
}

func TestNegotiatePartialSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node3": {},
	}

	result := Negotiate(nodeFeatures, 3)
	assert.Empty(t, result)
}

func TestNegotiateIgnoresUnknownFeature(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := Negotiate(nodeFeatures, 2)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
	assert.NotContains(t, result, proto.Feature_FEATURE_UNKNOWN)
}

func TestNegotiateHandlesDuplicates(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM, proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := Negotiate(nodeFeatures, 2)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
}

func TestNegotiateNoCommonFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {},
		"node3": {},
	}

	result := Negotiate(nodeFeatures, 3)
	assert.Empty(t, result)
}

func TestNegotiateOldNodeWithNoFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"new-node-2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"old-node":   nil,
	}

	result := Negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "should not enable features when old nodes are present")
}

func TestNegotiateNilNodeFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node3": nil,
	}

	result := Negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "nil feature info must not count as support")
}

func TestNegotiateMissingFeatureInfo(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := Negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "missing feature info must not count as support")
}

func TestNegotiateMixedVersionsRollingUpgrade(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"new-node-2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"old-node":   {},
	}

	result := Negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "features should not be enabled until all nodes are upgraded")

	nodeFeatures["old-node"] = []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}

	result = Negotiate(nodeFeatures, 3)
	assert.Contains(t, result, proto.Feature_FEATURE_DB_CHECKSUM, "feature should be enabled after all nodes are upgraded")
}

func TestNegotiateSecondaryIndexValidationRequiresFullUpgrade(t *testing.T) {
	currentFeatures := []proto.Feature{
		proto.Feature_FEATURE_DB_CHECKSUM,
		proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION,
	}
	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": currentFeatures,
		"new-node-2": currentFeatures,
		"old-node":   {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := Negotiate(nodeFeatures, 3)
	assert.Contains(t, result, proto.Feature_FEATURE_DB_CHECKSUM)
	assert.NotContains(t, result, proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION)

	nodeFeatures["old-node"] = currentFeatures
	result = Negotiate(nodeFeatures, 3)
	assert.Contains(t, result, proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION)
}

func TestNoOpSupplierReturnsNoFeatures(t *testing.T) {
	result := NoOpSupplier(nil)
	assert.NotNil(t, result)
	assert.Empty(t, result)

	server := &proto.DataServerIdentity{Internal: "node1", Public: "node1"}
	result = NoOpSupplier([]*proto.DataServerIdentity{server})
	assert.Contains(t, result, server.GetNameOrDefault())
	assert.Empty(t, result[server.GetNameOrDefault()])
}
