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

package feature

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestSupportedFeatures(t *testing.T) {
	features := SupportedFeatures()
	assert.NotEmpty(t, features)
	assert.Contains(t, features, proto.Feature_FEATURE_FINGERPRINT)
}

func TestNegotiate_AllNodesSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_FINGERPRINT},
		"node3": {proto.Feature_FEATURE_FINGERPRINT},
	}

	negotiated := Negotiate(nodeFeatures)
	assert.Len(t, negotiated, 1)
	assert.Contains(t, negotiated, proto.Feature_FEATURE_FINGERPRINT)
}

func TestNegotiate_SomeNodesDoNotSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
		"node2": {}, // Old node without fingerprint support
		"node3": {proto.Feature_FEATURE_FINGERPRINT},
	}

	negotiated := Negotiate(nodeFeatures)
	assert.Empty(t, negotiated)
}

func TestNegotiate_EmptyNodeFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{}
	negotiated := Negotiate(nodeFeatures)
	assert.Nil(t, negotiated)
}

func TestNegotiate_IgnoresUnknownFeature(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_FINGERPRINT},
	}

	negotiated := Negotiate(nodeFeatures)
	assert.Len(t, negotiated, 1)
	assert.Contains(t, negotiated, proto.Feature_FEATURE_FINGERPRINT)
	assert.NotContains(t, negotiated, proto.Feature_FEATURE_UNKNOWN)
}

func TestNegotiate_HandlesDuplicateFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT, proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_FINGERPRINT},
	}

	negotiated := Negotiate(nodeFeatures)
	assert.Len(t, negotiated, 1)
	assert.Contains(t, negotiated, proto.Feature_FEATURE_FINGERPRINT)
}

func TestContains(t *testing.T) {
	features := []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}

	assert.True(t, Contains(features, proto.Feature_FEATURE_FINGERPRINT))
	assert.False(t, Contains(features, proto.Feature_FEATURE_UNKNOWN))
	assert.False(t, Contains(nil, proto.Feature_FEATURE_FINGERPRINT))
	assert.False(t, Contains([]proto.Feature{}, proto.Feature_FEATURE_FINGERPRINT))
}
