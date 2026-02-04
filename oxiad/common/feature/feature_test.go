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

	// Should return a non-empty list
	assert.NotEmpty(t, features)

	// Should include FEATURE_FINGERPRINT
	assert.Contains(t, features, proto.Feature_FEATURE_FINGERPRINT)

	// Should not include FEATURE_UNKNOWN
	assert.NotContains(t, features, proto.Feature_FEATURE_UNKNOWN)
}

func TestSupportedFeatures_Deterministic(t *testing.T) {
	// Multiple calls should return the same features
	features1 := SupportedFeatures()
	features2 := SupportedFeatures()

	assert.Equal(t, features1, features2)
}
