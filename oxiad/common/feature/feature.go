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

// Package feature provides feature negotiation capabilities for Oxia.
// It enables safe rolling upgrades where new and old nodes can coexist
// by negotiating which features are supported by all members of a quorum.
package feature

import (
	"github.com/oxia-db/oxia/common/proto"
)

func SupportedFeatures() []proto.Feature {
	return []proto.Feature{
		proto.Feature_FEATURE_DB_CHECKSUM,
	}
}

// IsSupported reports whether this binary implements the given feature.
func IsSupported(feature proto.Feature) bool {
	for _, f := range SupportedFeatures() {
		if f == feature {
			return true
		}
	}
	return false
}

// Unsupported returns the subset of features that this binary does not
// implement. An empty result means all the features are supported.
func Unsupported(features []proto.Feature) []proto.Feature {
	var unsupported []proto.Feature
	for _, f := range features {
		if !IsSupported(f) {
			unsupported = append(unsupported, f)
		}
	}
	return unsupported
}

// Missing returns the features in required that are not contained in
// available. An empty result means all the required features are available.
func Missing(required []proto.Feature, available []proto.Feature) []proto.Feature {
	var missing []proto.Feature
	for _, r := range required {
		found := false
		for _, a := range available {
			if a == r {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, r)
		}
	}
	return missing
}
