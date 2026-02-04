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

// Package feature provides feature negotiation capabilities for Oxia.
// It enables safe rolling upgrades where new and old nodes can coexist
// by negotiating which features are supported by all members of a quorum.
package feature

import (
	"github.com/oxia-db/oxia/common/proto"
)

// SupportedFeatures returns the list of features supported by this node.
// This list should be updated whenever new features are added that require
// negotiation during leader election.
func SupportedFeatures() []proto.Feature {
	return []proto.Feature{
		proto.Feature_FEATURE_FINGERPRINT,
	}
}

// Negotiate computes the intersection of features supported by all nodes.
// It takes a map of node identifiers to their supported features and returns
// the list of features that are supported by ALL nodes.
//
// This is used during leader election to determine which features can be
// safely enabled for the quorum.
func Negotiate(nodeFeatures map[string][]proto.Feature) []proto.Feature {
	if len(nodeFeatures) == 0 {
		return nil
	}

	// Start with all supported features
	featureCount := make(map[proto.Feature]int)
	nodeCount := len(nodeFeatures)

	for _, features := range nodeFeatures {
		// Track unique features per node to handle duplicates
		seen := make(map[proto.Feature]bool)
		for _, f := range features {
			if f == proto.Feature_FEATURE_UNKNOWN {
				continue
			}
			if !seen[f] {
				seen[f] = true
				featureCount[f]++
			}
		}
	}

	// Only include features supported by ALL nodes
	var negotiated []proto.Feature
	for feature, count := range featureCount {
		if count == nodeCount {
			negotiated = append(negotiated, feature)
		}
	}

	return negotiated
}

// Contains checks if a feature list contains a specific feature.
func Contains(features []proto.Feature, target proto.Feature) bool {
	for _, f := range features {
		if f == target {
			return true
		}
	}
	return false
}
