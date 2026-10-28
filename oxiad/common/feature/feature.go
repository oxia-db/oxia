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
	"slices"

	"github.com/oxia-db/oxia/common/proto"
)

type Checker interface {
	IsFeatureEnabled(feature proto.Feature) bool
}

func SupportedFeatures() []proto.Feature {
	return []proto.Feature{
		proto.Feature_FEATURE_DB_CHECKSUM,
		proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION,
		proto.Feature_FEATURE_ORDERED_WRITES,
		proto.Feature_FEATURE_EPHEMERAL_SECONDARY_INDEX_CLEANUP,
		proto.Feature_FEATURE_SEQUENCE_LAST_KEY_SEPARATOR,
		proto.Feature_FEATURE_EPHEMERAL_CLEANUP_NATURAL_SORTING,
		proto.Feature_FEATURE_DELETE_RANGE_NOTIFICATION_RECORDS,
		proto.Feature_FEATURE_SEQUENCE_KEY_VALIDATION,
		proto.Feature_FEATURE_SECONDARY_INDEX_SKIP_UNCHANGED,
		proto.Feature_FEATURE_SPLIT_DEFERRED_FILTER,
		proto.Feature_FEATURE_COMMIT_ADVERTISEMENT,
		proto.Feature_FEATURE_RANGE_END_INCLUSIVE,
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

// Union returns the sorted features that are in a or in b, without duplicates.
func Union(a []proto.Feature, b []proto.Feature) []proto.Feature {
	union := slices.Concat(a, b)
	slices.Sort(union)
	return slices.Compact(union)
}

// Negotiate returns the features that every one of the given number of data
// servers supports. A data server missing from nodeFeatures supports none.
func Negotiate(nodeFeatures map[string][]proto.Feature, candidates int) []proto.Feature {
	if candidates == 0 || len(nodeFeatures) == 0 {
		return nil
	}

	// Start with all supported features
	featureCount := make(map[proto.Feature]int)

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
	for f, count := range featureCount {
		if count == candidates {
			negotiated = append(negotiated, f)
		}
	}

	return negotiated
}

// Supplier returns the features that each data server supports, by name, as
// their handshakes reported them.
type Supplier = func(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature

// NoOpSupplier reports no features for any data server.
func NoOpSupplier(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature {
	features := make(map[string][]proto.Feature, len(dataServers))
	for _, dataServer := range dataServers {
		features[dataServer.GetNameOrDefault()] = []proto.Feature{}
	}
	return features
}

// NegotiateDataServers returns the features that all the given data servers
// support.
func NegotiateDataServers(dataServers []*proto.DataServerIdentity, supplier Supplier) []proto.Feature {
	return Negotiate(supplier(dataServers), len(dataServers))
}
