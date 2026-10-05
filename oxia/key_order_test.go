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

package oxia

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestKeyOrder(t *testing.T) {
	for _, test := range []struct {
		a, b string
		// The expected comparison of a and b for the natural and hierarchical key
		// sortings, and when the key sorting is not known
		natural, hierarchical, unknown int
	}{
		{"a", "a", 0, 0, 0},
		{"a0", "a/y/z", +1, -1, -1},
		{"a/x", "ab/y", -1, +1, -1},
		{"b/x", "a/y/z", +1, -1, +1},
		{"a/z", "a/y/z", +1, -1, -1},
		// The internal keys sort after the other keys
		{"__oxia/x", "z/z", +1, +1, -1},
	} {
		for keySorting, expected := range map[proto.KeySorting]int{
			proto.KeySorting_KEY_SORTING_NATURAL:      test.natural,
			proto.KeySorting_KEY_SORTING_HIERARCHICAL: test.hierarchical,
			proto.KeySorting_KEY_SORTING_UNKNOWN:      test.unknown,
		} {
			order := newKeyOrder(keySorting)
			assert.Equal(t, expected, order.compare(test.a, test.b), "%s: %q vs %q", keySorting, test.a, test.b)
			assert.Equal(t, -expected, order.compare(test.b, test.a), "%s: %q vs %q", keySorting, test.b, test.a)
		}
	}
}

// A range scan without a partition key merges the records of the shards, which
// each shard returns in the order of its keys.
func TestAggregateAndSortRangeScanAcrossShardsKeySorting(t *testing.T) {
	for _, test := range []struct {
		keySorting proto.KeySorting
		shards     [][]string
		expected   []string
	}{
		{
			proto.KeySorting_KEY_SORTING_NATURAL,
			[][]string{{"a/x", "a0"}, {"a/y/z", "ab/y"}},
			[]string{"a/x", "a/y/z", "a0", "ab/y"},
		},
		{
			proto.KeySorting_KEY_SORTING_HIERARCHICAL,
			[][]string{{"a0", "a/x"}, {"ab/y", "a/y/z"}},
			[]string{"a0", "ab/y", "a/x", "a/y/z"},
		},
		{
			proto.KeySorting_KEY_SORTING_UNKNOWN,
			[][]string{{"a0", "a/x"}, {"a/y/z", "ab/y"}},
			[]string{"a0", "a/x", "a/y/z", "ab/y"},
		},
	} {
		t.Run(test.keySorting.String(), func(t *testing.T) {
			channels := make([]chan rangeScanResult, len(test.shards))
			for i, keys := range test.shards {
				channels[i] = make(chan rangeScanResult, len(keys))
				for _, key := range keys {
					channels[i] <- rangeScanResult{gr: GetResult{Key: key}}
				}
				close(channels[i])
			}

			outCh := make(chan GetResult, 100)
			aggregateAndSortRangeScanAcrossShards(newKeyOrder(test.keySorting), channels, outCh)

			var merged []string
			for result := range outCh {
				merged = append(merged, result.Key)
			}
			assert.Equal(t, test.expected, merged)
		})
	}
}

// A range scan with an index merges the records of the shards, which each
// shard returns in the order of their secondary keys.
func TestAggregateAndSortRangeScanAcrossShardsSecondaryKeys(t *testing.T) {
	type record struct{ key, secondaryKey string }
	for _, test := range []struct {
		name       string
		keySorting proto.KeySorting
		shards     [][]record
		expected   []string
	}{
		{
			"natural",
			proto.KeySorting_KEY_SORTING_NATURAL,
			[][]record{{{"p4", "a/x"}, {"p2", "a0"}}, {{"p3", "a/y/z"}, {"p1", "ab/y"}}},
			[]string{"p4", "p3", "p2", "p1"},
		},
		{
			"hierarchical",
			proto.KeySorting_KEY_SORTING_HIERARCHICAL,
			[][]record{{{"p4", "a0"}, {"p2", "a/x"}}, {{"p3", "ab/y"}, {"p1", "a/y/z"}}},
			[]string{"p4", "p3", "p2", "p1"},
		},
		{
			"unknown",
			proto.KeySorting_KEY_SORTING_UNKNOWN,
			[][]record{{{"p4", "a0"}, {"p2", "a/x"}}, {{"p3", "a/y/z"}, {"p1", "ab/y"}}},
			[]string{"p4", "p2", "p3", "p1"},
		},
		{
			// The records with the same secondary key are ordered by their keys
			"same secondary key",
			proto.KeySorting_KEY_SORTING_NATURAL,
			[][]record{{{"p2", "a"}, {"p3", "b"}}, {{"p1", "b"}, {"p4", "c"}}},
			[]string{"p2", "p1", "p3", "p4"},
		},
		{
			"empty secondary key",
			proto.KeySorting_KEY_SORTING_NATURAL,
			[][]record{{{"p2", ""}}, {{"p1", "a"}}},
			[]string{"p2", "p1"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			channels := make([]chan rangeScanResult, len(test.shards))
			for i, records := range test.shards {
				channels[i] = make(chan rangeScanResult, len(records))
				for _, r := range records {
					channels[i] <- rangeScanResult{gr: GetResult{Key: r.key}, secondaryIndexKey: new(r.secondaryKey)}
				}
				close(channels[i])
			}

			outCh := make(chan GetResult, 100)
			aggregateAndSortRangeScanAcrossShards(newKeyOrder(test.keySorting), channels, outCh)

			var merged []string
			for result := range outCh {
				merged = append(merged, result.Key)
			}
			assert.Equal(t, test.expected, merged)
		})
	}
}
