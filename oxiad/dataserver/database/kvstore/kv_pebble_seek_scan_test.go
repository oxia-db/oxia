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

package kvstore

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

// seekScanKeys returns the regular keys of the Seek and Scan tests, with the
// key sorting. With the hierarchical one, a key with a raw 0xff byte decodes to
// a key with a separator instead, which encodes to a different level: only the
// stored key keeps its position.
func seekScanKeys(keySorting proto.KeySortingType) []string {
	if keySorting == proto.KeySortingType_NATURAL {
		return []string{"", "a", "c", "e", keyBeforeInternalRegion, keyAfterInternalRegion}
	}
	return []string{"", "/a", "/a/b", "/c", "a\xff/", "a\xff/b", "b/x", "c/y"}
}

func newSeekScanKV(t *testing.T, keySorting proto.KeySortingType) *Pebble {
	t.Helper()
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, factory.Close()) })
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, keySorting)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })

	putAll(t, kv, seekScanKeys(keySorting)...)
	putAll(t, kv, constant.InternalKeyPrefix+"a", constant.InternalKeyPrefix+"z")
	return kv.(*Pebble)
}

// storedOrder returns the keys sorted as the store sorts them, as the store
// returns them.
func storedOrder(p *Pebble, keys []string) []string {
	sorted := slices.Clone(keys)
	slices.SortFunc(sorted, func(a, b string) int {
		return bytes.Compare(p.keyEncoder.Encode(a), p.keyEncoder.Encode(b))
	})
	for i, key := range sorted {
		sorted[i] = p.keyEncoder.Decode(p.keyEncoder.Encode(key))
	}
	return sorted
}

func TestPebbleSeek(t *testing.T) {
	for _, keySorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(keySorting.String(), func(t *testing.T) {
			p := newSeekScanKV(t, keySorting)
			keys := seekScanKeys(keySorting)
			probes := append(slices.Clone(keys), "b", "/b", "a//", "a//b", "\xff", constant.InternalKeyPrefix+"m")

			for _, probe := range probes {
				encodedProbe := p.keyEncoder.Encode(probe)
				for _, comparison := range []ComparisonType{
					ComparisonFloor, ComparisonLower, ComparisonCeiling, ComparisonHigher,
				} {
					// The keys that the iterator visits, moving away from the probe
					var expected []string
					for _, key := range keys {
						cmp := bytes.Compare(p.keyEncoder.Encode(key), encodedProbe)
						if comparison == ComparisonFloor && cmp <= 0 || comparison == ComparisonLower && cmp < 0 ||
							comparison == ComparisonCeiling && cmp >= 0 || comparison == ComparisonHigher && cmp > 0 {
							expected = append(expected, key)
						}
					}
					expected = storedOrder(p, expected)
					backward := comparison == ComparisonFloor || comparison == ComparisonLower
					if backward {
						slices.Reverse(expected)
					}

					it, err := p.Seek(probe, comparison)
					if errors.Is(err, ErrKeyNotFound) {
						assert.Empty(t, expected, "%v %q", comparison, probe)
						continue
					}
					require.NoError(t, err)
					var visited []string
					for it.Valid() {
						value, err := it.Value()
						require.NoError(t, err)
						assert.Equal(t, "v-", string(value[:2]))
						visited = append(visited, it.Key())
						if backward {
							it.Prev()
						} else {
							it.Next()
						}
					}
					require.NoError(t, it.Close())
					assert.Equal(t, expected, visited, "%v %q", comparison, probe)
				}
			}
		})
	}
}

func TestPebbleScan(t *testing.T) {
	for _, keySorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(keySorting.String(), func(t *testing.T) {
			p := newSeekScanKV(t, keySorting)
			keys := seekScanKeys(keySorting)
			expected := storedOrder(p, keys)

			for limit := 1; limit <= len(keys)+1; limit++ {
				var visited []string
				var start []byte
				scans := 0
				for {
					count := 0
					next, err := p.Scan(start, func(key string, value []byte) (bool, error) {
						assert.Equal(t, "v-", string(value[:2]))
						visited = append(visited, key)
						count++
						return count < limit, nil
					})
					require.NoError(t, err)
					scans++
					if next == nil {
						break
					}
					assert.Equal(t, limit, count)
					start = next
				}
				assert.Equal(t, expected, visited, "limit %d", limit)
				// A scan that stops on the last key returns no next one
				assert.Equal(t, (len(keys)+limit-1)/limit, scans, "limit %d", limit)
			}

			// The error of a visit stops the scan
			failure := errors.New("failure")
			_, err := p.Scan(nil, func(string, []byte) (bool, error) { return true, failure })
			assert.ErrorIs(t, err, failure)
		})
	}
}
