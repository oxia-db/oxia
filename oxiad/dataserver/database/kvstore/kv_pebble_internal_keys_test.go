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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

// Under natural sorting a regular key is stored raw, so a key whose bytes sort
// after the encoded internal prefix ("\xff\xffoxia/") lives *after* the whole
// internal-key region. Pruning the region with an upper bound would silently
// hide it — these keys are the reason the region has to be seeked over instead.
const (
	keyAfterInternalRegion  = "\xff\xffz"
	keyBeforeInternalRegion = "\xff\xffa"
)

func putAll(t *testing.T, kv KV, keys ...string) {
	t.Helper()
	wb := kv.NewWriteBatch()
	for _, k := range keys {
		assert.NoError(t, wb.Put(k, []byte("v-"+k)))
	}
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())
}

// getKey returns the key found, closing the value reader. It never dereferences
// a nil closer, so a lookup that unexpectedly misses fails the assertion instead
// of panicking.
func getKey(t *testing.T, kv KV, key string, cmp ComparisonType, opts IteratorOpts) (string, error) {
	t.Helper()
	found, _, closer, err := kv.Get(key, cmp, opts)
	if err != nil {
		return "", err
	}
	assert.NoError(t, closer.Close())
	return found, nil
}

func scanAllKeys(t *testing.T, kv KV, opts IteratorOpts) []string {
	t.Helper()
	it, err := kv.RangeScan("", "", opts)
	assert.NoError(t, err)
	defer it.Close()

	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, it.Key())
	}
	return keys
}

// A scan that excludes internal keys must still return the regular keys that
// sort after them. This is the case an upper-bound prune would break.
func TestPebbleScanSkipsInternalRegionButKeepsKeysAfterIt(t *testing.T) {
	for _, sorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(sorting.String(), func(t *testing.T) {
			factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
			assert.NoError(t, err)
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, sorting)
			assert.NoError(t, err)

			putAll(t, kv, "a", "b",
				"__oxia/notifications/0001", "__oxia/notifications/0002", "__oxia/session/0001")

			expected := []string{"a", "b"}
			if sorting == proto.KeySortingType_NATURAL {
				// Only the natural encoding can place regular keys around the
				// internal region; hierarchical sorts internal keys strictly last.
				putAll(t, kv, keyBeforeInternalRegion, keyAfterInternalRegion)
				expected = []string{"a", "b", keyBeforeInternalRegion, keyAfterInternalRegion}
			}

			assert.Equal(t, expected, scanAllKeys(t, kv, NoInternalKeys))

			// With internal keys included, everything shows up.
			all := scanAllKeys(t, kv, ShowInternalKeys)
			assert.Contains(t, all, "__oxia/notifications/0001")
			assert.Contains(t, all, "__oxia/session/0001")
			for _, k := range expected {
				assert.Contains(t, all, k)
			}

			assert.NoError(t, kv.Close())
			assert.NoError(t, factory.Close())
		})
	}
}

// The point lookups must seek over the internal region in both directions and
// still see the regular keys on the far side of it.
func TestPebbleGetAcrossInternalRegionNatural(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
	assert.NoError(t, err)

	putAll(t, kv, "a", "__oxia/notifications/0001", "__oxia/notifications/0002", keyAfterInternalRegion)

	// CEILING from past the last "normal" key must jump the internal region and
	// land on the regular key beyond it — not on an internal key, and not
	// NOT_FOUND (which is what bounding the region away would give).
	found, err := getKey(t, kv, "b", ComparisonCeiling, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, keyAfterInternalRegion, found)

	// HIGHER, same jump.
	found, err = getKey(t, kv, "a", ComparisonHigher, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, keyAfterInternalRegion, found)

	// FLOOR/LOWER from above the region must step back over it to the regular
	// key before it, rather than returning an internal key.
	found, err = getKey(t, kv, keyAfterInternalRegion, ComparisonLower, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", found)

	found, err = getKey(t, kv, "\xff\xffy", ComparisonFloor, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "a", found)

	// And with internal keys allowed, the ceiling is the internal key itself.
	found, err = getKey(t, kv, "b", ComparisonCeiling, ShowInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "__oxia/notifications/0001", found)

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

// With hierarchical sorting the internal keys are last, so a ceiling past the
// user keys is genuinely NOT_FOUND — and must not walk the region to find that out.
func TestPebbleGetPastInternalRegionHierarchical(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	putAll(t, kv, "a", "__oxia/notifications/0001", "__oxia/notifications/0002")

	_, _, _, err = kv.Get("b", ComparisonCeiling, NoInternalKeys)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	found, err := getKey(t, kv, "b", ComparisonCeiling, ShowInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, "__oxia/notifications/0001", found)

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

// A ceiling lookup past the last user key used to step through the entire
// internal-key backlog (pebble's SkipPoint is a filter, not a seek). The cost
// must now be flat in the size of that backlog.
func BenchmarkPebbleGetCeilingPastUserKeys(b *testing.B) {
	for _, backlog := range []int{100, 10_000, 100_000} {
		b.Run(fmt.Sprintf("backlog-%d", backlog), func(b *testing.B) {
			factory, err := NewPebbleKVFactory(&FactoryOptions{DataDir: b.TempDir()})
			assert.NoError(b, err)
			defer factory.Close()
			kv, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_NATURAL)
			assert.NoError(b, err)
			defer kv.Close()

			wb := kv.NewWriteBatch()
			assert.NoError(b, wb.Put("a", []byte("v")))
			for i := 0; i < backlog; i++ {
				assert.NoError(b, wb.Put(fmt.Sprintf("__oxia/notifications/%016x", i), []byte("n")))
			}
			assert.NoError(b, wb.Commit())
			assert.NoError(b, wb.Close())
			assert.NoError(b, kv.Flush())

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _, err := kv.Get("b", ComparisonCeiling, NoInternalKeys)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					b.Fatal(err)
				}
			}
		})
	}
}
