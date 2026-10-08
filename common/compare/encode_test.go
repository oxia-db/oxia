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

package compare

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
)

func TestEncodeDecode(t *testing.T) {
	for _, test := range []struct {
		key string
	}{
		{"aaa"},
		{"zzzzz"},
		{""},
		{"a"},
		{"/"},
		{"/aaaa"},
		{"/aa/a"},
		{"/aaaa/a"},
		{"/aaaa/a/a"},
		{"/bbbbbbbbbb"},
		{"/aaaa/bbbbbbbbbb"},
		{"/a/b/a/a/a"},
		{"/a/b/a/b"},
	} {
		t.Run(fmt.Sprintf("%v", test.key), func(t *testing.T) {
			e := EncoderHierarchical.Encode(test.key)
			d := EncoderHierarchical.Decode(e)
			assert.Equal(t, test.key, d)
		})
	}
}

func TestEncodeCompare(t *testing.T) {
	cmp := bytes.Compare
	enc := EncoderHierarchical.Encode

	assert.Equal(t, 0, cmp(enc("aaaaa"), enc("aaaaa")))
	assert.Equal(t, -1, cmp(enc("aaaaa"), enc("zzzzz")))
	assert.Equal(t, +1, cmp(enc("bbbbb"), enc("aaaaa")))

	assert.Equal(t, +1, cmp(enc("aaaaa"), enc("")))
	assert.Equal(t, -1, cmp(enc(""), enc("aaaaaa")))
	assert.Equal(t, 0, cmp(enc(""), enc("")))

	assert.Equal(t, -1, cmp(enc("aaaaa"), enc("aaaaaaaaaaa")))
	assert.Equal(t, +1, cmp(enc("aaaaaaaaaaa"), enc("aaa")))

	assert.Equal(t, -1, cmp(enc("a"), enc("/")))
	assert.Equal(t, +1, cmp(enc("/"), enc("a")))

	assert.Equal(t, -1, cmp(enc("/aaaa"), enc("/bbbbb")))
	assert.Equal(t, -1, cmp(enc("/aaaa"), enc("/aa/a")))
	assert.Equal(t, -1, cmp(enc("/aaaa/a"), enc("/aaaa/b")))
	assert.Equal(t, +1, cmp(enc("/aaaa/a/a"), enc("/bbbbbbbbbb")))
	assert.Equal(t, +1, cmp(enc("/aaaa/a/a"), enc("/aaaa/bbbbbbbbbb")))

	assert.Equal(t, +1, cmp(enc("/a/b/a/a/a"), enc("/a/b/a/b")))

	assert.Equal(t, -1, cmp(enc("/a"), enc("/b")))
	assert.Equal(t, -1, cmp(enc("/a"), enc("/a/")))
	assert.Equal(t, -1, cmp(enc("/a/"), enc("/a//")))
	assert.Equal(t, -1, cmp(enc("/a/a-2"), enc("/b/c")))
	assert.Equal(t, -1, cmp(enc("/"), enc("/a")))
	assert.Equal(t, -1, cmp(enc("/a"), enc("//")))
	assert.Equal(t, -1, cmp(enc("/b"), enc("//")))
	assert.Equal(t, -1, cmp(enc("//"), enc("/a/a-1")))

	assert.Equal(t, -1, cmp(enc("//"), enc("/a/a-1")))

	assert.Equal(t, -1, cmp(enc("/a/a-1"), enc("/a//")))
	assert.Equal(t, -1, cmp(enc("/a//"), enc("/b/c")))
}

func TestEncodeInternalKeys(t *testing.T) {
	cmp := bytes.Compare

	for _, encoder := range []Encoder{EncoderNatural, EncoderHierarchical} {
		t.Run(encoder.Name(), func(t *testing.T) {
			enc := encoder.Encode
			assert.False(t, encoder.IsInternalKey(enc("my-key")))
			assert.False(t, encoder.IsInternalKey(enc("/my-key")))

			assert.True(t, encoder.IsInternalKey(enc("__oxia/")))
			assert.True(t, encoder.IsInternalKey(enc("__oxia/xyz")))

			assert.Equal(t, -1, cmp(enc("my-key"), enc("__oxia/xyz")))
			assert.Equal(t, -1, cmp(enc("/my-key"), enc("__oxia/xyz")))
			assert.Equal(t, -1, cmp(enc("/my-key"), enc("__oxia/xyz")))

			k := "__oxia/xyz"
			assert.Equal(t, k, encoder.Decode(encoder.Encode(k)))
		})
	}
}

// A key carries its own separator count in the 2-byte prefix, and that count
// shares the prefix with the internal-key marker. Keys with more separators
// than the prefix can hold must not reach into the marker, nor wrap around and
// come back as a different key.
func TestEncodeDeeplyNestedKeys(t *testing.T) {
	enc := EncoderHierarchical

	for _, test := range []struct {
		name     string
		sepCount int
	}{
		{"below the marker", maxSeparatorCount - 1},
		{"at the marker", internalKeysBitMarker},
		{"past the marker", internalKeysBitMarker + 1},
		{"wrapping the prefix", 1 << 16},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The trailing "a" keeps the last two bytes from being separators,
			// which would otherwise drop the count by one
			key := strings.Repeat("/", test.sepCount) + "a"

			encoded := enc.Encode(key)
			assert.False(t, enc.IsInternalKey(encoded))
			assert.Equal(t, key, enc.Decode(encoded))

			// and it still sorts before the internal keys
			assert.Equal(t, -1, bytes.Compare(encoded, enc.Encode("__oxia/xyz")))
		})
	}
}

func TestEncodeDeeplyNestedInternalKeys(t *testing.T) {
	enc := EncoderHierarchical

	key := constant.InternalKeyPrefix + strings.Repeat("/", 1<<16) + "a"
	encoded := enc.Encode(key)

	assert.True(t, enc.IsInternalKey(encoded))
	assert.Equal(t, key, enc.Decode(encoded))
}

// The keys with a prefix are in a single range with the natural encoder, and in
// one range per level with the hierarchical one. From a key without the prefix,
// NextStart and PrevEnd have to move past the key, towards the nearest keys
// with the prefix, without skipping any.
func TestPrefixRanges(t *testing.T) {
	// Every key of up to 4 bytes made of 'a', 'b' and the separator, and keys
	// deeper than the level can count, each as a regular and an internal key
	var keys []string
	var add func(key string)
	add = func(key string) {
		keys = append(keys, key, constant.InternalKeyPrefix+key)
		if len(key) < 4 {
			for _, c := range []string{"a", "b", "/"} {
				add(key + c)
			}
		}
	}
	add("")
	for _, key := range []string{strings.Repeat("a/", 1<<15) + "a", strings.Repeat("b/", 1<<15) + "b"} {
		keys = append(keys, key, constant.InternalKeyPrefix+key)
	}

	for _, encoder := range []Encoder{EncoderNatural, EncoderHierarchical} {
		t.Run(encoder.Name(), func(t *testing.T) {
			encoded := make(map[string][]byte, len(keys))
			for _, key := range keys {
				encoded[key] = encoder.Encode(key)
			}
			slices.SortFunc(keys, func(a, b string) int { return bytes.Compare(encoded[a], encoded[b]) })

			for _, prefix := range []string{"", "a", "a/", "a//", "/", "//", "b/a", "ba/"} {
				for _, prefix := range []string{prefix, constant.InternalKeyPrefix + prefix} {
					ranges := encoder.PrefixRanges(prefix)
					lower, upper := ranges.Bounds()

					// The positions of the keys with the prefix
					var withPrefix []int
					for i, key := range keys {
						hasPrefix := strings.HasPrefix(key, prefix)
						assert.Equal(t, hasPrefix, ranges.Contains(encoded[key]), "%q %.40q", prefix, key)
						if hasPrefix {
							withPrefix = append(withPrefix, i)
							assert.LessOrEqual(t, bytes.Compare(lower, encoded[key]), 0, "%q %.40q", prefix, key)
							assert.True(t, upper == nil || bytes.Compare(encoded[key], upper) < 0, "%q %.40q", prefix, key)
						}
					}

					for i, key := range keys {
						if strings.HasPrefix(key, prefix) {
							continue
						}
						n, _ := slices.BinarySearch(withPrefix, i)
						before, after := withPrefix[:n], withPrefix[n:]

						if next := ranges.NextStart(encoded[key]); next == nil {
							assert.Empty(t, after, "%q %.40q", prefix, key)
						} else {
							assert.Positive(t, bytes.Compare(next, encoded[key]), "%q %.40q", prefix, key)
							if len(after) > 0 {
								assert.GreaterOrEqual(t, bytes.Compare(encoded[keys[after[0]]], next), 0, "%q %.40q", prefix, key)
							}
						}

						if prev := ranges.PrevEnd(encoded[key]); prev == nil {
							assert.Empty(t, before, "%q %.40q", prefix, key)
						} else {
							assert.LessOrEqual(t, bytes.Compare(prev, encoded[key]), 0, "%q %.40q", prefix, key)
							if len(before) > 0 {
								assert.Negative(t, bytes.Compare(encoded[keys[before[len(before)-1]]], prev), "%q %.40q", prefix, key)
							}
						}
					}
				}
			}
		})
	}
}

// The buffer passed to Decode is Pebble memory, handed out under an explicit
// read-only contract (Iterator.Key): it must not be modified. In the current
// pebble version it is the iterator's own position buffer, which pebble keeps
// using internally — the layers below alias the shared block cache, with the
// top-level iterator copying every key before it reaches the caller.
func TestEncoderNaturalDecodeDoesNotMutateInput(t *testing.T) {
	encoded := []byte("\xff\xffoxia/session/123")
	original := bytes.Clone(encoded)

	decoded := encoderNatural{}.Decode(encoded)
	assert.Equal(t, "__oxia/session/123", decoded)
	assert.Equal(t, original, encoded)

	// Non-internal keys are returned as-is, also untouched
	plain := []byte("a/b/c")
	originalPlain := bytes.Clone(plain)
	assert.Equal(t, "a/b/c", encoderNatural{}.Decode(plain))
	assert.Equal(t, originalPlain, plain)
}

func BenchmarkEncoderNaturalDecode(b *testing.B) {
	internal := []byte("\xff\xffoxia/session/0000000000123456")
	plain := []byte("/some/regular/application/key-123456")

	b.Run("internal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = encoderNatural{}.Decode(internal)
		}
	})
	b.Run("plain", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = encoderNatural{}.Decode(plain)
		}
	})
}
