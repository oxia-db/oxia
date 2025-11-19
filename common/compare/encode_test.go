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

package compare

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
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
