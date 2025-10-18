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
	"strings"
	"testing"
	"unsafe"
)

var benchKeyA = "/test/aaaaaaaaaaa/bbbbbbbbbbb/cccccccccccc/dddddddddddddd"
var benchKeyB = "/test/aaaaaaaaaaa/bbbbbbbbbbb/ccccccccccccddddddddddddddd"

var benchKeyAns = "test-aaaaaaaaaaa-bbbbbbbbbbb-cccccccccccc-dddddddddddddd"
var benchKeyBns = "test-aaaaaaaaaaa-bbbbbbbbbbb-ccccccccccccddddddddddddddd"

func Benchmark_BytesCompareCopy(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bytes.Compare([]byte(benchKeyA), []byte(benchKeyB))
	}
}

func Benchmark_BytesCompareUnsafe(b *testing.B) {
	for i := 0; i < b.N; i++ {
		da := unsafe.StringData(benchKeyA)
		ka := unsafe.Slice(da, len(benchKeyA))
		db := unsafe.StringData(benchKeyB)
		kb := unsafe.Slice(db, len(benchKeyB))
		bytes.Compare(ka, kb)
	}
}

func Benchmark_BytesCompareNoCopy(b *testing.B) {
	ka := []byte(benchKeyA)
	kb := []byte(benchKeyB)
	for i := 0; i < b.N; i++ {
		bytes.Compare(ka, kb)
	}
}

func Benchmark_StringsCompare(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strings.Compare(benchKeyA, benchKeyB)
	}
}

func Benchmark_CompareWithSlash(b *testing.B) {
	ka := []byte(benchKeyA)
	kb := []byte(benchKeyB)

	for i := 0; i < b.N; i++ {
		CompareWithSlash(ka, kb)
	}
}

func Benchmark_CompareWithSlashButNoSlashes(b *testing.B) {
	ka := []byte(benchKeyAns)
	kb := []byte(benchKeyBns)

	for i := 0; i < b.N; i++ {
		CompareWithSlash(ka, kb)
	}
}

func Benchmark_EncodeHierarchical(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncoderHierarchical.Encode(benchKeyA)
	}
}

func Benchmark_EncodeHierarchicalButNoSlashes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncoderHierarchical.Encode(benchKeyAns)
	}
}

func Benchmark_DecodeHierarchical(b *testing.B) {
	e := EncoderHierarchical.Encode(benchKeyA)
	for i := 0; i < b.N; i++ {
		EncoderHierarchical.Decode(e)
	}
}

func Benchmark_DecodeHierarchicalButNoSlashes(b *testing.B) {
	e := EncoderHierarchical.Encode(benchKeyAns)
	for i := 0; i < b.N; i++ {
		EncoderHierarchical.Decode(e)
	}
}

func Benchmark_EncodeNatural(b *testing.B) {
	for i := 0; i < b.N; i++ {
		EncoderNatural.Encode(benchKeyA)
	}
}

func Benchmark_DecodeNatural(b *testing.B) {
	e := EncoderHierarchical.Encode(benchKeyA)
	for i := 0; i < b.N; i++ {
		EncoderNatural.Decode(e)
	}
}
