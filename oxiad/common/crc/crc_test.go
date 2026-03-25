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

package crc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChecksum_Update(t *testing.T) {
	// Test that updating with same data produces same result
	cs1 := Checksum(0).Update([]byte("hello"))
	cs2 := Checksum(0).Update([]byte("hello"))
	assert.Equal(t, cs1, cs2)

	// Test that different data produces different checksum
	cs3 := Checksum(0).Update([]byte("world"))
	assert.NotEqual(t, cs1, cs3)

	// Test chained updates are deterministic
	cs4 := Checksum(0).Update([]byte("hello")).Update([]byte("world"))
	cs5 := Checksum(0).Update([]byte("hello")).Update([]byte("world"))
	assert.Equal(t, cs4, cs5)

	// CRC32 chained updates equal single concatenated update
	csSingle := Checksum(0).Update([]byte("helloworld"))
	assert.Equal(t, cs4, csSingle)
}

func TestChecksum_Value(t *testing.T) {
	// Test that Value() applies the magic number transformation
	cs := Checksum(0).Update([]byte("test"))
	val := cs.Value()

	// Value should be non-zero due to magic number addition
	assert.NotZero(t, val)

	// Value should be deterministic
	assert.Equal(t, val, cs.Value())
}

func TestChecksum_InitialValue(t *testing.T) {
	// Test starting with different initial values
	cs1 := Checksum(0).Update([]byte("data"))
	cs2 := Checksum(100).Update([]byte("data"))
	assert.NotEqual(t, cs1, cs2)
}

func TestChecksum_EmptyData(t *testing.T) {
	// Test with empty data
	cs1 := Checksum(0).Update([]byte{})
	cs2 := Checksum(0).Update([]byte{})
	assert.Equal(t, cs1, cs2)
	assert.Equal(t, Checksum(0), cs1)
}

func TestChecksum_LargeData(t *testing.T) {
	// Test with larger data
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	cs1 := Checksum(0).Update(largeData)
	cs2 := Checksum(0).Update(largeData)
	assert.Equal(t, cs1, cs2)
}
