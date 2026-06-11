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

package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalToBuffer(t *testing.T) {
	entry := &LogEntry{Term: 5, Offset: 42, Timestamp: 123, Value: []byte("hello")}

	buf, data, err := MarshalToBuffer(nil, entry)
	require.NoError(t, err)
	expected, err := entry.MarshalVT()
	require.NoError(t, err)
	assert.Equal(t, expected, data)

	// A larger message grows the buffer
	larger := &LogEntry{Term: 6, Offset: 43, Value: bytes.Repeat([]byte("x"), 1024)}
	buf, data, err = MarshalToBuffer(buf, larger)
	require.NoError(t, err)
	expected, err = larger.MarshalVT()
	require.NoError(t, err)
	assert.Equal(t, expected, data)
	grownCap := cap(buf)

	// A smaller message reuses the buffer, with no allocation
	smaller := &LogEntry{Term: 7, Offset: 44, Value: []byte("tiny")}
	buf, data, err = MarshalToBuffer(buf, smaller)
	require.NoError(t, err)
	expected, err = smaller.MarshalVT()
	require.NoError(t, err)
	assert.Equal(t, expected, data)
	assert.Equal(t, grownCap, cap(buf))

	decoded := &LogEntry{}
	require.NoError(t, decoded.UnmarshalVT(data))
	assert.EqualValues(t, 7, decoded.Term)
	assert.EqualValues(t, 44, decoded.Offset)
	assert.Equal(t, []byte("tiny"), decoded.Value)

	allocs := testing.AllocsPerRun(100, func() {
		_, _, _ = MarshalToBuffer(buf, smaller)
	})
	assert.Zero(t, allocs)
}
