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

package follow

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func addEntries(a *appendedEntries, firstOffset, lastOffset int64, valueSize int) {
	for offset := firstOffset; offset <= lastOffset; offset++ {
		a.add(&proto.LogEntry{Term: 1, Offset: offset, Value: make([]byte, valueSize)}, uint32(offset))
	}
}

func assertTakes(t *testing.T, a *appendedEntries, firstOffset, lastOffset int64) {
	t.Helper()
	for offset := firstOffset; offset <= lastOffset; offset++ {
		taken, _, ok := a.take(offset)
		if assert.Truef(t, ok, "take of offset %d", offset) {
			assert.EqualValues(t, offset, taken.entry.Offset)
			assert.EqualValues(t, offset, taken.entryCrc)
		}
	}
}

// assertMisses checks that the take of offset finds firstOffset as the first
// entry held instead.
func assertMisses(t *testing.T, a *appendedEntries, offset, firstOffset int64) {
	t.Helper()
	_, first, ok := a.take(offset)
	assert.Falsef(t, ok, "take of offset %d", offset)
	assert.EqualValuesf(t, firstOffset, first, "first offset held at the take of offset %d", offset)
}

func TestAppendedEntries_Take(t *testing.T) {
	a := newAppendedEntries()
	assertMisses(t, a, 0, math.MaxInt64)

	addEntries(a, 5, 9, 10)

	// The entries before the first one are not held
	assertMisses(t, a, 4, 5)

	assertTakes(t, a, 5, 6)
	assertMisses(t, a, 6, 7)

	// The entries before the one taken are dropped: the state applier read
	// them from the wal
	assertTakes(t, a, 8, 8)
	assertMisses(t, a, 8, 9)

	// Taking past the last entry drops them all
	assertMisses(t, a, 10, math.MaxInt64)
	assert.Zero(t, a.bytes)
}

func TestAppendedEntries_ConsecutiveOnly(t *testing.T) {
	a := newAppendedEntries()
	addEntries(a, 0, 1, 10)
	addEntries(a, 3, 3, 10)
	assertTakes(t, a, 0, 1)
	assertMisses(t, a, 2, math.MaxInt64)

	// Once empty, it holds the entries from any offset on
	addEntries(a, 3, 4, 10)
	assertTakes(t, a, 3, 4)
}

func TestAppendedEntries_MaxEntries(t *testing.T) {
	a := newAppendedEntries()
	addEntries(a, 0, maxAppendedEntries, 10)
	assert.Equal(t, maxAppendedEntries, a.size)

	// Room for more entries doesn't bring back the ones dropped: the entries
	// after them are dropped too, until the state applier took all of them
	assertTakes(t, a, 0, 9)
	addEntries(a, maxAppendedEntries+1, maxAppendedEntries+1, 10)
	assertTakes(t, a, 10, maxAppendedEntries-1)
	assertMisses(t, a, maxAppendedEntries, math.MaxInt64)

	addEntries(a, maxAppendedEntries+2, maxAppendedEntries+2, 10)
	assertTakes(t, a, maxAppendedEntries+2, maxAppendedEntries+2)
}

func TestAppendedEntries_MaxBytes(t *testing.T) {
	const valueSize = maxAppendedEntriesBytes / 4
	a := newAppendedEntries()
	addEntries(a, 0, 4, valueSize)
	assert.Equal(t, 4, a.size)
	assert.Equal(t, maxAppendedEntriesBytes, a.bytes)

	assertTakes(t, a, 0, 3)
	assertMisses(t, a, 4, math.MaxInt64)
	assert.Zero(t, a.bytes)

	// An entry bigger than the limit is never held
	addEntries(a, 5, 5, maxAppendedEntriesBytes+1)
	assertMisses(t, a, 5, math.MaxInt64)
}

func TestAppendedEntries_Clear(t *testing.T) {
	a := newAppendedEntries()
	addEntries(a, 0, 9, 10)
	a.clear()
	assert.Zero(t, a.size)
	assert.Zero(t, a.bytes)
	assertMisses(t, a, 0, math.MaxInt64)

	addEntries(a, 10, 11, 10)
	assertTakes(t, a, 10, 11)
}
