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

package wal

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

func cacheEntry(offset int64, value string) *proto.LogEntry {
	return &proto.LogEntry{Term: 1, Offset: offset, Value: []byte(value)}
}

func TestTailCache_AddGet(t *testing.T) {
	c := newTailCache(1 << 20)

	_, _, _, ok := c.get(0)
	assert.False(t, ok)

	c.add(cacheEntry(0, "a"), 10, 11)
	c.add(cacheEntry(1, "bb"), 11, 12)

	entry, prev, cur, ok := c.get(0)
	assert.True(t, ok)
	assert.EqualValues(t, 0, entry.Offset)
	assert.Equal(t, []byte("a"), entry.Value)
	assert.EqualValues(t, 10, prev)
	assert.EqualValues(t, 11, cur)

	entry, prev, cur, ok = c.get(1)
	assert.True(t, ok)
	assert.Equal(t, []byte("bb"), entry.Value)
	assert.EqualValues(t, 11, prev)
	assert.EqualValues(t, 12, cur)

	// Out of range
	_, _, _, ok = c.get(2)
	assert.False(t, ok)
	_, _, _, ok = c.get(-1)
	assert.False(t, ok)
}

// The cached entry must be independent of the source LogEntry, whose Value
// aliases a reusable buffer the next append overwrites.
func TestTailCache_AddCopiesEntry(t *testing.T) {
	c := newTailCache(1 << 20)
	value := []byte("original")
	c.add(&proto.LogEntry{Term: 1, Offset: 0, Value: value}, 0, 1)

	// Overwrite the source buffer, as the next append would
	copy(value, []byte("OVERWRIT"))

	entry, _, _, ok := c.get(0)
	assert.True(t, ok)
	assert.Equal(t, []byte("original"), entry.Value)
}

func TestTailCache_Eviction(t *testing.T) {
	// Each entry's SizeVT is well above 4 bytes, so a 64-byte budget holds
	// only a handful of entries.
	c := newTailCache(64)
	const n = 50
	for i := int64(0); i < n; i++ {
		c.add(cacheEntry(i, fmt.Sprintf("value-%04d", i)), uint32(i), uint32(i+1))
	}

	// The newest entry is always retained; the oldest are gone.
	_, _, _, ok := c.get(n - 1)
	assert.True(t, ok)
	_, _, _, ok = c.get(0)
	assert.False(t, ok)

	// The retained window is contiguous up to the head, and within budget.
	assert.LessOrEqual(t, c.curBytes, c.maxBytes)
	for off := c.baseOffset; off < n; off++ {
		_, _, _, ok := c.get(off)
		assert.True(t, ok, "offset %d should be cached", off)
	}
	// Nothing below baseOffset.
	_, _, _, ok = c.get(c.baseOffset - 1)
	assert.False(t, ok)
}

func TestTailCache_Truncate(t *testing.T) {
	c := newTailCache(1 << 20)
	for i := int64(0); i < 10; i++ {
		c.add(cacheEntry(i, "v"), uint32(i), uint32(i+1))
	}

	c.truncate(5)
	_, _, _, ok := c.get(5)
	assert.True(t, ok)
	_, _, _, ok = c.get(6)
	assert.False(t, ok)

	// Truncating below the base clears everything.
	c.truncate(-1)
	_, _, _, ok = c.get(0)
	assert.False(t, ok)
	assert.EqualValues(t, InvalidOffset, c.baseOffset)
}

func TestTailCache_Clear(t *testing.T) {
	c := newTailCache(1 << 20)
	c.add(cacheEntry(0, "v"), 0, 1)
	c.clear()
	_, _, _, ok := c.get(0)
	assert.False(t, ok)
	assert.EqualValues(t, InvalidOffset, c.baseOffset)
	assert.Equal(t, 0, c.curBytes)
}

// A non-contiguous offset (e.g. after the WAL is cleared and restarts from a
// non-initial position) resets the window instead of leaving a gap.
func TestTailCache_NonContiguousReset(t *testing.T) {
	c := newTailCache(1 << 20)
	c.add(cacheEntry(0, "a"), 0, 1)
	c.add(cacheEntry(1, "b"), 1, 2)

	c.add(cacheEntry(100, "z"), 0, 1)

	_, _, _, ok := c.get(0)
	assert.False(t, ok)
	_, _, _, ok = c.get(1)
	assert.False(t, ok)
	entry, _, _, ok := c.get(100)
	assert.True(t, ok)
	assert.Equal(t, []byte("z"), entry.Value)
}

// Reads of recently appended entries must be served from the tail cache (same
// shared pointer), while reads of entries no longer cached fall back to the
// WAL segments and return byte-identical content and CRCs.
func TestWAL_ReadServedFromTailCache(t *testing.T) {
	f, w := createWal(t)
	defer func() {
		assert.NoError(t, w.Close())
		assert.NoError(t, f.Close())
	}()

	tw := w.(*wal)
	const n = 20
	for i := int64(0); i < n; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: i, Value: []byte(fmt.Sprintf("value-%d", i))}))
	}

	// A recent offset is in the cache: readAtIndex returns the very pointer
	// the cache holds, proving it did not re-read and re-unmarshal.
	cached, cachedPrev, cachedCur, ok := tw.tailCache.get(n - 1)
	assert.True(t, ok)
	got, prev, cur, err := tw.readAtIndex(n - 1)
	assert.NoError(t, err)
	assert.Same(t, cached, got)
	assert.Equal(t, cachedPrev, prev)
	assert.Equal(t, cachedCur, cur)

	// Drop the cache: the same read now comes from the segment — a distinct
	// object, but byte-identical content and identical CRCs.
	tw.tailCache.clear()
	fromSegment, segPrev, segCur, err := tw.readAtIndex(n - 1)
	assert.NoError(t, err)
	assert.NotSame(t, cached, fromSegment)
	assert.Equal(t, cached.Value, fromSegment.Value)
	assert.EqualValues(t, cached.Offset, fromSegment.Offset)
	assert.Equal(t, cachedPrev, segPrev)
	assert.Equal(t, cachedCur, segCur)
}

func BenchmarkWALReadAtIndex(b *testing.B) {
	dir := b.TempDir()
	f := NewWalFactory(&FactoryOptions{
		BaseWalDir:  dir,
		Retention:   1 * time.Hour,
		SegmentSize: 1 << 20,
		SyncData:    false,
	})
	defer f.Close()
	w, err := f.NewWal(constant.DefaultNamespace, 1, nil)
	assert.NoError(b, err)
	defer w.Close()

	tw := w.(*wal)
	const n = 1000
	value := make([]byte, 256)
	for i := int64(0); i < n; i++ {
		assert.NoError(b, w.Append(&proto.LogEntry{Term: 1, Offset: i, Value: value}))
	}

	b.Run("cache-hit", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, _, err := tw.readAtIndex(n - 1); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("segment", func(b *testing.B) {
		tw.tailCache.clear()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, _, err := tw.readAtIndex(0); err != nil {
				b.Fatal(err)
			}
		}
	})
}
