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
	"sync"

	"github.com/oxia-db/oxia/common/proto"
)

// defaultTailCacheMaxBytes bounds the in-memory tail cache per WAL. It only
// needs to span the lag of caught-up followers: a follower further behind than
// this falls back to reading the WAL segments, which is the cold catch-up path
// anyway.
const defaultTailCacheMaxBytes = 4 * 1024 * 1024

// tailCache keeps the most recently appended entries in memory so that
// follower cursors tailing the log can read them without touching the WAL
// segments: no copy out of the mmap, no per-follower UnmarshalVT, and — the
// point of the separate mutex — without taking the WAL lock, so reads never
// stall queued appenders.
//
// The cached entries cover a contiguous offset window [baseOffset, baseOffset +
// len(entries)). The oldest entries are evicted once the cached bytes exceed
// maxBytes; the window is therefore always a suffix of the log, which is
// exactly what tailing readers want.
type tailCache struct {
	mu         sync.RWMutex
	entries    []*cachedEntry
	baseOffset int64
	curBytes   int
	maxBytes   int
}

type cachedEntry struct {
	// entry is a private deep copy: the LogEntry handed to the WAL aliases a
	// reusable marshal buffer that the next append overwrites. It is shared
	// read-only with every reader that hits the cache.
	entry       *proto.LogEntry
	previousCrc uint32
	entryCrc    uint32
	size        int
}

func newTailCache(maxBytes int) *tailCache {
	return &tailCache{
		baseOffset: InvalidOffset,
		maxBytes:   maxBytes,
	}
}

// add records the entry just appended at offset entry.Offset. It must be called
// while holding the WAL write lock, so appends, truncations and clears are
// serialized. A non-contiguous offset (after a clear or a restart from a
// non-initial position) resets the window rather than leaving a gap.
func (c *tailCache) add(entry *proto.LogEntry, previousCrc, entryCrc uint32) {
	// CloneVT outside the lock: the entry is owned by the appender goroutine
	// and not yet visible to readers.
	cloned := entry.CloneVT()
	size := cloned.SizeVT()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.baseOffset != InvalidOffset && entry.Offset != c.baseOffset+int64(len(c.entries)) {
		c.resetLocked()
	}
	if c.baseOffset == InvalidOffset {
		c.baseOffset = entry.Offset
	}

	c.entries = append(c.entries, &cachedEntry{
		entry:       cloned,
		previousCrc: previousCrc,
		entryCrc:    entryCrc,
		size:        size,
	})
	c.curBytes += size

	// Evict the oldest entries until within budget, but always keep the one
	// just added.
	for c.curBytes > c.maxBytes && len(c.entries) > 1 {
		c.curBytes -= c.entries[0].size
		c.entries[0] = nil
		c.entries = c.entries[1:]
		c.baseOffset++
	}
}

// get returns the cached entry at the given offset, if present. The returned
// LogEntry is shared and must not be modified.
func (c *tailCache) get(offset int64) (entry *proto.LogEntry, previousCrc, entryCrc uint32, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.baseOffset == InvalidOffset || offset < c.baseOffset {
		return nil, 0, 0, false
	}
	idx := offset - c.baseOffset
	if idx >= int64(len(c.entries)) {
		return nil, 0, 0, false
	}
	ce := c.entries[idx]
	return ce.entry, ce.previousCrc, ce.entryCrc, true
}

// truncate drops the cached entries with an offset greater than lastSafeOffset.
// Must be called while holding the WAL write lock.
func (c *tailCache) truncate(lastSafeOffset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.baseOffset == InvalidOffset {
		return
	}
	if lastSafeOffset < c.baseOffset {
		c.resetLocked()
		return
	}
	keep := lastSafeOffset - c.baseOffset + 1
	if keep >= int64(len(c.entries)) {
		return
	}
	for i := keep; i < int64(len(c.entries)); i++ {
		c.curBytes -= c.entries[i].size
		c.entries[i] = nil
	}
	c.entries = c.entries[:keep]
}

// clear empties the cache. Must be called while holding the WAL write lock.
func (c *tailCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetLocked()
}

func (c *tailCache) resetLocked() {
	c.entries = nil
	c.baseOffset = InvalidOffset
	c.curBytes = 0
}
