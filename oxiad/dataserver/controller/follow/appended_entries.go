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
	"sync"

	"github.com/oxia-db/oxia/common/proto"
)

const (
	// A follower that keeps up with the leader only needs to keep the entries
	// between its commit offset and its head. One further behind reads the
	// entries past these limits from the wal.
	maxAppendedEntries      = 1024
	maxAppendedEntriesBytes = 4 * 1024 * 1024
)

type appendedEntry struct {
	entry *proto.LogEntry
	// The chained crc of the wal after the entry
	entryCrc uint32
}

// appendedEntries is a bounded FIFO of the entries that a replication stream
// appended to the wal, which the state applier applies instead of reading them
// back from the wal. It holds consecutive entries, within maxAppendedEntries
// entries and maxAppendedEntriesBytes bytes of values: once an entry doesn't
// fit, it drops the next ones too, until the state applier has taken all the
// entries it holds. The state applier reads the dropped entries from the wal.
type appendedEntries struct {
	sync.Mutex
	ring  []appendedEntry
	head  int
	size  int
	bytes int
}

func newAppendedEntries() *appendedEntries {
	return &appendedEntries{
		ring: make([]appendedEntry, maxAppendedEntries),
	}
}

// at returns the i-th oldest entry.
func (a *appendedEntries) at(i int) *appendedEntry {
	return &a.ring[(a.head+i)%len(a.ring)]
}

// add holds an entry appended to the wal, unless it doesn't follow the last
// entry held, or doesn't fit.
func (a *appendedEntries) add(entry *proto.LogEntry, entryCrc uint32) {
	a.Lock()
	defer a.Unlock()
	if a.size > 0 && entry.Offset != a.at(a.size-1).entry.Offset+1 {
		return
	}
	if a.size == len(a.ring) || a.bytes+len(entry.Value) > maxAppendedEntriesBytes {
		return
	}
	*a.at(a.size) = appendedEntry{entry: entry, entryCrc: entryCrc}
	a.size++
	a.bytes += len(entry.Value)
}

// take removes and returns the entry at offset, when it is the first one held
// after dropping the entries before it, which the state applier read from the
// wal instead. Otherwise, it returns the offset of the first entry held, or
// math.MaxInt64 when there is none.
func (a *appendedEntries) take(offset int64) (appendedEntry, int64, bool) {
	a.Lock()
	defer a.Unlock()
	for a.size > 0 && a.at(0).entry.Offset < offset {
		a.dropFirst()
	}
	if a.size == 0 {
		return appendedEntry{}, math.MaxInt64, false
	}
	first := *a.at(0)
	if first.entry.Offset != offset {
		return appendedEntry{}, first.entry.Offset, false
	}
	a.dropFirst()
	return first, offset, true
}

// clear drops all the entries held.
func (a *appendedEntries) clear() {
	a.Lock()
	defer a.Unlock()
	for a.size > 0 {
		a.dropFirst()
	}
}

func (a *appendedEntries) dropFirst() {
	first := a.at(0)
	a.bytes -= len(first.entry.Value)
	*first = appendedEntry{}
	a.head = (a.head + 1) % len(a.ring)
	a.size--
}
