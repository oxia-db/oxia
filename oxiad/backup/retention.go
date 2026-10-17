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

package backup

import (
	"slices"
	"sort"
	"time"
)

// Expired returns, among the backups of a namespace, the ones to expire at
// now under the retention: a backup expires once it is older than the
// retention, and a newer backup, or a set of newer backups, covering its
// whole hash range is older than the retention too. Until then it is the
// newest backup of some part of its range as of the start of the retention
// window, which a restore to that time needs.
//
// The newer backups of a range include the backups of the same shard, and
// those of the children of a split, which cover the range of their parent
// between them.
//
// The age of a backup is its start time (see Summary.StartedAt), the time a
// restore selects by too; the ID only breaks the ties of backups started at
// the same instant.
func Expired(backups []Summary, retention time.Duration, now time.Time) []Summary {
	cutoff := now.Add(-retention)
	// The backups older than the retention, newest first: only they can
	// expire, and only they can expire another one.
	old := make([]Summary, 0, len(backups))
	for _, b := range backups {
		if b.StartedAt().Before(cutoff) {
			old = append(old, b)
		}
	}
	slices.SortFunc(old, func(a, b Summary) int {
		if c := b.StartedAt().Compare(a.StartedAt()); c != 0 {
			return c
		}
		return b.ID.Compare(a.ID)
	})

	var expired []Summary
	var newer coverage
	for _, b := range old {
		// The newer ones were added before b
		if newer.covers(b.Range) {
			expired = append(expired, b)
		}
		newer.add(b.Range)
	}
	return expired
}

// coverage is the union of a set of ranges, kept as sorted ranges that
// neither overlap nor touch, so that a range is covered by the union exactly
// when one of them contains it.
type coverage []Range

// covers reports whether the union covers the whole of r.
func (c coverage) covers(r Range) bool {
	// The last range starting at or before r.Min is the only one that can
	// contain r
	i := sort.Search(len(c), func(i int) bool { return c[i].Min > r.Min }) - 1
	return i >= 0 && c[i].Max >= r.Max
}

// add merges r into the union.
func (c *coverage) add(r Range) {
	s := *c
	// The ranges from lo to hi overlap or touch r, and merge with it
	lo := sort.Search(len(s), func(i int) bool { return uint64(s[i].Max)+1 >= uint64(r.Min) })
	hi := sort.Search(len(s), func(i int) bool { return uint64(s[i].Min) > uint64(r.Max)+1 })
	merged := r
	if lo < hi {
		merged.Min = min(merged.Min, s[lo].Min)
		merged.Max = max(merged.Max, s[hi-1].Max)
	}
	*c = slices.Replace(s, lo, hi, merged)
}
