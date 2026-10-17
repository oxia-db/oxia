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
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

// ErrNoBackup is returned by Plan when a part of the hash space has no
// backup at the requested time.
var ErrNoBackup = errors.New("backup: no backup covers the range at that time")

// Part is a shard of a restored namespace: the backup it is restored from,
// and its hash range, which is the range of the backup, or a part of it when
// a newer backup covers the rest.
type Part struct {
	Range  Range
	Source Summary
}

// Plan picks, for every part of the hash space, the newest backup started
// at or before asOf (by CreatedAt, or by the time of the ID without it), and
// returns the parts in range order. It returns ErrNoBackup
// when some part has none. A backup can be the source of several parts, when
// newer backups cover parts of its range: e.g. after a split, when only one
// child has a backup at asOf, the other half of the space is restored from
// the parent.
func Plan(backups []Summary, asOf time.Time) ([]Part, error) {
	candidates := make([]Summary, 0, len(backups))
	for _, b := range backups {
		if !b.StartedAt().After(asOf) {
			candidates = append(candidates, b)
		}
	}
	// Newest first: the first candidate that covers a leaf is its source
	slices.SortFunc(candidates, func(a, b Summary) int { return b.ID.Compare(a.ID) })

	// The leaves are the parts of the space between the bounds of the
	// candidates: every candidate either covers a leaf or doesn't touch it
	bounds := []uint64{0, uint64(math.MaxUint32) + 1}
	for _, c := range candidates {
		bounds = append(bounds, uint64(c.Range.Min), uint64(c.Range.Max)+1)
	}
	slices.Sort(bounds)
	bounds = slices.Compact(bounds)

	var parts []Part
	for i := 0; i+1 < len(bounds); i++ {
		leaf := Range{Min: uint32(bounds[i]), Max: uint32(bounds[i+1] - 1)}
		source, ok := newestCovering(candidates, leaf)
		if !ok {
			return nil, fmt.Errorf("%w: %s as of %s", ErrNoBackup, leaf, asOf.UTC().Format(time.RFC3339))
		}
		if n := len(parts); n > 0 && parts[n-1].Source.ID == source.ID && parts[n-1].Source.Shard == source.Shard {
			parts[n-1].Range.Max = leaf.Max
			continue
		}
		parts = append(parts, Part{Range: leaf, Source: source})
	}
	return parts, nil
}

func newestCovering(candidates []Summary, leaf Range) (Summary, bool) {
	for _, c := range candidates {
		if c.Range.Covers(leaf) {
			return c, true
		}
	}
	return Summary{}, false
}
