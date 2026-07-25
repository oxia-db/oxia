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

package database

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/proto"
)

const maxSequence = uint64(math.MaxUint64)

const (
	// sequenceLen is the fixed width of one formatted sequence value:
	// 20 decimal digits, enough for any uint64.
	sequenceLen = 20

	// maxSequenceCacheEntries bounds the per-shard cache of last sequence
	// values. When exceeded, the cache is simply dropped: entries are
	// re-established from storage on the next put for their prefix.
	maxSequenceCacheEntries = 1024
)

// generateUniqueKeyFromSequences computes the key for a sequential put by
// advancing the last sequence values of the prefix.
//
// The last values per prefix are cached on the db: ProcessWrite is
// single-threaded per shard, so the cache needs no locking and later
// sequential puts — including in the same write batch — read it instead of
// re-scanning the indexed batch and re-parsing the last key. The cache is
// invalidated wherever storage can move without it: a direct put of a
// sequence-shaped key and deletes (see invalidateSequenceCache), and range
// deletes drop it entirely. Ephemeral sequence keys removed by a session
// expiry bypass the invalidation, which is benign: the cache stays ahead of
// storage, so subsequent keys are unique with a gap, exactly as if the
// deleted key still existed.
func (d *db) generateUniqueKeyFromSequences(batch kvstore.WriteBatch, req *proto.PutRequest) (string, error) {
	if req.PartitionKey == nil {
		// All the keys need to be in same shard to guarantee atomicity
		return "", ErrMissingPartitionKey
	}

	if req.ExpectedVersionId != nil {
		// Cannot set an expected version id when key is sequential
		return "", ErrBadVersionId
	}

	lastValues, cached := d.sequenceCache[req.Key]
	if !cached {
		parts, err := findCurrentLastKeyInSequence(batch, req)
		if err != nil {
			return "", err
		}
		lastValues = make([]uint64, len(parts))
		for idx, part := range parts {
			value, err := strconv.ParseUint(part, 10, 64)
			if err != nil {
				return "", errors.Wrapf(err, "failed to parse sequence %q", part)
			}
			lastValues[idx] = value
		}
	} else if len(lastValues) > len(req.SequenceKeyDelta) {
		// Same check that findCurrentLastKeyInSequence applies on the
		// storage-derived last key
		return "", ErrMissingSequenceDeltas
	}

	newValues := make([]uint64, len(req.SequenceKeyDelta))
	newKey := make([]byte, 0, len(req.Key)+(sequenceLen+1)*len(req.SequenceKeyDelta))
	newKey = append(newKey, req.Key...)
	for idx, delta := range req.SequenceKeyDelta {
		if idx == 0 && delta == 0 {
			// The first delta in the list must be strictly > 0
			// Otherwise there would be possibility of reordering of keys
			return "", ErrSequenceDeltaIsZero
		}

		var lastValue uint64
		if idx < len(lastValues) {
			lastValue = lastValues[idx]
		}
		newValues[idx] = lastValue + delta
		newKey = appendSequence(newKey, newValues[idx])
	}

	if len(d.sequenceCache) >= maxSequenceCacheEntries {
		clear(d.sequenceCache)
	}
	d.sequenceCache[req.Key] = newValues

	return string(newKey), nil
}

// appendSequence appends "-" plus the value formatted as 20 zero-padded
// decimal digits, the same layout fmt.Sprintf("%s-%020d", ...) produced.
func appendSequence(b []byte, value uint64) []byte {
	b = append(b, '-')
	var digits [sequenceLen]byte
	formatted := strconv.AppendUint(digits[:0], value, 10)
	for i := len(formatted); i < sequenceLen; i++ {
		b = append(b, '0')
	}
	return append(b, formatted...)
}

// invalidateSequenceCache drops the cached sequence values whose prefix the
// given key belongs to. A key belongs to a sequence when it is the prefix
// followed by one or more "-<20 digits>" segments: a direct put of such a key
// moves the storage-side tail without going through the cache, and a delete
// of the last key moves it backwards — both must force the next sequential
// put to re-read storage.
func (d *db) invalidateSequenceCache(key string) {
	if len(d.sequenceCache) == 0 {
		return
	}
	for {
		n := len(key)
		if n < sequenceLen+1 || key[n-(sequenceLen+1)] != '-' {
			return
		}
		for i := n - sequenceLen; i < n; i++ {
			if key[i] < '0' || key[i] > '9' {
				return
			}
		}
		key = key[:n-(sequenceLen+1)]
		delete(d.sequenceCache, key)
	}
}

func findCurrentLastKeyInSequence(wb kvstore.WriteBatch, req *proto.PutRequest) ([]string, error) {
	prefixKey := req.Key
	maxKey := fmt.Sprintf("%s-%020d", prefixKey, maxSequence)
	lastKeyInSequence, err := wb.FindLower(maxKey)
	if err != nil && !errors.Is(err, kvstore.ErrKeyNotFound) {
		return nil, err
	}

	if errors.Is(err, kvstore.ErrKeyNotFound) || !strings.HasPrefix(lastKeyInSequence, prefixKey) {
		lastKeyInSequence = ""
	} else {
		lastKeyInSequence = strings.TrimPrefix(lastKeyInSequence, prefixKey)
	}

	parts := strings.Split(lastKeyInSequence, "-")[1:]
	if len(parts) > len(req.SequenceKeyDelta) {
		// The request has less sequence key deltas than there are already
		// available in the sequence
		return nil, ErrMissingSequenceDeltas
	}
	return parts, nil
}
