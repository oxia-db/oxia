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
	"log/slog"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/proto"
)

const (
	sessionKeyPrefix = constant.InternalKeyPrefix + "session"
	sessionKeyLength = len(sessionKeyPrefix) + 1 + 16 // __oxia/session/ + 16 hex digits

	// Filtering deletes about half the shard: accumulated in a single indexed
	// batch, that grows to GB scale (batch arena plus the batch skiplist).
	// Committing in bounded chunks keeps the memory flat. This is safe here:
	// nothing reads through the batch (every classification reads the
	// iterators, which keep the consistent view they were created with across
	// commits), the child is not serving yet so there are no concurrent
	// writers, and the filter is idempotent — on a failed snapshot load it
	// either re-runs or the snapshot is re-sent wholesale.
)

// Vars, not consts, so tests can shrink them to exercise the chunk rotation
// with small datasets.
var (
	splitFilterMaxBatchCount = 100_000
	splitFilterMaxBatchBytes = 8 * 1024 * 1024
)

// FilterDBForSplit removes keys that do not belong to the specified hash range.
// This is called on a child shard after loading a parent's snapshot.
//
// Key classification:
//   - User data keys: filter by hash of partition_key (or key if no partition_key)
//   - __oxia/last-version-id, commit-offset, features, term, term-options: keep
//   - __oxia/checksum: delete (invalid after filtering)
//   - __oxia/notifications/{offset}: filter each notification by its record
//     (see isNotificationInRange); delete if empty
//   - __oxia/session/{id}: keep (session metadata duplicated to both children)
//   - __oxia/session/{id}/{user-key}: filter by the record at user-key
//   - __oxia/idx/{idx}/{sec}\x01{pri}: filter by the record at pri
//
// The internal keys that belong to a record follow it, so they are placed by
// the record's partition key too, not by the record key. The ones whose record
// does not exist are deleted: no child holds the record.
func FilterDBForSplit(kv kvstore.KV, hashRange *proto.HashRange) error {
	slog.Info(
		"Filtering database for shard split",
		slog.Any("hash-range-min", hashRange.GetMin()),
		slog.Any("hash-range-max", hashRange.GetMax()),
	)

	batch := kv.NewWriteBatch()
	// Closes whichever batch is current when returning; rotated chunks are
	// closed at rotation
	defer func() { _ = batch.Close() }()

	it, err := kv.RangeScan("", "", kvstore.ShowInternalKeys)
	if err != nil {
		return errors.Wrap(err, "failed to create range scan for split filter")
	}
	defer it.Close()

	// The records that the internal keys belong to are looked up with a
	// second iterator. By the time an internal key is reached, the chunks
	// committed along the way may have deleted its record (with hierarchical
	// sorting, every user key comes first): like the scan, the lookups must
	// see the records as they were before filtering, so the iterator is
	// created before the first commit.
	records, err := kv.RangeScan("", "", kvstore.NoInternalKeys)
	if err != nil {
		return errors.Wrap(err, "failed to create record lookup for split filter")
	}
	defer records.Close()

	var deletedKeys, keptKeys, filteredNotifications, deletedNotifications int64
	chunks := int64(1)

	for it.Valid() {
		key := it.Key()

		if strings.HasPrefix(key, constant.InternalKeyPrefix) {
			action, err := classifyInternalKey(key, it, records, batch, hashRange)
			if err != nil {
				return errors.Wrapf(err, "failed to classify internal key %q", key)
			}
			switch action {
			case splitActionDelete:
				if err := batch.Delete(key); err != nil {
					return err
				}
				deletedKeys++
			case splitActionFilteredNotification:
				filteredNotifications++
			case splitActionDeletedNotification:
				deletedNotifications++
				deletedKeys++
			default:
				keptKeys++
			}
		} else {
			deleted, err := filterUserKey(it, batch, key, hashRange)
			if err != nil {
				return err
			}
			if deleted {
				deletedKeys++
			} else {
				keptKeys++
			}
		}

		if batch, err = rotateSplitBatchIfFull(kv, batch, &chunks); err != nil {
			return err
		}

		it.Next()
	}

	slog.Info(
		"Split filter complete, committing",
		slog.Int64("deleted-keys", deletedKeys),
		slog.Int64("kept-keys", keptKeys),
		slog.Int64("filtered-notifications", filteredNotifications),
		slog.Int64("deleted-notifications", deletedNotifications),
		slog.Int64("chunks", chunks),
	)

	return batch.Commit()
}

// filterUserKey deletes the user key when it falls outside the child's hash
// range, reporting whether it did.
func filterUserKey(it kvstore.KeyValueIterator, batch kvstore.WriteBatch, key string, hashRange *proto.HashRange) (bool, error) {
	value, err := it.Value()
	if err != nil {
		return false, errors.Wrapf(err, "failed to read value for key %q", key)
	}

	if isUserKeyInRange(key, value, hashRange) {
		return false, nil
	}
	if err := batch.Delete(key); err != nil {
		return false, err
	}
	return true, nil
}

// rotateSplitBatchIfFull commits and closes the batch once it reaches the
// chunk thresholds, returning a fresh batch to continue with.
func rotateSplitBatchIfFull(kv kvstore.KV, batch kvstore.WriteBatch, chunks *int64) (kvstore.WriteBatch, error) {
	if batch.Count() < splitFilterMaxBatchCount && batch.Size() < splitFilterMaxBatchBytes {
		return batch, nil
	}
	if err := batch.Commit(); err != nil {
		return batch, errors.Wrap(err, "failed to commit split filter chunk")
	}
	if err := batch.Close(); err != nil {
		return batch, errors.Wrap(err, "failed to close split filter chunk")
	}
	(*chunks)++
	return kv.NewWriteBatch(), nil
}

type splitAction int

const (
	splitActionKeep splitAction = iota
	splitActionDelete
	splitActionFilteredNotification
	splitActionDeletedNotification
)

func classifyInternalKey(
	key string,
	it kvstore.KeyValueIterator,
	records kvstore.KeyValueIterator,
	batch kvstore.WriteBatch,
	hashRange *proto.HashRange,
) (splitAction, error) {
	switch {
	case key == commitOffsetKey,
		key == commitLastVersionIdKey,
		key == termKey,
		key == termOptionsKey,
		strings.HasPrefix(key, featureFlagKeyPrefix+"/"):
		// Metadata keys: keep in both children
		return splitActionKeep, nil

	case key == commitChecksumKey:
		// Checksum is invalid after filtering
		return splitActionDelete, nil

	case strings.HasPrefix(key, notificationsPrefix+"/"):
		return filterNotificationKey(key, it, records, batch, hashRange)

	case isSessionMetadataKey(key):
		// Session metadata: keep in both children (session may own keys in either)
		return splitActionKeep, nil

	case strings.HasPrefix(key, sessionKeyPrefix+"/"):
		// Session shadow key: __oxia/session/{id}/{url_escaped_user_key}
		return classifySessionShadowKey(key, records, hashRange)

	case strings.HasPrefix(key, idxKeyPrefix+"/"):
		// Secondary index key: __oxia/idx/{name}/{secondary}\x01{url_escaped_primary}
		return classifySecondaryIndexKey(key, records, hashRange)

	default:
		// Unknown internal key: keep by default (safe)
		return splitActionKeep, nil
	}
}

// isSessionMetadataKey checks if the key is a session metadata key (not a shadow key).
// Session metadata key: __oxia/session/{16hex} (exact length)
// Shadow key: __oxia/session/{16hex}/{user_key} (longer).
func isSessionMetadataKey(key string) bool {
	if !strings.HasPrefix(key, sessionKeyPrefix) {
		return false
	}
	return len(key) == sessionKeyLength
}

// filterNotificationKey deserializes the notification batch, removes entries
// that do not belong to the hash range, and either rewrites the batch or
// deletes it.
func filterNotificationKey(
	key string,
	it kvstore.KeyValueIterator,
	records kvstore.KeyValueIterator,
	batch kvstore.WriteBatch,
	hashRange *proto.HashRange,
) (splitAction, error) {
	value, err := it.Value()
	if err != nil {
		return splitActionKeep, errors.Wrap(err, "failed to read notification value")
	}

	nb := &proto.NotificationBatch{}
	if err := nb.UnmarshalVT(value); err != nil {
		return splitActionKeep, errors.Wrap(err, "failed to deserialize notification batch")
	}

	// Filter: remove notifications that belong to the other child. The slice
	// keeps its original (sorted) order, so the rewrite below stays
	// deterministic.
	originalLen := len(nb.Notifications)
	kept := nb.Notifications[:0]
	for _, entry := range nb.Notifications {
		inRange, err := isNotificationInRange(entry, records, hashRange)
		if err != nil {
			return splitActionKeep, err
		}
		if inRange {
			kept = append(kept, entry)
		}
	}
	nb.Notifications = kept

	if len(nb.Notifications) == 0 {
		// All notifications were for keys outside our range: delete entirely
		if err := batch.Delete(key); err != nil {
			return splitActionKeep, err
		}
		return splitActionDeletedNotification, nil
	}

	if len(nb.Notifications) < originalLen {
		// Some notifications were removed: rewrite
		if err := batch.PutMarshalable(key, nb); err != nil {
			return splitActionKeep, err
		}
		return splitActionFilteredNotification, nil
	}

	// All notifications were in range: keep as-is
	return splitActionKeep, nil
}

// isNotificationInRange places a notification with the record it is about,
// like the record's index entries and shadow key. A notification does not carry
// the partition key, so the one of a record that no longer exists falls back to
// the hash of its key: exact for the records written without a partition key.
// A range deletion is kept in both children: every shard applies it, and so do
// both children for the range deletions they apply from the log.
func isNotificationInRange(
	entry *proto.NotificationEntry,
	records kvstore.KeyValueIterator,
	hashRange *proto.HashRange,
) (bool, error) {
	if entry.GetValue().GetType() == proto.NotificationType_KEY_RANGE_DELETED {
		return true, nil
	}

	found, inRange, err := lookupRecord(records, entry.GetKey(), hashRange)
	switch {
	case err != nil:
		return false, err
	case found:
		return inRange, nil
	default:
		return isHashInRange(hash.Xxh332(entry.GetKey()), hashRange), nil
	}
}

// classifySessionShadowKey keeps a session shadow key where the ephemeral
// record it belongs to is kept.
// Shadow key format: __oxia/session/{16hex}/{url_escaped_user_key}.
func classifySessionShadowKey(
	key string,
	records kvstore.KeyValueIterator,
	hashRange *proto.HashRange,
) (splitAction, error) {
	// Find the position after "__oxia/session/{16hex}/"
	prefix := sessionKeyPrefix + "/"
	rest := key[len(prefix):]

	// Skip the 16-hex-char session ID
	slashIdx := strings.Index(rest, "/")
	if slashIdx < 0 {
		// This is a session metadata key (no user key suffix), keep it
		return splitActionKeep, nil
	}

	escapedUserKey := rest[slashIdx+1:]
	if userKey, err := url.PathUnescape(escapedUserKey); err == nil {
		return classifyByRecord(records, userKey, hashRange)
	}
	// Can't parse: keep to be safe
	return splitActionKeep, nil
}

// classifySecondaryIndexKey keeps a secondary index entry where the record it
// points to is kept.
// Format: __oxia/idx/{name}/{secondary}\x01{url_escaped_primary}.
func classifySecondaryIndexKey(
	key string,
	records kvstore.KeyValueIterator,
	hashRange *proto.HashRange,
) (splitAction, error) {
	if primaryKey, _, err := ParseSecondaryIndexKey(key); err == nil {
		return classifyByRecord(records, primaryKey, hashRange)
	}
	// Can't parse: keep to be safe
	return splitActionKeep, nil
}

// classifyByRecord keeps an internal key that belongs to a record where the
// record is kept. One whose record does not exist points to nothing: no child
// holds the record, so neither keeps it.
func classifyByRecord(
	records kvstore.KeyValueIterator,
	recordKey string,
	hashRange *proto.HashRange,
) (splitAction, error) {
	found, inRange, err := lookupRecord(records, recordKey, hashRange)
	switch {
	case err != nil:
		return splitActionKeep, err
	case found && inRange:
		return splitActionKeep, nil
	default:
		return splitActionDelete, nil
	}
}

// lookupRecord reports whether the record at key exists, as it was before
// filtering, and whether it belongs to the hash range, by the same rule that
// places the record itself.
func lookupRecord(
	records kvstore.KeyValueIterator,
	key string,
	hashRange *proto.HashRange,
) (found, inRange bool, err error) {
	if !records.SeekGE(key) || records.Key() != key {
		return false, false, nil
	}

	value, err := records.Value()
	if err != nil {
		return false, false, errors.Wrapf(err, "failed to read record %q", key)
	}
	return true, isUserKeyInRange(key, value, hashRange), nil
}

// isUserKeyInRange determines if a user data key belongs to the given hash range.
// If the StorageEntry has a partition_key, that is hashed; otherwise the key itself.
func isUserKeyInRange(key string, value []byte, hashRange *proto.HashRange) bool {
	se := proto.StorageEntryFromVTPool()
	defer se.ReturnToVTPool()

	if err := Deserialize(value, se); err != nil {
		// Can't deserialize: hash by key
		h := hash.Xxh332(key)
		return isHashInRange(h, hashRange)
	}

	var h uint32
	if se.PartitionKey != nil {
		h = hash.Xxh332(*se.PartitionKey)
	} else {
		h = hash.Xxh332(key)
	}

	return isHashInRange(h, hashRange)
}

func isHashInRange(h uint32, hashRange *proto.HashRange) bool {
	return h >= hashRange.GetMin() && h <= hashRange.GetMax()
}

// FilterWriteRequestForSplit filters a WriteRequest to only include operations
// for keys within the given hash range. Returns nil if nothing remains.
// This is used at the state machine apply level when a child shard processes
// WAL entries inherited from its parent.
func FilterWriteRequestForSplit(req *proto.WriteRequest, hashRange *proto.HashRange) *proto.WriteRequest {
	if req == nil {
		return nil
	}

	var puts []*proto.PutRequest
	for _, p := range req.Puts {
		if strings.HasPrefix(p.Key, constant.InternalKeyPrefix) {
			// Not a record of either child: a session is created by a put of
			// its metadata key, and both children need it, as FilterDBForSplit
			// keeps the sessions of the snapshot in both. Placed by the hash
			// of its key, the other child would reject the ephemeral records
			// of the session for an unknown session.
			puts = append(puts, p)
			continue
		}

		var h uint32
		if p.PartitionKey != nil {
			h = hash.Xxh332(*p.PartitionKey)
		} else {
			h = hash.Xxh332(p.Key)
		}
		if isHashInRange(h, hashRange) {
			puts = append(puts, p)
		}
	}

	var deletes []*proto.DeleteRequest
	for _, d := range req.Deletes {
		if d.PartitionKey == nil {
			// For backward compatibility, a missing partition key cannot be
			// treated like it is for puts. Older clients used the partition key
			// to route a delete but did not serialize it into DeleteRequest, so
			// nil does not prove that the delete was routed by its record key.
			// Keep it in both children to avoid resurrecting deleted records.
			deletes = append(deletes, d)
			continue
		}

		// An explicitly empty partition key is still present: existing clients
		// accept it and route the request using hash("").
		if isHashInRange(hash.Xxh332(*d.PartitionKey), hashRange) {
			deletes = append(deletes, d)
		}
	}

	deleteRanges := make([]*proto.DeleteRangeRequest, 0, len(req.DeleteRanges))
	deleteRanges = append(deleteRanges, req.DeleteRanges...)

	if len(puts) == 0 && len(deletes) == 0 && len(deleteRanges) == 0 {
		return nil
	}

	return &proto.WriteRequest{
		Shard:        req.Shard,
		Puts:         puts,
		Deletes:      deletes,
		DeleteRanges: deleteRanges,
	}
}
