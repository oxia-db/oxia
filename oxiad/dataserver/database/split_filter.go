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
	"encoding/json"
	"fmt"
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
	// iterator, which keeps the consistent view it was created with across
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
//   - __oxia/notifications/{offset}: filter notification map by key hash; delete if empty
//   - __oxia/session/{id}: keep (session metadata duplicated to both children)
//   - __oxia/session/{id}/{user-key}, __oxia/idx/{idx}/{sec}\x01{pri}: delete
//     with the record at user-key or pri
//
// The session shadow key and the index entries of a record go where the
// record goes, which its partition key decides, not the hash of its key: the
// record lists them, and they are deleted with it, as the leader deletes them
// with a record. One left without its record by some other bug is kept by
// both children: telling it apart would take a lookup of the record for each.
//
// A notification does not carry the partition key: it stays placed by the
// hash of its key, which can put the one of a record written with a partition
// key in the other child. Placing it with its record would take a random read
// for each retained notification, while the clients never read the
// notifications a child inherits: they subscribe to a new shard from its
// commit offset.
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

	var deletedKeys, keptKeys, filteredNotifications, deletedNotifications int64
	chunks := int64(1)

	for it.Valid() {
		key := it.Key()

		if strings.HasPrefix(key, constant.InternalKeyPrefix) {
			action, err := classifyInternalKey(key, it, batch, hashRange)
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

	// The iteration also stops when a read fails
	if err := it.Error(); err != nil {
		return errors.Wrap(err, "failed to scan the database for split filter")
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
// range, reporting whether it did. The internal keys that belong to the record
// are deleted with it.
func filterUserKey(it kvstore.KeyValueIterator, batch kvstore.WriteBatch, key string, hashRange *proto.HashRange) (bool, error) {
	value, err := it.Value()
	if err != nil {
		return false, errors.Wrapf(err, "failed to read value for key %q", key)
	}

	se := proto.StorageEntryFromVTPool()
	defer se.ReturnToVTPool()
	if err := Deserialize(value, se); err != nil {
		// Can't deserialize: hash by key, and no internal keys to delete
		se.ResetVT()
	}

	if isUserKeyInRange(key, se, hashRange) {
		return false, nil
	}
	if err := batch.Delete(key); err != nil {
		return false, err
	}
	return true, deleteRecordInternalKeys(batch, key, se)
}

// deleteRecordInternalKeys deletes the internal keys that belong to a record,
// as the leader does when it deletes the record: its secondary index entries
// and, for an ephemeral record, its session shadow key. They are built from the
// record like the leader writes them (lead.secondaryIndexKey, lead.ShadowKey).
func deleteRecordInternalKeys(batch kvstore.WriteBatch, key string, se *proto.StorageEntry) error {
	escapedKey := url.PathEscape(key)
	for _, si := range se.SecondaryIndexes {
		idxKey := fmt.Sprintf("%s/%s/%s%s%s", idxKeyPrefix, si.IndexName, si.SecondaryKey, idxSeparator, escapedKey)
		if err := batch.Delete(idxKey); err != nil {
			return err
		}
	}
	if se.SessionId != nil {
		shadow := fmt.Sprintf("%s/%016x/%s", sessionKeyPrefix, *se.SessionId, escapedKey)
		if err := batch.Delete(shadow); err != nil {
			return err
		}
	}
	return nil
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
		return filterNotificationKey(key, it, batch, hashRange)

	case isSessionMetadataKey(key):
		// Session metadata: keep in both children (session may own keys in either)
		return splitActionKeep, nil

	case strings.HasPrefix(key, sessionKeyPrefix+"/"),
		strings.HasPrefix(key, idxKeyPrefix+"/"):
		// Session shadow key, __oxia/session/{id}/{url_escaped_user_key}, or
		// secondary index key, __oxia/idx/{name}/{secondary}\x01{url_escaped_primary}:
		// deleted with its record when that goes to the other child
		return splitActionKeep, nil

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
// for keys outside the hash range, and either rewrites the batch or deletes it.
func filterNotificationKey(
	key string,
	it kvstore.KeyValueIterator,
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

	// Filter: remove notifications for keys outside this child's range. The
	// slice keeps its original (sorted) order, so the rewrite below stays
	// deterministic.
	originalLen := len(nb.Notifications)
	kept := nb.Notifications[:0]
	for _, entry := range nb.Notifications {
		h := hash.Xxh332(entry.GetKey())
		if isHashInRange(h, hashRange) {
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

// isUserKeyInRange determines if a user data key belongs to the given hash range.
// If the StorageEntry has a partition_key, that is hashed; otherwise the key itself.
func isUserKeyInRange(key string, se *proto.StorageEntry, hashRange *proto.HashRange) bool {
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

// SplitFilter is what a split child keeps of the data of its parent shard: the
// keys whose hash is in [MinHash, MaxHash], in the parent's snapshot as well as
// in the parent's log entries that the child applies after it.
type SplitFilter struct {
	MinHash uint32
	MaxHash uint32

	// ParentTerm is the term of the parent when it sent the snapshot: the
	// parent's entries are of terms up to it, while the child writes its own
	// entries in the later terms it leads in.
	ParentTerm int64
}

// SetSplitFilter records the filter of a split child. The child doesn't apply
// the parent's entries only as the follower of the parent, which filters them:
// e.g. it replays them from its log after a restart, as a leader too, and the
// replay filters the entries of terms up to ParentTerm with the recorded one.
func (d *db) SetSplitFilter(filter *SplitFilter) error {
	value, err := json.Marshal(filter)
	if err != nil {
		return err
	}

	batch := d.kv.NewWriteBatch()
	defer batch.Close()
	if _, err := d.applyPut(batch, nil, nil, &proto.PutRequest{
		Key:   splitFilterKey,
		Value: value,
	}, now(), NoOpCallback, true); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return err
	}

	// The database runs without the Pebble WAL: flush, so that the filter,
	// and the filtering of the parent's snapshot before it, survive a crash
	if err := d.kv.Flush(); err != nil {
		return err
	}
	d.splitFilter.Store(filter)
	return nil
}

func (d *db) SplitFilter() *SplitFilter {
	return d.splitFilter.Load()
}

func (d *db) recoverSplitFilter() error {
	gr, err := applyGet(d.kv, &proto.GetRequest{Key: splitFilterKey, IncludeValue: true})
	if err != nil {
		return err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return nil
	}

	filter := &SplitFilter{}
	if err := json.Unmarshal(gr.Value, filter); err != nil {
		return errors.Wrap(err, "invalid split filter")
	}
	d.splitFilter.Store(filter)
	return nil
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
