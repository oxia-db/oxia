// Copyright 2023-2025 The Oxia Authors
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
	"regexp"
	"strings"

	"github.com/pkg/errors"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/proto"
)

const (
	sessionKeyPrefix = constant.InternalKeyPrefix + "session"
	idxKeyPrefix     = constant.InternalKeyPrefix + "idx"
	idxSeparator     = "\x01"
	sessionKeyLength = len(sessionKeyPrefix) + 1 + 16 // __oxia/session/ + 16 hex digits
)

var secondaryIdxRegex = regexp.MustCompile(
	"^" + idxKeyPrefix + "/[^/]+/([^" + idxSeparator + "]+)" + idxSeparator + "(.+)$",
)

// FilterDBForSplit removes keys that do not belong to the specified hash range.
// This is called on a child shard after loading a parent's snapshot.
//
// Key classification:
//   - User data keys: filter by hash of partition_key (or key if no partition_key)
//   - __oxia/last-version-id, commit-offset, term, term-options: keep
//   - __oxia/checksum: delete (invalid after filtering)
//   - __oxia/notifications/{offset}: filter notification map by key hash; delete if empty
//   - __oxia/session/{id}: keep (session metadata duplicated to both children)
//   - __oxia/session/{id}/{user-key}: filter by user key hash
//   - __oxia/idx/{idx}/{sec}\x01{pri}: filter by primary key hash
func FilterDBForSplit(kv kvstore.KV, hashRange model.Int32HashRange) error {
	slog.Info(
		"Filtering database for shard split",
		slog.Any("hash-range-min", hashRange.Min),
		slog.Any("hash-range-max", hashRange.Max),
	)

	batch := kv.NewWriteBatch()
	defer batch.Close()

	it, err := kv.RangeScan("", "", kvstore.ShowInternalKeys)
	if err != nil {
		return errors.Wrap(err, "failed to create range scan for split filter")
	}
	defer it.Close()

	var deletedKeys, keptKeys, filteredNotifications, deletedNotifications int64

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
			// User data key
			value, err := it.Value()
			if err != nil {
				return errors.Wrapf(err, "failed to read value for key %q", key)
			}

			if !isUserKeyInRange(key, value, hashRange) {
				if err := batch.Delete(key); err != nil {
					return err
				}
				deletedKeys++
			} else {
				keptKeys++
			}
		}

		it.Next()
	}

	slog.Info(
		"Split filter complete, committing",
		slog.Int64("deleted-keys", deletedKeys),
		slog.Int64("kept-keys", keptKeys),
		slog.Int64("filtered-notifications", filteredNotifications),
		slog.Int64("deleted-notifications", deletedNotifications),
	)

	return batch.Commit()
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
	hashRange model.Int32HashRange,
) (splitAction, error) {
	switch {
	case key == commitOffsetKey,
		key == commitLastVersionIdKey,
		key == termKey,
		key == termOptionsKey:
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

	case strings.HasPrefix(key, sessionKeyPrefix+"/"):
		// Session shadow key: __oxia/session/{id}/{url_escaped_user_key}
		return classifySessionShadowKey(key, hashRange), nil

	case strings.HasPrefix(key, idxKeyPrefix+"/"):
		// Secondary index key: __oxia/idx/{name}/{secondary}\x01{url_escaped_primary}
		return classifySecondaryIndexKey(key, hashRange), nil

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
	hashRange model.Int32HashRange,
) (splitAction, error) {
	value, err := it.Value()
	if err != nil {
		return splitActionKeep, errors.Wrap(err, "failed to read notification value")
	}

	nb := &proto.NotificationBatch{}
	if err := nb.UnmarshalVT(value); err != nil {
		return splitActionKeep, errors.Wrap(err, "failed to deserialize notification batch")
	}

	// Filter: remove notifications for keys outside this child's range
	originalLen := len(nb.Notifications)
	for notifKey := range nb.Notifications {
		h := hash.Xxh332(notifKey)
		if !isHashInRange(h, hashRange) {
			delete(nb.Notifications, notifKey)
		}
	}

	if len(nb.Notifications) == 0 {
		// All notifications were for keys outside our range: delete entirely
		if err := batch.Delete(key); err != nil {
			return splitActionKeep, err
		}
		return splitActionDeletedNotification, nil
	}

	if len(nb.Notifications) < originalLen {
		// Some notifications were removed: rewrite
		newValue, err := pb.MarshalOptions{Deterministic: true}.Marshal(nb)
		if err != nil {
			return splitActionKeep, errors.Wrap(err, "failed to marshal filtered notification batch")
		}
		if err := batch.Put(key, newValue); err != nil {
			return splitActionKeep, err
		}
		return splitActionFilteredNotification, nil
	}

	// All notifications were in range: keep as-is
	return splitActionKeep, nil
}

// classifySessionShadowKey extracts the user key from a shadow key and checks
// if it belongs to this child's hash range.
// Shadow key format: __oxia/session/{16hex}/{url_escaped_user_key}.
func classifySessionShadowKey(key string, hashRange model.Int32HashRange) splitAction {
	// Find the position after "__oxia/session/{16hex}/"
	prefix := sessionKeyPrefix + "/"
	rest := key[len(prefix):]

	// Skip the 16-hex-char session ID
	slashIdx := strings.Index(rest, "/")
	if slashIdx < 0 {
		// This is a session metadata key (no user key suffix), keep it
		return splitActionKeep
	}

	escapedUserKey := rest[slashIdx+1:]
	userKey, err := url.PathUnescape(escapedUserKey)
	if err != nil {
		// Can't parse: keep to be safe
		return splitActionKeep
	}

	h := hash.Xxh332(userKey)
	if isHashInRange(h, hashRange) {
		return splitActionKeep
	}
	return splitActionDelete
}

// classifySecondaryIndexKey extracts the primary key from a secondary index key
// and checks if it belongs to this child's hash range.
// Format: __oxia/idx/{name}/{secondary}\x01{url_escaped_primary}.
func classifySecondaryIndexKey(key string, hashRange model.Int32HashRange) splitAction {
	matches := secondaryIdxRegex.FindStringSubmatch(key)
	if len(matches) != 3 {
		// Can't parse: keep to be safe
		return splitActionKeep
	}

	primaryKey, err := url.PathUnescape(matches[2])
	if err != nil {
		return splitActionKeep
	}

	h := hash.Xxh332(primaryKey)
	if isHashInRange(h, hashRange) {
		return splitActionKeep
	}
	return splitActionDelete
}

// isUserKeyInRange determines if a user data key belongs to the given hash range.
// If the StorageEntry has a partition_key, that is hashed; otherwise the key itself.
func isUserKeyInRange(key string, value []byte, hashRange model.Int32HashRange) bool {
	se := proto.StorageEntryFromVTPool()
	defer se.ReturnToVTPool()

	if err := Deserialize(value, se); err != nil {
		// Can't deserialize: hash by key
		h := hash.Xxh332(key)
		return isHashInRange(h, hashRange)
	}

	var h uint32
	if se.PartitionKey != nil && *se.PartitionKey != "" {
		h = hash.Xxh332(*se.PartitionKey)
	} else {
		h = hash.Xxh332(key)
	}

	return isHashInRange(h, hashRange)
}

func isHashInRange(h uint32, hashRange model.Int32HashRange) bool {
	return h >= hashRange.Min && h <= hashRange.Max
}

// FilterWriteRequestForSplit filters a WriteRequest to only include operations
// for keys within the given hash range. Returns nil if nothing remains.
// This is used at the state machine apply level when a child shard processes
// WAL entries inherited from its parent.
func FilterWriteRequestForSplit(req *proto.WriteRequest, hashRange model.Int32HashRange) *proto.WriteRequest {
	if req == nil {
		return nil
	}

	var puts []*proto.PutRequest
	for _, p := range req.Puts {
		var h uint32
		if p.PartitionKey != nil && *p.PartitionKey != "" {
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
		h := hash.Xxh332(d.Key)
		if isHashInRange(h, hashRange) {
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
