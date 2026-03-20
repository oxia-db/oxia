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
	"fmt"
	"math"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/proto"
)

func newTestKV(t *testing.T) kvstore.KV {
	t.Helper()
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	t.Cleanup(func() { factory.Close() })

	kv, err := factory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)
	return kv
}

func putRawKey(t *testing.T, kv kvstore.KV, key string, value []byte) {
	t.Helper()
	batch := kv.NewWriteBatch()
	assert.NoError(t, batch.Put(key, value))
	assert.NoError(t, batch.Commit())
	assert.NoError(t, batch.Close())
}

func putStorageEntry(t *testing.T, kv kvstore.KV, key string, partitionKey *string) {
	t.Helper()
	se := &proto.StorageEntry{
		Value:        []byte("v"),
		VersionId:    0,
		PartitionKey: partitionKey,
	}
	data, err := se.MarshalVT()
	assert.NoError(t, err)
	putRawKey(t, kv, key, data)
}

func putNotificationBatch(t *testing.T, kv kvstore.KV, offset int64, keys map[string]*proto.Notification) {
	t.Helper()
	nb := &proto.NotificationBatch{
		Shard:         0,
		Offset:        offset,
		Timestamp:     1000,
		Notifications: keys,
	}
	data, err := pb.MarshalOptions{Deterministic: true}.Marshal(nb)
	assert.NoError(t, err)
	putRawKey(t, kv, notificationKey(offset), data)
}

func keyExists(t *testing.T, kv kvstore.KV, key string) bool {
	t.Helper()
	_, _, closer, err := kv.Get(key, kvstore.ComparisonEqual, kvstore.ShowInternalKeys)
	if err != nil {
		return false
	}
	closer.Close()
	return true
}

func readNotificationBatch(t *testing.T, kv kvstore.KV, offset int64) *proto.NotificationBatch {
	t.Helper()
	_, value, closer, err := kv.Get(notificationKey(offset), kvstore.ComparisonEqual, kvstore.ShowInternalKeys)
	if err != nil {
		return nil
	}
	defer closer.Close()
	nb := &proto.NotificationBatch{}
	assert.NoError(t, nb.UnmarshalVT(value))
	return nb
}

// leftRange covers [0, midpoint]
// rightRange covers [midpoint+1, max].
func splitRanges() (left, right model.Int32HashRange) {
	mid := uint32(math.MaxUint32 / 2)
	return model.Int32HashRange{Min: 0, Max: mid},
		model.Int32HashRange{Min: mid + 1, Max: math.MaxUint32}
}

func TestFilterDBForSplit_UserKeys(t *testing.T) {
	kv := newTestKV(t)

	// Insert user keys. We'll determine which side they fall on by hash.
	leftRange, rightRange := splitRanges()

	// Find two keys: one for each side
	var leftKey, rightKey string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%d", i)
		h := hash.Xxh332(k)
		if leftKey == "" && h >= leftRange.Min && h <= leftRange.Max {
			leftKey = k
		}
		if rightKey == "" && h >= rightRange.Min && h <= rightRange.Max {
			rightKey = k
		}
		if leftKey != "" && rightKey != "" {
			break
		}
	}
	assert.NotEmpty(t, leftKey, "should find a key in left range")
	assert.NotEmpty(t, rightKey, "should find a key in right range")

	putStorageEntry(t, kv, leftKey, nil)
	putStorageEntry(t, kv, rightKey, nil)

	// Filter for left range
	assert.NoError(t, FilterDBForSplit(kv, leftRange))

	assert.True(t, keyExists(t, kv, leftKey), "left key should survive")
	assert.False(t, keyExists(t, kv, rightKey), "right key should be deleted")
}

func TestFilterDBForSplit_PartitionKey(t *testing.T) {
	kv := newTestKV(t)

	leftRange, _ := splitRanges()

	// Find a partition_key that hashes to left range
	var leftPK string
	for i := 0; i < 1000; i++ {
		pk := fmt.Sprintf("pk-%d", i)
		if isHashInRange(hash.Xxh332(pk), leftRange) {
			leftPK = pk
			break
		}
	}
	assert.NotEmpty(t, leftPK)

	// Key "some-key" but with partition_key that hashes to left range
	putStorageEntry(t, kv, "some-key", &leftPK)

	assert.NoError(t, FilterDBForSplit(kv, leftRange))
	assert.True(t, keyExists(t, kv, "some-key"), "key with left partition_key should survive")
}

func TestFilterDBForSplit_MetadataKeys(t *testing.T) {
	kv := newTestKV(t)

	leftRange, _ := splitRanges()

	// Write metadata keys
	putRawKey(t, kv, commitOffsetKey, []byte("42"))
	putRawKey(t, kv, commitLastVersionIdKey, []byte("100"))
	putRawKey(t, kv, commitChecksumKey, []byte("999"))
	putRawKey(t, kv, termKey, []byte("5"))
	putRawKey(t, kv, termOptionsKey, []byte(`{"notificationsEnabled":true}`))

	assert.NoError(t, FilterDBForSplit(kv, leftRange))

	// Kept
	assert.True(t, keyExists(t, kv, commitOffsetKey))
	assert.True(t, keyExists(t, kv, commitLastVersionIdKey))
	assert.True(t, keyExists(t, kv, termKey))
	assert.True(t, keyExists(t, kv, termOptionsKey))

	// Deleted
	assert.False(t, keyExists(t, kv, commitChecksumKey))
}

func TestFilterDBForSplit_Notifications(t *testing.T) {
	kv := newTestKV(t)

	leftRange, _ := splitRanges()

	// Find keys for each side
	var leftKey, rightKey string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("nk-%d", i)
		h := hash.Xxh332(k)
		if leftKey == "" && isHashInRange(h, leftRange) {
			leftKey = k
		}
		if rightKey == "" && !isHashInRange(h, leftRange) {
			rightKey = k
		}
		if leftKey != "" && rightKey != "" {
			break
		}
	}

	vid := int64(0)

	// Notification with only left-side key
	putNotificationBatch(t, kv, 0, map[string]*proto.Notification{
		leftKey: {Type: proto.NotificationType_KEY_CREATED, VersionId: &vid},
	})

	// Notification with only right-side key
	putNotificationBatch(t, kv, 1, map[string]*proto.Notification{
		rightKey: {Type: proto.NotificationType_KEY_CREATED, VersionId: &vid},
	})

	// Notification with both
	putNotificationBatch(t, kv, 2, map[string]*proto.Notification{
		leftKey:  {Type: proto.NotificationType_KEY_MODIFIED, VersionId: &vid},
		rightKey: {Type: proto.NotificationType_KEY_DELETED},
	})

	assert.NoError(t, FilterDBForSplit(kv, leftRange))

	// Offset 0: kept (left key only)
	nb0 := readNotificationBatch(t, kv, 0)
	assert.NotNil(t, nb0)
	assert.Equal(t, 1, len(nb0.Notifications))
	assert.Contains(t, nb0.Notifications, leftKey)

	// Offset 1: deleted (right key only)
	nb1 := readNotificationBatch(t, kv, 1)
	assert.Nil(t, nb1)

	// Offset 2: filtered (both -> only left)
	nb2 := readNotificationBatch(t, kv, 2)
	assert.NotNil(t, nb2)
	assert.Equal(t, 1, len(nb2.Notifications))
	assert.Contains(t, nb2.Notifications, leftKey)
	assert.NotContains(t, nb2.Notifications, rightKey)
}

func TestFilterDBForSplit_SessionKeys(t *testing.T) {
	kv := newTestKV(t)

	leftRange, _ := splitRanges()

	// Session metadata key: should always be kept
	sessionMetaKey := fmt.Sprintf("%s/%016x", sessionKeyPrefix, 1)
	putRawKey(t, kv, sessionMetaKey, []byte(`{"timeout_ms":5000}`))

	// Find user keys for each side
	var leftUserKey, rightUserKey string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("sess-key-%d", i)
		h := hash.Xxh332(k)
		if leftUserKey == "" && isHashInRange(h, leftRange) {
			leftUserKey = k
		}
		if rightUserKey == "" && !isHashInRange(h, leftRange) {
			rightUserKey = k
		}
		if leftUserKey != "" && rightUserKey != "" {
			break
		}
	}

	// Shadow keys
	leftShadow := fmt.Sprintf("%s/%016x/%s", sessionKeyPrefix, 1, url.PathEscape(leftUserKey))
	rightShadow := fmt.Sprintf("%s/%016x/%s", sessionKeyPrefix, 1, url.PathEscape(rightUserKey))
	putRawKey(t, kv, leftShadow, []byte{})
	putRawKey(t, kv, rightShadow, []byte{})

	assert.NoError(t, FilterDBForSplit(kv, leftRange))

	// Session metadata kept
	assert.True(t, keyExists(t, kv, sessionMetaKey))
	// Left shadow kept, right shadow deleted
	assert.True(t, keyExists(t, kv, leftShadow))
	assert.False(t, keyExists(t, kv, rightShadow))
}

func TestFilterDBForSplit_SecondaryIndexKeys(t *testing.T) {
	kv := newTestKV(t)

	leftRange, _ := splitRanges()

	// Find primary keys for each side
	var leftPrimaryKey, rightPrimaryKey string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("pri-%d", i)
		h := hash.Xxh332(k)
		if leftPrimaryKey == "" && isHashInRange(h, leftRange) {
			leftPrimaryKey = k
		}
		if rightPrimaryKey == "" && !isHashInRange(h, leftRange) {
			rightPrimaryKey = k
		}
		if leftPrimaryKey != "" && rightPrimaryKey != "" {
			break
		}
	}

	// Secondary index keys: __oxia/idx/{name}/{secondary}\x01{url_escaped_primary}
	leftIdxKey := fmt.Sprintf("%s/myidx/sec1%s%s", idxKeyPrefix, idxSeparator, url.PathEscape(leftPrimaryKey))
	rightIdxKey := fmt.Sprintf("%s/myidx/sec2%s%s", idxKeyPrefix, idxSeparator, url.PathEscape(rightPrimaryKey))
	putRawKey(t, kv, leftIdxKey, []byte{})
	putRawKey(t, kv, rightIdxKey, []byte{})

	assert.NoError(t, FilterDBForSplit(kv, leftRange))

	assert.True(t, keyExists(t, kv, leftIdxKey))
	assert.False(t, keyExists(t, kv, rightIdxKey))
}

func TestFilterWriteRequestForSplit_Puts(t *testing.T) {
	leftRange, _ := splitRanges()

	var leftKey, rightKey string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("wr-key-%d", i)
		h := hash.Xxh332(k)
		if leftKey == "" && isHashInRange(h, leftRange) {
			leftKey = k
		}
		if rightKey == "" && !isHashInRange(h, leftRange) {
			rightKey = k
		}
		if leftKey != "" && rightKey != "" {
			break
		}
	}

	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: leftKey, Value: []byte("v1")},
			{Key: rightKey, Value: []byte("v2")},
		},
	}

	filtered := FilterWriteRequestForSplit(req, leftRange)
	assert.NotNil(t, filtered)
	assert.Equal(t, 1, len(filtered.Puts))
	assert.Equal(t, leftKey, filtered.Puts[0].Key)
}

func TestFilterWriteRequestForSplit_PartitionKey(t *testing.T) {
	leftRange, _ := splitRanges()

	var leftPK string
	for i := 0; i < 1000; i++ {
		pk := fmt.Sprintf("wrpk-%d", i)
		if isHashInRange(hash.Xxh332(pk), leftRange) {
			leftPK = pk
			break
		}
	}

	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "any-key", Value: []byte("v1"), PartitionKey: &leftPK},
		},
	}

	filtered := FilterWriteRequestForSplit(req, leftRange)
	assert.NotNil(t, filtered)
	assert.Equal(t, 1, len(filtered.Puts))
}

func TestFilterWriteRequestForSplit_Deletes(t *testing.T) {
	leftRange, _ := splitRanges()

	var leftKey, rightKey string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("del-key-%d", i)
		h := hash.Xxh332(k)
		if leftKey == "" && isHashInRange(h, leftRange) {
			leftKey = k
		}
		if rightKey == "" && !isHashInRange(h, leftRange) {
			rightKey = k
		}
		if leftKey != "" && rightKey != "" {
			break
		}
	}

	req := &proto.WriteRequest{
		Deletes: []*proto.DeleteRequest{
			{Key: leftKey},
			{Key: rightKey},
		},
	}

	filtered := FilterWriteRequestForSplit(req, leftRange)
	assert.NotNil(t, filtered)
	assert.Equal(t, 1, len(filtered.Deletes))
	assert.Equal(t, leftKey, filtered.Deletes[0].Key)
}

func TestFilterWriteRequestForSplit_AllFiltered(t *testing.T) {
	_, rightRange := splitRanges()

	var rightKey string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("x-key-%d", i)
		if isHashInRange(hash.Xxh332(k), rightRange) {
			rightKey = k
			break
		}
	}

	// Only right-side key, filtered for left range
	leftRange, _ := splitRanges()
	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: rightKey, Value: []byte("v1")},
		},
	}

	filtered := FilterWriteRequestForSplit(req, leftRange)
	assert.Nil(t, filtered)
}

func TestFilterWriteRequestForSplit_Nil(t *testing.T) {
	leftRange, _ := splitRanges()
	assert.Nil(t, FilterWriteRequestForSplit(nil, leftRange))
}
