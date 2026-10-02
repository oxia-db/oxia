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
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
)

// cacheNotifications makes d cache the batches it commits from now on, as the
// first read of a subscriber does.
func cacheNotifications(t *testing.T, d DB) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := d.ReadNextNotifications(ctx, math.MaxInt64)
	require.ErrorIs(t, err, context.Canceled)
}

// notificationRecord returns the stored bytes of the batch at offset.
func notificationRecord(t *testing.T, kv kvstore.KV, offset int64) []byte {
	t.Helper()
	_, value, closer, err := kv.Get(notificationKey(offset), kvstore.ComparisonEqual, kvstore.ShowInternalKeys)
	require.NoError(t, err)
	defer closer.Close()
	return slices.Clone(value)
}

func newNotificationsTestDB(t *testing.T) DB {
	t.Helper()
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	d, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, d.Close())
		assert.NoError(t, factory.Close())
	})
	return d
}

// writeNotification commits a write at offset, which notifies a put of key k<offset>.
func writeNotification(t *testing.T, d DB, offset int64) {
	t.Helper()
	_, err := d.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: fmt.Sprintf("k%d", offset), Value: []byte("v")}},
	}, offset, now(), NoOpCallback)
	require.NoError(t, err)
}

func TestParseNotificationBatch(t *testing.T) {
	versionId := int64(7)
	many := newNotifications(2, 1<<40, 1)
	for i := range 30 {
		many.Modified(fmt.Sprintf("key-%d", i), int64(i), 1)
	}

	for i, n := range []*Notifications{
		newNotifications(0, 0, 0),
		newNotifications(5, 42, 1234567890),
		notificationsFromMap(3, 11, 100, map[string]*proto.Notification{
			"a": {Type: proto.NotificationType_KEY_MODIFIED, VersionId: &versionId},
			"z": {Type: proto.NotificationType_KEY_DELETED},
		}),
		many,
	} {
		nb := n.seal()
		data, err := nb.MarshalVT()
		require.NoError(t, err)

		offset, notifications, err := parseNotificationBatch(data)
		require.NoError(t, err, "batch %d", i)
		assert.Equal(t, nb.Offset, offset, "batch %d", i)
		assert.Equal(t, len(nb.Notifications), notifications, "batch %d", i)

		if len(data) > 0 {
			_, _, err = parseNotificationBatch(data[:len(data)-1])
			assert.Error(t, err, "batch %d", i)
		}
	}
}

// The subscribers that keep up read the batches from the cache, as the db
// stores them.
func TestDB_NotificationsReadFromCache(t *testing.T) {
	d := newNotificationsTestDB(t)
	cacheNotifications(t, d)

	stored := make([][]byte, 3)
	for offset := range int64(3) {
		writeNotification(t, d, offset)
		stored[offset] = notificationRecord(t, d.RawKV(), offset)
	}

	// Deleted behind the back of the tracker: only the cache has them now. A
	// read from the db would wait for the next batch.
	wb := d.RawKV().NewWriteBatch()
	require.NoError(t, wb.DeleteRange(firstNotificationKey, lastNotificationKey))
	require.NoError(t, wb.Commit())
	require.NoError(t, wb.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	batches, err := d.ReadNextNotifications(ctx, 0)
	require.NoError(t, err)
	require.Len(t, batches, 3)
	for i, batch := range batches {
		assert.EqualValues(t, i, batch.Offset)
		assert.Equal(t, stored[i], batch.Data)
	}
}

// A leader caches the batches once it has subscribers, while its followers
// never do: the records, which feed the DB checksum, must be the same either way.
func TestDB_NotificationsCacheChecksum(t *testing.T) {
	replicas := []DB{newNotificationsTestDB(t), newNotificationsTestDB(t)}
	for _, d := range replicas {
		d.EnableFeature(proto.Feature_FEATURE_DB_CHECKSUM)
	}
	cacheNotifications(t, replicas[0])

	for offset := range int64(5) {
		req := &proto.WriteRequest{
			Puts:    []*proto.PutRequest{{Key: fmt.Sprintf("k%d", offset), Value: []byte("v")}},
			Deletes: []*proto.DeleteRequest{{Key: fmt.Sprintf("k%d", offset-1)}},
		}
		for _, d := range replicas {
			_, err := d.ProcessWrite(req.CloneVT(), offset, uint64(offset), NoOpCallback)
			require.NoError(t, err)
		}
		require.False(t, replicas[0].ReadChecksum().IsZero())
		require.Equal(t, replicas[0].ReadChecksum(), replicas[1].ReadChecksum(), "offset %d", offset)
	}
	assert.True(t, replicas[0].(*db).notificationsTracker.Caching())
	assert.False(t, replicas[1].(*db).notificationsTracker.Caching())
}

// The first read can create the cache while a batch gets committed without
// the bytes for it: the cache starts after that batch, read from the db.
func TestDB_NotificationsCacheStartsAfterUncachedBatch(t *testing.T) {
	d := newNotificationsTestDB(t)
	cacheNotifications(t, d)
	nt := d.(*db).notificationsTracker

	// As if the cache didn't exist yet when the batch was stored
	nt.caching.Store(false)
	writeNotification(t, d, 0)
	nt.caching.Store(true)
	writeNotification(t, d, 1)

	batches, err := readNotifications(context.Background(), d, 0)
	require.NoError(t, err)
	require.Len(t, batches, 2)
	for i, nb := range batches {
		assert.EqualValues(t, i, nb.Offset)
		assert.Equal(t, []string{fmt.Sprintf("k%d", i)}, notificationKeys(nb))
	}
}

// A subscriber further behind than the cache reads from the db.
func TestDB_NotificationsBehindTheCache(t *testing.T) {
	d := newNotificationsTestDB(t)
	cacheNotifications(t, d)

	total := int64(maxCachedNotificationBatches + 10)
	for offset := range total {
		writeNotification(t, d, offset)
	}

	// From the db, then from the cache
	for _, startOffset := range []int64{0, total - 5} {
		batches, err := readNotifications(context.Background(), d, startOffset)
		require.NoError(t, err)
		require.NotEmpty(t, batches)
		for i, nb := range batches {
			offset := startOffset + int64(i)
			assert.Equal(t, offset, nb.Offset)
			assert.Equal(t, []string{fmt.Sprintf("k%d", offset)}, notificationKeys(nb))
		}
	}
}

// A delete range over the internal keys drops the cached batches, whose
// records it might delete.
func TestDB_DeleteRangeNotificationRecordsCached(t *testing.T) {
	d := newNotificationsTestDB(t)
	d.EnableFeature(proto.Feature_FEATURE_DELETE_RANGE_NOTIFICATION_RECORDS)
	cacheNotifications(t, d)

	for offset := range int64(3) {
		writeNotification(t, d, offset)
	}
	res, err := d.ProcessWrite(&proto.WriteRequest{
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "__oxia/notifications/",
			EndExclusive:   "__oxia/notifications/~",
		}},
	}, 3, now(), NoOpCallback)
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, res.DeleteRanges[0].Status)

	// Only the batch of the delete range itself is left
	assert.EqualValues(t, 3, firstNotification(t, d))
}
