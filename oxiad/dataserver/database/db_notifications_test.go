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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/common/logging"

	"github.com/oxia-db/oxia/common/proto"
)

func init() {
	logging.ConfigureLogger()
}

func findNotification(nb *proto.NotificationBatch, key string) (*proto.Notification, bool) {
	for _, e := range nb.Notifications {
		if e.GetKey() == key {
			return e.Value, true
		}
	}
	return nil, false
}

func TestDB_Notifications(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)

	t0 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("0"),
		}},
	}, 0, t0, NoOpCallback)

	notifications, err := db.ReadNextNotifications(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(notifications))

	nb := notifications[0]
	assert.Equal(t, t0, nb.Timestamp)
	assert.EqualValues(t, 0, nb.Offset)
	assert.EqualValues(t, 1, nb.Shard)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found := findNotification(nb, "a")
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_CREATED, n.Type)
	assert.EqualValues(t, 0, *n.VersionId)

	t1 := now()
	wr1, _ := db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("1"),
		}},
	}, 1, t1, NoOpCallback)

	t2 := now()
	wr2, _ := db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "b",
			Value: []byte("0"),
		}},
	}, 2, t2, NoOpCallback)

	notifications, err = db.ReadNextNotifications(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(notifications))

	nb = notifications[0]
	assert.Equal(t, t1, nb.Timestamp)
	assert.EqualValues(t, 1, nb.Offset)
	assert.EqualValues(t, 1, nb.Shard)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found = findNotification(nb, "a")
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_MODIFIED, n.Type)
	assert.EqualValues(t, wr1.Puts[0].Version.VersionId, *n.VersionId)

	nb = notifications[1]
	assert.Equal(t, t2, nb.Timestamp)
	assert.EqualValues(t, 2, nb.Offset)
	assert.EqualValues(t, 1, nb.Shard)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found = findNotification(nb, "b")
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_CREATED, n.Type)
	assert.EqualValues(t, wr2.Puts[0].Version.VersionId, *n.VersionId)

	// Write one batch
	t3 := now()
	wr3, _ := db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "c",
			Value: []byte("0"),
		}, {
			Key:   "d",
			Value: []byte("0"),
		}},
		Deletes: []*proto.DeleteRequest{{
			Key: "a",
		}},
	}, 3, t3, NoOpCallback)

	notifications, err = db.ReadNextNotifications(context.Background(), 3)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(notifications))

	nb = notifications[0]
	assert.Equal(t, t3, nb.Timestamp)
	assert.EqualValues(t, 3, nb.Offset)
	assert.EqualValues(t, 1, nb.Shard)
	assert.Equal(t, 3, len(nb.Notifications))
	n, found = findNotification(nb, "c")
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_CREATED, n.Type)
	assert.EqualValues(t, wr3.Puts[0].Version.VersionId, *n.VersionId)
	n, found = findNotification(nb, "d")
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_CREATED, n.Type)
	assert.EqualValues(t, wr3.Puts[1].Version.VersionId, *n.VersionId)
	n, found = findNotification(nb, "a")
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_DELETED, n.Type)
	assert.Nil(t, n.VersionId)

	// When there are multiple keys in one batch, only 1 notification
	// is going to get triggered
	t4 := now()
	wr4, _ := db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "x1",
			Value: []byte("0"),
		}, {
			Key:   "x1",
			Value: []byte("1"),
		}},
	}, 4, t4, NoOpCallback)

	notifications, err = db.ReadNextNotifications(context.Background(), 4)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(notifications))

	nb = notifications[0]
	assert.Equal(t, t4, nb.Timestamp)
	assert.EqualValues(t, 4, nb.Offset)
	assert.EqualValues(t, 1, nb.Shard)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found = findNotification(nb, "x1")
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_MODIFIED, n.Type)
	assert.EqualValues(t, wr4.Puts[1].Version.VersionId, *n.VersionId)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_NotificationsCancelWait(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)

	t0 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("0"),
		}},
	}, 0, t0, NoOpCallback)

	ctx, cancel := context.WithCancel(context.Background())

	doneCh := make(chan error)

	go func() {
		notifications, err := db.ReadNextNotifications(ctx, 5)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, notifications)
		close(doneCh)
	}()

	// Cancel the context to trigger exit from the wait
	cancel()

	select {
	case <-doneCh:
	// Ok

	case <-time.After(1 * time.Second):
		assert.Fail(t, "Should not have timed out")
	}

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

// hookedCondition runs beforeWait on the first Wait, while the waiter still
// holds the tracker lock: after it has checked its condition, but before it is
// parked on the condition.
type hookedCondition struct {
	concurrent.ConditionContext
	once       sync.Once
	beforeWait func()
}

func (c *hookedCondition) Wait(ctx context.Context) error {
	c.once.Do(c.beforeWait)
	return c.ConditionContext.Wait(ctx)
}

// runBeforeFirstWait makes action run concurrently with the first waiter of
// the tracker, between its condition check and its parking. The waiter gives
// action a grace period to complete, which it can only do if it does not need
// the tracker lock. The returned channel is closed once action has completed.
func runBeforeFirstWait(nt *notificationsTracker, action func()) <-chan struct{} {
	done := make(chan struct{})
	nt.cond = &hookedCondition{
		ConditionContext: nt.cond,
		beforeWait: func() {
			go func() {
				action()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(100 * time.Millisecond):
			}
		},
	}
	return done
}

func TestDB_NotificationsCommitBeforeWaiterParks(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	d, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)

	_, err = d.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: "a", Value: []byte("0")}},
	}, 0, now(), NoOpCallback)
	require.NoError(t, err)

	// The batch at offset 1 gets committed after the reader found it missing,
	// but before the reader parks: the commit must still wake it up
	writeDone := runBeforeFirstWait(d.(*db).notificationsTracker, func() {
		_, err := d.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{Key: "b", Value: []byte("0")}},
		}, 1, now(), NoOpCallback)
		assert.NoError(t, err)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	notifications, err := d.ReadNextNotifications(ctx, 1)
	require.NoError(t, err)
	require.Len(t, notifications, 1)
	assert.EqualValues(t, 1, notifications[0].Offset)

	<-writeDone
	assert.NoError(t, d.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_NotificationsCloseBeforeWaiterParks(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	d, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)

	// The tracker gets closed after the reader checked it, but before the
	// reader parks: the close must still wake it up
	nt := d.(*db).notificationsTracker
	closeDone := runBeforeFirstWait(nt, func() {
		assert.NoError(t, nt.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	notifications, err := d.ReadNextNotifications(ctx, 0)
	require.ErrorIs(t, err, constant.ErrResourceUnavailable)
	assert.Nil(t, notifications)

	<-closeDone
	assert.NoError(t, d.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_NotificationsWaitAfterControlOnlyTailReopen(t *testing.T) {
	tests := []struct {
		name    string
		request *proto.ControlRequest
	}{
		{
			name: "record checksum",
			request: &proto.ControlRequest{
				Value: &proto.ControlRequest_RecordChecksum{
					RecordChecksum: &proto.RecordChecksumRequest{},
				},
			},
		},
		{
			name: "feature enable",
			request: &proto.ControlRequest{
				Value: &proto.ControlRequest_FeatureEnable{
					FeatureEnable: &proto.FeatureEnableRequest{
						Features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			assert.NoError(t, err)
			db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
			assert.NoError(t, err)

			_, err = db.ProcessWrite(&proto.WriteRequest{
				Puts: []*proto.PutRequest{{
					Key:   "a",
					Value: []byte("0"),
				}},
			}, 0, now(), NoOpCallback)
			assert.NoError(t, err)

			_, err = db.ProcessControlRequest(tt.request, 1, now(), NoOpCallback)
			assert.NoError(t, err)
			assert.NoError(t, db.Close())

			db, err = NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
			assert.NoError(t, err)
			defer db.Close()
			defer factory.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			notifications, err := db.ReadNextNotifications(ctx, 1)
			cancel()
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Nil(t, notifications)

			_, err = db.ProcessWrite(&proto.WriteRequest{
				Puts: []*proto.PutRequest{{
					Key:   "b",
					Value: []byte("1"),
				}},
			}, 2, now(), NoOpCallback)
			assert.NoError(t, err)

			notifications, err = db.ReadNextNotifications(context.Background(), 1)
			assert.NoError(t, err)
			if assert.Len(t, notifications, 1) {
				assert.EqualValues(t, 2, notifications[0].Offset)
			}
		})
	}
}

func TestDB_NotificationsDisabled(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)

	db.EnableNotifications(false)
	t0 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("0"),
		}},
	}, 0, t0, NoOpCallback)

	notifications, err := db.ReadNextNotifications(context.Background(), 0)
	assert.Error(t, ErrNotificationsDisabled, err)
	assert.Nil(t, notifications)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_NotificationsDeleteRange(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)

	t0 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "a", Value: []byte("0")},
			{Key: "b", Value: []byte("1")},
			{Key: "c", Value: []byte("2")},
		},
	}, 0, t0, NoOpCallback)

	t1 := now()
	_, _ = db.ProcessWrite(&proto.WriteRequest{
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "a",
			EndExclusive:   "c",
		}},
	}, 1, t1, NoOpCallback)

	notifications, err := db.ReadNextNotifications(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(notifications))

	nb := notifications[0]
	assert.Equal(t, t1, nb.Timestamp)
	assert.EqualValues(t, 1, nb.Offset)
	assert.EqualValues(t, 1, nb.Shard)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found := findNotification(nb, "a")
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_RANGE_DELETED, n.Type)
	assert.Equal(t, "c", *n.KeyRangeLast)
	assert.Nil(t, n.VersionId)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

// The notification records are not storage entries. Without the feature, a
// delete range that covers them rejects the whole write request, where some
// are left. With it, a range over the internal keys deletes them.
func TestDB_DeleteRangeNotificationRecords(t *testing.T) {
	for _, test := range []struct {
		name       string
		keySorting proto.KeySortingType
		start, end string
	}{
		{"natural", proto.KeySortingType_NATURAL, "__oxia/notifications/", "__oxia/notifications/~"},
		{"hierarchical", proto.KeySortingType_HIERARCHICAL, "__oxia/notifications/", "__oxia/notifications//"},
	} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%v", test.name, enabled), func(t *testing.T) {
				factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
				require.NoError(t, err)
				db, err := NewDB(constant.DefaultNamespace, 1, factory, test.keySorting, 1*time.Hour, time2.SystemClock)
				require.NoError(t, err)
				if enabled {
					db.EnableFeature(proto.Feature_FEATURE_DELETE_RANGE_NOTIFICATION_RECORDS)
				}

				userKeys := []string{"k0", "k1", "k2"}
				for i, key := range userKeys {
					_, err := db.ProcessWrite(&proto.WriteRequest{
						Puts: []*proto.PutRequest{{Key: key, Value: []byte("v")}},
					}, int64(i), now(), NoOpCallback)
					require.NoError(t, err)
				}

				res, err := db.ProcessWrite(&proto.WriteRequest{
					DeleteRanges: []*proto.DeleteRangeRequest{{StartInclusive: test.start, EndExclusive: test.end}},
				}, 3, now(), NoOpCallback)
				if !enabled {
					assert.ErrorIs(t, err, ErrNotificationRecord)
					assert.EqualValues(t, 0, firstNotification(t, db))
				} else {
					require.NoError(t, err)
					assert.Equal(t, proto.Status_OK, res.DeleteRanges[0].Status)

					// Only the batch of the delete range itself is left
					assert.EqualValues(t, 3, firstNotification(t, db))
				}

				for _, key := range userKeys {
					gr, err := db.Get(&proto.GetRequest{Key: key})
					require.NoError(t, err)
					assert.Equal(t, proto.Status_OK, gr.Status, key)
				}
				commitOffset, err := db.ReadCommitOffset()
				require.NoError(t, err)
				assert.EqualValues(t, 3, commitOffset)

				assert.NoError(t, db.Close())
				assert.NoError(t, factory.Close())
			})
		}
	}
}

// Each replica trims its notification records on its own schedule. A delete
// range must not depend on which ones are still there: the batch feeds the DB
// checksum, which must stay the same on every replica.
func TestDB_DeleteRangeNotificationRecordsChecksum(t *testing.T) {
	for _, test := range []struct {
		keySorting proto.KeySortingType
		start, end string
	}{
		{proto.KeySortingType_NATURAL, "__oxia/notifications/", "__oxia/notifications/~"},
		{proto.KeySortingType_HIERARCHICAL, "__oxia/notifications/", "__oxia/notifications//"},
	} {
		t.Run(test.keySorting.String(), func(t *testing.T) {
			replicas := make([]DB, 2)
			factories := make([]kvstore.Factory, 2)
			for i := range replicas {
				factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
				require.NoError(t, err)
				db, err := NewDB(constant.DefaultNamespace, 1, factory, test.keySorting, 1*time.Hour, time2.SystemClock)
				require.NoError(t, err)
				db.EnableFeature(proto.Feature_FEATURE_DB_CHECKSUM)
				db.EnableFeature(proto.Feature_FEATURE_DELETE_RANGE_NOTIFICATION_RECORDS)
				replicas[i], factories[i] = db, factory
			}
			apply := func(offset int64, req *proto.WriteRequest) {
				for _, db := range replicas {
					_, err := db.ProcessWrite(req.CloneVT(), offset, uint64(offset), NoOpCallback)
					require.NoError(t, err)
				}
			}

			for i := int64(0); i < 5; i++ {
				apply(i, &proto.WriteRequest{
					Puts: []*proto.PutRequest{{Key: fmt.Sprintf("k%d", i), Value: []byte("v")}},
				})
			}

			// The second replica trims the oldest records, as its trimmer does,
			// outside of the checksum
			wb := replicas[1].RawKV().NewWriteBatch()
			require.NoError(t, wb.DeleteRange(notificationKey(0), notificationKey(3)))
			require.NoError(t, wb.Commit())
			require.NoError(t, wb.Close())
			require.Equal(t, replicas[0].ReadChecksum(), replicas[1].ReadChecksum())

			apply(5, &proto.WriteRequest{
				DeleteRanges: []*proto.DeleteRangeRequest{{StartInclusive: test.start, EndExclusive: test.end}},
			})
			assert.Equal(t, replicas[0].ReadChecksum(), replicas[1].ReadChecksum())

			for i, db := range replicas {
				// Only the batch of the delete range itself is left
				assert.EqualValues(t, 5, firstNotification(t, db), "replica %d", i)

				assert.NoError(t, db.Close())
				assert.NoError(t, factories[i].Close())
			}
		})
	}
}

type deleteCountingCallback struct {
	noopCallback
	deletes int
}

func (c *deleteCountingCallback) OnDeleteWithEntry(kvstore.WriteBatch, *Notifications, string, *proto.StorageEntry, feature.Checker) error {
	c.deletes++
	return nil
}

// A delete range that covers both regular keys and internal keys is rejected
// before the callbacks run and before its notification: it would delete the
// term, the enabled features and the other internal keys with the regular
// ones.
func TestDB_DeleteRangeRegularAndInternalKeys(t *testing.T) {
	for _, test := range []struct {
		name       string
		keySorting proto.KeySortingType
		start, end string
	}{
		// The internal keys sort after the regular ones with the natural sorting
		{"natural", proto.KeySortingType_NATURAL, "a", "__oxia/zzz"},
		{"natural without end", proto.KeySortingType_NATURAL, "a", ""},
		{"hierarchical", proto.KeySortingType_HIERARCHICAL, "a", "__oxia/zzz"},
	} {
		t.Run(test.name, func(t *testing.T) {
			factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			db, err := NewDB(constant.DefaultNamespace, 1, factory, test.keySorting, 1*time.Hour, time2.SystemClock)
			require.NoError(t, err)
			db.EnableFeature(proto.Feature_FEATURE_DELETE_RANGE_NOTIFICATION_RECORDS)

			_, err = db.ProcessWrite(&proto.WriteRequest{
				Puts: []*proto.PutRequest{{Key: "a", Value: []byte("v")}, {Key: "b", Value: []byte("v")}},
			}, 0, now(), NoOpCallback)
			require.NoError(t, err)

			callback := &deleteCountingCallback{}
			res, err := db.ProcessWrite(&proto.WriteRequest{
				DeleteRanges: []*proto.DeleteRangeRequest{{StartInclusive: test.start, EndExclusive: test.end}},
			}, 1, now(), callback)
			require.NoError(t, err)
			assert.Equal(t, proto.Status_INVALID_ARGUMENT, res.DeleteRanges[0].Status)
			assert.Zero(t, callback.deletes)

			for _, key := range []string{"a", "b"} {
				gr, err := db.Get(&proto.GetRequest{Key: key})
				require.NoError(t, err)
				assert.Equal(t, proto.Status_OK, gr.Status, key)
			}

			// No notification for the rejected range
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			notifications, err := db.ReadNextNotifications(ctx, 0)
			require.NoError(t, err)
			require.Len(t, notifications, 2)
			assert.Empty(t, notifications[1].Notifications)

			assert.NoError(t, db.Close())
			assert.NoError(t, factory.Close())
		})
	}
}

// A subscriber catching up from an old offset must receive the backlog in
// bounded chunks, not the entire retention window in one slice: the dispatch
// loop in the leader controller resumes from the last delivered offset + 1.
func TestDB_NotificationsReadBatchLimit(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)

	const total = maxNotificationBatchSize + 50
	for i := 0; i < total; i++ {
		_, err := db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{Key: "a", Value: []byte("v")}},
		}, int64(i), now(), NoOpCallback)
		assert.NoError(t, err)
	}

	// First read is capped at maxNotificationBatchSize batches. require, not
	// assert: with an uncapped read the resume below would ask for an offset
	// past the backlog and block until the suite timeout.
	notifications, err := db.ReadNextNotifications(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, maxNotificationBatchSize, len(notifications))
	assert.EqualValues(t, 0, notifications[0].Offset)
	lastDelivered := notifications[len(notifications)-1].Offset
	assert.EqualValues(t, maxNotificationBatchSize-1, lastDelivered)

	// Resuming from the last delivered offset + 1 returns the remainder
	rest, err := db.ReadNextNotifications(context.Background(), lastDelivered+1)
	assert.NoError(t, err)
	assert.Equal(t, total-maxNotificationBatchSize, len(rest))
	assert.EqualValues(t, maxNotificationBatchSize, rest[0].Offset)
	assert.EqualValues(t, total-1, rest[len(rest)-1].Offset)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

// A read with no batch retained from its start offset onwards must wait for
// the next one: the leader dispatch loop retries an empty result at once and
// would busy-spin until the next write to the shard.
func TestDB_NotificationsWaitWhenNothingRetained(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer factory.Close()
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)
	defer db.Close()

	write := func(offset int64) {
		_, err := db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{Key: "a", Value: []byte("0")}},
		}, offset, now(), NoOpCallback)
		require.NoError(t, err)
	}
	assertReadWaits := func(startOffset int64) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		notifications, err := db.ReadNextNotifications(ctx, startOffset)
		assert.ErrorIs(t, err, context.DeadlineExceeded, "start offset %d", startOffset)
		assert.Nil(t, notifications, "start offset %d", startOffset)
	}

	// No batch was ever written. MinInt64 is what the leader reads from for
	// StartOffsetExclusive=MaxInt64.
	assertReadWaits(-5)
	assertReadWaits(math.MinInt64)

	write(0)
	write(1)

	// A negative start offset reads from the first retained batch
	notifications, err := db.ReadNextNotifications(context.Background(), -5)
	require.NoError(t, err)
	require.Len(t, notifications, 2)
	assert.EqualValues(t, 0, notifications[0].Offset)

	// Delete every batch, as the trimmer does once they are all past the
	// retention time, while the last notification offset stays at 1
	wb := db.RawKV().NewWriteBatch()
	require.NoError(t, wb.DeleteRange(firstNotificationKey, lastNotificationKey))
	require.NoError(t, wb.Commit())
	require.NoError(t, wb.Close())

	assertReadWaits(-5)
	assertReadWaits(0)
	assertReadWaits(1)

	// A reader waiting past the deleted batches gets the next one. The sleep
	// lets it reach the wait before the batch is written.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	readCh := make(chan []*proto.NotificationBatch, 1)
	go func() {
		notifications, err := db.ReadNextNotifications(ctx, 0)
		assert.NoError(t, err)
		readCh <- notifications
	}()

	time.Sleep(100 * time.Millisecond)
	write(2)

	notifications = <-readCh
	require.Len(t, notifications, 1)
	assert.EqualValues(t, 2, notifications[0].Offset)
}
