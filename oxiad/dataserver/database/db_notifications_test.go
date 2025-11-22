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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/logging"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
)

func init() {
	logging.ConfigureLogger()
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
	n, found := nb.Notifications["a"]
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
	n, found = nb.Notifications["a"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_MODIFIED, n.Type)
	assert.EqualValues(t, wr1.Puts[0].Version.VersionId, *n.VersionId)

	nb = notifications[1]
	assert.Equal(t, t2, nb.Timestamp)
	assert.EqualValues(t, 2, nb.Offset)
	assert.EqualValues(t, 1, nb.Shard)
	assert.Equal(t, 1, len(nb.Notifications))
	n, found = nb.Notifications["b"]
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
	n, found = nb.Notifications["c"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_CREATED, n.Type)
	assert.EqualValues(t, wr3.Puts[0].Version.VersionId, *n.VersionId)
	n, found = nb.Notifications["d"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_CREATED, n.Type)
	assert.EqualValues(t, wr3.Puts[1].Version.VersionId, *n.VersionId)
	n, found = nb.Notifications["a"]
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
	n, found = nb.Notifications["x1"]
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
	n, found := nb.Notifications["a"]
	assert.True(t, found)
	assert.Equal(t, proto.NotificationType_KEY_RANGE_DELETED, n.Type)
	assert.Equal(t, "c", *n.KeyRangeLast)
	assert.Nil(t, n.VersionId)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}
