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
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
)

func TestNotificationsTrimmer(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%v", cached), func(t *testing.T) {
			clock := &time2.MockedClock{}

			factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			assert.NoError(t, err)
			dbx, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 10*time.Millisecond, clock)
			assert.NoError(t, err)
			defer dbx.Close()
			if cached {
				// The trimmer drops the batches from the cache as well
				cacheNotifications(t, dbx)
			}

			for i := int64(0); i < 100; i++ {
				_, err = dbx.ProcessWrite(&proto.WriteRequest{
					Puts: []*proto.PutRequest{{
						Key:   fmt.Sprintf("key-%d", i),
						Value: []byte("0"),
					}},
				}, i, uint64(i), NoOpCallback)
				assert.NoError(t, err)
			}

			time.Sleep(1 * time.Second)
			// No entries should have been trimmed
			assert.EqualValues(t, 0, firstNotification(t, dbx))

			// Clock has advanced, though not enough to have the trimming started
			clock.Set(3)

			time.Sleep(1 * time.Second)
			// No entries should have been trimmed
			assert.EqualValues(t, 0, firstNotification(t, dbx))

			clock.Set(15)

			assert.Eventually(t, func() bool {
				return firstNotification(t, dbx) == 6
			}, 10*time.Second, 1*time.Second)

			clock.Set(75)

			assert.Eventually(t, func() bool {
				return firstNotification(t, dbx) == 66
			}, 10*time.Second, 1*time.Second)

			clock.Set(120)

			assert.Eventually(t, func() bool {
				return firstNotification(t, dbx) == -1
			}, 10*time.Second, 1*time.Second)
		})
	}
}

// TestNotificationsTrimmer_TrimmedOffset checks that the database records the
// offset of the last batch that the retention deleted: a read that would skip
// some of them fails, even after a restart.
func TestNotificationsTrimmer_TrimmedOffset(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%v", cached), func(t *testing.T) {
			clock := &time2.MockedClock{}
			factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			defer func() { assert.NoError(t, factory.Close()) }()
			db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 10*time.Millisecond, clock)
			require.NoError(t, err)
			if cached {
				cacheNotifications(t, db)
			}
			assert.EqualValues(t, -1, db.TrimmedNotificationsOffset())

			for i := int64(0); i < 10; i++ {
				_, err = db.ProcessWrite(&proto.WriteRequest{
					Puts: []*proto.PutRequest{{Key: fmt.Sprintf("key-%d", i), Value: []byte("0")}},
				}, i, uint64(i), NoOpCallback)
				require.NoError(t, err)
			}

			// The batches up to offset 4 are past the retention time
			clock.Set(14)
			require.Eventually(t, func() bool {
				return db.TrimmedNotificationsOffset() == 4
			}, 10*time.Second, 10*time.Millisecond)

			assertTrimmed := func(db DB) {
				t.Helper()
				for _, startOffset := range []int64{-1, 0, 4} {
					_, err := db.ReadNextNotifications(context.Background(), startOffset)
					assert.ErrorIs(t, err, ErrNotificationsTrimmed, "start offset %d", startOffset)
					assert.ErrorIs(t, err, constant.ErrResourceUnavailable, "start offset %d", startOffset)
				}
				notifications, err := readNotifications(context.Background(), db, 5)
				require.NoError(t, err)
				assert.EqualValues(t, 5, notifications[0].Offset)
			}
			assertTrimmed(db)

			require.NoError(t, db.Close())
			db, err = NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, clock)
			require.NoError(t, err)
			defer func() { assert.NoError(t, db.Close()) }()
			assert.EqualValues(t, 4, db.TrimmedNotificationsOffset())
			assertTrimmed(db)
		})
	}
}

func firstNotification(t *testing.T, db DB) int64 {
	t.Helper()

	// Once every batch is trimmed, the read waits for the next one. A read from
	// a trimmed batch fails, as some of the batches it reads are gone.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	nextNotifications, err := db.ReadNextNotifications(ctx, db.TrimmedNotificationsOffset()+1)
	if errors.Is(err, context.DeadlineExceeded) {
		return -1
	}
	assert.NoError(t, err)

	if len(nextNotifications) == 0 {
		return -1
	}

	return nextNotifications[0].Offset
}
