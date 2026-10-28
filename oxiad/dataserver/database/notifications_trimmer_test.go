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
	"log/slog"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/concurrent"
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

			// The batches up to offset 4 are past the retention time. The
			// trimmer records the trimmed offset before it deletes them, and
			// drops them from the cache after: wait until a read finds them
			// trimmed too
			clock.Set(14)
			require.Eventually(t, func() bool {
				_, err := db.ReadNextNotifications(context.Background(), 0)
				return db.TrimmedNotificationsOffset() == 4 && errors.Is(err, ErrNotificationsTrimmed)
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

func TestNotificationsTrimmerTrimNotificationsFailsOnCorruptedFirstBatch(t *testing.T) {
	trimmer, kv, listener := newIdleNotificationsTrimmer(t)
	putRawKey(t, kv, notificationKey(0), []byte{0xff})
	putNotificationBatch(t, kv, 1, map[string]*proto.Notification{"a": {}})

	err := trimmer.trimNotifications()
	assert.ErrorContains(t, err, "failed to deserialize notification batch")
	assert.Empty(t, listener.trimmedOffsets)
	assert.True(t, keyExists(t, kv, notificationKey(0)))
}

func TestNotificationsTrimmerTrimNotificationsFailsOnCorruptedBatchInBinarySearch(t *testing.T) {
	trimmer, kv, listener := newIdleNotificationsTrimmer(t)
	for offset := int64(0); offset < 5; offset++ {
		putNotificationBatch(t, kv, offset, map[string]*proto.Notification{"a": {}})
	}
	// The first batch the binary search reads over [0, 4]
	putRawKey(t, kv, notificationKey(2), []byte{0xff})

	err := trimmer.trimNotifications()
	assert.ErrorContains(t, err, "failed to perform binary search")
	assert.Empty(t, listener.trimmedOffsets)
	assert.True(t, keyExists(t, kv, notificationKey(0)))
}

func TestNotificationsTrimmerTrimNotificationsFailsWhenTrimmingFails(t *testing.T) {
	trimmer, kv, listener := newIdleNotificationsTrimmer(t)
	for offset := int64(0); offset < 5; offset++ {
		putNotificationBatch(t, kv, offset, map[string]*proto.Notification{"a": {}})
	}
	trimmingErr := errors.New("trimming failed")
	listener.trimmingHook = func(kvstore.WriteBatch, int64) (func(), error) { return nil, trimmingErr }

	err := trimmer.trimNotifications()
	assert.ErrorIs(t, err, trimmingErr)
	assert.Empty(t, listener.trimmedOffsets)
	for offset := int64(0); offset < 5; offset++ {
		assert.True(t, keyExists(t, kv, notificationKey(offset)), "offset %d", offset)
	}
}

func TestNotificationsTrimmerRunKeepsTrimmingAfterFailure(t *testing.T) {
	kv := &hookedKV{KV: newTestKV(t)}
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })
	kv.failOn(opKeyRangeScan, "")
	ctx, cancel := context.WithCancel(context.Background())
	waitClose := concurrent.NewWaitGroup(1)
	trimmer := newNotificationsTrimmer(ctx, constant.DefaultNamespace, 1, kv, time.Millisecond, waitClose,
		time2.SystemClock, &fakeTrimListener{})
	assert.Equal(t, minNotificationTrimmingInterval, trimmer.interval)

	assert.Eventually(t, func() bool { return kv.failures.Load() >= 2 }, 10*time.Second, 10*time.Millisecond)
	cancel()
	assert.NoError(t, waitClose.Wait(context.Background()))
}

func TestNotificationsTrimmerTrimNotificationsFailsWhenStorageFails(t *testing.T) {
	for _, test := range []struct {
		name string
		op   string
		key  string
	}{
		{"first", opKeyRangeScan, ""},
		{"last", opKeyRangeScanReverse, ""},
		{"read", opGet, notificationKey(0)},
		{"delete", opBatchDeleteRange, ""},
		{"commit", opBatchCommit, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			trimmer, kv, listener := newIdleNotificationsTrimmer(t)
			for offset := int64(0); offset < 5; offset++ {
				putNotificationBatch(t, kv, offset, map[string]*proto.Notification{"a": {}})
			}
			hooked := &hookedKV{KV: kv}
			trimmer.kv = hooked
			hooked.failOn(test.op, test.key)
			reverted := false
			listener.trimmingHook = func(kvstore.WriteBatch, int64) (func(), error) {
				return func() { reverted = true }, nil
			}

			err := trimmer.trimNotifications()
			assert.ErrorIs(t, err, errInjected)
			assert.Equal(t, test.op == opBatchCommit, reverted)
			assert.Empty(t, listener.trimmedOffsets)
			assert.True(t, keyExists(t, kv, notificationKey(0)))
		})
	}
}

func TestNotificationsTrimmerGetFirstLastFailsOnInvalidKey(t *testing.T) {
	for _, op := range []string{opKeyRangeScan, opKeyRangeScanReverse} {
		t.Run(op, func(t *testing.T) {
			trimmer, kv, _ := newIdleNotificationsTrimmer(t)
			putNotificationBatch(t, kv, 0, map[string]*proto.Notification{"a": {}})
			putRawKey(t, kv, notificationsPrefix+"/z", []byte{})
			hooked := &hookedKV{KV: kv}
			trimmer.kv = hooked
			hooked.scanBounds = func(scanOp, lowerBound, upperBound string) (string, string) {
				if scanOp == op {
					return notificationsPrefix + "/z", notificationsPrefix + "/zz"
				}
				return lowerBound, upperBound
			}

			_, _, err := trimmer.getFirstLast()
			assert.Error(t, err)
		})
	}
}

func TestNotificationsTrimmerGetFirstLastFailsWhenCloseFails(t *testing.T) {
	for _, test := range []struct {
		name    string
		batches int64
		closes  int64
	}{
		{"empty", 0, 1},
		{"batches", 2, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			trimmer, kv, _ := newIdleNotificationsTrimmer(t)
			for offset := int64(0); offset < test.batches; offset++ {
				putNotificationBatch(t, kv, offset, map[string]*proto.Notification{"a": {}})
			}
			hooked := &hookedKV{KV: kv}
			trimmer.kv = hooked
			hooked.failOn(opKeyIteratorClose, "")

			_, _, err := trimmer.getFirstLast()
			assert.ErrorIs(t, err, errInjected)
			assert.Equal(t, test.closes, hooked.failures.Load())
		})
	}
}

func TestNotificationsTrimmerGetFirstLastWhenBatchesVanish(t *testing.T) {
	trimmer, kv, _ := newIdleNotificationsTrimmer(t)
	putNotificationBatch(t, kv, 0, map[string]*proto.Notification{"a": {}})
	hooked := &hookedKV{KV: kv}
	trimmer.kv = hooked
	hooked.scanBounds = func(op, lowerBound, upperBound string) (string, string) {
		if op == opKeyRangeScanReverse {
			return notificationsPrefix + "/z", notificationsPrefix + "/zz"
		}
		return lowerBound, upperBound
	}

	first, last, err := trimmer.getFirstLast()
	require.NoError(t, err)
	assert.Equal(t, int64(-1), first)
	assert.Equal(t, int64(-1), last)
}

// A batch deleted in the meantime is no longer to trim.
func TestNotificationsTrimmerTrimNotificationsSkipsVanishedFirstBatch(t *testing.T) {
	trimmer, kv, listener := newIdleNotificationsTrimmer(t)
	putNotificationBatch(t, kv, 0, map[string]*proto.Notification{"a": {}})
	hooked := &hookedKV{KV: kv}
	trimmer.kv = hooked
	hooked.failWith(opGet, notificationKey(0), errs.ErrDBKeyNotFound)

	assert.NoError(t, trimmer.trimNotifications())
	assert.Empty(t, listener.trimmedOffsets)
	assert.True(t, keyExists(t, kv, notificationKey(0)))
}

// The binary search over offsets with gaps probes offsets without a batch,
// whose next batch is past the end of the window it searches.
func TestNotificationsTrimmerTrimNotificationsWithSparseOffsets(t *testing.T) {
	trimmer, kv, listener := newIdleNotificationsTrimmer(t)
	putNotificationBatchAt(t, kv, 0, 0)
	// Within the retention
	putNotificationBatchAt(t, kv, 10, trimmer.clock.Now().UnixMilli())

	require.NoError(t, trimmer.trimNotifications())
	assert.Equal(t, []int64{0}, listener.trimmedOffsets)
	assert.False(t, keyExists(t, kv, notificationKey(0)))
	assert.True(t, keyExists(t, kv, notificationKey(10)))
}

func TestNotificationsTrimmerReadFromPastLastBatch(t *testing.T) {
	trimmer, kv, _ := newIdleNotificationsTrimmer(t)
	putNotificationBatch(t, kv, 0, map[string]*proto.Notification{"a": {}})

	// The next key is not a batch
	putRawKey(t, kv, sessionKeyPrefix+"/0000000000000001", []byte{})
	_, _, err := trimmer.readFrom(1)
	assert.ErrorIs(t, err, errs.ErrDBKeyNotFound)

	// The next key is not a valid batch key
	putRawKey(t, kv, notificationsPrefix+"/z", []byte{})
	_, _, err = trimmer.readFrom(1)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, errs.ErrDBKeyNotFound)
}

// newIdleNotificationsTrimmer returns a trimmer that runs only when the test
// calls it, with every batch past its retention, its store and its listener.
func newIdleNotificationsTrimmer(t *testing.T) (*notificationsTrimmer, kvstore.KV, *fakeTrimListener) {
	t.Helper()
	kv := newTestKV(t)
	t.Cleanup(func() { assert.NoError(t, kv.Close()) })
	clock := &time2.MockedClock{}
	clock.Set(time.Hour.Milliseconds())
	listener := &fakeTrimListener{}
	return &notificationsTrimmer{
		kv:                         kv,
		notificationsRetentionTime: time.Millisecond,
		clock:                      clock,
		log:                        slog.Default(),
		listener:                   listener,
	}, kv, listener
}

// putNotificationBatchAt writes the batch at offset, of the time timestampMillis.
func putNotificationBatchAt(t *testing.T, kv kvstore.KV, offset int64, timestampMillis int64) {
	t.Helper()
	notifications := newNotifications(0, offset, uint64(timestampMillis), 1)
	notifications.add("a", &proto.Notification{})
	data, err := notifications.seal().MarshalVT()
	require.NoError(t, err)
	putRawKey(t, kv, notificationKey(offset), data)
}

// fakeTrimListener records the trimmed offsets, and runs trimmingHook, if set,
// on trimming.
type fakeTrimListener struct {
	trimmingHook   func(batch kvstore.WriteBatch, offset int64) (func(), error)
	trimmedOffsets []int64
}

func (l *fakeTrimListener) trimming(batch kvstore.WriteBatch, offset int64) (func(), error) {
	if l.trimmingHook != nil {
		return l.trimmingHook(batch, offset)
	}
	return func() {}, nil
}

func (l *fakeTrimListener) trimmed(offset int64) {
	l.trimmedOffsets = append(l.trimmedOffsets, offset)
}
