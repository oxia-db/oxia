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

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

func TestSplitNotificationsRange(t *testing.T) {
	parent := &proto.HashRange{Min: 100, Max: 199}
	for _, test := range []struct {
		name    string
		child   *proto.HashRange
		parent  *proto.HashRange
		minHash uint32
		maxHash uint32
	}{
		{"low child", &proto.HashRange{Min: 100, Max: 149}, parent, 0, 149},
		{"high child", &proto.HashRange{Min: 150, Max: 199}, parent, 150, math.MaxUint32},
		{"low child of the first shard", &proto.HashRange{Min: 0, Max: 49}, &proto.HashRange{Min: 0, Max: 99}, 0, 49},
		{"high child of the last shard", &proto.HashRange{Min: 200, Max: math.MaxUint32},
			&proto.HashRange{Min: 100, Max: math.MaxUint32}, 200, math.MaxUint32},
		// Without the range of the parent, a child that reaches a bound of
		// the hash space is on that side, and any other delivers everything
		{"unknown parent, first child", &proto.HashRange{Min: 0, Max: 49}, nil, 0, 49},
		{"unknown parent, last child", &proto.HashRange{Min: 50, Max: math.MaxUint32}, nil, 50, math.MaxUint32},
		{"unknown parent, inner child", &proto.HashRange{Min: 150, Max: 199}, nil, 0, math.MaxUint32},
	} {
		t.Run(test.name, func(t *testing.T) {
			minHash, maxHash := splitNotificationsRange(test.child, test.parent)
			assert.Equal(t, test.minHash, minHash)
			assert.Equal(t, test.maxHash, maxHash)
		})
	}
}

// quarterKeys returns a key whose hash is in each quarter of the hash space,
// sorted as the quarters, with the bounds of the quarters.
func quarterKeys(t *testing.T) (keys []string, quarters []*proto.HashRange) {
	t.Helper()
	const quarter = math.MaxUint32/4 + 1
	for i := uint32(0); i < 4; i++ {
		hashRange := &proto.HashRange{Min: i * quarter, Max: i*quarter + quarter - 1}
		quarters = append(quarters, hashRange)
		keys = append(keys, keyInRange(t, fmt.Sprintf("quarter-%d-key-%%d", i), hashRange))
	}
	return keys, quarters
}

func keysBatch(t *testing.T, offset int64, keys ...string) proto.EncodedNotificationBatch {
	t.Helper()
	nb := &proto.NotificationBatch{Offset: offset}
	for _, key := range keys {
		nb.Notifications = append(nb.Notifications, &proto.NotificationEntry{
			Key:   &key,
			Value: &proto.Notification{Type: proto.NotificationType_KEY_CREATED},
		})
	}
	data, err := nb.MarshalVT()
	require.NoError(t, err)
	return proto.EncodedNotificationBatch{Offset: offset, Data: data}
}

// batchKeys returns the keys of the notifications of each batch.
func batchKeys(t *testing.T, batches []proto.EncodedNotificationBatch) map[int64][]string {
	t.Helper()
	res := make(map[int64][]string)
	for _, batch := range batches {
		nb := &proto.NotificationBatch{}
		require.NoError(t, nb.UnmarshalVT(batch.Data))
		require.Equal(t, batch.Offset, nb.Offset)
		keys := []string{}
		for _, entry := range nb.Notifications {
			keys = append(keys, entry.GetKey())
		}
		slices.Sort(keys)
		res[batch.Offset] = keys
	}
	return res
}

// TestInheritedNotifications_Filter follows a shard through two splits: the
// first one, of the whole hash space, at offset 10, then its high child's,
// whose low child is the shard, at offset 20. Of every batch, each notification
// is delivered by one of the three shards that replace the first one.
func TestInheritedNotifications_Filter(t *testing.T) {
	keys, quarters := quarterKeys(t)
	whole := &proto.HashRange{Min: 0, Max: math.MaxUint32}
	low := &proto.HashRange{Min: quarters[0].Min, Max: quarters[1].Max}
	high := &proto.HashRange{Min: quarters[2].Min, Max: quarters[3].Max}

	children := map[string]inheritedNotifications{
		"low": inheritedNotifications(nil).withSplit(splitNotificationsRange(low, whole)).closedAt(10),
	}
	highChild := inheritedNotifications(nil).withSplit(splitNotificationsRange(high, whole)).closedAt(10)
	children["high-low"] = highChild.withSplit(splitNotificationsRange(quarters[2], high)).closedAt(20)
	children["high-high"] = highChild.withSplit(splitNotificationsRange(quarters[3], high)).closedAt(20)

	// The batches of the shard that was split first, of its high child, and
	// of the shards that replaced them
	batches := []proto.EncodedNotificationBatch{
		keysBatch(t, 5, keys...),
		keysBatch(t, 15, keys...),
		keysBatch(t, 25, keys...),
	}
	delivered := map[int64][]string{}
	for name, child := range children {
		filtered, err := child.filter(batches)
		require.NoError(t, err)
		for offset, keys := range batchKeys(t, filtered) {
			if offset == 25 {
				// Its own batch, delivered whole
				assert.Len(t, keys, 4, "%s, batch at %d", name, offset)
				continue
			}
			if name == "low" && offset == 15 {
				// Not a batch of the low child: it has its own at this offset
				continue
			}
			delivered[offset] = append(delivered[offset], keys...)
		}
	}
	for _, offset := range []int64{5, 15} {
		slices.Sort(delivered[offset])
		assert.Equal(t, keys, delivered[offset], "batch at %d", offset)
	}

	// The shard of the third quarter delivers that quarter of the batch of the
	// first shard, and the low half of the batch of the high child
	filtered, err := children["high-low"].filter(batches)
	require.NoError(t, err)
	assert.Equal(t, map[int64][]string{5: {keys[2]}, 15: keys[:3], 25: keys}, batchKeys(t, filtered))

	// A batch with nothing to drop is returned as it is
	filtered, err = children["high-low"].filter(batches[2:])
	require.NoError(t, err)
	assert.Same(t, &batches[2].Data[0], &filtered[0].Data[0])
}

// readNotifiedKeys returns the keys of the notifications of each batch from
// startOffset on.
func readNotifiedKeys(t *testing.T, db DB, startOffset int64) map[int64][]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	batches, err := db.ReadNextNotifications(ctx, startOffset)
	require.NoError(t, err)
	return batchKeys(t, batches)
}

func putKeys(keys []string, partitionKey *string) *proto.WriteRequest {
	request := &proto.WriteRequest{}
	for _, key := range keys {
		request.Puts = append(request.Puts, &proto.PutRequest{Key: key, Value: []byte(key), PartitionKey: partitionKey})
	}
	return request
}

// TestDB_InheritedNotifications checks the notifications that the high child of
// a split delivers of the batches it inherits: those of its side of the split,
// until it writes its own batches, which it delivers whole.
func TestDB_InheritedNotifications(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, factory.Close()) }()
	child, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
	require.NoError(t, err)

	keys, _ := quarterKeys(t)
	left, right := splitRanges()

	// A batch of the parent in the snapshot that seeds the child, and one that
	// the child applies after it
	_, err = child.ProcessWrite(putKeys(keys, nil), 0, 0, NoOpCallback)
	require.NoError(t, err)
	require.NoError(t, child.SetDeferredSplitFilter(&DeferredSplitFilter{
		MinHash: right.Min, MaxHash: right.Max, ParentTerm: 1,
	}, &proto.HashRange{Min: 0, Max: math.MaxUint32}))
	_, err = child.ProcessSplitParentWrite(putKeys(keys, nil), 1, 0, NoOpCallback)
	require.NoError(t, err)
	assert.Equal(t, map[int64][]string{0: keys[2:], 1: keys[2:]}, readNotifiedKeys(t, child, 0))

	// The first write of the child's own terms: the records it places by their
	// partition key can have their key outside its range
	_, err = child.ProcessWrite(putKeys(keys, partitionKeyIn(t, right)), 2, 0, NoOpCallback)
	require.NoError(t, err)
	_, err = child.ProcessWrite(putKeys(keys[:1], partitionKeyIn(t, right)), 3, 0, NoOpCallback)
	require.NoError(t, err)
	expected := map[int64][]string{0: keys[2:], 1: keys[2:], 2: keys, 3: keys[:1]}
	assert.Equal(t, expected, readNotifiedKeys(t, child, 0))
	assert.Equal(t, map[int64][]string{2: keys, 3: keys[:1]}, readNotifiedKeys(t, child, 2))

	// The low child, which holds the same inherited batches, delivers the
	// others
	lowChild := inheritedNotifications(nil).withSplit(splitNotificationsRange(left,
		&proto.HashRange{Min: 0, Max: math.MaxUint32})).closedAt(1)
	inherited, err := child.(*db).notificationsTracker.ReadNextNotifications(context.Background(), 0)
	require.NoError(t, err)
	filtered, err := lowChild.filter(inherited[:2])
	require.NoError(t, err)
	assert.Equal(t, map[int64][]string{0: keys[:2], 1: keys[:2]}, batchKeys(t, filtered))

	// Recorded in the database
	require.NoError(t, child.Close())
	child, err = NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
	require.NoError(t, err)
	defer func() { assert.NoError(t, child.Close()) }()
	assert.Equal(t, expected, readNotifiedKeys(t, child, 0))
}

// TestDB_InheritedNotificationsFilterCompletion checks that a child whose
// deferred split filter completes before it writes records there where its
// inherited batches end: its writes after it are its own.
func TestDB_InheritedNotificationsFilterCompletion(t *testing.T) {
	db := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	keys, _ := quarterKeys(t)
	_, right := splitRanges()

	_, err := db.ProcessWrite(putKeys(keys, nil), 0, 0, NoOpCallback)
	require.NoError(t, err)
	require.NoError(t, db.SetDeferredSplitFilter(&DeferredSplitFilter{
		MinHash: right.Min, MaxHash: right.Max, ParentTerm: 1,
	}, &proto.HashRange{Min: 0, Max: math.MaxUint32}))

	filterKeys, complete, err := db.SplitFilterKeys(nil)
	require.NoError(t, err)
	require.True(t, complete)
	_, err = db.ProcessControlRequest(&proto.ControlRequest{Value: &proto.ControlRequest_SplitFilter{
		SplitFilter: &proto.SplitFilterRequest{Keys: filterKeys, Complete: true},
	}}, 1, 0, NoOpCallback)
	require.NoError(t, err)
	require.Nil(t, db.DeferredSplitFilter())

	_, err = db.ProcessWrite(putKeys(keys, partitionKeyIn(t, right)), 2, 0, NoOpCallback)
	require.NoError(t, err)
	assert.Equal(t, map[int64][]string{0: keys[2:], 2: keys}, readNotifiedKeys(t, db, 0))
}

// TestDB_InheritedNotificationsChildOfChild splits the high child of a split
// in turn: the low child of the high child delivers, of the batches of the
// first parent, those of the third quarter of the hash space, and of the
// batches of the high child, those of the low three quarters.
func TestDB_InheritedNotificationsChildOfChild(t *testing.T) {
	db := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	keys, quarters := quarterKeys(t)
	_, high := splitRanges()

	// The high child of the whole hash space
	_, err := db.ProcessWrite(putKeys(keys, nil), 0, 0, NoOpCallback)
	require.NoError(t, err)
	require.NoError(t, db.SetDeferredSplitFilter(&DeferredSplitFilter{
		MinHash: high.Min, MaxHash: high.Max, ParentTerm: 1,
	}, &proto.HashRange{Min: 0, Max: math.MaxUint32}))
	_, err = db.ProcessWrite(putKeys(keys, partitionKeyIn(t, high)), 1, 0, NoOpCallback)
	require.NoError(t, err)

	// Its low child, seeded with its snapshot
	require.NoError(t, db.SetDeferredSplitFilter(&DeferredSplitFilter{
		MinHash: quarters[2].Min, MaxHash: quarters[2].Max, ParentTerm: 2,
	}, high))
	_, err = db.ProcessWrite(putKeys(keys, partitionKeyIn(t, quarters[2])), 2, 0, NoOpCallback)
	require.NoError(t, err)

	assert.Equal(t, map[int64][]string{0: keys[2:3], 1: keys[:3], 2: keys}, readNotifiedKeys(t, db, 0))
}
