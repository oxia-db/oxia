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

package lead

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
	time2 "github.com/oxia-db/oxia/common/time"
	constant2 "github.com/oxia-db/oxia/oxiad/dataserver/constant"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/statemachine"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
)

// The hash range of the split children in these tests: the left half.
var splitTestChildRange = &proto.HashRange{Min: 0, Max: math.MaxUint32 / 2}

// splitKeys returns keys whose hash falls within hashRange, and keys whose hash
// falls outside of it.
func splitKeys(hashRange *proto.HashRange) (inRange []string, outOfRange []string) {
	return splitKeysWith("key-%d", hashRange, 5)
}

// splitKeysWith returns count keys built by format whose hash falls within
// hashRange, and count keys whose hash falls outside of it.
func splitKeysWith(format string, hashRange *proto.HashRange, count int) (inRange []string, outOfRange []string) {
	for i := 0; len(inRange) < count || len(outOfRange) < count; i++ {
		key := fmt.Sprintf(format, i)
		if isKeyInRange(key, hashRange) {
			if len(inRange) < count {
				inRange = append(inRange, key)
			}
		} else if len(outOfRange) < count {
			outOfRange = append(outOfRange, key)
		}
	}
	return inRange, outOfRange
}

func isKeyInRange(key string, hashRange *proto.HashRange) bool {
	h := hash.Xxh332(key)
	return h >= hashRange.Min && h <= hashRange.Max
}

// A split child seeded with all the data of its parent applies the parent's
// entries as the parent applied them, the puts of the other child included:
// every record of the child gets the version id that the parent returned to
// the client, a conditional write that the parent accepted applies on the
// child too, and the child records the notifications of the parent. After the
// split, the child assigns version ids from where the parent had got to.
func TestLeaderController_SplitChildCopiesParent(t *testing.T) {
	var shard int64 = 1
	var childShard int64 = 2
	ctx := context.Background()

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)
	rpcClient := rpc.NewMockRpcClient()

	lc, err := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpcClient,
		walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(ctx, &proto.BecomeLeaderRequest{Shard: shard, Term: 1, ReplicationFactor: 1,
		FeaturesSupported: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}})
	require.NoError(t, err)

	left, right := splitKeys(splitTestChildRange)
	write := func(puts ...*proto.PutRequest) *proto.WriteResponse {
		t.Helper()
		res, err := lc.WriteBlock(ctx, &proto.WriteRequest{Shard: &shard, Puts: puts})
		require.NoError(t, err)
		return res
	}
	put := func(key string, expectedVersionId *int64) *proto.PutRequest {
		return &proto.PutRequest{Key: key, Value: []byte("value-" + key), ExpectedVersionId: expectedVersionId}
	}

	// Records in the snapshot the child is seeded with
	seeded := write(put(left[0], nil), put(right[0], nil))

	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		Shard:               shard,
		Term:                1,
		FollowerName:        "child-leader",
		FollowerHeadEntryId: constant2.InvalidEntryId,
		Observer:            true,
		TargetShard:         &childShard,
		SplitHashRange: &proto.Int32HashRange{
			MinHashInclusive: splitTestChildRange.Min,
			MaxHashInclusive: splitTestChildRange.Max,
		},
	})
	require.NoError(t, err)
	childDb, snapshotOffset := installSplitSnapshot(t, rpcClient, kvFactory, childShard)

	// The writes the child applies from the parent's log. The puts of the
	// other child take version ids, and a conditional write on a record of the
	// other child succeeds, as on the parent.
	write(put(right[1], nil), put(left[1], nil))
	write(put(right[2], nil))
	assert.Equal(t, proto.Status_OK, write(put(right[0], &seeded.Puts[1].Version.VersionId)).Puts[0].Status)
	// A put that fails does not take any version id
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, write(put(right[1], pb.Int64(1000))).Puts[0].Status)
	// Neither does a request the parent rejects as a whole
	_, err = lc.WriteBlock(ctx, &proto.WriteRequest{Shard: &shard, Puts: []*proto.PutRequest{{
		Key: "seq", Value: []byte("seq"), PartitionKey: &left[0], SequenceKeyDelta: []uint64{0},
	}}})
	assert.ErrorIs(t, err, database.ErrWriteRejected)
	// A conditional write with the version id the parent returned
	current, err := readAll(ctx, lc, &proto.ReadRequest{Shard: &shard, Gets: []*proto.GetRequest{{Key: left[1]}}})
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, write(put(left[1], &current[0].Version.VersionId)).Puts[0].Status)
	write(put(left[2], nil), put(right[3], nil), put(left[3], nil))

	status, err := lc.GetStatus(&proto.GetStatusRequest{Shard: shard})
	require.NoError(t, err)
	applyObserverEntries(t, rpcClient, childDb, status.HeadOffset)

	parentDb := lc.(*leaderController).db
	for _, key := range left[:4] {
		expected, err := parentDb.Get(&proto.GetRequest{Key: key})
		require.NoError(t, err)
		actual, err := childDb.Get(&proto.GetRequest{Key: key})
		require.NoError(t, err)
		assert.True(t, expected.EqualVT(actual), "%s: parent %v, child %v", key, expected, actual)
	}
	for _, key := range right[:4] {
		res, err := childDb.Get(&proto.GetRequest{Key: key})
		require.NoError(t, err)
		assert.Equal(t, proto.Status_KEY_NOT_FOUND, res.Status, key)
	}

	// The notifications are those of the parent, but for the shard they are
	// recorded on
	parentNotifications := recordedNotifications(t, parentDb, snapshotOffset+1, status.HeadOffset)
	childNotifications := recordedNotifications(t, childDb, snapshotOffset+1, status.HeadOffset)
	assert.Len(t, childNotifications, len(parentNotifications))
	for offset, expected := range parentNotifications {
		actual := childNotifications[offset].CloneVT()
		assert.Equal(t, childShard, actual.GetShard())
		actual.Shard = expected.Shard
		assert.True(t, expected.EqualVT(actual), "notifications at offset %d: parent %v, child %v",
			offset, expected, actual)
	}

	// Of those, the child delivers the ones of its records, as the other child
	// delivers the others
	delivered := deliveredNotifications(t, childDb, snapshotOffset+1, status.HeadOffset)
	assert.Len(t, delivered, len(parentNotifications))
	for offset, parent := range parentNotifications {
		var expected, actual []string
		for _, n := range parent.Notifications {
			if isKeyInRange(n.GetKey(), splitTestChildRange) {
				expected = append(expected, n.GetKey())
			}
		}
		for _, n := range delivered[offset].GetNotifications() {
			actual = append(actual, n.GetKey())
		}
		assert.Equal(t, expected, actual, "notifications at offset %d", offset)
	}

	// After the split, the child assigns version ids from where the parent
	// had got to
	next := &proto.WriteRequest{Puts: []*proto.PutRequest{put(left[4], nil)}}
	expected, err := parentDb.ProcessWrite(next.CloneVT(), status.HeadOffset+1, 0, WrapperUpdateOperationCallback)
	require.NoError(t, err)
	actual, err := childDb.ProcessWrite(next.CloneVT(), status.HeadOffset+1, 0, WrapperUpdateOperationCallback)
	require.NoError(t, err)
	assert.Equal(t, expected.Puts[0].Version.VersionId, actual.Puts[0].Version.VersionId)

	assert.NoError(t, childDb.Close())
	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// installSplitSnapshot seeds the database of a split child with the snapshot
// the parent leader sends to its observer, and records the deferred filter, as
// the child does, and returns it with the offset of the snapshot.
func installSplitSnapshot(t *testing.T, rpcClient *rpc.MockRpcClient, kvFactory kvstore.Factory,
	childShard int64) (database.DB, int64) {
	t.Helper()

	loader, err := kvFactory.NewSnapshotLoader(constant.DefaultNamespace, childShard)
	require.NoError(t, err)
	for _, chunk := range receiveSnapshot(t, rpcClient) {
		require.NoError(t, loader.AddChunk(chunk.Name, chunk.ChunkIndex, chunk.ChunkCount, chunk.Content))
	}
	require.NoError(t, loader.Complete())
	require.NoError(t, loader.Close())

	childDb, err := database.NewDB(constant.DefaultNamespace, childShard, kvFactory, proto.KeySortingType_UNKNOWN,
		time.Hour, time2.SystemClock)
	require.NoError(t, err)
	parentTerm, _, err := childDb.ReadTerm()
	require.NoError(t, err)
	require.NoError(t, childDb.SetDeferredSplitFilter(&database.DeferredSplitFilter{
		MinHash: splitTestChildRange.Min, MaxHash: splitTestChildRange.Max, ParentTerm: parentTerm,
	}, nil))

	offset, err := childDb.ReadCommitOffset()
	require.NoError(t, err)
	rpcClient.SendSnapshotStream.Response <- &proto.SnapshotResponse{AckOffset: offset}
	return childDb, offset
}

// receiveSnapshot returns the chunks of the snapshot an observer sends.
func receiveSnapshot(t *testing.T, rpcClient *rpc.MockRpcClient) []*proto.SnapshotChunk {
	t.Helper()

	var chunks []*proto.SnapshotChunk
	for {
		select {
		case chunk, ok := <-rpcClient.SendSnapshotStream.Requests:
			if !ok {
				return chunks
			}
			chunks = append(chunks, chunk)
		case <-time.After(10 * time.Second):
			require.FailNow(t, "the observer did not send a snapshot")
		}
	}
}

// applyObserverEntries applies the entries the observer sends to the split
// child, up to lastOffset, the way the child applies them.
func applyObserverEntries(t *testing.T, rpcClient *rpc.MockRpcClient, childDb database.DB, lastOffset int64) {
	t.Helper()

	for {
		select {
		case req := <-rpcClient.AppendReqs:
			if req.Entry == nil {
				// Commit offset advertisement
				continue
			}
			_, err := statemachine.ApplyLogEntry(childDb, req.Entry, WrapperUpdateOperationCallback)
			require.NoError(t, err)
			rpcClient.AckResps <- &proto.Ack{Offset: req.Entry.Offset}
			if req.Entry.Offset == lastOffset {
				return
			}
		case <-time.After(10 * time.Second):
			require.FailNow(t, "the observer did not send the entries", "last offset %d", lastOffset)
		}
	}
}

// readNotifications returns the notification batches of the database from
// firstOffset to lastOffset, by offset.
// recordedNotifications returns the notification batches that db recorded,
// from firstOffset to lastOffset: a split child records those of its parent as
// they are, and delivers them filtered (see deliveredNotifications).
func recordedNotifications(t *testing.T, db database.DB,
	firstOffset, lastOffset int64) map[int64]*proto.NotificationBatch {
	t.Helper()

	notificationKey := func(offset int64) string {
		return fmt.Sprintf("%snotifications/%016x", constant.InternalKeyPrefix, offset)
	}
	it, err := db.RawKV().RangeScan(notificationKey(firstOffset), notificationKey(lastOffset+1), kvstore.ShowInternalKeys)
	require.NoError(t, err)
	defer func() { assert.NoError(t, it.Close()) }()

	batches := make(map[int64]*proto.NotificationBatch)
	for ; it.Valid(); it.Next() {
		value, err := it.Value()
		require.NoError(t, err)
		nb := &proto.NotificationBatch{}
		require.NoError(t, nb.UnmarshalVT(value))
		batches[nb.Offset] = nb
	}
	require.NoError(t, it.Error())
	return batches
}

// deliveredNotifications returns the notification batches that db delivers to
// its subscribers, from firstOffset to lastOffset.
func deliveredNotifications(t *testing.T, db database.DB,
	firstOffset, lastOffset int64) map[int64]*proto.NotificationBatch {
	t.Helper()

	batches := make(map[int64]*proto.NotificationBatch)
	for offset := firstOffset; offset <= lastOffset; {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := db.ReadNextNotifications(ctx, offset)
		cancel()
		require.NoError(t, err)
		for _, encoded := range res {
			nb := decodeNotificationBatch(t, &encoded)
			if nb.Offset > lastOffset {
				return batches
			}
			batches[nb.Offset] = nb
			offset = nb.Offset + 1
		}
	}
	return batches
}

// newSplitChildDB returns the database of a split child that holds all the
// records of its parent, written by write, until it deletes the ones outside
// splitTestChildRange.
func newSplitChildDB(t *testing.T, kvFactory kvstore.Factory, shard int64, write *proto.WriteRequest) database.DB {
	t.Helper()

	db, err := database.NewDB(constant.DefaultNamespace, shard, kvFactory, proto.KeySortingType_HIERARCHICAL,
		time.Hour, time2.SystemClock)
	require.NoError(t, err)
	_, err = db.ProcessControlRequest(&proto.ControlRequest{Value: &proto.ControlRequest_FeatureEnable{
		FeatureEnable: &proto.FeatureEnableRequest{Features: []proto.Feature{proto.Feature_FEATURE_SPLIT_DEFERRED_FILTER}},
	}}, 0, 0, WrapperUpdateOperationCallback)
	require.NoError(t, err)
	_, err = db.ProcessWrite(write, 1, 0, WrapperUpdateOperationCallback)
	require.NoError(t, err)
	require.NoError(t, db.SetDeferredSplitFilter(&database.DeferredSplitFilter{
		MinHash: splitTestChildRange.Min, MaxHash: splitTestChildRange.Max, ParentTerm: 1,
	}, nil))
	return db
}

// splitChildRecords returns the write of records of both children, with
// secondary indexes, and the ephemeral records of a session.
func splitChildRecords(sessionId SessionId, left, right []string) *proto.WriteRequest {
	metadata, _ := (&proto.SessionMetadata{TimeoutMs: 10_000}).MarshalVT()
	sid := int64(sessionId)
	write := &proto.WriteRequest{Puts: []*proto.PutRequest{{Key: SessionKey(sessionId), Value: metadata}}}
	for i, key := range slices.Concat(left, right) {
		put := &proto.PutRequest{
			Key:              key,
			Value:            []byte("value-" + key),
			SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "idx", SecondaryKey: fmt.Sprintf("sec-%02d", i%7)}},
		}
		if i%2 == 0 {
			put.SessionId = &sid
		}
		write.Puts = append(write.Puts, put)
	}
	return write
}

// The leader of a split child deletes the records outside its hash range, with
// their secondary index entries and session shadow keys, once elected, in
// several steps, and then no longer holds the filter.
func TestLeaderController_DeferredSplitFilter(t *testing.T) {
	var shard int64 = 2
	var sessionId SessionId = 5
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)
	left, right := splitKeys(splitTestChildRange)
	write := splitChildRecords(sessionId, left, right)
	// More records than a step deletes
	bulkLeft, bulkRight := splitKeysWith("bulk-%d", splitTestChildRange, 1500)
	for _, key := range slices.Concat(bulkLeft, bulkRight) {
		write.Puts = append(write.Puts, &proto.PutRequest{Key: key, Value: []byte("v")})
	}
	require.NoError(t, newSplitChildDB(t, kvFactory, shard, write).Close())

	rpcClient := rpc.NewMockRpcClient()
	lc, err := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpcClient,
		walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: shard, Term: 2,
		ReplicationFactor: 1, FeaturesSupported: []proto.Feature{proto.Feature_FEATURE_SPLIT_DEFERRED_FILTER}})
	require.NoError(t, err)

	db := lc.(*leaderController).db
	assert.Eventually(t, func() bool { return db.DeferredSplitFilter() == nil }, 10*time.Second, 10*time.Millisecond)

	kv := db.RawKV()
	exists := func(key string) bool {
		_, _, closer, err := kv.Get(key, kvstore.ComparisonEqual, kvstore.ShowInternalKeys)
		if err != nil {
			assert.ErrorIs(t, err, kvstore.ErrKeyNotFound)
			return false
		}
		assert.NoError(t, closer.Close())
		return true
	}
	for _, put := range write.Puts[1:] {
		kept := isKeyInRange(put.Key, splitTestChildRange)
		assert.Equal(t, kept, exists(put.Key), put.Key)
		if len(put.SecondaryIndexes) > 0 {
			assert.Equal(t, kept, exists(secondaryIndexKey(put.Key, put.SecondaryIndexes[0])), put.Key)
		}
		if put.SessionId != nil {
			assert.Equal(t, kept, exists(ShadowKey(sessionId, put.Key)), put.Key)
		}
	}
	assert.True(t, exists(SessionKey(sessionId)))

	// A split can start again
	var childShard int64 = 3
	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		Shard: shard, Term: 2, FollowerName: "child", FollowerHeadEntryId: constant2.InvalidEntryId,
		Observer: true, TargetShard: &childShard, SplitHashRange: &proto.Int32HashRange{MaxHashInclusive: 10},
	})
	assert.NoError(t, err)
	receiveSnapshot(t, rpcClient)
	rpcClient.SendSnapshotStream.Response <- &proto.SnapshotResponse{AckOffset: lc.CommitOffset()}

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// The leader of a split child doesn't start deleting the records outside its
// hash range until a majority of the child holds its entries, and the child
// can't be split meanwhile.
func TestLeaderController_DeferredSplitFilterWaitsForQuorum(t *testing.T) {
	var shard int64 = 2
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)
	left, right := splitKeys(splitTestChildRange)
	require.NoError(t, newSplitChildDB(t, kvFactory, shard, splitChildRecords(5, left, right)).Close())

	lc, err := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(),
		walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	require.NoError(t, err)
	// No follower holds the entries of the leader
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: shard, Term: 2,
		ReplicationFactor: 3, FeaturesSupported: []proto.Feature{proto.Feature_FEATURE_SPLIT_DEFERRED_FILTER}})
	require.NoError(t, err)

	time.Sleep(10 * splitFilterQuorumCheckInterval)
	assert.EqualValues(t, -1, lc.(*leaderController).wal.LastOffset(), "the leader proposed a step of the filter")
	assert.NotNil(t, lc.(*leaderController).db.DeferredSplitFilter())

	var childShard int64 = 3
	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		Shard: shard, Term: 2, FollowerName: "child", FollowerHeadEntryId: constant2.InvalidEntryId,
		Observer: true, TargetShard: &childShard, SplitHashRange: &proto.Int32HashRange{MaxHashInclusive: 10},
	})
	assert.ErrorIs(t, err, errSplitFilterPending)

	// The reads skip the records outside the hash range
	res, err := readAll(context.Background(), lc, &proto.ReadRequest{Shard: &shard, Gets: []*proto.GetRequest{
		{Key: left[0]}, {Key: right[0]},
	}})
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, res[0].Status)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, res[1].Status)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// The secondary indexes of a split child skip the records outside its hash
// range until it deletes them, in every query.
func TestSecondaryIndexes_DeferredSplitFilter(t *testing.T) {
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, kvFactory.Close()) }()
	left, right := splitKeys(splitTestChildRange)
	write := splitChildRecords(5, left, right)
	db := newSplitChildDB(t, kvFactory, 2, write)
	defer func() { assert.NoError(t, db.Close()) }()

	// The kept entries of the index, in its order
	type entry struct{ secondary, primary string }
	var kept []entry
	for _, put := range write.Puts[1:] {
		if isKeyInRange(put.Key, splitTestChildRange) {
			kept = append(kept, entry{put.SecondaryIndexes[0].SecondaryKey, put.Key})
		}
	}
	slices.SortFunc(kept, func(a, b entry) int {
		if c := db.CompareKeys(a.secondary, b.secondary); c != 0 {
			return c
		}
		return db.CompareKeys(a.primary, b.primary)
	})

	idx := "idx"
	it, err := newSecondaryIndexListIterator(&proto.ListRequest{
		SecondaryIndexName: &idx, StartInclusive: "sec-", EndExclusive: "sec-~",
	}, db)
	require.NoError(t, err)
	var listed []string
	for ; it.Valid(); it.Next() {
		listed = append(listed, it.Key())
	}
	require.NoError(t, it.Close())
	var expectedPrimaries []string
	for _, e := range kept {
		expectedPrimaries = append(expectedPrimaries, e.primary)
	}
	assert.Equal(t, expectedPrimaries, listed)

	scan, err := newSecondaryIndexRangeScanIterator(&proto.RangeScanRequest{
		SecondaryIndexName: &idx, StartInclusive: "sec-", EndExclusive: "sec-~",
	}, db)
	require.NoError(t, err)
	var scanned []string
	for ; scan.Valid(); scan.Next() {
		res, err := scan.Value()
		require.NoError(t, err)
		assert.Equal(t, proto.Status_OK, res.Status)
		scanned = append(scanned, res.GetKey())
	}
	require.NoError(t, scan.Close())
	assert.Equal(t, expectedPrimaries, scanned)

	// A get finds the closest kept entry, by the comparison of the request
	for i := -1; i <= 7; i++ {
		probe := fmt.Sprintf("sec-%02d", i)
		for _, comparison := range []proto.KeyComparisonType{
			proto.KeyComparisonType_EQUAL, proto.KeyComparisonType_FLOOR, proto.KeyComparisonType_LOWER,
			proto.KeyComparisonType_CEILING, proto.KeyComparisonType_HIGHER,
		} {
			var expected *entry
			for _, e := range kept {
				cmp := db.CompareKeys(e.secondary, probe)
				match := false
				switch comparison {
				case proto.KeyComparisonType_EQUAL:
					match = cmp == 0 && expected == nil
				case proto.KeyComparisonType_FLOOR:
					match = cmp <= 0 && (expected == nil || e.secondary != expected.secondary)
				case proto.KeyComparisonType_LOWER:
					match = cmp < 0 && (expected == nil || e.secondary != expected.secondary)
				case proto.KeyComparisonType_CEILING:
					match = cmp >= 0 && expected == nil
				case proto.KeyComparisonType_HIGHER:
					match = cmp > 0 && expected == nil
				default:
					require.FailNow(t, "unknown comparison", comparison)
				}
				if match {
					expected = &e
				}
			}
			res, err := secondaryIndexGet(&proto.GetRequest{
				Key: probe, SecondaryIndexName: &idx, ComparisonType: comparison,
			}, db)
			require.NoError(t, err)
			if expected == nil {
				assert.Equal(t, proto.Status_KEY_NOT_FOUND, res.Status, "%v %s", comparison, probe)
				continue
			}
			if assert.Equal(t, proto.Status_OK, res.Status, "%v %s", comparison, probe) {
				assert.Equal(t, expected.secondary, res.GetSecondaryIndexKey(), "%v %s", comparison, probe)
				assert.True(t, isKeyInRange(res.GetKey(), splitTestChildRange), "%v %s: %s", comparison, probe,
					res.GetKey())
			}
		}
	}
}

// When a session ends on a split child that still holds the ephemeral records
// of the other child, the child deletes them, but notifies the deletion of its
// own records only. It notifies all of them for an entry of its parent.
func TestSessionDelete_DeferredSplitFilterNotifications(t *testing.T) {
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, kvFactory.Close()) }()
	left, right := splitKeys(splitTestChildRange)
	write := splitChildRecords(5, left, right)
	// A second session, ended by an entry of the parent
	metadata, _ := (&proto.SessionMetadata{TimeoutMs: 10_000}).MarshalVT()
	var otherSession int64 = 6
	otherLeft, otherRight := splitKeysWith("other-%d", splitTestChildRange, 1)
	write.Puts = append(write.Puts,
		&proto.PutRequest{Key: SessionKey(SessionId(otherSession)), Value: metadata},
		&proto.PutRequest{Key: otherLeft[0], Value: []byte("v"), SessionId: &otherSession},
		&proto.PutRequest{Key: otherRight[0], Value: []byte("v"), SessionId: &otherSession})
	db := newSplitChildDB(t, kvFactory, 2, write)
	defer func() { assert.NoError(t, db.Close()) }()

	_, err = db.ProcessSplitParentWrite(&proto.WriteRequest{Deletes: []*proto.DeleteRequest{
		{Key: SessionKey(SessionId(otherSession))},
	}}, 2, 0, WrapperUpdateOperationCallback)
	require.NoError(t, err)
	_, err = db.ProcessWrite(&proto.WriteRequest{Deletes: []*proto.DeleteRequest{{Key: SessionKey(5)}}}, 3, 0,
		WrapperUpdateOperationCallback)
	require.NoError(t, err)

	notifications := recordedNotifications(t, db, 2, 3)
	var parentDeleted []string
	for _, n := range notifications[2].GetNotifications() {
		assert.Equal(t, proto.NotificationType_KEY_DELETED, n.GetValue().GetType())
		parentDeleted = append(parentDeleted, n.GetKey())
	}
	assert.ElementsMatch(t, []string{otherLeft[0], otherRight[0]}, parentDeleted)

	var expected, deleted []string
	for _, put := range write.Puts {
		if put.SessionId != nil && *put.SessionId == 5 && isKeyInRange(put.Key, splitTestChildRange) {
			expected = append(expected, put.Key)
		}
	}
	for _, n := range notifications[3].GetNotifications() {
		assert.Equal(t, proto.NotificationType_KEY_DELETED, n.GetValue().GetType())
		deleted = append(deleted, n.GetKey())
	}
	assert.ElementsMatch(t, expected, deleted)

	// All the ephemeral records of the session are gone
	keys, complete, err := db.SplitFilterKeys(nil)
	require.NoError(t, err)
	assert.True(t, complete)
	for _, put := range write.Puts {
		if put.SessionId != nil {
			assert.NotContains(t, keys, put.Key)
		}
	}
}
