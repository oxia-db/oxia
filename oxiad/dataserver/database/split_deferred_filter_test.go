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
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

func newDeferredSplitTestDB(t *testing.T, keySorting proto.KeySortingType) DB {
	t.Helper()
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = factory.Close() })
	db, err := NewDB(constant.DefaultNamespace, 1, factory, keySorting, time.Hour, time2.SystemClock)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// deferredSplitRecord is a record of the parent of a split, which the left
// child keeps or not.
type deferredSplitRecord struct {
	key          string
	partitionKey *string
	kept         bool
}

// partitionKeyIn returns a partition key whose hash is in the hash range.
func partitionKeyIn(t *testing.T, hashRange *proto.HashRange) *string {
	t.Helper()
	return pb.String(keyInRange(t, "pk-%d", hashRange))
}

// deferredSplitRecords returns records with the keys built by format, half of
// which the left child keeps. One in three has a partition key on the other
// side of its key: the partition key places it.
func deferredSplitRecords(t *testing.T, format string) []deferredSplitRecord {
	t.Helper()
	left, right := splitRanges()
	var records []deferredSplitRecord
	for i := 0; i < 24; i++ {
		record := deferredSplitRecord{key: fmt.Sprintf(format, i)}
		keyHash := hash.Xxh332(record.key)
		record.kept = isHashInRange(keyHash, left)
		if i%3 == 2 {
			if record.kept {
				record.partitionKey = partitionKeyIn(t, right)
			} else {
				record.partitionKey = partitionKeyIn(t, left)
			}
			record.kept = !record.kept
		}
		records = append(records, record)
	}
	return records
}

func putDeferredSplitRecords(t *testing.T, db DB, records []deferredSplitRecord) {
	t.Helper()
	request := &proto.WriteRequest{}
	for _, record := range records {
		request.Puts = append(request.Puts, &proto.PutRequest{
			Key:          record.key,
			Value:        []byte("value-" + record.key),
			PartitionKey: record.partitionKey,
		})
	}
	_, err := db.ProcessWrite(request, 0, 0, NoOpCallback)
	require.NoError(t, err)
}

// keptKeys returns the keys of the records that the left child keeps, in the
// order of the shard.
func keptKeys(db DB, records []deferredSplitRecord) []string {
	var keys []string
	for _, record := range records {
		if record.kept {
			keys = append(keys, record.key)
		}
	}
	slices.SortFunc(keys, db.CompareKeys)
	return keys
}

func setLeftChildFilter(t *testing.T, db DB) {
	t.Helper()
	left, _ := splitRanges()
	require.NoError(t, db.SetDeferredSplitFilter(&DeferredSplitFilter{
		MinHash: left.Min, MaxHash: left.Max, ParentTerm: 1,
	}))
}

// expectedGet returns the key that a Get of probe with comparison finds among
// the sorted keys, or "" if it finds none.
func expectedGet(db DB, keys []string, probe string, comparison proto.KeyComparisonType) (string, bool) {
	var found string
	ok := false
	for _, key := range keys {
		cmp := db.CompareKeys(key, probe)
		switch comparison {
		case proto.KeyComparisonType_EQUAL:
			if cmp == 0 {
				return key, true
			}
		case proto.KeyComparisonType_FLOOR:
			if cmp <= 0 {
				found, ok = key, true
			}
		case proto.KeyComparisonType_LOWER:
			if cmp < 0 {
				found, ok = key, true
			}
		case proto.KeyComparisonType_CEILING:
			if cmp >= 0 {
				return key, true
			}
		case proto.KeyComparisonType_HIGHER:
			if cmp > 0 {
				return key, true
			}
		default:
			panic(fmt.Sprintf("unknown comparison %v", comparison))
		}
	}
	return found, ok
}

func TestDeferredSplitFilter_Reads(t *testing.T) {
	for _, tc := range []struct {
		keySorting proto.KeySortingType
		format     string
	}{
		{proto.KeySortingType_NATURAL, "key-%03d"},
		{proto.KeySortingType_HIERARCHICAL, "/a/key-%03d"},
		{proto.KeySortingType_HIERARCHICAL, "/a/%d/b"},
	} {
		t.Run(fmt.Sprintf("%v %s", tc.keySorting, tc.format), func(t *testing.T) {
			db := newDeferredSplitTestDB(t, tc.keySorting)
			records := deferredSplitRecords(t, tc.format)
			putDeferredSplitRecords(t, db, records)
			setLeftChildFilter(t, db)
			kept := keptKeys(db, records)
			require.NotEmpty(t, kept)
			require.Less(t, len(kept), len(records))

			probes := []string{"", "/", "~", "/a/", "/a/~"}
			for _, record := range records {
				probes = append(probes, record.key, record.key+"0", record.key[:len(record.key)-1])
			}
			for _, probe := range probes {
				for _, comparison := range []proto.KeyComparisonType{
					proto.KeyComparisonType_EQUAL, proto.KeyComparisonType_FLOOR, proto.KeyComparisonType_LOWER,
					proto.KeyComparisonType_CEILING, proto.KeyComparisonType_HIGHER,
				} {
					res, err := db.Get(&proto.GetRequest{Key: probe, IncludeValue: true, ComparisonType: comparison})
					require.NoError(t, err)
					expected, found := expectedGet(db, kept, probe, comparison)
					if !found {
						assert.Equal(t, proto.Status_KEY_NOT_FOUND, res.Status, "%v %q", comparison, probe)
						continue
					}
					if assert.Equal(t, proto.Status_OK, res.Status, "%v %q", comparison, probe) {
						assert.Equal(t, []byte("value-"+expected), res.Value, "%v %q", comparison, probe)
						if comparison != proto.KeyComparisonType_EQUAL {
							assert.Equal(t, expected, res.GetKey(), "%v %q", comparison, probe)
						}
					}
				}
			}

			it, err := db.List(&proto.ListRequest{})
			require.NoError(t, err)
			var listed []string
			for ; it.Valid(); it.Next() {
				listed = append(listed, it.Key())
			}
			require.NoError(t, it.Close())
			assert.Equal(t, kept, listed)

			scan, err := db.RangeScan(&proto.RangeScanRequest{})
			require.NoError(t, err)
			var scanned []string
			for ; scan.Valid(); scan.Next() {
				res, err := scan.Value()
				require.NoError(t, err)
				assert.Equal(t, []byte("value-"+res.GetKey()), res.Value)
				scanned = append(scanned, res.GetKey())
			}
			require.NoError(t, scan.Close())
			assert.Equal(t, kept, scanned)
		})
	}
}

func TestDeferredSplitFilter_ListSeek(t *testing.T) {
	db := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, db, records)
	setLeftChildFilter(t, db)
	kept := keptKeys(db, records)

	it, err := db.List(&proto.ListRequest{})
	require.NoError(t, err)
	defer func() { assert.NoError(t, it.Close()) }()

	for _, record := range records {
		ceiling, found := expectedGet(db, kept, record.key, proto.KeyComparisonType_CEILING)
		assert.Equal(t, found, it.SeekGE(record.key))
		if found {
			assert.Equal(t, ceiling, it.Key())
		}
		lower, found := expectedGet(db, kept, record.key, proto.KeyComparisonType_LOWER)
		assert.Equal(t, found, it.SeekLT(record.key))
		if found {
			assert.Equal(t, lower, it.Key())
			// Back to the kept record before it
			before, found := expectedGet(db, kept, lower, proto.KeyComparisonType_LOWER)
			assert.Equal(t, found, it.Prev())
			if found {
				assert.Equal(t, before, it.Key())
			}
		}
	}
}

func TestDeferredSplitFilter_InternalKeys(t *testing.T) {
	db := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, db, records)
	setLeftChildFilter(t, db)

	it, err := db.List(&proto.ListRequest{IncludeInternalKeys: true})
	require.NoError(t, err)
	var listed []string
	for ; it.Valid(); it.Next() {
		listed = append(listed, it.Key())
	}
	require.NoError(t, it.Close())
	assert.Contains(t, listed, deferredSplitFilterKey)
	assert.Contains(t, listed, commitOffsetKey)
	for _, record := range records {
		assert.Equal(t, record.kept, slices.Contains(listed, record.key), record.key)
	}

	res, err := db.Get(&proto.GetRequest{Key: commitLastVersionIdKey, IncludeValue: true})
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, res.Status)
}

func TestDeferredSplitFilter_Recovery(t *testing.T) {
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, factory.Close()) }()

	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
	require.NoError(t, err)
	assert.Nil(t, db.DeferredSplitFilter())

	// The snapshot of a parent that is itself the child of an earlier split
	require.NoError(t, db.SetSplitFilter(&SplitFilter{MinHash: 0, MaxHash: 100, ParentTerm: 2}))
	filter := &DeferredSplitFilter{MinHash: 10, MaxHash: 20, ParentTerm: 5}
	require.NoError(t, db.SetDeferredSplitFilter(filter))
	assert.Equal(t, filter, db.DeferredSplitFilter())
	assert.Nil(t, db.SplitFilter())
	require.NoError(t, db.Close())

	db, err = NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	assert.Equal(t, filter, db.DeferredSplitFilter())
	assert.Nil(t, db.SplitFilter())
}

// TestDeferredSplitFilter_ParentWrites applies the same writes to a parent, and
// to a child that holds all its records: the child applies them as the parent
// does, with the records of the other child, and assigns the same version ids.
func TestDeferredSplitFilter_ParentWrites(t *testing.T) {
	parent := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	child := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, parent, records)
	putDeferredSplitRecords(t, child, records)
	setLeftChildFilter(t, child)

	// For each record: a conditional update on the version it has, a
	// conditional create that fails, and every other record deleted
	var requests []*proto.WriteRequest
	for i, record := range records {
		requests = append(requests, &proto.WriteRequest{Puts: []*proto.PutRequest{
			{Key: record.key, Value: []byte("updated"), PartitionKey: record.partitionKey,
				ExpectedVersionId: pb.Int64(int64(i))},
			{Key: record.key, Value: []byte("created"), PartitionKey: record.partitionKey, ExpectedVersionId: pb.Int64(-1)},
		}})
		if i%2 == 0 {
			requests = append(requests, &proto.WriteRequest{Deletes: []*proto.DeleteRequest{
				{Key: record.key, PartitionKey: record.partitionKey},
			}})
		}
	}
	requests = append(requests, &proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "last", Value: []byte("last")}}})

	for i, request := range requests {
		offset := int64(i + 1)
		parentRes, err := parent.ProcessWrite(request.CloneVT(), offset, 0, NoOpCallback)
		require.NoError(t, err)
		childRes, err := child.ProcessSplitParentWrite(request.CloneVT(), offset, 0, NoOpCallback)
		require.NoError(t, err)
		assert.True(t, parentRes.EqualVT(childRes), "write %d: parent %v, child %v", i, parentRes, childRes)
	}

	for _, record := range records {
		if !record.kept {
			continue
		}
		parentRes, err := parent.Get(&proto.GetRequest{Key: record.key, IncludeValue: true})
		require.NoError(t, err)
		childRes, err := child.Get(&proto.GetRequest{Key: record.key, IncludeValue: true})
		require.NoError(t, err)
		assert.True(t, parentRes.EqualVT(childRes), "%s: parent %v, child %v", record.key, parentRes, childRes)
	}
}

// TestDeferredSplitFilter_ChildWrites checks that the writes of the child's own
// terms don't see the records it doesn't keep: a record written at the key of
// one is created, and a delete doesn't find it.
func TestDeferredSplitFilter_ChildWrites(t *testing.T) {
	left, _ := splitRanges()
	db := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, db, records)
	setLeftChildFilter(t, db)

	var dropped []deferredSplitRecord
	for _, record := range records {
		if !record.kept {
			dropped = append(dropped, record)
		}
	}
	require.GreaterOrEqual(t, len(dropped), 2)
	created, deleted := dropped[0], dropped[1]

	// A record of the child at the key of a record it doesn't keep
	res, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:               created.key,
		Value:             []byte("child"),
		PartitionKey:      partitionKeyIn(t, left),
		ExpectedVersionId: pb.Int64(-1),
	}}}, 100, 0, NoOpCallback)
	require.NoError(t, err)
	require.Equal(t, proto.Status_OK, res.Puts[0].Status)
	assert.Equal(t, int64(0), res.Puts[0].Version.ModificationsCount)

	get, err := db.Get(&proto.GetRequest{Key: created.key, IncludeValue: true})
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, get.Status)
	assert.Equal(t, []byte("child"), get.Value)
	assert.Equal(t, res.Puts[0].Version.VersionId, get.Version.VersionId)

	res, err = db.ProcessWrite(&proto.WriteRequest{Deletes: []*proto.DeleteRequest{
		{Key: deleted.key, PartitionKey: deleted.partitionKey},
	}}, 101, 0, NoOpCallback)
	require.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, res.Deletes[0].Status)

	// Both records of the parent are gone: the filter has none of them left
	// to delete
	keys, complete, err := db.SplitFilterKeys(nil)
	require.NoError(t, err)
	assert.True(t, complete)
	assert.NotContains(t, keys, created.key)
	assert.NotContains(t, keys, deleted.key)
	assert.Len(t, keys, len(dropped)-2)
}

func TestDeferredSplitFilter_Steps(t *testing.T) {
	maxKeys := splitFilterMaxKeys
	splitFilterMaxKeys = 3
	defer func() { splitFilterMaxKeys = maxKeys }()

	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, factory.Close()) }()
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
	require.NoError(t, err)

	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, db, records)
	setLeftChildFilter(t, db)

	var expected []string
	for _, record := range records {
		if !record.kept {
			expected = append(expected, record.key)
		}
	}
	slices.SortFunc(expected, db.CompareKeys)
	require.Greater(t, len(expected), 2*splitFilterMaxKeys)

	var steps [][]string
	var after *string
	offset := int64(100)
	for {
		keys, complete, err := db.SplitFilterKeys(after)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(keys), splitFilterMaxKeys)
		steps = append(steps, keys)
		_, err = db.ProcessControlRequest(&proto.ControlRequest{Value: &proto.ControlRequest_SplitFilter{
			SplitFilter: &proto.SplitFilterRequest{Keys: keys, Complete: complete},
		}}, offset, 0, NoOpCallback)
		require.NoError(t, err)
		offset++
		if complete {
			break
		}
		assert.NotNil(t, db.DeferredSplitFilter())
		after = &keys[len(keys)-1]
	}
	assert.Equal(t, expected, slices.Concat(steps...))
	assert.Nil(t, db.DeferredSplitFilter())

	commitOffset, err := db.ReadCommitOffset()
	require.NoError(t, err)
	assert.Equal(t, offset-1, commitOffset)

	// A step applied once the filter completed has no effect
	_, err = db.ProcessControlRequest(&proto.ControlRequest{Value: &proto.ControlRequest_SplitFilter{
		SplitFilter: &proto.SplitFilterRequest{Keys: []string{records[0].key}, Complete: true},
	}}, offset, 0, NoOpCallback)
	require.NoError(t, err)

	require.NoError(t, db.Close())
	db, err = NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	assert.Nil(t, db.DeferredSplitFilter())
	for _, record := range records {
		_, _, closer, err := db.RawKV().Get(record.key, kvstore.ComparisonEqual, kvstore.NoInternalKeys)
		if record.kept {
			if assert.NoError(t, err, record.key) {
				assert.NoError(t, closer.Close())
			}
		} else {
			assert.ErrorIs(t, err, kvstore.ErrKeyNotFound, record.key)
		}
	}
}

// TestDeferredSplitFilter_StepSkipsKeptRecords checks that a step doesn't delete
// a record that a write of the child replaced since the leader read its key.
func TestDeferredSplitFilter_StepSkipsKeptRecords(t *testing.T) {
	left, _ := splitRanges()
	db := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, db, records)
	setLeftChildFilter(t, db)

	keys, complete, err := db.SplitFilterKeys(nil)
	require.NoError(t, err)
	require.True(t, complete)

	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key: keys[0], Value: []byte("child"), PartitionKey: partitionKeyIn(t, left),
	}}}, 100, 0, NoOpCallback)
	require.NoError(t, err)

	_, err = db.ProcessControlRequest(&proto.ControlRequest{Value: &proto.ControlRequest_SplitFilter{
		SplitFilter: &proto.SplitFilterRequest{Keys: keys, Complete: true},
	}}, 101, 0, NoOpCallback)
	require.NoError(t, err)
	assert.Nil(t, db.DeferredSplitFilter())

	res, err := db.Get(&proto.GetRequest{Key: keys[0], IncludeValue: true})
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, res.Status)
	assert.Equal(t, []byte("child"), res.Value)
}
