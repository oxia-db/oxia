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

	// Both records of the parent are gone, and the others are still there
	held := heldKeys(t, db)
	assert.Contains(t, held, created.key)
	assert.NotContains(t, held, deleted.key)
	assert.Len(t, held, len(records)-1)
}

// heldKeys returns the keys of the records that the database holds, whether it
// keeps them or not, in the order of the shard.
func heldKeys(t *testing.T, db DB) []string {
	t.Helper()
	it, err := db.RawKV().KeyRangeScan("", "", kvstore.NoInternalKeys)
	require.NoError(t, err)
	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, it.Key())
	}
	require.NoError(t, it.Close())
	return keys
}

// applySplitFilterStep applies a step of the deferred split filter that goes
// through up to maxRecords records.
func applySplitFilterStep(t *testing.T, db DB, offset int64, maxRecords uint32) {
	t.Helper()
	_, err := db.ProcessControlRequest(&proto.ControlRequest{Value: &proto.ControlRequest_SplitFilter{
		SplitFilter: &proto.SplitFilterRequest{MaxRecords: maxRecords},
	}}, offset, 0, NoOpCallback)
	require.NoError(t, err)
}

// TestDeferredSplitFilter_Steps checks that each step of the deferred split
// filter goes through the next records, from where the previous one stopped,
// after a restart too, and deletes the ones the child doesn't keep, until a
// step reaches the last record.
func TestDeferredSplitFilter_Steps(t *testing.T) {
	const stepRecords = 5
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, factory.Close()) }()
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
	require.NoError(t, err)

	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, db, records)
	setLeftChildFilter(t, db)
	all := make([]string, 0, len(records))
	for _, record := range records {
		all = append(all, record.key)
	}
	slices.SortFunc(all, db.CompareKeys)
	kept := keptKeys(db, records)
	assert.True(t, db.Stats().GetSplitFilterPending())

	offset := int64(100)
	for step := 1; db.DeferredSplitFilter() != nil; step++ {
		require.LessOrEqual(t, step, (len(all)+stepRecords-1)/stepRecords, "too many steps")
		applySplitFilterStep(t, db, offset, stepRecords)
		offset++

		// The step deleted the records that the child doesn't keep among the
		// ones it went through, and only them
		var expected []string
		for i, key := range all {
			if i >= step*stepRecords || slices.Contains(kept, key) {
				expected = append(expected, key)
			}
		}
		assert.Equal(t, expected, heldKeys(t, db), "step %d", step)

		if step == 2 {
			filter := db.DeferredSplitFilter()
			require.NotNil(t, filter)
			assert.NotEmpty(t, filter.Cursor)
			require.NoError(t, db.Close())
			db, err = NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour,
				time2.SystemClock)
			require.NoError(t, err)
			assert.Equal(t, filter, db.DeferredSplitFilter())
		}
	}
	assert.Equal(t, kept, heldKeys(t, db))
	assert.False(t, db.Stats().GetSplitFilterPending())

	commitOffset, err := db.ReadCommitOffset()
	require.NoError(t, err)
	assert.Equal(t, offset-1, commitOffset)

	// A step applied once the filter ended has no effect
	applySplitFilterStep(t, db, offset, stepRecords)
	assert.Nil(t, db.DeferredSplitFilter())
	assert.Equal(t, kept, heldKeys(t, db))

	require.NoError(t, db.Close())
	db, err = NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()
	assert.Nil(t, db.DeferredSplitFilter())
	assert.Equal(t, kept, heldKeys(t, db))
}

// TestDeferredSplitFilter_StepBytes checks that a step stops after the record
// whose entry takes the entries it went through to its bytes.
func TestDeferredSplitFilter_StepBytes(t *testing.T) {
	db := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, db, records)
	setLeftChildFilter(t, db)

	step := func(offset int64, maxBytes uint32) {
		t.Helper()
		_, err := db.ProcessControlRequest(&proto.ControlRequest{Value: &proto.ControlRequest_SplitFilter{
			SplitFilter: &proto.SplitFilterRequest{MaxRecords: uint32(len(records)), MaxBytes: maxBytes},
		}}, offset, 0, NoOpCallback)
		require.NoError(t, err)
	}

	// Each step goes through a single record
	step(100, 1)
	step(101, 1)
	held := heldKeys(t, db)
	all := make([]string, 0, len(records))
	for _, record := range records {
		all = append(all, record.key)
	}
	slices.SortFunc(all, db.CompareKeys)
	kept := keptKeys(db, records)
	for _, key := range all[2:] {
		assert.Contains(t, held, key)
	}
	for _, key := range all[:2] {
		assert.Equal(t, slices.Contains(kept, key), slices.Contains(held, key), key)
	}
	require.NotNil(t, db.DeferredSplitFilter())

	// Without a size bound, the step goes through all the others
	step(102, 0)
	assert.Nil(t, db.DeferredSplitFilter())
	assert.Equal(t, kept, heldKeys(t, db))
}

// TestDeferredSplitFilter_StepsHierarchicalKeys checks that a step goes on from
// the key where the previous one stopped as the store holds it: with the
// hierarchical key sorting, a key with a raw 0xff byte doesn't keep its
// position once decoded.
func TestDeferredSplitFilter_StepsHierarchicalKeys(t *testing.T) {
	left, right := splitRanges()
	db := newDeferredSplitTestDB(t, proto.KeySortingType_HIERARCHICAL)
	// The first key, of the first level, decodes to "a//b", of the second one
	_, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{
		{Key: "a\xff/b", Value: []byte("kept"), PartitionKey: partitionKeyIn(t, left)},
		{Key: "b/x", Value: []byte("other"), PartitionKey: partitionKeyIn(t, right)},
		{Key: "c/y", Value: []byte("other"), PartitionKey: partitionKeyIn(t, right)},
	}}, 0, 0, NoOpCallback)
	require.NoError(t, err)
	setLeftChildFilter(t, db)

	for offset := int64(100); db.DeferredSplitFilter() != nil; offset++ {
		require.Less(t, offset, int64(103), "too many steps")
		applySplitFilterStep(t, db, offset, 1)
	}
	res, err := db.Get(&proto.GetRequest{Key: "a\xff/b", IncludeValue: true})
	require.NoError(t, err)
	assert.Equal(t, []byte("kept"), res.Value)
	assert.Len(t, heldKeys(t, db), 1)
}

// TestDeferredSplitFilter_StepKeepsChildRecords checks that a step doesn't
// delete the record that a write of the child put at the key of a record the
// child didn't keep.
func TestDeferredSplitFilter_StepKeepsChildRecords(t *testing.T) {
	left, _ := splitRanges()
	db := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, db, records)
	setLeftChildFilter(t, db)

	var replaced string
	for _, record := range records {
		if !record.kept {
			replaced = record.key
			break
		}
	}
	_, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key: replaced, Value: []byte("child"), PartitionKey: partitionKeyIn(t, left),
	}}}, 100, 0, NoOpCallback)
	require.NoError(t, err)

	applySplitFilterStep(t, db, 101, uint32(len(records)))
	assert.Nil(t, db.DeferredSplitFilter())

	res, err := db.Get(&proto.GetRequest{Key: replaced, IncludeValue: true})
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, res.Status)
	assert.Equal(t, []byte("child"), res.Value)
}

// TestDeferredSplitFilter_UnreadableRecord checks that a record whose entry
// can't be read is placed by its key, and that a step drops it if the child
// doesn't keep it.
func TestDeferredSplitFilter_UnreadableRecord(t *testing.T) {
	left, right := splitRanges()
	db := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	keptKey, otherKey := keyInRange(t, "kept-%d", left), keyInRange(t, "other-%d", right)
	batch := db.RawKV().NewWriteBatch()
	for _, key := range []string{keptKey, otherKey} {
		// A truncated tag
		require.NoError(t, batch.Put(key, []byte{0xff, 0xff}))
	}
	require.NoError(t, batch.Commit())
	require.NoError(t, batch.Close())
	setLeftChildFilter(t, db)

	it, err := db.List(&proto.ListRequest{})
	require.NoError(t, err)
	var listed []string
	for ; it.Valid(); it.Next() {
		listed = append(listed, it.Key())
	}
	require.NoError(t, it.Close())
	assert.Equal(t, []string{keptKey}, listed)

	applySplitFilterStep(t, db, 100, 10)
	assert.Nil(t, db.DeferredSplitFilter())
	assert.Equal(t, []string{keptKey}, heldKeys(t, db))
}

// hookedRangeScanKV runs a hook once the next RangeScan created its iterator.
type hookedRangeScanKV struct {
	kvstore.KV
	afterRangeScan func()
}

func (kv *hookedRangeScanKV) RangeScan(lowerBound, upperBound string, opts kvstore.IteratorOpts) (
	kvstore.KeyValueIterator, error) {
	it, err := kv.KV.RangeScan(lowerBound, upperBound, opts)
	if hook := kv.afterRangeScan; hook != nil {
		kv.afterRangeScan = nil
		hook()
	}
	return it, err
}

// TestDeferredSplitFilter_RangeScanRacingLastStep checks that a range scan that
// starts before the last step of the deferred split filter skips the records
// that the step deletes, which its iterator still holds.
func TestDeferredSplitFilter_RangeScanRacingLastStep(t *testing.T) {
	child := newDeferredSplitTestDB(t, proto.KeySortingType_NATURAL)
	records := deferredSplitRecords(t, "key-%03d")
	putDeferredSplitRecords(t, child, records)
	setLeftChildFilter(t, child)
	kept := keptKeys(child, records)

	d := child.(*db)
	d.kv = &hookedRangeScanKV{KV: d.kv, afterRangeScan: func() {
		applySplitFilterStep(t, child, 100, uint32(len(records)))
	}}

	it, err := child.RangeScan(&proto.RangeScanRequest{})
	require.NoError(t, err)
	require.Nil(t, child.DeferredSplitFilter(), "the last step didn't run")
	var scanned []string
	for ; it.Valid(); it.Next() {
		res, err := it.Value()
		require.NoError(t, err)
		scanned = append(scanned, res.GetKey())
	}
	require.NoError(t, it.Close())
	assert.Equal(t, kept, scanned)
}
