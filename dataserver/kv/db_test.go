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

package kv

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/compare"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/dataserver/wal"
	"github.com/oxia-db/oxia/proto"
)

func TestDBSimple(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{ // Should succeed: no version check
				Key:               "a",
				Value:             []byte("0"),
				ExpectedVersionId: nil,
			},
			{ // Should succeed: asserting that the key does not exist
				Key:               "b",
				Value:             []byte("1"),
				ExpectedVersionId: pb.Int64(-1),
			},
			{ // Should fail: the version would mean that the key exists
				Key:               "c",
				Value:             []byte("2"),
				ExpectedVersionId: pb.Int64(0),
			},
			{ // Should fail: the version would mean that the key exists
				Key:               "d",
				Value:             []byte("3"),
				ExpectedVersionId: pb.Int64(0),
			},
			{ // Should succeed: asserting that the key does not exist
				Key:               "c",
				Value:             []byte("1"),
				ExpectedVersionId: pb.Int64(-1),
			},
		},
		Deletes: []*proto.DeleteRequest{
			{ // Should fail, non-existing key
				Key:               "non-existing",
				ExpectedVersionId: pb.Int64(-1),
			},
			{ // Should fail, version mismatch
				Key:               "c",
				ExpectedVersionId: pb.Int64(1),
			},
			{ // Should succeed, the key was inserted in the batch
				Key:               "c",
				ExpectedVersionId: pb.Int64(2),
			},
			{ // Should fail: the key was already just deleted
				Key:               "c",
				ExpectedVersionId: nil,
			},
		},
	}

	res, err := db.ProcessWrite(req, 0, 0, NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(res.Puts))
	r0 := res.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 0, r0.Version.VersionId)
	assert.EqualValues(t, 0, r0.Version.ModificationsCount)

	r1 := res.Puts[1]
	assert.Equal(t, proto.Status_OK, r1.Status)
	assert.EqualValues(t, 1, r1.Version.VersionId)
	assert.EqualValues(t, 0, r1.Version.ModificationsCount)

	r2 := res.Puts[2]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r2.Status)
	assert.Nil(t, r2.Version)

	r3 := res.Puts[3]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r3.Status)
	assert.Nil(t, r3.Version)

	r4 := res.Puts[4]
	assert.Equal(t, proto.Status_OK, r4.Status)
	assert.EqualValues(t, 2, r4.Version.VersionId)
	assert.EqualValues(t, 0, r4.Version.ModificationsCount)

	assert.Equal(t, 4, len(res.Deletes))
	r5 := res.Deletes[0]
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, r5.Status)

	r6 := res.Deletes[1]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r6.Status)

	r7 := res.Deletes[2]
	assert.Equal(t, proto.Status_OK, r7.Status)

	r8 := res.Deletes[3]
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, r8.Status)

	// TODO: Add the request call and the verification
	// Second batch
	// req = &proto.WriteRequest{
	//	Puts: []*proto.PutRequest{
	//		{ // Should succeed: no version check
	//			Key:             "a",
	//			Value:         []byte("00"),
	//			ExpectedVersion: nil,
	//		},
	//		{ // Should succeed: The version currently in the store is 0, the update just above will change
	//			// the version, though we do the verification before the batch is applied
	//			Key:             "a",
	//			Value:         []byte("111"),
	//			ExpectedVersion: pb.Int64(0),
	//		},
	//		{ // Should fail: the key already exists
	//			Key:             "b",
	//			Value:         []byte("2"),
	//			ExpectedVersion: pb.Int64(-1),
	//		},
	//		{ // Should succeed: the version is correct
	//			Key:             "b",
	//			Value:         []byte("2"),
	//			ExpectedVersion: pb.Int64(0),
	//		},
	//	},
	//	Deletes: []*proto.DeleteRequest{
	//		{ // Should fail: the key was not inserted before the batch
	//			Key:             "a",
	//			ExpectedVersion: nil,
	//		},
	//		{ // Should fail: the key was not inserted before the batch
	//			Key:             "c",
	//			ExpectedVersion: pb.Int64(-1),
	//		},
	//		{ // Should fail: the key was not inserted before the batch
	//			Key:             "c",
	//			ExpectedVersion: pb.Int64(0),
	//		},
	//	},
	// }

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDBSameKeyMutations(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{ // Should succeed: no version check
				Key:               "k1",
				Value:             []byte("v0"),
				ExpectedVersionId: nil,
			},
		},
	}

	t0 := now()
	writeRes, err := db.ProcessWrite(writeReq, 0, t0, NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(writeRes.Puts))
	r0 := writeRes.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 0, r0.Version.VersionId)
	assert.Equal(t, t0, r0.Version.CreatedTimestamp)
	assert.Equal(t, t0, r0.Version.ModifiedTimestamp)

	writeReq = &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{ // Should succeed: version is correct
				Key:               "k1",
				Value:             []byte("v1"),
				ExpectedVersionId: pb.Int64(0),
			},
			{ // Should fail: version has now changed to 1
				Key:               "k1",
				Value:             []byte("v2"),
				ExpectedVersionId: pb.Int64(0),
			},
		},
		Deletes: []*proto.DeleteRequest{
			{ // Should fail: the key was not inserted before the batch
				Key:               "k1",
				ExpectedVersionId: pb.Int64(0),
			},
		},
	}

	t1 := now()
	writeRes, err = db.ProcessWrite(writeReq, 1, t1, NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(writeRes.Puts))
	r0 = writeRes.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, writeRes.Puts[0].Version.VersionId, r0.Version.VersionId)
	assert.EqualValues(t, 1, r0.Version.ModificationsCount)
	assert.Equal(t, t0, r0.Version.CreatedTimestamp)
	assert.Equal(t, t1, r0.Version.ModifiedTimestamp)

	r1 := writeRes.Puts[1]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r1.Status)
	assert.Nil(t, r1.Version)

	r2 := writeRes.Deletes[0]
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, r2.Status)

	getRes, err := db.Get(&proto.GetRequest{
		// Should return version v1
		Key:          "k1",
		IncludeValue: true,
	})
	assert.NoError(t, err)

	assert.Equal(t, proto.Status_OK, getRes.Status)
	assert.EqualValues(t, writeRes.Puts[0].Version.VersionId, getRes.Version.VersionId)
	assert.Equal(t, "v1", string(getRes.Value))
	assert.Equal(t, t0, getRes.Version.CreatedTimestamp)
	assert.Equal(t, t1, getRes.Version.ModifiedTimestamp)

	getRes, err = db.Get(&proto.GetRequest{
		// Should return version v1, with no value
		Key:          "k1",
		IncludeValue: false,
	})
	assert.NoError(t, err)

	assert.Equal(t, proto.Status_OK, getRes.Status)
	assert.EqualValues(t, writeRes.Puts[0].Version.VersionId, getRes.Version.VersionId)
	assert.Nil(t, getRes.Value)
	assert.Equal(t, t0, getRes.Version.CreatedTimestamp)
	assert.Equal(t, t1, getRes.Version.ModifiedTimestamp)

	getRes, err = db.Get(&proto.GetRequest{
		// Should fail since the key is not there
		Key:          "non-existing",
		IncludeValue: true,
	})
	assert.NoError(t, err)

	assert.Equal(t, proto.Status_KEY_NOT_FOUND, getRes.Status)
	assert.Nil(t, getRes.Version)
	assert.Nil(t, getRes.Value)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDBList(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}, {
			Key:   "b",
			Value: []byte("b"),
		}, {
			Key:   "c",
			Value: []byte("c"),
		}, {
			Key:   "d",
			Value: []byte("d"),
		}, {
			Key:   "e",
			Value: []byte("e"),
		}},
	}

	writeRes, err := db.ProcessWrite(writeReq, wal.InvalidOffset, now(), NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(writeRes.Puts))

	listReq1 := &proto.ListRequest{
		StartInclusive: "a",
		EndExclusive:   "c",
	}
	listReq2 := &proto.ListRequest{
		StartInclusive: "a",
		EndExclusive:   "d",
	}
	listReq3 := &proto.ListRequest{
		StartInclusive: "xyz",
		EndExclusive:   "zzz",
	}

	// ["a", "c")
	keys1 := keyIteratorToSlice(db.List(listReq1))
	assert.Len(t, keys1, 2)
	assert.Equal(t, "a", keys1[0])
	assert.Equal(t, "b", keys1[1])

	// ["a", "d")
	keys2 := keyIteratorToSlice(db.List(listReq2))
	assert.Len(t, keys2, 3)
	assert.Equal(t, "a", keys2[0])
	assert.Equal(t, "b", keys2[1])
	assert.Equal(t, "c", keys2[2])

	// ["xyz", "zzz")
	keys3 := keyIteratorToSlice(db.List(listReq3))
	assert.Len(t, keys3, 0)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func keyIteratorToSlice(it KeyIterator, err error) []string {
	assert.NoError(nil, err)
	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, it.Key())
	}
	_ = it.Close()
	return keys
}

func TestDBDeleteRange(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}, {
			Key:   "b",
			Value: []byte("b"),
		}, {
			Key:   "c",
			Value: []byte("c"),
		}, {
			Key:   "d",
			Value: []byte("d"),
		}, {
			Key:   "e",
			Value: []byte("e"),
		}},
	}

	_, err = db.ProcessWrite(writeReq, wal.InvalidOffset, 0, NoOpCallback)
	assert.NoError(t, err)

	writeReq = &proto.WriteRequest{
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "b",
			EndExclusive:   "c",
		}, {
			StartInclusive: "b",
			EndExclusive:   "e",
		}},
	}

	writeRes, err := db.ProcessWrite(writeReq, wal.InvalidOffset, 0, NoOpCallback)
	assert.NoError(t, err)

	keys := make([]string, 0)
	listIt, err := db.List(&proto.ListRequest{
		StartInclusive: "a",
		EndExclusive:   "z",
	})

	assert.NoError(t, err)
	for ; listIt.Valid(); listIt.Next() {
		keys = append(keys, listIt.Key())
	}
	err = listIt.Close()
	assert.NoError(t, err)

	assert.Equal(t, 2, len(writeRes.DeleteRanges))
	assert.Equal(t, proto.Status_OK, writeRes.DeleteRanges[0].Status)
	assert.Equal(t, proto.Status_OK, writeRes.DeleteRanges[1].Status)

	// ["a", "z")
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, "a", keys[0])
	assert.Equal(t, "e", keys[1])

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_ReadCommitOffset(t *testing.T) {
	offset := int64(13)

	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	commitOffset, err := db.ReadCommitOffset()
	assert.NoError(t, err)
	assert.Equal(t, wal.InvalidOffset, commitOffset)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}},
	}
	_, err = db.ProcessWrite(writeReq, offset, 0, NoOpCallback)
	assert.NoError(t, err)

	commitOffset, err = db.ReadCommitOffset()
	assert.NoError(t, err)
	assert.Equal(t, offset, commitOffset)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDb_UpdateTerm(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	term, options, err := db.ReadTerm()
	assert.NoError(t, err)
	assert.Equal(t, wal.InvalidOffset, term)
	assert.Equal(t, TermOptions{}, options)

	err = db.UpdateTerm(1, TermOptions{NotificationsEnabled: true})
	assert.NoError(t, err)

	term, options, err = db.ReadTerm()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, term)
	assert.Equal(t, TermOptions{NotificationsEnabled: true}, options)

	assert.NoError(t, db.Close())

	// Reopen and verify the term is maintained
	db, err = NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	term, _, err = db.ReadTerm()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, term)

	assert.NoError(t, factory.Close())
}

func TestDB_Delete(t *testing.T) {
	offset := int64(13)

	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}},
	}
	_, err = db.ProcessWrite(writeReq, offset, 0, NoOpCallback)
	assert.NoError(t, err)

	assert.NoError(t, db.Delete())

	// Reopen and verify the db is empty
	db, err = NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	getRes, err := db.Get(&proto.GetRequest{
		Key:          "a",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, getRes.Status)

	assert.NoError(t, factory.Close())
}

func TestDB_FloorCeiling(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderHierarchical, 0, time.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("0"),
			// }, { // Intentionally skipped
			//	Key:   "b",
			//	Value: []byte("1"),
		}, {
			Key:   "c",
			Value: []byte("2"),
		}, {
			Key:   "d",
			Value: []byte("3"),
		}, {
			Key:   "e",
			Value: []byte("4"),
		}},
	}
	_, err = db.ProcessWrite(writeReq, 0, 0, NoOpCallback)
	assert.NoError(t, err)

	// ---------------------------------------------------------------

	getRes, err := db.Get(&proto.GetRequest{Key: "a", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, "", getRes.GetKey())
	assert.Equal(t, "0", string(getRes.GetValue()))

	getRes, err = db.Get(&proto.GetRequest{Key: "a", IncludeValue: true, ComparisonType: proto.KeyComparisonType_EQUAL})
	assert.NoError(t, err)
	assert.Equal(t, "", getRes.GetKey())
	assert.Equal(t, "0", string(getRes.GetValue()))

	getRes, err = db.Get(&proto.GetRequest{Key: "a", ComparisonType: proto.KeyComparisonType_FLOOR})
	assert.NoError(t, err)
	assert.Equal(t, "a", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "a", ComparisonType: proto.KeyComparisonType_CEILING})
	assert.NoError(t, err)
	assert.Equal(t, "a", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "a", ComparisonType: proto.KeyComparisonType_LOWER})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, getRes.Status)

	getRes, err = db.Get(&proto.GetRequest{Key: "a", ComparisonType: proto.KeyComparisonType_HIGHER})
	assert.NoError(t, err)
	assert.Equal(t, "c", getRes.GetKey())

	// ---------------------------------------------------------------

	getRes, err = db.Get(&proto.GetRequest{Key: "b"})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, getRes.Status)

	getRes, err = db.Get(&proto.GetRequest{Key: "b", ComparisonType: proto.KeyComparisonType_EQUAL})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, getRes.Status)

	getRes, err = db.Get(&proto.GetRequest{Key: "b", ComparisonType: proto.KeyComparisonType_FLOOR})
	assert.NoError(t, err)
	assert.Equal(t, "a", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "b", ComparisonType: proto.KeyComparisonType_CEILING})
	assert.NoError(t, err)
	assert.Equal(t, "c", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "b", ComparisonType: proto.KeyComparisonType_LOWER})
	assert.NoError(t, err)
	assert.Equal(t, "a", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "b", ComparisonType: proto.KeyComparisonType_HIGHER})
	assert.NoError(t, err)
	assert.Equal(t, "c", getRes.GetKey())

	// ---------------------------------------------------------------

	getRes, err = db.Get(&proto.GetRequest{Key: "c", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, "", getRes.GetKey())
	assert.Equal(t, "2", string(getRes.GetValue()))

	getRes, err = db.Get(&proto.GetRequest{Key: "c", IncludeValue: true, ComparisonType: proto.KeyComparisonType_EQUAL})
	assert.NoError(t, err)
	assert.Equal(t, "", getRes.GetKey())
	assert.Equal(t, "2", string(getRes.GetValue()))

	getRes, err = db.Get(&proto.GetRequest{Key: "c", ComparisonType: proto.KeyComparisonType_FLOOR})
	assert.NoError(t, err)
	assert.Equal(t, "c", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "c", ComparisonType: proto.KeyComparisonType_CEILING})
	assert.NoError(t, err)
	assert.Equal(t, "c", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "c", ComparisonType: proto.KeyComparisonType_LOWER})
	assert.NoError(t, err)
	assert.Equal(t, "a", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "c", ComparisonType: proto.KeyComparisonType_HIGHER})
	assert.NoError(t, err)
	assert.Equal(t, "d", getRes.GetKey())

	// ---------------------------------------------------------------

	getRes, err = db.Get(&proto.GetRequest{Key: "d", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, "", getRes.GetKey())
	assert.Equal(t, "3", string(getRes.GetValue()))

	getRes, err = db.Get(&proto.GetRequest{Key: "d", IncludeValue: true, ComparisonType: proto.KeyComparisonType_EQUAL})
	assert.NoError(t, err)
	assert.Equal(t, "", getRes.GetKey())
	assert.Equal(t, "3", string(getRes.GetValue()))

	getRes, err = db.Get(&proto.GetRequest{Key: "d", ComparisonType: proto.KeyComparisonType_FLOOR})
	assert.NoError(t, err)
	assert.Equal(t, "d", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "d", ComparisonType: proto.KeyComparisonType_CEILING})
	assert.NoError(t, err)
	assert.Equal(t, "d", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "d", ComparisonType: proto.KeyComparisonType_LOWER})
	assert.NoError(t, err)
	assert.Equal(t, "c", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "d", ComparisonType: proto.KeyComparisonType_HIGHER})
	assert.NoError(t, err)
	assert.Equal(t, "e", getRes.GetKey())

	// ---------------------------------------------------------------

	getRes, err = db.Get(&proto.GetRequest{Key: "e", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, "", getRes.GetKey())
	assert.Equal(t, "4", string(getRes.GetValue()))

	getRes, err = db.Get(&proto.GetRequest{Key: "e", IncludeValue: true, ComparisonType: proto.KeyComparisonType_EQUAL})
	assert.NoError(t, err)
	assert.Equal(t, "", getRes.GetKey())
	assert.Equal(t, "4", string(getRes.GetValue()))

	getRes, err = db.Get(&proto.GetRequest{Key: "e", ComparisonType: proto.KeyComparisonType_FLOOR})
	assert.NoError(t, err)
	assert.Equal(t, "e", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "e", ComparisonType: proto.KeyComparisonType_CEILING})
	assert.NoError(t, err)
	assert.Equal(t, "e", getRes.GetKey())

	getRes, err = db.Get(&proto.GetRequest{Key: "e", ComparisonType: proto.KeyComparisonType_LOWER})
	assert.NoError(t, err)
	assert.Equal(t, "d", getRes.GetKey())

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_SequentialKeys(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "a",
		Value:            []byte("0"),
		SequenceKeyDelta: []uint64{3},
	}}}, 0, 0, NoOpCallback)
	assert.ErrorIs(t, err, ErrMissingPartitionKey)

	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "a",
		Value:            []byte("0"),
		PartitionKey:     pb.String("x"),
		SequenceKeyDelta: []uint64{0},
	}}}, 0, 0, NoOpCallback)
	assert.ErrorIs(t, err, ErrSequenceDeltaIsZero)

	resp, err := db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:               "a",
		Value:             []byte("0"),
		PartitionKey:      pb.String("x"),
		ExpectedVersionId: pb.Int64(1),
		SequenceKeyDelta:  []uint64{5},
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_UNEXPECTED_VERSION_ID, resp.GetPuts()[0].Status)

	resp, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "a",
		Value:            []byte("0"),
		PartitionKey:     pb.String("x"),
		SequenceKeyDelta: []uint64{5},
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, resp.GetPuts()[0].Status)
	assert.Equal(t, fmt.Sprintf("a-%020d", 5), resp.GetPuts()[0].GetKey())

	resp, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "a",
		Value:            []byte("0"),
		PartitionKey:     pb.String("x"),
		SequenceKeyDelta: []uint64{3},
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, resp.GetPuts()[0].Status)
	assert.Equal(t, fmt.Sprintf("a-%020d", 8), resp.GetPuts()[0].GetKey())

	// Add an extra sequence
	resp, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "a",
		Value:            []byte("0"),
		PartitionKey:     pb.String("x"),
		SequenceKeyDelta: []uint64{6, 9},
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, resp.GetPuts()[0].Status)
	assert.Equal(t, fmt.Sprintf("a-%020d-%020d", 14, 9), resp.GetPuts()[0].GetKey())

	// We cannot pass less sequence keys than what already present
	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "a",
		Value:            []byte("0"),
		PartitionKey:     pb.String("x"),
		SequenceKeyDelta: []uint64{2},
	}}}, 0, 0, NoOpCallback)
	assert.ErrorIs(t, err, ErrMissingSequenceDeltas)

	// Put bad existing suffix
	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:          "b+xxxx",
		Value:        []byte("0"),
		PartitionKey: pb.String("x"),
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)

	resp, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "b",
		Value:            []byte("0"),
		PartitionKey:     pb.String("x"),
		SequenceKeyDelta: []uint64{2},
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, resp.GetPuts()[0].Status)
	assert.Equal(t, fmt.Sprintf("b-%020d", 2), resp.GetPuts()[0].GetKey())

	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:          "c+.....",
		Value:        []byte("0"),
		PartitionKey: pb.String("x"),
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)

	resp, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "c",
		Value:            []byte("0"),
		PartitionKey:     pb.String("x"),
		SequenceKeyDelta: []uint64{2},
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, resp.GetPuts()[0].Status)
	assert.Equal(t, fmt.Sprintf("c-%020d", 2), resp.GetPuts()[0].GetKey())

	// With 3 sequences
	resp, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "a",
		Value:            []byte("0"),
		PartitionKey:     pb.String("x"),
		SequenceKeyDelta: []uint64{6, 9, 15},
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, resp.GetPuts()[0].Status)
	assert.Equal(t, fmt.Sprintf("a-%020d-%020d-%020d", 20, 18, 15), resp.GetPuts()[0].GetKey())
}

func rangeScanIteratorToSlice(it RangeScanIterator, err error) []string {
	assert.NoError(nil, err)
	var keys []string
	for ; it.Valid(); it.Next() {
		v, err := it.Value()
		assert.NoError(nil, err)
		keys = append(keys, v.GetKey())
	}
	_ = it.Close()
	return keys
}

func TestDBRangeScan(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}, {
			Key:   "b",
			Value: []byte("b"),
		}, {
			Key:   "c",
			Value: []byte("c"),
		}, {
			Key:   "d",
			Value: []byte("d"),
		}, {
			Key:   "e",
			Value: []byte("e"),
		}},
	}

	writeRes, err := db.ProcessWrite(writeReq, wal.InvalidOffset, now(), NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(writeRes.Puts))

	rangeScanReq1 := &proto.RangeScanRequest{
		StartInclusive: "a",
		EndExclusive:   "c",
	}
	rangeScanReq2 := &proto.RangeScanRequest{
		StartInclusive: "a",
		EndExclusive:   "d",
	}
	rangeScanReq3 := &proto.RangeScanRequest{
		StartInclusive: "xyz",
		EndExclusive:   "zzz",
	}

	// ["a", "c")
	keys1 := rangeScanIteratorToSlice(db.RangeScan(rangeScanReq1))
	assert.Len(t, keys1, 2)
	assert.Equal(t, "a", keys1[0])
	assert.Equal(t, "b", keys1[1])

	// ["a", "d")
	keys2 := rangeScanIteratorToSlice(db.RangeScan(rangeScanReq2))
	assert.Len(t, keys2, 3)
	assert.Equal(t, "a", keys2[0])
	assert.Equal(t, "b", keys2[1])
	assert.Equal(t, "c", keys2[2])

	// ["xyz", "zzz")
	keys3 := rangeScanIteratorToSlice(db.RangeScan(rangeScanReq3))
	assert.Len(t, keys3, 0)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDb_versionId(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{
				Key:   "a",
				Value: []byte("0"),
			},
			{
				Key:   "a",
				Value: []byte("1"),
			},
			{
				Key:   "a",
				Value: []byte("2"),
			},
		},
	}

	res, err := db.ProcessWrite(req, 0, 0, NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 3, len(res.Puts))
	r0 := res.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 0, r0.Version.VersionId)
	assert.EqualValues(t, 0, r0.Version.ModificationsCount)

	r1 := res.Puts[1]
	assert.Equal(t, proto.Status_OK, r1.Status)
	assert.EqualValues(t, 1, r1.Version.VersionId)
	assert.EqualValues(t, 1, r1.Version.ModificationsCount)

	r2 := res.Puts[2]
	assert.Equal(t, proto.Status_OK, r2.Status)
	assert.EqualValues(t, 2, r2.Version.VersionId)
	assert.EqualValues(t, 2, r2.Version.ModificationsCount)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

var _ UpdateOperationCallback = &FailureCallback{}

type FailureCallback struct{}

const FailureCallbackKey = "failure"

func (f FailureCallback) OnPut(_ WriteBatch, _ *Notifications, req *proto.PutRequest, _ *proto.StorageEntry) (proto.Status, error) {
	if req.Key == FailureCallbackKey {
		return proto.Status_SESSION_DOES_NOT_EXIST, errors.New("failure injection")
	}
	return proto.Status_OK, nil
}
func (f FailureCallback) OnDelete(_ WriteBatch, _ *Notifications, key string) error {
	if key == FailureCallbackKey {
		return errors.New("failure injection")
	}
	return nil
}
func (f FailureCallback) OnDeleteWithEntry(_ WriteBatch, _ *Notifications, key string, _ *proto.StorageEntry) error {
	if key == FailureCallbackKey {
		return errors.New("failure injection")
	}
	return nil
}
func (f FailureCallback) OnDeleteRange(WriteBatch, *Notifications, string, string) error {
	return nil
}

func TestDBVersionIDWithError(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	errReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{
				Key:   "/version-id/success",
				Value: []byte("3"),
			},
			{
				Key:   FailureCallbackKey,
				Value: []byte("0"),
			},
			{
				Key:   FailureCallbackKey,
				Value: []byte("2"),
			},
		},
	}
	_, err = db.ProcessWrite(errReq, 0, 0, &FailureCallback{})
	assert.Error(t, err)

	req := &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{
				Key:   "a",
				Value: []byte("0"),
			},
			{
				Key:   "b",
				Value: []byte("1"),
			},
			{
				Key:   "c",
				Value: []byte("2"),
			},
		},
	}

	res, err := db.ProcessWrite(req, 0, 0, &FailureCallback{})
	assert.NoError(t, err)

	assert.Equal(t, 3, len(res.Puts))
	r0 := res.Puts[0]
	assert.Equal(t, proto.Status_OK, r0.Status)
	assert.EqualValues(t, 0, r0.Version.VersionId)
	assert.EqualValues(t, 0, r0.Version.ModificationsCount)

	r1 := res.Puts[1]
	assert.Equal(t, proto.Status_OK, r1.Status)
	assert.EqualValues(t, 1, r1.Version.VersionId)
	assert.EqualValues(t, 0, r1.Version.ModificationsCount)

	r2 := res.Puts[2]
	assert.Equal(t, proto.Status_OK, r2.Status)
	assert.EqualValues(t, 2, r2.Version.VersionId)
	assert.EqualValues(t, 0, r2.Version.ModificationsCount)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_SequentialKeysNotification(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	sw, err := db.GetSequenceUpdates("a")
	assert.NoError(t, err)

	assert.Empty(t, sw.Ch())

	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "a",
		Value:            []byte("0"),
		SequenceKeyDelta: []uint64{3},
		PartitionKey:     pb.String("x"),
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)

	n1 := <-sw.Ch()
	assert.Equal(t, fmt.Sprintf("a-%020d", 3), n1)

	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:              "a",
		Value:            []byte("1"),
		SequenceKeyDelta: []uint64{5},
		PartitionKey:     pb.String("x"),
	}, {
		Key:              "a",
		Value:            []byte("1"),
		SequenceKeyDelta: []uint64{10},
		PartitionKey:     pb.String("x"),
	}}}, 0, 0, NoOpCallback)
	assert.NoError(t, err)

	// Only get notification of the last value
	n2 := <-sw.Ch()
	assert.Equal(t, fmt.Sprintf("a-%020d", 18), n2)

	assert.Empty(t, sw.Ch())
	assert.NoError(t, sw.Close())

	// Create a new one will get notification of existing last key
	sw2, err := db.GetSequenceUpdates("a")
	assert.NoError(t, err)

	n2 = <-sw2.Ch()
	assert.Equal(t, fmt.Sprintf("a-%020d", 18), n2)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}

func TestDB_ShowInternalKeys(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time.SystemClock)
	assert.NoError(t, err)

	writeReq := &proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("a"),
		}, {
			Key:   "b",
			Value: []byte("b"),
		}, {
			Key:   "c",
			Value: []byte("c"),
		}, {
			Key:   "__oxia/a-test",
			Value: []byte("x"),
		}},
	}

	_, err = db.ProcessWrite(writeReq, wal.InvalidOffset, now(), NoOpCallback)
	assert.NoError(t, err)

	listReq1 := &proto.ListRequest{
		StartInclusive: "a",
		EndExclusive:   "",
	}

	keys1 := keyIteratorToSlice(db.List(listReq1))
	assert.Len(t, keys1, 3)
	assert.Equal(t, "a", keys1[0])
	assert.Equal(t, "b", keys1[1])
	assert.Equal(t, "c", keys1[2])

	listReq2 := &proto.ListRequest{
		StartInclusive:      "a",
		EndExclusive:        "",
		IncludeInternalKeys: true,
	}

	keys2 := keyIteratorToSlice(db.List(listReq2))
	assert.Equal(t, "a", keys2[0])
	assert.Equal(t, "b", keys2[1])
	assert.Equal(t, "c", keys2[2])
	assert.Equal(t, "__oxia/a-test", keys2[3])

	// Range Scan

	scanIt, err := db.RangeScan(&proto.RangeScanRequest{
		StartInclusive:      "a",
		EndExclusive:        "",
		IncludeInternalKeys: false,
	})
	assert.NoError(t, err)
	gr, _ := scanIt.Value()
	assert.Equal(t, "a", *gr.Key)
	assert.True(t, scanIt.Next())
	gr, _ = scanIt.Value()
	assert.Equal(t, "b", *gr.Key)
	assert.True(t, scanIt.Next())
	gr, _ = scanIt.Value()
	assert.Equal(t, "c", *gr.Key)
	assert.False(t, scanIt.Next())
	assert.False(t, scanIt.Valid())

	assert.NoError(t, scanIt.Close())

	scanIt, err = db.RangeScan(&proto.RangeScanRequest{
		StartInclusive:      "a",
		EndExclusive:        "",
		IncludeInternalKeys: true,
	})
	assert.NoError(t, err)
	gr, _ = scanIt.Value()
	assert.Equal(t, "a", *gr.Key)
	assert.True(t, scanIt.Next())
	gr, _ = scanIt.Value()
	assert.Equal(t, "b", *gr.Key)
	assert.True(t, scanIt.Next())
	gr, _ = scanIt.Value()
	assert.Equal(t, "c", *gr.Key)
	assert.True(t, scanIt.Next())
	gr, _ = scanIt.Value()
	assert.Equal(t, "__oxia/a-test", *gr.Key)
	assert.True(t, scanIt.Next())
	assert.True(t, scanIt.Valid())

	assert.NoError(t, scanIt.Close())

	// Gets
	gr, err = db.Get(&proto.GetRequest{
		Key:            "c",
		IncludeValue:   true,
		ComparisonType: proto.KeyComparisonType_HIGHER,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, gr.Status)

	_, err = db.Get(&proto.GetRequest{
		Key:            "z",
		IncludeValue:   true,
		ComparisonType: proto.KeyComparisonType_CEILING,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, gr.Status)

	gr, err = db.Get(&proto.GetRequest{
		Key:            "__oxia/a-test",
		IncludeValue:   true,
		ComparisonType: proto.KeyComparisonType_EQUAL,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, gr.Status)

	assert.NoError(t, db.Close())
	assert.NoError(t, factory.Close())
}
