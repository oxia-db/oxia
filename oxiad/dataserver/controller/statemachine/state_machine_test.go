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

package statemachine

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

func newTestDB(t *testing.T) database.DB {
	t.Helper()
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	t.Cleanup(func() { kvFactory.Close() })
	db, err := database.NewDB(constant.DefaultNamespace, 0, kvFactory,
		proto.KeySortingType_NATURAL, 0, time.SystemClock)
	assert.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// marshalProposal serializes a proposal into LogEntry bytes, matching the WAL path.
func marshalProposal(t *testing.T, proposal Proposal) []byte {
	t.Helper()
	entryValue := &proto.LogEntryValue{}
	proposal.ToLogEntry(entryValue)
	value, err := entryValue.MarshalVT()
	assert.NoError(t, err)
	return value
}

// --- ApplyProposal tests (leader path) ---

func TestApplyProposal_Write(t *testing.T) {
	db := newTestDB(t)

	proposal := NewWriteProposal(0, &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "key1", Value: []byte("value1")},
		},
	})

	response, err := proposal.Apply(db, database.NoOpCallback)
	assert.NoError(t, err)
	assert.NotNil(t, response.WriteResponse)
	assert.Equal(t, 1, len(response.WriteResponse.Puts))
	assert.Equal(t, proto.Status_OK, response.WriteResponse.Puts[0].Status)

	// Verify data is readable from DB
	getResp, err := db.Get(&proto.GetRequest{Key: "key1", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, getResp.Status)
	assert.Equal(t, []byte("value1"), getResp.Value)
}

func TestApplyProposal_Control_FeatureEnable(t *testing.T) {
	db := newTestDB(t)

	assert.False(t, db.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM))

	proposal := NewControlProposal(0, &proto.ControlRequest{
		Value: &proto.ControlRequest_FeatureEnable{
			FeatureEnable: &proto.FeatureEnableRequest{
				Features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
			},
		},
	})

	response, err := proposal.Apply(db, database.NoOpCallback)
	assert.NoError(t, err)
	assert.Nil(t, response.WriteResponse)

	assert.True(t, db.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM))
}

func TestApplyProposal_Write_MultipleKeys(t *testing.T) {
	db := newTestDB(t)

	// First write key "a"
	_, err := NewWriteProposal(0, &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "a", Value: []byte("v1")},
		},
	}).Apply(db, database.NoOpCallback)
	assert.NoError(t, err)

	// Second write: put "b", delete non-existing "c", delete existing "a"
	response, err := NewWriteProposal(1, &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "b", Value: []byte("v2"), ExpectedVersionId: pb.Int64(-1)},
		},
		Deletes: []*proto.DeleteRequest{
			{Key: "c", ExpectedVersionId: pb.Int64(-1)}, // should fail: key doesn't exist
			{Key: "a"}, // should succeed
		},
	}).Apply(db, database.NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(response.WriteResponse.Puts))
	assert.Equal(t, proto.Status_OK, response.WriteResponse.Puts[0].Status)

	assert.Equal(t, 2, len(response.WriteResponse.Deletes))
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, response.WriteResponse.Deletes[0].Status)
	assert.Equal(t, proto.Status_OK, response.WriteResponse.Deletes[1].Status)
}

// --- ApplyLogEntry tests (follower/replay path) ---

func TestApplyLogEntry_WriteRequest(t *testing.T) {
	db := newTestDB(t)

	proposal := NewWriteProposal(0, &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "follower-key", Value: []byte("follower-val")},
		},
	})

	entry := &proto.LogEntry{
		Term:      1,
		Offset:    0,
		Value:     marshalProposal(t, proposal),
		Timestamp: proposal.GetTimestamp(),
	}

	_, err := ApplyLogEntry(db, entry, database.NoOpCallback)
	assert.NoError(t, err)

	getResp, err := db.Get(&proto.GetRequest{Key: "follower-key", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, getResp.Status)
	assert.Equal(t, []byte("follower-val"), getResp.Value)
}

func TestApplyLogEntry_ControlRequest(t *testing.T) {
	db := newTestDB(t)

	assert.False(t, db.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM))

	proposal := NewControlProposal(0, &proto.ControlRequest{
		Value: &proto.ControlRequest_FeatureEnable{
			FeatureEnable: &proto.FeatureEnableRequest{
				Features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
			},
		},
	})

	entry := &proto.LogEntry{
		Term:      1,
		Offset:    0,
		Value:     marshalProposal(t, proposal),
		Timestamp: proposal.GetTimestamp(),
	}

	_, err := ApplyLogEntry(db, entry, database.NoOpCallback)
	assert.NoError(t, err)

	assert.True(t, db.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM))
}

// A split child filters the entries it inherited from its parent, of terms up
// to the parent term, however it applies them. It applies its own entries, of
// later terms, as they are: e.g. the records of its sessions hash anywhere in
// the hash space.
func TestApplyLogEntry_SplitFilter(t *testing.T) {
	db := newTestDB(t)
	// The lower half of the hash space holds the key "a", and not "d"
	assert.NoError(t, db.SetSplitFilter(&database.SplitFilter{MinHash: 0, MaxHash: 0x7FFFFFFF, ParentTerm: 1}))

	apply := func(term int64, offset int64, value string) {
		proposal := NewWriteProposal(offset, &proto.WriteRequest{
			Puts: []*proto.PutRequest{
				{Key: "a", Value: []byte(value)},
				{Key: "d", Value: []byte(value)},
			},
		})
		_, err := ApplyLogEntry(db, &proto.LogEntry{
			Term:      term,
			Offset:    offset,
			Value:     marshalProposal(t, proposal),
			Timestamp: proposal.GetTimestamp(),
		}, database.NoOpCallback)
		assert.NoError(t, err)

		commitOffset, err := db.ReadCommitOffset()
		assert.NoError(t, err)
		assert.Equal(t, offset, commitOffset)
	}
	assertValue := func(key string, expected string) {
		res, err := db.Get(&proto.GetRequest{Key: key, IncludeValue: true})
		assert.NoError(t, err)
		if expected == "" {
			assert.Equal(t, proto.Status_KEY_NOT_FOUND, res.Status, key)
			return
		}
		assert.Equal(t, proto.Status_OK, res.Status, key)
		assert.Equal(t, expected, string(res.Value), key)
	}

	apply(1, 0, "parent")
	assertValue("a", "parent")
	assertValue("d", "")

	apply(2, 1, "child")
	assertValue("a", "child")
	assertValue("d", "child")
}

func TestApplyLogEntry_InvalidBytes(t *testing.T) {
	db := newTestDB(t)

	entry := &proto.LogEntry{
		Term:   1,
		Offset: 0,
		Value:  []byte("invalid-protobuf-bytes"),
	}

	_, err := ApplyLogEntry(db, entry, database.NoOpCallback)
	assert.Error(t, err)
}

// --- Integration-style tests ---

func TestApplyProposal_ThenApplyLogEntry(t *testing.T) {
	leaderDB := newTestDB(t)
	followerDB := newTestDB(t)

	// Leader applies a write proposal
	proposal := NewWriteProposal(0, &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "sync-key", Value: []byte("sync-val")},
		},
	})

	_, err := proposal.Apply(leaderDB, database.NoOpCallback)
	assert.NoError(t, err)

	// Follower replays the same entry from the WAL
	entry := &proto.LogEntry{
		Term:      1,
		Offset:    0,
		Value:     marshalProposal(t, proposal),
		Timestamp: proposal.GetTimestamp(),
	}
	_, err = ApplyLogEntry(followerDB, entry, database.NoOpCallback)
	assert.NoError(t, err)

	// Both DBs should have the same data
	leaderGet, err := leaderDB.Get(&proto.GetRequest{Key: "sync-key", IncludeValue: true})
	assert.NoError(t, err)
	followerGet, err := followerDB.Get(&proto.GetRequest{Key: "sync-key", IncludeValue: true})
	assert.NoError(t, err)

	assert.Equal(t, leaderGet.Status, followerGet.Status)
	assert.Equal(t, leaderGet.Value, followerGet.Value)
}

// A write request that can't be applied has no effect: the leader answers the
// client with the error, and the followers, split children included, apply
// the entry all the same.
func TestApplyLogEntry_RejectedWrite(t *testing.T) {
	leaderDB := newTestDB(t)
	followerDB := newTestDB(t)
	childDB := newTestDB(t)

	sequentialPut := func(deltas ...uint64) *proto.WriteRequest {
		return &proto.WriteRequest{Puts: []*proto.PutRequest{{
			Key:              "s",
			Value:            []byte("s"),
			PartitionKey:     pb.String("s"),
			SequenceKeyDelta: deltas,
		}}}
	}

	for offset, request := range []*proto.WriteRequest{
		sequentialPut(1, 1),
		// Fewer deltas than the parts of the sequence
		sequentialPut(1),
	} {
		proposal := NewWriteProposal(int64(offset), request)
		entry := &proto.LogEntry{
			Term:      1,
			Offset:    int64(offset),
			Value:     marshalProposal(t, proposal),
			Timestamp: proposal.GetTimestamp(),
		}

		_, err := proposal.Apply(leaderDB, database.NoOpCallback)
		if offset == 1 {
			assert.ErrorIs(t, err, database.ErrWriteRejected)
		} else {
			assert.NoError(t, err)
		}
		_, err = ApplyLogEntry(followerDB, entry, database.NoOpCallback)
		assert.NoError(t, err)
		_, err = ApplyLogEntryWithSplitFilter(childDB, entry, database.NoOpCallback,
			&proto.HashRange{Min: 0, Max: math.MaxUint32})
		assert.NoError(t, err)
	}

	for _, db := range []database.DB{leaderDB, followerDB, childDB} {
		commitOffset, err := db.ReadCommitOffset()
		assert.NoError(t, err)
		assert.EqualValues(t, 1, commitOffset)
	}
}

func TestApplyLogEntry_MultipleEntries(t *testing.T) {
	db := newTestDB(t)

	// Entry 1: Enable feature
	controlProposal := NewControlProposal(0, &proto.ControlRequest{
		Value: &proto.ControlRequest_FeatureEnable{
			FeatureEnable: &proto.FeatureEnableRequest{
				Features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
			},
		},
	})
	_, err := ApplyLogEntry(db, &proto.LogEntry{
		Term:      1,
		Offset:    0,
		Value:     marshalProposal(t, controlProposal),
		Timestamp: controlProposal.GetTimestamp(),
	}, database.NoOpCallback)
	assert.NoError(t, err)
	assert.True(t, db.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM))

	// Entry 2: Write key "x"
	write1 := NewWriteProposal(1, &proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: "x", Value: []byte("1")}},
	})
	_, err = ApplyLogEntry(db, &proto.LogEntry{
		Term:      1,
		Offset:    1,
		Value:     marshalProposal(t, write1),
		Timestamp: write1.GetTimestamp(),
	}, database.NoOpCallback)
	assert.NoError(t, err)

	// Entry 3: Write key "y"
	write2 := NewWriteProposal(2, &proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: "y", Value: []byte("2")}},
	})
	_, err = ApplyLogEntry(db, &proto.LogEntry{
		Term:      1,
		Offset:    2,
		Value:     marshalProposal(t, write2),
		Timestamp: write2.GetTimestamp(),
	}, database.NoOpCallback)
	assert.NoError(t, err)

	// Verify cumulative state
	getX, err := db.Get(&proto.GetRequest{Key: "x", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, getX.Status)
	assert.Equal(t, []byte("1"), getX.Value)

	getY, err := db.Get(&proto.GetRequest{Key: "y", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, getY.Status)
	assert.Equal(t, []byte("2"), getY.Value)
}

func TestApplyLogEntry_ControlRequestPersistsCommitOffsetForWalReplay(t *testing.T) {
	const shard = int64(19)

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kvFactory.Close()) })

	db, err := database.NewDB(constant.DefaultNamespace, shard, kvFactory,
		proto.KeySortingType_NATURAL, 0, time.SystemClock)
	assert.NoError(t, err)

	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})
	t.Cleanup(func() { assert.NoError(t, walFactory.Close()) })

	w, err := walFactory.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, w.Close()) })

	writeProposal := NewWriteProposal(0, &proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: "a", Value: []byte("0")}},
	})
	controlProposal := NewControlProposal(1, &proto.ControlRequest{
		Value: &proto.ControlRequest_RecordChecksum{
			RecordChecksum: &proto.RecordChecksumRequest{},
		},
	})

	for _, proposal := range []Proposal{writeProposal, controlProposal} {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:      1,
			Offset:    proposal.GetOffset(),
			Value:     marshalProposal(t, proposal),
			Timestamp: proposal.GetTimestamp(),
		}))
	}

	reader, err := w.NewReader(wal.InvalidOffset)
	assert.NoError(t, err)
	for reader.HasNext() {
		entry, _, _, err := reader.ReadNext()
		assert.NoError(t, err)
		_, err = ApplyLogEntry(db, entry, database.NoOpCallback)
		assert.NoError(t, err)
	}
	assert.NoError(t, reader.Close())
	assert.NoError(t, db.Close())

	db, err = database.NewDB(constant.DefaultNamespace, shard, kvFactory,
		proto.KeySortingType_NATURAL, 0, time.SystemClock)
	assert.NoError(t, err)
	defer db.Close()

	commitOffset, err := db.ReadCommitOffset()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, commitOffset)

	assert.NoError(t, w.Clear())
	nextWriteProposal := NewWriteProposal(2, &proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: "b", Value: []byte("1")}},
	})
	assert.NoError(t, w.Append(&proto.LogEntry{
		Term:      1,
		Offset:    nextWriteProposal.GetOffset(),
		Value:     marshalProposal(t, nextWriteProposal),
		Timestamp: nextWriteProposal.GetTimestamp(),
	}))

	reader, err = w.NewReader(commitOffset)
	if assert.NoError(t, err) {
		if assert.True(t, reader.HasNext()) {
			entry, _, _, err := reader.ReadNext()
			assert.NoError(t, err)
			assert.EqualValues(t, 2, entry.Offset)
		}
		assert.NoError(t, reader.Close())
	}
}
