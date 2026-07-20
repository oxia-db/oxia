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
	"bytes"
	"context"
	"fmt"
	"testing"
	stdtime "time"

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

// TestApplyLogEntry_DeleteRangeDoesNotCorruptAliasedEntry guards the lifetime
// contract of the zero-copy decode in ApplyLogEntry: applyPut must detach the
// request-owned buffers from the pooled StorageEntry before returning it, or
// the pooled Deserialize in applyDeleteRange appends the deleted entry's
// stored value over the WAL entry payload that the remaining operations of
// the same entry still alias.
//
// The entry holds two puts followed by two delete-ranges, and the first range
// deletes a key whose stored value is sized to fill the donated capacity
// exactly: without the detach, that Deserialize overwrites the second
// delete-range's keys and the captured notification keys in place.
func TestApplyLogEntry_DeleteRangeDoesNotCorruptAliasedEntry(t *testing.T) {
	db := newTestDB(t)

	// Several rounds, with distinct keys and offsets: the overwrite needs the
	// delete-range's pool Get to return the object the last put just
	// recycled, which holds on the same P but is not guaranteed by sync.Pool
	// (preemption or a GC can break the chain in any single round).
	for i := int64(0); i < 10; i++ {
		p := fmt.Sprintf("it-%d-", i)
		v2 := []byte(p + "marker-value-2")

		proposal := NewWriteProposal(2*i+1, &proto.WriteRequest{
			Puts: []*proto.PutRequest{
				{Key: p + "put-1", Value: []byte(p + "value-1")},
				{Key: p + "put-2", Value: v2},
			},
			DeleteRanges: []*proto.DeleteRangeRequest{
				{StartInclusive: p + "range-a", EndExclusive: p + "range-b"},
				{StartInclusive: p + "range-m", EndExclusive: p + "range-n"},
			},
		})
		entryValue := marshalProposal(t, proposal)

		// The pooled StorageEntry recycled by the last put keeps, as spare
		// capacity, a slice of the entry payload running from v2 to the end
		// of the buffer. A stored value of exactly that size makes a pooled
		// append overwrite everything after v2, delete-range keys included.
		v2Off := bytes.Index(entryValue, v2)
		assert.Greater(t, v2Off, 0)
		storedValue := bytes.Repeat([]byte("Z"), len(entryValue)-v2Off)

		_, err := ApplyLogEntry(db, &proto.LogEntry{
			Term:   1,
			Offset: 2 * i,
			Value: marshalProposal(t, NewWriteProposal(2*i, &proto.WriteRequest{
				Puts: []*proto.PutRequest{
					{Key: p + "range-a-key", Value: storedValue},
					{Key: p + "range-m-key", Value: []byte(p + "m-value")},
				},
			})),
			Timestamp: 1,
		}, database.NoOpCallback)
		assert.NoError(t, err)

		_, err = ApplyLogEntry(db, &proto.LogEntry{
			Term:      1,
			Offset:    2*i + 1,
			Value:     entryValue,
			Timestamp: 2,
		}, database.NoOpCallback)
		assert.NoError(t, err)

		// Both ranges must have been deleted under their original keys.
		for _, key := range []string{p + "range-a-key", p + "range-m-key"} {
			resp, err := db.Get(&proto.GetRequest{Key: key})
			assert.NoError(t, err)
			assert.Equal(t, proto.Status_KEY_NOT_FOUND, resp.Status, key)
		}
		for key, expected := range map[string][]byte{
			p + "put-1": []byte(p + "value-1"),
			p + "put-2": v2,
		} {
			resp, err := db.Get(&proto.GetRequest{Key: key, IncludeValue: true})
			assert.NoError(t, err)
			assert.Equal(t, proto.Status_OK, resp.Status, key)
			assert.Equal(t, expected, resp.Value, key)
		}

		// The notification batch must carry the original keys, not bytes of
		// the deleted entry's stored value.
		ctx, cancel := context.WithTimeout(context.Background(), 10*stdtime.Second)
		batches, err := db.ReadNextNotifications(ctx, 2*i+1)
		cancel()
		assert.NoError(t, err)
		if assert.NotEmpty(t, batches) {
			keys := make(map[string]proto.NotificationType)
			for _, n := range batches[0].Notifications {
				keys[n.GetKey()] = n.Value.Type
			}
			assert.Equal(t, map[string]proto.NotificationType{
				p + "put-1":   proto.NotificationType_KEY_CREATED,
				p + "put-2":   proto.NotificationType_KEY_CREATED,
				p + "range-a": proto.NotificationType_KEY_RANGE_DELETED,
				p + "range-m": proto.NotificationType_KEY_RANGE_DELETED,
			}, keys)
		}
	}
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
