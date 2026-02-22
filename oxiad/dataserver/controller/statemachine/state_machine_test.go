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
	"testing"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
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

	err := ApplyLogEntry(db, entry, database.NoOpCallback)
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

	err := ApplyLogEntry(db, entry, database.NoOpCallback)
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

	err := ApplyLogEntry(db, entry, database.NoOpCallback)
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
	err = ApplyLogEntry(followerDB, entry, database.NoOpCallback)
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
	err := ApplyLogEntry(db, &proto.LogEntry{
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
	err = ApplyLogEntry(db, &proto.LogEntry{
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
	err = ApplyLogEntry(db, &proto.LogEntry{
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
