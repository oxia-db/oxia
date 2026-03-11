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

package follow

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"

	dserror "github.com/oxia-db/oxia/oxiad/dataserver/errors"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	constant2 "github.com/oxia-db/oxia/oxiad/dataserver/constant"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/logging"

	"github.com/oxia-db/oxia/common/proto"
)

func init() {
	logging.ConfigureLogger()
}

func newTestWalFactory(t *testing.T) wal.Factory {
	t.Helper()

	return wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  t.TempDir(),
		SegmentSize: 128 * 1024,
	})
}

func TestFollower(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	fenceRes, err := fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, constant2.InvalidEntryId, fenceRes.HeadEntryId)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	truncateResp, err := fc.Truncate(&proto.TruncateRequest{
		Term: 1,
		HeadEntryId: &proto.EntryId{
			Term:   1,
			Offset: 0,
		},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, truncateResp.HeadEntryId.Term)
	assert.Equal(t, wal.InvalidOffset, truncateResp.HeadEntryId.Offset)

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	stream := rpc.NewMockServerReplicateStream()

	wg := concurrent.NewWaitGroup(1)

	go func() {
		_ = fc.AppendEntries(stream)
		stream.Cancel()
		wg.Done()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	// Wait for response
	response := stream.GetResponse()

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	assert.EqualValues(t, 0, response.Offset)

	// Write next entry
	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "4", "b": "5"}, wal.InvalidOffset))

	// Wait for response
	response = stream.GetResponse()
	assert.EqualValues(t, 1, response.Offset)

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	// close follower
	assert.NoError(t, fc.Close())

	// new term to test if we can continue replicate messages
	fc, err = NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 2})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 2, fc.Term())
	truncateResp, err = fc.Truncate(&proto.TruncateRequest{
		Term: 2,
		HeadEntryId: &proto.EntryId{
			Term:   1,
			Offset: 0,
		},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, truncateResp.HeadEntryId.Term)

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	stream = rpc.NewMockServerReplicateStream()
	wg2 := concurrent.NewWaitGroup(1)
	go func() {
		err := fc.AppendEntries(stream)
		assert.ErrorIs(t, err, context.Canceled)
		stream.Cancel()
		wg2.Done()
	}()
	stream.AddRequest(createAddRequest(t, 2, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))
	// Wait for response
	response = stream.GetResponse()
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 0, response.Offset)
	// Write next entry
	stream.AddRequest(createAddRequest(t, 2, 1, map[string]string{"a": "4", "b": "5"}, wal.InvalidOffset))

	// Wait for response
	response = stream.GetResponse()
	assert.EqualValues(t, 1, response.Offset)

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 2, fc.Term())

	stream.AddRequest(createAddRequest(t, 2, 2, map[string]string{"a": "4", "b": "5"}, wal.InvalidOffset))
	response = stream.GetResponse()
	assert.EqualValues(t, 2, response.Offset)
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 2, fc.Term())

	// Double-check the values in the DB
	// Keys are not there because they were not part of the commit offset
	dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{
		Key:          "a",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Status)

	dbRes, err = fc.(*followerController).db.Get(&proto.GetRequest{
		Key:          "b",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Status)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())

	_ = wg2.Wait(context.Background())
}

func TestReadingUpToCommitOffset(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	_, err = fc.Truncate(&proto.TruncateRequest{
		Term: 1,
		HeadEntryId: &proto.EntryId{
			Term:   0,
			Offset: wal.InvalidOffset,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "2", "b": "3"},
		// Commit offset points to previous entry
		0))

	// Wait for acks
	r1 := stream.GetResponse()

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	assert.EqualValues(t, 0, r1.Offset)

	r2 := stream.GetResponse()

	assert.EqualValues(t, 1, r2.Offset)

	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 0
	}, 10*time.Second, 10*time.Millisecond)

	dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{
		Key:          "a",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, dbRes.Status)
	assert.Equal(t, []byte("0"), dbRes.Value)

	dbRes, err = fc.(*followerController).db.Get(&proto.GetRequest{
		Key:          "b",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, dbRes.Status)
	assert.Equal(t, []byte("1"), dbRes.Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_RestoreCommitOffset(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	db, err := database.NewDB(constant.DefaultNamespace, shardId, kvFactory, proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)
	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:   "xx",
		Value: []byte(""),
	}}}, 9, 0, database.NoOpCallback)
	assert.NoError(t, err)

	assert.NoError(t, db.UpdateTerm(6, database.TermOptions{}))
	assert.NoError(t, db.Close())

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 6, fc.Term())
	assert.EqualValues(t, 9, fc.CommitOffset())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// If a follower receives a commit offset from the leader that is ahead
// of the current follower head offset, it needs to advance the commit
// offset only up to the current head.
func TestFollower_AdvanceCommitOffsetToHead(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, _ := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 10))

	// Wait for acks
	r1 := stream.GetResponse()

	assert.EqualValues(t, 0, r1.Offset)

	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 0
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_NewTerm(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	// We cannot fence with earlier term
	fr, err := fc.NewTerm(&proto.NewTermRequest{Term: 0})
	assert.Nil(t, fr)
	assert.Equal(t, dserror.ErrInvalidTerm, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	// A fence with same term needs to be accepted
	fr, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NotNil(t, fr)
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	// Higher term will work
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 3})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 3, fc.Term())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_DuplicateNewTermInFollowerState(t *testing.T) {
	var shardId int64 = 5
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, _ := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 10))

	// Wait for acks
	r1 := stream.GetResponse()

	assert.EqualValues(t, 0, r1.Offset)

	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 0
	}, 10*time.Second, 10*time.Millisecond)

	r, err := fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.EqualValues(t, r1.Offset, r.HeadEntryId.Offset)
	assert.EqualValues(t, 1, r.HeadEntryId.Term)

	stream = rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "1", "b": "2"}, 11))

	// Wait for acks
	r2 := stream.GetResponse()

	assert.EqualValues(t, 1, r2.Offset)

	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 1
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// If a node is restarted, it might get the truncate request
// when it's in the `NotMember` state. That is ok, provided
// the request comes in the same term that the follower
// currently has.
func TestFollower_TruncateAfterRestart(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	// Follower needs to be in "Fenced" state to receive a Truncate request
	tr, err := fc.Truncate(&proto.TruncateRequest{
		Term: 1,
		HeadEntryId: &proto.EntryId{
			Term:   0,
			Offset: 0,
		},
	})

	assert.Equal(t, dserror.ErrInvalidStatus, err)
	assert.Nil(t, tr)
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	_, err = fc.NewTerm(&proto.NewTermRequest{
		Shard: shardId,
		Term:  2,
	})
	assert.NoError(t, err)
	fc.Close()

	// Restart
	fc, err = NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())

	tr, err = fc.Truncate(&proto.TruncateRequest{
		Term: 2,
		HeadEntryId: &proto.EntryId{
			Term:   -1,
			Offset: -1,
		},
	})

	assert.NoError(t, err)
	assertProtoEqual(t, &proto.EntryId{Term: 2, Offset: -1}, tr.HeadEntryId)
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_PersistentTerm(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())
	assert.Equal(t, wal.InvalidTerm, fc.Term())

	fenceRes, err := fc.NewTerm(&proto.NewTermRequest{Term: 4})
	assert.NoError(t, err)
	assert.Equal(t, constant2.InvalidEntryId, fenceRes.HeadEntryId)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 4, fc.Term())

	assert.NoError(t, fc.Close())

	// Reopen and verify term
	fc, err = NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 4, fc.Term())

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_CommitOffsetLastEntry(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 0))

	// Wait for acks
	r1 := stream.GetResponse()

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	assert.EqualValues(t, 0, r1.Offset)

	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 0
	}, 10*time.Second, 10*time.Millisecond)

	dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{
		Key:          "a",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, dbRes.Status)
	assert.Equal(t, []byte("0"), dbRes.Value)

	dbRes, err = fc.(*followerController).db.Get(&proto.GetRequest{
		Key:          "b",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, dbRes.Status)
	assert.Equal(t, []byte("1"), dbRes.Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollowerController_RejectEntriesWithDifferentTerm(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)

	db, err := database.NewDB(constant.DefaultNamespace, shardId, kvFactory, proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)
	// Force a new term in the DB before opening
	assert.NoError(t, db.UpdateTerm(5, database.TermOptions{}))
	assert.NoError(t, db.Close())

	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	stream := rpc.NewMockServerReplicateStream()
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "1", "b": "1"}, wal.InvalidOffset))

	// Follower will reject the entry because it's from an earlier term
	err = fc.AppendEntries(stream)
	assert.Error(t, err)
	assert.Equal(t, dserror.ErrInvalidTerm, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())
	stream.Cancel()

	stream = rpc.NewMockServerReplicateStream()
	// If we send an entry of same term, it will be accepted
	stream.AddRequest(createAddRequest(t, 5, 0, map[string]string{"a": "2", "b": "2"}, wal.InvalidOffset))

	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	// Wait for acks
	r1 := stream.GetResponse()

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 0, r1.Offset)
	assert.NoError(t, fc.Close())

	// A higher term will also be rejected
	fc, err = NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	stream = rpc.NewMockServerReplicateStream()
	stream.AddRequest(createAddRequest(t, 6, 0, map[string]string{"a": "2", "b": "2"}, wal.InvalidOffset))
	err = fc.AppendEntries(stream)
	stream.Cancel()
	assert.Equal(t, dserror.ErrInvalidTerm, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_RejectTruncateInvalidTerm(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	fenceRes, err := fc.NewTerm(&proto.NewTermRequest{Term: 5})
	assert.NoError(t, err)
	assert.Equal(t, constant2.InvalidEntryId, fenceRes.HeadEntryId)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	// Lower term should be rejected
	truncateResp, err := fc.Truncate(&proto.TruncateRequest{
		Term: 4,
		HeadEntryId: &proto.EntryId{
			Term:   1,
			Offset: 0,
		},
	})
	assert.Nil(t, truncateResp)
	assert.Equal(t, dserror.ErrInvalidTerm, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	// Truncate with higher term should also fail
	truncateResp, err = fc.Truncate(&proto.TruncateRequest{
		Term: 6,
		HeadEntryId: &proto.EntryId{
			Term:   1,
			Offset: 0,
		},
	})
	assert.Nil(t, truncateResp)
	assert.Equal(t, dserror.ErrInvalidTerm, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())
}

func prepareTestDb(t *testing.T, term int64) kvstore.Snapshot {
	t.Helper()

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := database.NewDB(constant.DefaultNamespace, 0, kvFactory, proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err := db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{
				Key:   fmt.Sprintf("key-%d", i),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			}},
		}, int64(i), 0, database.NoOpCallback)
		assert.NoError(t, err)
	}
	assert.NoError(t, db.UpdateTerm(term, database.TermOptions{}))

	snapshot, err := db.Snapshot()
	assert.NoError(t, err)

	assert.NoError(t, kvFactory.Close())

	return snapshot
}

func TestFollower_HandleSnapshot(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		assert.NoError(t, fc.AppendEntries(stream))
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 0))

	// Wait for acks
	r1 := stream.GetResponse()
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 0, r1.Offset)
	close(stream.Requests)

	// Load snapshot into follower
	snapshot := prepareTestDb(t, 1)

	snapshotStream := rpc.NewMockServerSendSnapshotStream()
	wg := sync.WaitGroup{}
	wg.Go(func() {
		err := fc.InstallSnapshot(snapshotStream)
		assert.NoError(t, err)
	})

	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		assert.NoError(t, err)
		content := chunk.Content()
		snapshotStream.AddChunk(&proto.SnapshotChunk{
			Term:       1,
			Name:       chunk.Name(),
			Content:    content,
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		})
	}

	close(snapshotStream.Chunks)

	// Wait for follower to fully load the snapshot
	wg.Wait()

	statusRes, err := fc.(*followerController).GetStatus(&proto.GetStatusRequest{
		Shard: shardId,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FOLLOWER, statusRes.Status)
	assert.EqualValues(t, 1, statusRes.Term)
	assert.EqualValues(t, 99, statusRes.HeadOffset)
	assert.EqualValues(t, 99, statusRes.CommitOffset)

	// At this point the content of the follower should only include the
	// data from the snapshot and any existing data should be gone

	dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{
		Key:          "a",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Status)
	assert.Nil(t, dbRes.Value)

	dbRes, err = fc.(*followerController).db.Get(&proto.GetRequest{
		Key:          "b",
		IncludeValue: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, dbRes.Status)
	assert.Nil(t, dbRes.Value)

	for i := 0; i < 100; i++ {
		dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{
			Key:          fmt.Sprintf("key-%d", i),
			IncludeValue: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, proto.Status_OK, dbRes.Status)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), dbRes.Value)
	}

	assert.Equal(t, wal.InvalidOffset, fc.(*followerController).wal.LastOffset())

	assert.NoError(t, fc.Close())

	// Re-Open the follower controller
	fc, err = NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	statusRes, err = fc.(*followerController).GetStatus(&proto.GetStatusRequest{
		Shard: shardId,
	})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, statusRes.Status)
	assert.EqualValues(t, 1, statusRes.Term)
	assert.EqualValues(t, 99, statusRes.HeadOffset)
	assert.EqualValues(t, 99, statusRes.CommitOffset)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// TestFollower_SnapshotRecoveryFromNotMember verifies that when a follower
// has its data cleaned up (status=NOT_MEMBER, term=-1) and the leader
// sends a snapshot without a preceding NewTerm, the follower transitions
// to FOLLOWER after the snapshot so that subsequent AppendEntries succeed.
func TestFollower_SnapshotRecoveryFromNotMember(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	// Fresh follower: NOT_MEMBER, term=-1
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())
	assert.EqualValues(t, wal.InvalidTerm, fc.Term())

	// Simulate leader sending a snapshot directly (no NewTerm first).
	// This happens when the leader's FollowerCursor retries after the
	// follower restarted with clean data.
	snapshot := prepareTestDb(t, 5)

	snapshotStream := rpc.NewMockServerSendSnapshotStream()
	wg := sync.WaitGroup{}
	wg.Go(func() {
		err := fc.InstallSnapshot(snapshotStream)
		assert.NoError(t, err)
	})

	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		assert.NoError(t, err)
		content := chunk.Content()
		snapshotStream.AddChunk(&proto.SnapshotChunk{
			Term:       5,
			Name:       chunk.Name(),
			Content:    content,
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		})
	}
	close(snapshotStream.Chunks)
	wg.Wait()

	// After snapshot install, status must be FOLLOWER (not NOT_MEMBER).
	// The snapshot provides a clean state — no truncation needed — so
	// the node is ready for replication immediately.
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 5, fc.Term())
	assert.EqualValues(t, 99, fc.CommitOffset())

	// Verify the snapshot data is present
	for i := 0; i < 100; i++ {
		dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{
			Key:          fmt.Sprintf("key-%d", i),
			IncludeValue: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, proto.Status_OK, dbRes.Status)
	}

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_DisconnectLeader(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := rpc.NewMockServerReplicateStream()

	go func() {
		// cancelled due to NewTerm(2) below which closes the logSynchronizer
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	assert.Eventually(t, closeChanIsNotNil(fc), 10*time.Second, 10*time.Millisecond)

	// It's not possible to add a new leader stream
	assert.ErrorIs(t, fc.AppendEntries(stream), dserror.ErrResourceConflict)

	// When we fence again, the leader should have been cutoff
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 2})
	assert.NoError(t, err)

	stream = rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	assert.Eventually(t, closeChanIsNotNil(fc), 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_DupEntries(t *testing.T) {
	var shardId int64
	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	// Wait for responses
	r1 := stream.GetResponse()
	assert.EqualValues(t, 0, r1.Offset)

	r2 := stream.GetResponse()
	assert.EqualValues(t, 0, r2.Offset)

	// Write next entry
	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "4", "b": "5"}, wal.InvalidOffset))
	r3 := stream.GetResponse()
	assert.EqualValues(t, 1, r3.Offset)

	// Go back with older offset
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "4", "b": "5"}, wal.InvalidOffset))
	r4 := stream.GetResponse()
	assert.EqualValues(t, 0, r4.Offset)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollowerController_DeleteShard(t *testing.T) {
	var shardId int64
	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	// Wait for responses
	r1 := stream.GetResponse()
	assert.EqualValues(t, 0, r1.Offset)

	_, err := fc.Delete(&proto.DeleteShardRequest{
		Namespace: constant.DefaultNamespace,
		Shard:     shardId,
		Term:      1,
	})

	assert.NoError(t, err)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollowerController_DeleteShard_WrongTerm(t *testing.T) {
	var shardId int64
	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 2})

	_, err := fc.Delete(&proto.DeleteShardRequest{
		Namespace: constant.DefaultNamespace,
		Shard:     shardId,
		Term:      1,
	})

	assert.ErrorIs(t, err, dserror.ErrInvalidTerm)
}

func TestFollowerController_Closed(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shard, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, fc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	assert.NoError(t, fc.Close())

	res, err := fc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  2,
	})

	assert.Nil(t, res)
	assert.Equal(t, dserror.ErrResourceConflict, err)

	res2, err := fc.Truncate(&proto.TruncateRequest{
		Shard: shard,
		Term:  2,
		HeadEntryId: &proto.EntryId{
			Term:   2,
			Offset: 1,
		},
	})

	assert.Nil(t, res2)
	assert.Equal(t, dserror.ErrResourceConflict, err)

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_GetStatus(t *testing.T) {
	var shardId int64
	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 2})

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 2, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))
	stream.AddRequest(createAddRequest(t, 2, 1, map[string]string{"a": "0", "b": "1"}, 0))
	stream.AddRequest(createAddRequest(t, 2, 2, map[string]string{"a": "0", "b": "1"}, 1))

	// Wait for responses
	r1 := stream.GetResponse()
	assert.EqualValues(t, 0, r1.Offset)

	r2 := stream.GetResponse()
	assert.EqualValues(t, 1, r2.Offset)

	r3 := stream.GetResponse()
	assert.EqualValues(t, 2, r3.Offset)

	assert.Eventually(t, func() bool {
		res, _ := fc.GetStatus(&proto.GetStatusRequest{Shard: shardId})
		return res.CommitOffset == 1
	}, 10*time.Second, 100*time.Millisecond)

	res, err := fc.GetStatus(&proto.GetStatusRequest{Shard: shardId})
	assert.NoError(t, err)
	assert.Equal(t, &proto.GetStatusResponse{
		Term:         2,
		Status:       proto.ServingStatus_FOLLOWER,
		HeadOffset:   2,
		CommitOffset: 1,
	}, res)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_HandleSnapshotWithWrongTerm(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		assert.NoError(t, fc.AppendEntries(stream))
		stream.Cancel()
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 0))

	// Wait for acks
	r1 := stream.GetResponse()
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 0, r1.Offset)
	close(stream.Requests)

	// Load snapshot into follower
	snapshot := prepareTestDb(t, 2)

	snapshotStream := rpc.NewMockServerSendSnapshotStream()

	wg := concurrent.NewWaitGroup(1)

	go func() {
		err := fc.InstallSnapshot(snapshotStream)
		if err != nil {
			wg.Fail(err)
		} else {
			wg.Done()
		}
	}()

	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		assert.NoError(t, err)
		content := chunk.Content()
		snapshotStream.AddChunk(&proto.SnapshotChunk{
			Term:       2,
			Name:       chunk.Name(),
			Content:    content,
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		})
	}

	close(snapshotStream.Chunks)

	// The snapshot sending should fail because the term is invalid
	assert.ErrorIs(t, dserror.ErrInvalidTerm, wg.Wait(context.Background()))

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 5, Options: &proto.NewTermOptions{
		EnableNotifications: true,
		KeySorting:          proto.KeySortingType_UNKNOWN,
	}})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// TestFollower_SplitHashRangeFiltering verifies that when a follower has a
// split hash range set, only keys whose hash falls within the range are
// applied to the database. Keys outside the range are filtered out both
// during snapshot installation (FilterDBForSplit) and WAL catch-up
// (ApplyLogEntryWithSplitFilter).
//
// Hash values (xxh3):
//
//	"a" → 0x1e964e1f (in lower half)
//	"b" → 0x44d8843f (in lower half)
//	"c" → 0x46b9f81b (in lower half)
//	"d" → 0xc9c7a7ca (in upper half)
//	"e" → 0x3bec4a78 (in lower half)
//	"f" → 0x9ff3ba9a (in upper half)
func TestFollower_SplitHashRangeFiltering(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)

	// Set the split hash range to the lower half of the hash space.
	// Keys a, b, c, e are in range; d, f are out of range.
	fc.SetSplitHashRange(&model.Int32HashRange{
		Min: 0,
		Max: 0x7FFFFFFF, // 2147483647
	})

	// --- Phase 1: Snapshot installation with filtering ---
	// Prepare a snapshot DB containing keys a..f
	snapshotKvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	snapshotDb, err := database.NewDB(constant.DefaultNamespace, 0, snapshotKvFactory, proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)

	snapshotKeys := []string{"a", "b", "c", "d", "e", "f"}
	for i, key := range snapshotKeys {
		_, err := snapshotDb.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{
				Key:   key,
				Value: []byte(fmt.Sprintf("snapshot-%s", key)),
			}},
		}, int64(i), 0, database.NoOpCallback)
		assert.NoError(t, err)
	}
	assert.NoError(t, snapshotDb.UpdateTerm(1, database.TermOptions{}))

	snapshot, err := snapshotDb.Snapshot()
	assert.NoError(t, err)
	assert.NoError(t, snapshotKvFactory.Close())

	// Install snapshot
	snapshotStream := rpc.NewMockServerSendSnapshotStream()
	wg := sync.WaitGroup{}
	wg.Go(func() {
		err := fc.InstallSnapshot(snapshotStream)
		assert.NoError(t, err)
	})

	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		assert.NoError(t, err)
		content := chunk.Content()
		snapshotStream.AddChunk(&proto.SnapshotChunk{
			Term:       1,
			Name:       chunk.Name(),
			Content:    content,
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		})
	}
	close(snapshotStream.Chunks)
	wg.Wait()

	// After snapshot + FilterDBForSplit, only keys in the lower hash half should remain
	fci := fc.(*followerController)
	for _, key := range []string{"a", "b", "c", "e"} {
		dbRes, err := fci.db.Get(&proto.GetRequest{Key: key, IncludeValue: true})
		assert.NoError(t, err)
		assert.Equalf(t, proto.Status_OK, dbRes.Status, "key %q should be present after snapshot filtering", key)
		assert.Equalf(t, []byte(fmt.Sprintf("snapshot-%s", key)), dbRes.Value, "key %q has wrong value", key)
	}

	for _, key := range []string{"d", "f"} {
		dbRes, err := fci.db.Get(&proto.GetRequest{Key: key, IncludeValue: true})
		assert.NoError(t, err)
		assert.Equalf(t, proto.Status_KEY_NOT_FOUND, dbRes.Status, "key %q should have been filtered out", key)
	}

	// --- Phase 2: WAL catch-up with filtering ---
	// Replicate entries that write both in-range and out-of-range keys.
	// The follower should apply only the in-range operations.
	stream := rpc.NewMockServerReplicateStream()
	go func() {
		_ = fc.AppendEntries(stream)
		stream.Cancel()
	}()

	// Entry at offset 6: put "a" (in range) and "d" (out of range)
	stream.AddRequest(createAddRequest(t, 1, 6, map[string]string{
		"a": "wal-a",
		"d": "wal-d",
	}, wal.InvalidOffset))

	// After snapshot install the WAL is empty (lastOffset=-1). When the syncer
	// acks it sends offsets from oldHead+1 to newHead, so we may receive
	// multiple acks (0..6). Drain until we see offset 6.
	for {
		r := stream.GetResponse()
		if r.Offset == 6 {
			break
		}
	}

	// Entry at offset 7: put "f" (out of range) and "c" (in range)
	stream.AddRequest(createAddRequest(t, 1, 7, map[string]string{
		"f": "wal-f",
		"c": "wal-c",
	}, 7)) // commit up to offset 7

	r := stream.GetResponse()
	assert.EqualValues(t, 7, r.Offset)

	// Wait for commit offset to advance
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 7
	}, 10*time.Second, 10*time.Millisecond)

	// Verify: in-range keys were updated via WAL catch-up
	dbRes, err := fci.db.Get(&proto.GetRequest{Key: "a", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, dbRes.Status)
	assert.Equal(t, []byte("wal-a"), dbRes.Value)

	dbRes, err = fci.db.Get(&proto.GetRequest{Key: "c", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, dbRes.Status)
	assert.Equal(t, []byte("wal-c"), dbRes.Value)

	// Verify: out-of-range keys were NOT applied from WAL
	dbRes, err = fci.db.Get(&proto.GetRequest{Key: "d", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equalf(t, proto.Status_KEY_NOT_FOUND, dbRes.Status, "key 'd' should still be absent (out of hash range)")

	dbRes, err = fci.db.Get(&proto.GetRequest{Key: "f", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equalf(t, proto.Status_KEY_NOT_FOUND, dbRes.Status, "key 'f' should still be absent (out of hash range)")

	// In-range keys that were only in snapshot (not in WAL) should still be present
	dbRes, err = fci.db.Get(&proto.GetRequest{Key: "b", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, dbRes.Status)
	assert.Equal(t, []byte("snapshot-b"), dbRes.Value)

	dbRes, err = fci.db.Get(&proto.GetRequest{Key: "e", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, dbRes.Status)
	assert.Equal(t, []byte("snapshot-e"), dbRes.Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func closeChanIsNotNil(fc FollowerController) func() bool {
	return func() bool {
		fci := fc.(*followerController)
		fci.rwMutex.RLock()
		defer fci.rwMutex.RUnlock()
		return fci.logSynchronizer.IsValid()
	}
}

func createAddRequest(t *testing.T, term int64, offset int64,
	kvs map[string]string,
	commitOffset int64) *proto.Append {
	t.Helper()

	br := &proto.WriteRequest{}

	for k, v := range kvs {
		br.Puts = append(br.Puts, &proto.PutRequest{
			Key:   k,
			Value: []byte(v),
		})
	}

	entry, err := pb.Marshal(wrapInLogEntryValue(br))
	assert.NoError(t, err)

	le := &proto.LogEntry{
		Term:   term,
		Offset: offset,
		Value:  entry,
	}

	return &proto.Append{
		Term:         term,
		Entry:        le,
		CommitOffset: commitOffset,
	}
}

func assertProtoEqual(t *testing.T, expected, actual pb.Message) {
	t.Helper()

	if !pb.Equal(expected, actual) {
		protoMarshal := protojson.MarshalOptions{
			EmitUnpopulated: true,
		}
		expectedJSON, _ := protoMarshal.Marshal(expected)
		actualJSON, _ := protoMarshal.Marshal(actual)
		assert.Equal(t, string(expectedJSON), string(actualJSON))
	}
}

func wrapInLogEntryValue(wr *proto.WriteRequest) *proto.LogEntryValue {
	return &proto.LogEntryValue{
		Value: &proto.LogEntryValue_Requests{
			Requests: &proto.WriteRequests{
				Writes: []*proto.WriteRequest{
					wr,
				},
			},
		},
	}
}
