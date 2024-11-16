// Copyright 2023 StreamNative, Inc.
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

package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
)

var testKVOptions = &kv.FactoryOptions{
	InMemory:    true,
	CacheSizeMB: 1,
}

func init() {
	common.LogLevel = slog.LevelDebug
	common.ConfigureLogger()
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
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	fenceRes, err := fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fenceRes.HeadEntryId)

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

	stream := newMockServerReplicateStream()

	wg := common.NewWaitGroup(1)

	go func() {
		_ = fc.Replicate(stream)
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
	fc, err = NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())
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
	stream = newMockServerReplicateStream()
	wg2 := common.NewWaitGroup(1)
	go func() {
		err := fc.Replicate(stream)
		assert.ErrorIs(t, err, context.Canceled)
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
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
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

	stream := newMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
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
	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	db, err := kv.NewDB(common.DefaultNamespace, shardId, kvFactory, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)
	_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:   "xx",
		Value: []byte(""),
	}}}, 9, 0, kv.NoOpCallback)
	assert.NoError(t, err)

	assert.NoError(t, db.UpdateTerm(6, kv.TermOptions{}))
	assert.NoError(t, db.Close())

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
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
	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, _ := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := newMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
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
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	// We cannot fence with earlier term
	fr, err := fc.NewTerm(&proto.NewTermRequest{Term: 0})
	assert.Nil(t, fr)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))
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
	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, _ := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := newMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 10))

	// Wait for acks
	r1 := stream.GetResponse()

	assert.EqualValues(t, 0, r1.Offset)

	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 0
	}, 10*time.Second, 10*time.Millisecond)

	r, err := fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.Nil(t, r)
	assert.Equal(t, common.CodeFollowerAlreadyFenced, status.Code(err))

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
	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{DataDir: t.TempDir()})
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	// Follower needs to be in "Fenced" state to receive a Truncate request
	tr, err := fc.Truncate(&proto.TruncateRequest{
		Term: 1,
		HeadEntryId: &proto.EntryId{
			Term:   0,
			Offset: 0,
		},
	})

	assert.Equal(t, common.CodeInvalidStatus, status.Code(err))
	assert.Nil(t, tr)
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	_, err = fc.NewTerm(&proto.NewTermRequest{
		Shard: shardId,
		Term:  2,
	})
	assert.NoError(t, err)
	fc.Close()

	// Restart
	fc, err = NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
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
	AssertProtoEqual(t, &proto.EntryId{Term: 2, Offset: -1}, tr.HeadEntryId)
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_PersistentTerm(t *testing.T) {
	var shardId int64
	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 1,
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())
	assert.Equal(t, wal.InvalidTerm, fc.Term())

	fenceRes, err := fc.NewTerm(&proto.NewTermRequest{Term: 4})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fenceRes.HeadEntryId)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 4, fc.Term())

	assert.NoError(t, fc.Close())

	// Reopen and verify term
	fc, err = NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 4, fc.Term())

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_CommitOffsetLastEntry(t *testing.T) {
	var shardId int64
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	stream := newMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
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
	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 1,
	})
	assert.NoError(t, err)

	db, err := kv.NewDB(common.DefaultNamespace, shardId, kvFactory, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)
	// Force a new term in the DB before opening
	assert.NoError(t, db.UpdateTerm(5, kv.TermOptions{}))
	assert.NoError(t, db.Close())

	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	stream := newMockServerReplicateStream()
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "1", "b": "1"}, wal.InvalidOffset))

	// Follower will reject the entry because it's from an earlier term
	err = fc.Replicate(stream)
	assert.Error(t, err)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	// If we send an entry of same term, it will be accepted
	stream.AddRequest(createAddRequest(t, 5, 0, map[string]string{"a": "2", "b": "2"}, wal.InvalidOffset))

	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	// Wait for acks
	r1 := stream.GetResponse()

	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 0, r1.Offset)
	assert.NoError(t, fc.Close())
	close(stream.requests)

	// A higher term will also be rejected
	fc, err = NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	stream = newMockServerReplicateStream()
	stream.AddRequest(createAddRequest(t, 6, 0, map[string]string{"a": "2", "b": "2"}, wal.InvalidOffset))
	err = fc.Replicate(stream)
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err), "Unexpected error: %s", err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_RejectTruncateInvalidTerm(t *testing.T) {
	var shardId int64
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	fenceRes, err := fc.NewTerm(&proto.NewTermRequest{Term: 5})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fenceRes.HeadEntryId)

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
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))
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
	assert.Equal(t, common.CodeInvalidTerm, status.Code(err))
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())
}

func prepareTestDb(t *testing.T) kv.Snapshot {
	t.Helper()

	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir: t.TempDir(),
	})
	assert.NoError(t, err)
	db, err := kv.NewDB(common.DefaultNamespace, 0, kvFactory, 1*time.Hour, common.SystemClock)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err := db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{
				Key:   fmt.Sprintf("key-%d", i),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			}},
		}, int64(i), 0, kv.NoOpCallback)
		assert.NoError(t, err)
	}

	snapshot, err := db.Snapshot()
	assert.NoError(t, err)

	assert.NoError(t, kvFactory.Close())

	return snapshot
}

func TestFollower_HandleSnapshot(t *testing.T) {
	var shardId int64
	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir: t.TempDir(),
	})
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	stream := newMockServerReplicateStream()
	go func() { assert.NoError(t, fc.Replicate(stream)) }()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 0))

	// Wait for acks
	r1 := stream.GetResponse()
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 0, r1.Offset)
	close(stream.requests)

	// Load snapshot into follower
	snapshot := prepareTestDb(t)

	snapshotStream := newMockServerSendSnapshotStream()
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		err := fc.SendSnapshot(snapshotStream)
		assert.NoError(t, err)
		wg.Done()
	}()

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

	close(snapshotStream.chunks)

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
	fc, err = NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
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

func TestFollower_DisconnectLeader(t *testing.T) {
	var shardId int64
	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := newMockServerReplicateStream()

	go func() { assert.NoError(t, fc.Replicate(stream)) }()

	assert.Eventually(t, closeChanIsNotNil(fc), 10*time.Second, 10*time.Millisecond)

	// It's not possible to add a new leader stream
	assert.ErrorIs(t, fc.Replicate(stream), common.ErrorLeaderAlreadyConnected)

	// When we fence again, the leader should have been cutoff
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 2})
	assert.NoError(t, err)

	assert.Nil(t, fc.(*followerController).closeStreamWg)

	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	assert.Eventually(t, closeChanIsNotNil(fc), 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_DupEntries(t *testing.T) {
	var shardId int64
	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := newMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
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
	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream := newMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
	}()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))

	// Wait for responses
	r1 := stream.GetResponse()
	assert.EqualValues(t, 0, r1.Offset)

	_, err := fc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: common.DefaultNamespace,
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
	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 2})

	_, err := fc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: common.DefaultNamespace,
		Shard:     shardId,
		Term:      1,
	})

	assert.ErrorIs(t, err, common.ErrorInvalidTerm)
}

func TestFollowerController_Closed(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shard, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, fc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	assert.NoError(t, fc.Close())

	res, err := fc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  2,
	})

	assert.Nil(t, res)
	assert.Equal(t, common.CodeAlreadyClosed, status.Code(err))

	res2, err := fc.Truncate(&proto.TruncateRequest{
		Shard: shard,
		Term:  2,
		HeadEntryId: &proto.EntryId{
			Term:   2,
			Offset: 1,
		},
	})

	assert.Nil(t, res2)
	assert.Equal(t, common.CodeAlreadyClosed, status.Code(err))

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_GetStatus(t *testing.T) {
	var shardId int64
	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 2})

	stream := newMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.Replicate(stream), context.Canceled)
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
	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir: t.TempDir(),
	})
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(Config{}, common.DefaultNamespace, shardId, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	stream := newMockServerReplicateStream()
	go func() { assert.NoError(t, fc.Replicate(stream)) }()

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, 0))

	// Wait for acks
	r1 := stream.GetResponse()
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 0, r1.Offset)
	close(stream.requests)

	// Load snapshot into follower
	snapshot := prepareTestDb(t)

	snapshotStream := newMockServerSendSnapshotStream()

	wg := common.NewWaitGroup(1)

	go func() {
		err := fc.SendSnapshot(snapshotStream)
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

	close(snapshotStream.chunks)

	// The snapshot sending should fail because the term is invalid
	assert.ErrorIs(t, common.ErrorInvalidTerm, wg.Wait(context.Background()))

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 5})
	assert.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 5, fc.Term())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func closeChanIsNotNil(fc FollowerController) func() bool {
	return func() bool {
		_fc := fc.(*followerController)
		_fc.Lock()
		defer _fc.Unlock()
		return _fc.closeStreamWg != nil
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
