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

package follow

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/rpc"
	constant2 "github.com/oxia-db/oxia/oxiad/dataserver/constant"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
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
	assert.Equal(t, constant.ErrInvalidTerm, err)
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

// After a leader reconnection, the entries are resent from the last ack
// position known to the leader. The follower might already have them and
// must still ack them, otherwise the leader would not make progress.
func TestFollower_DuplicateEntryAckAfterReconnect(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)

	stream := rpc.NewMockServerReplicateStream()
	wg := concurrent.NewWaitGroup(1)
	go func() {
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		wg.Done()
	}()

	for i := int64(0); i < 10; i++ {
		stream.AddRequest(createAddRequest(t, 1, i, map[string]string{"a": "0"}, wal.InvalidOffset))
		assert.EqualValues(t, i, stream.GetResponse().Offset)
	}

	// Simulate a leader reconnection: the acks were lost, so all the entries
	// are resent, followed by new entries
	stream.Cancel()
	assert.NoError(t, wg.Wait(context.Background()))

	stream = rpc.NewMockServerReplicateStream()
	wg = concurrent.NewWaitGroup(1)
	go func() {
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		wg.Done()
	}()

	for i := int64(0); i < 10; i++ {
		stream.AddRequest(createAddRequest(t, 1, i, map[string]string{"a": "0"}, wal.InvalidOffset))
	}
	for i := int64(0); i < 10; i++ {
		assert.EqualValues(t, i, stream.GetResponse().Offset)
	}

	stream.AddRequest(createAddRequest(t, 1, 10, map[string]string{"a": "1"}, wal.InvalidOffset))
	assert.EqualValues(t, 10, stream.GetResponse().Offset)

	stream.Cancel()
	assert.NoError(t, wg.Wait(context.Background()))

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// syncerOnlySendStream fails the test if an ack is sent from any goroutine
// other than the syncer: gRPC streams do not support concurrent Send() calls,
// so the syncer must be the only goroutine sending on the stream.
type syncerOnlySendStream struct {
	*rpc.MockServerReplicateStream
	t *testing.T
}

func (s *syncerOnlySendStream) Send(ack *proto.Ack) error {
	buf := make([]byte, 16*1024)
	stack := string(buf[:runtime.Stack(buf, false)])
	if !strings.Contains(stack, "bgSyncer") {
		s.t.Errorf("ack for offset %d was not sent from the syncer goroutine:\n%s", ack.Offset, stack)
	}
	return s.MockServerReplicateStream.Send(ack)
}

func TestFollower_DuplicateEntryAckConcurrentAppends(t *testing.T) {
	const numEntries = int64(100)

	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)

	stream := rpc.NewMockServerReplicateStream()
	wg := concurrent.NewWaitGroup(1)
	go func() {
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		wg.Done()
	}()

	for i := int64(0); i < numEntries; i++ {
		stream.AddRequest(createAddRequest(t, 1, i, map[string]string{"a": "0"}, wal.InvalidOffset))
	}
	for i := int64(0); i < numEntries; i++ {
		assert.EqualValues(t, i, stream.GetResponse().Offset)
	}

	// Simulate a leader reconnection
	stream.Cancel()
	assert.NoError(t, wg.Wait(context.Background()))

	stream2 := &syncerOnlySendStream{MockServerReplicateStream: rpc.NewMockServerReplicateStream(), t: t}
	wg = concurrent.NewWaitGroup(1)
	go func() {
		assert.ErrorIs(t, fc.AppendEntries(stream2), context.Canceled)
		wg.Done()
	}()

	// Resend all the entries (duplicates) immediately followed by new ones,
	// while concurrently consuming the acks
	go func() {
		for i := int64(0); i < 2*numEntries; i++ {
			stream2.AddRequest(createAddRequest(t, 1, i, map[string]string{"a": "0"}, wal.InvalidOffset))
		}
	}()

	// Every entry is acked exactly once and in order: first the duplicates,
	// then the newly appended entries
	for i := int64(0); i < 2*numEntries; i++ {
		assert.EqualValues(t, i, stream2.GetResponse().Offset)
	}

	stream2.Cancel()
	assert.NoError(t, wg.Wait(context.Background()))

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

	assert.Equal(t, constant.ErrInvalidStatus, err)
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
	assert.Equal(t, constant.ErrInvalidTerm, err)
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
	assert.Equal(t, constant.ErrInvalidTerm, err)
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
	assert.Equal(t, constant.ErrInvalidTerm, err)
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
	assert.Equal(t, constant.ErrInvalidTerm, err)
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

// sendSnapshot adds all the chunks of the snapshot to the stream, in the given
// term, and ends the stream.
func sendSnapshot(t *testing.T, stream *rpc.MockServerSendSnapshotStream, snapshot kvstore.Snapshot, term int64) {
	t.Helper()
	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		require.NoError(t, err)
		stream.AddChunk(&proto.SnapshotChunk{
			Term:       term,
			Name:       chunk.Name(),
			Content:    chunk.Content(),
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		})
	}
	close(stream.Chunks)
}

// A follower seeded from a snapshot has an empty wal, while its database holds
// the entries up to the snapshot's commit offset. Fenced with a new term, it
// must report them, so that an election doesn't take it for a follower without
// any data.
func TestFollower_NewTermAfterSnapshot(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory,
		kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)

	stream := rpc.NewMockServerSendSnapshotStream()
	sendSnapshot(t, stream, prepareTestDb(t, 1), 1)
	require.NoError(t, fc.InstallSnapshot(stream))

	res, err := fc.NewTerm(&proto.NewTermRequest{Term: 2})
	require.NoError(t, err)
	assertProtoEqual(t, &proto.EntryId{Term: wal.InvalidTerm, Offset: 99}, res.HeadEntryId)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// abortedSnapshotStream delivers the first chunk of a snapshot, then fails once
// aborted, like a stream that the leader closes when it gets fenced.
type abortedSnapshotStream struct {
	*rpc.MockServerSendSnapshotStream
	firstChunk *proto.SnapshotChunk
	abort      chan struct{}
}

func (s *abortedSnapshotStream) Recv() (*proto.SnapshotChunk, error) {
	if chunk := s.firstChunk; chunk != nil {
		s.firstChunk = nil
		return chunk, nil
	}
	<-s.abort
	return nil, context.Canceled
}

// The installation of a snapshot replaces the wal and the database of the
// follower, which must stop reporting the entries that it had: while it
// installs the snapshot, and when the installation fails, where it recovers an
// empty database.
func TestFollower_FailedSnapshotInstall(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory,
		kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)

	stream := rpc.NewMockServerSendSnapshotStream()
	sendSnapshot(t, stream, prepareTestDb(t, 1), 1)
	require.NoError(t, fc.InstallSnapshot(stream))
	status, err := fc.GetStatus(&proto.GetStatusRequest{Shard: shardId})
	require.NoError(t, err)
	assert.EqualValues(t, 99, status.HeadOffset)
	assert.EqualValues(t, 99, status.CommitOffset)

	snapshot := prepareTestDb(t, 1)
	chunk, err := snapshot.Chunk()
	require.NoError(t, err)
	abortedStream := &abortedSnapshotStream{
		MockServerSendSnapshotStream: rpc.NewMockServerSendSnapshotStream(),
		firstChunk: &proto.SnapshotChunk{
			Term:       1,
			Name:       chunk.Name(),
			Content:    chunk.Content(),
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		},
		abort: make(chan struct{}),
	}
	installErr := make(chan error, 1)
	go func() { installErr <- fc.InstallSnapshot(abortedStream) }()
	assert.Eventually(t, func() bool {
		status, err := fc.GetStatus(&proto.GetStatusRequest{Shard: shardId})
		return err == nil && status.HeadOffset == wal.InvalidOffset && status.CommitOffset == wal.InvalidOffset
	}, 10*time.Second, 10*time.Millisecond, "the follower reports the entries that it is replacing")
	close(abortedStream.abort)
	assert.Error(t, <-installErr)

	status, err = fc.GetStatus(&proto.GetStatusRequest{Shard: shardId})
	require.NoError(t, err)
	assert.EqualValues(t, wal.InvalidOffset, status.HeadOffset)
	assert.EqualValues(t, wal.InvalidOffset, status.CommitOffset)

	res, err := fc.NewTerm(&proto.NewTermRequest{Term: 2})
	require.NoError(t, err)
	assertProtoEqual(t, constant2.InvalidEntryId, res.HeadEntryId)

	assert.NoError(t, snapshot.Close())
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
	assert.ErrorIs(t, fc.AppendEntries(stream), constant.ErrResourceConflict)

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

	// Wait for the entry to be synced and acked, so that the duplicate below
	// is at or below the synced offset and gets re-acknowledged immediately.
	// (A duplicate that is not yet synced is only acked after its sync round:
	// see TestLogSynchronizer_DuplicateAckOnlySynced.)
	r1 := stream.GetResponse()
	assert.EqualValues(t, 0, r1.Offset)

	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0", "b": "1"}, wal.InvalidOffset))
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

// A fresh follower (empty wal and db) whose first entries were lost in
// transit receives its first append at a non-zero offset: the append must be
// rejected and the stream must fail, so that the leader reconnects and
// resends from the beginning. Accepting the gap would let the follower ack
// entries it does not have, and the leader would count those acks towards the
// commit quorum.
func TestFollower_RejectGapOnEmptyWal(t *testing.T) {
	var shardId int64
	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	fc, _ := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	_, _ = fc.NewTerm(&proto.NewTermRequest{Term: 1})

	stream1 := rpc.NewMockServerReplicateStream()
	errCh := make(chan error, 1)
	go func() {
		errCh <- fc.AppendEntries(stream1)
		stream1.Cancel()
	}()

	// First-ever entry arrives at offset 5: entries 0..4 were lost
	stream1.AddRequest(createAddRequest(t, 1, 5, map[string]string{"a": "0"}, wal.InvalidOffset))
	assert.ErrorIs(t, <-errCh, wal.ErrInvalidNextOffset)

	// The leader reconnects and resends from the beginning: the follower
	// accepts and acks the contiguous entries
	stream2 := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream2), context.Canceled)
		stream2.Cancel()
	}()

	stream2.AddRequest(createAddRequest(t, 1, 0, map[string]string{"a": "0"}, wal.InvalidOffset))
	r1 := stream2.GetResponse()
	assert.EqualValues(t, 0, r1.Offset)

	stream2.AddRequest(createAddRequest(t, 1, 1, map[string]string{"a": "1"}, wal.InvalidOffset))
	r2 := stream2.GetResponse()
	assert.EqualValues(t, 1, r2.Offset)

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

	assert.ErrorIs(t, err, constant.ErrInvalidTerm)
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
	assert.Equal(t, constant.ErrResourceConflict, err)

	res2, err := fc.Truncate(&proto.TruncateRequest{
		Shard: shard,
		Term:  2,
		HeadEntryId: &proto.EntryId{
			Term:   2,
			Offset: 1,
		},
	})

	assert.Nil(t, res2)
	assert.Equal(t, constant.ErrResourceConflict, err)

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
	assert.ErrorIs(t, constant.ErrInvalidTerm, wg.Wait(context.Background()))

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

// closeFailingKVFactory makes the next database Close fail once failNextClose
// is set. Like Pebble when it finds leaked iterators, it closes the database
// all the same.
type closeFailingKVFactory struct {
	kvstore.Factory
	failNextClose atomic.Bool
}

func (f *closeFailingKVFactory) NewKV(
	namespace string,
	shardId int64,
	keySorting proto.KeySortingType,
) (kvstore.KV, error) {
	kv, err := f.Factory.NewKV(namespace, shardId, keySorting)
	if err != nil {
		return nil, err
	}
	return &closeFailingKV{KV: kv, factory: f}, nil
}

type closeFailingKV struct {
	kvstore.KV
	factory *closeFailingKVFactory
}

func (kv *closeFailingKV) Close() error {
	err := kv.KV.Close()
	if kv.factory.failNextClose.CompareAndSwap(true, false) {
		return errors.New("leaked iterators")
	}
	return err
}

// A snapshot install that fails to close the old database must still leave the
// follower with a database: the leader retries the snapshot, and NewTerm, Close
// and the state applier use the database too.
func TestFollower_HandleSnapshotWithFailingDatabaseClose(t *testing.T) {
	var shardId int64
	pebbleFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	kvFactory := &closeFailingKVFactory{Factory: pebbleFactory}
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)

	snapshot := prepareTestDb(t, 1)
	var chunks []*proto.SnapshotChunk
	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		require.NoError(t, err)
		chunks = append(chunks, &proto.SnapshotChunk{
			Term:       1,
			Name:       chunk.Name(),
			Content:    chunk.Content(),
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		})
	}
	installSnapshot := func() error {
		snapshotStream := rpc.NewMockServerSendSnapshotStream()
		for _, chunk := range chunks {
			snapshotStream.AddChunk(chunk)
		}
		close(snapshotStream.Chunks)
		return fc.InstallSnapshot(snapshotStream)
	}

	kvFactory.failNextClose.Store(true)
	assert.ErrorContains(t, installSnapshot(), "failed to close Database")
	assert.NotNil(t, fc.(*followerController).db, "the follower was left without a database")

	// The leader retries the snapshot
	require.NotPanics(t, func() { err = installSnapshot() })
	require.NoError(t, err)
	assert.Equal(t, proto.ServingStatus_FOLLOWER, fc.Status())
	assert.EqualValues(t, 1, fc.Term())
	assert.EqualValues(t, 99, fc.CommitOffset())
	for i := 0; i < 100; i++ {
		dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{
			Key:          fmt.Sprintf("key-%d", i),
			IncludeValue: true,
		})
		require.NoError(t, err)
		assert.Equal(t, proto.Status_OK, dbRes.Status)
	}

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
	fc.SetSplitHashRange(&proto.HashRange{
		Min: 0,
		Max: 0x7FFFFFFF, // 2147483647
	}, 1)

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

// A split child acks the parent's snapshot once it has filtered it, and the
// parent then only tails its log to the child: it doesn't send the snapshot
// again. So the filtered snapshot must survive a crash of the child, although
// the database runs without the Pebble WAL.
func TestFollower_SplitSnapshotFilterSurvivesCrash(t *testing.T) {
	var shardId int64
	kvOptions := kvstore.NewFactoryOptionsForTest(t)
	kvFactory, err := kvstore.NewPebbleKVFactory(kvOptions)
	require.NoError(t, err)
	walDir := t.TempDir()
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: walDir})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)
	fc.SetSplitHashRange(splitTestHashRange, 1)
	installSplitTestSnapshot(t, fc, 1)

	crashedKvFactory, crashedWalFactory := crashImage(t, kvOptions.DataDir, wal.FactoryOptions{BaseWalDir: walDir})
	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())

	// The child restarts from what was on disk when it crashed, as the parent
	// reconnects to it
	fc, err = NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, crashedWalFactory, crashedKvFactory, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 5, fc.CommitOffset())
	assertSplitTestKeys(t, fc.(*followerController).db, map[string]string{
		"a": "snapshot-a", "b": "snapshot-b", "c": "snapshot-c", "e": "snapshot-e",
	})

	assert.NoError(t, fc.Close())
	assert.NoError(t, crashedKvFactory.Close())
	assert.NoError(t, crashedWalFactory.Close())
}

// The parent's entries that a split child applies stay in its database
// memtable until the next flush. After a crash, the child applies them again
// from its log, and not always as the follower of the parent that filters
// them: e.g. the split re-elects the child at its end. They must still be
// filtered.
func TestFollower_SplitReplayAfterCrashKeepsFilter(t *testing.T) {
	var shardId int64
	kvOptions := kvstore.NewFactoryOptionsForTest(t)
	kvFactory, err := kvstore.NewPebbleKVFactory(kvOptions)
	require.NoError(t, err)
	walDir := t.TempDir()
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: walDir})

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)
	fc.SetSplitHashRange(splitTestHashRange, 1)
	installSplitTestSnapshot(t, fc, 1)

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		_ = fc.AppendEntries(stream)
		stream.Cancel()
	}()
	stream.AddRequest(createAddRequest(t, 1, 6, map[string]string{"a": "wal-a", "d": "wal-d"}, wal.InvalidOffset))
	stream.AddRequest(createAddRequest(t, 1, 7, map[string]string{"f": "wal-f", "c": "wal-c"}, 7))
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 7
	}, 10*time.Second, 10*time.Millisecond)
	expected := map[string]string{"a": "wal-a", "b": "snapshot-b", "c": "wal-c", "e": "snapshot-e"}
	assertSplitTestKeys(t, fc.(*followerController).db, expected)

	crashedKvFactory, crashedWalFactory := crashImage(t, kvOptions.DataDir, wal.FactoryOptions{BaseWalDir: walDir})
	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())

	// The child restarts, and is elected leader in a new term, as at the end
	// of the split
	lc, err := lead.NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, nil,
		crashedWalFactory, crashedKvFactory, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shardId, Term: 2})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shardId,
		Term:              2,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)
	require.NoError(t, lc.Close())

	db, err := database.NewDB(constant.DefaultNamespace, shardId, crashedKvFactory, proto.KeySortingType_UNKNOWN,
		1*time.Hour, time2.SystemClock)
	require.NoError(t, err)
	assertSplitTestKeys(t, db, expected)

	assert.NoError(t, db.Close())
	assert.NoError(t, crashedKvFactory.Close())
	assert.NoError(t, crashedWalFactory.Close())
}

// A node that observed the parent for a split child can stay a follower of the
// child in a later term, e.g. when the split elects the child on another node.
// From then on, it gets the child's own entries and snapshots, which must not
// be filtered: e.g. the child's session records hash anywhere in the hash
// space. The split range only holds in the parent's term.
func TestFollower_SplitHashRangeEndsAtNewTerm(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)
	fc.SetSplitHashRange(splitTestHashRange, 1)
	installSplitTestSnapshot(t, fc, 1)

	// A late stream of the parent, in the parent's term, doesn't set the range
	// again either
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 2})
	require.NoError(t, err)
	fc.SetSplitHashRange(splitTestHashRange, 1)

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		_ = fc.AppendEntries(stream)
		stream.Cancel()
	}()
	stream.AddRequest(createAddRequest(t, 2, 6, map[string]string{"a": "child-a", "d": "child-d"}, 6))
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 6
	}, 10*time.Second, 10*time.Millisecond)
	fci := fc.(*followerController)
	assertSplitTestKeys(t, fci.db, map[string]string{
		"a": "child-a", "b": "snapshot-b", "c": "snapshot-c", "d": "child-d", "e": "snapshot-e",
	})

	close(stream.Requests)
	assert.Eventually(t, func() bool {
		return !closeChanIsNotNil(fc)()
	}, 10*time.Second, 10*time.Millisecond)
	installSplitTestSnapshot(t, fc, 2)
	assertSplitTestKeys(t, fci.db, map[string]string{
		"a": "snapshot-a", "b": "snapshot-b", "c": "snapshot-c", "d": "snapshot-d", "e": "snapshot-e", "f": "snapshot-f",
	})
	assert.Nil(t, fci.db.SplitFilter())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// splitTestHashRange is the lower half of the hash space: of the keys a..f, it
// keeps a, b, c and e (see TestFollower_SplitHashRangeFiltering).
var splitTestHashRange = &proto.HashRange{Min: 0, Max: 0x7FFFFFFF}

// installSplitTestSnapshot installs, at the given term, the snapshot of a shard
// holding the keys a..f, written at the offsets 0..5.
func installSplitTestSnapshot(t *testing.T, fc FollowerController, term int64) {
	t.Helper()

	parentKvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	parentDb, err := database.NewDB(constant.DefaultNamespace, 0, parentKvFactory, proto.KeySortingType_HIERARCHICAL,
		1*time.Hour, time2.SystemClock)
	require.NoError(t, err)
	for i, key := range []string{"a", "b", "c", "d", "e", "f"} {
		_, err := parentDb.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{Key: key, Value: []byte("snapshot-" + key)}},
		}, int64(i), 0, database.NoOpCallback)
		require.NoError(t, err)
	}
	require.NoError(t, parentDb.UpdateTerm(term, database.TermOptions{}))
	snapshot, err := parentDb.Snapshot()
	require.NoError(t, err)

	stream := rpc.NewMockServerSendSnapshotStream()
	wg := sync.WaitGroup{}
	wg.Go(func() {
		assert.NoError(t, fc.InstallSnapshot(stream))
	})
	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		require.NoError(t, err)
		stream.AddChunk(&proto.SnapshotChunk{
			Term:       term,
			Name:       chunk.Name(),
			Content:    chunk.Content(),
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		})
	}
	close(stream.Chunks)
	wg.Wait()

	assert.NoError(t, snapshot.Close())
	assert.NoError(t, parentDb.Close())
	assert.NoError(t, parentKvFactory.Close())
}

// The follower applies the entries to its database, which runs without the
// Pebble WAL: they are durable only once the database gets flushed. Past the
// retention, the trimming deletes the WAL segments of the applied entries: after
// a crash, the database must still hold them, as the follower can't apply them
// again.
func TestFollower_CrashAfterWalTrim(t *testing.T) {
	var shardId int64
	kvOptions := kvstore.NewFactoryOptionsForTest(t)
	kvFactory, err := kvstore.NewPebbleKVFactory(kvOptions)
	require.NoError(t, err)
	clock := &time2.MockedClock{}
	walOptions := wal.FactoryOptions{
		BaseWalDir:           t.TempDir(),
		SegmentSize:          4 * 1024,
		Clock:                clock,
		TrimmerCheckInterval: 10 * time.Millisecond,
	}
	walFactory := wal.NewWalFactory(&walOptions)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId,
		walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		_ = fc.AppendEntries(stream)
		stream.Cancel()
	}()
	// The entries fill a few WAL segments, and a small part of a database
	// memtable
	value := strings.Repeat("v", 500)
	for offset := int64(0); offset <= 20; offset++ {
		stream.AddRequest(createAddRequest(t, 1, offset, map[string]string{fmt.Sprintf("key-%02d", offset): value}, offset-1))
	}
	require.Eventually(t, func() bool {
		return fc.CommitOffset() == 19
	}, 10*time.Second, 10*time.Millisecond)

	// Hours later, the trimming deletes the segments of the first entries
	clock.Set((2 * time.Hour).Milliseconds())
	followerWal := fc.(*followerController).wal
	require.Eventually(t, func() bool {
		return followerWal.FirstOffset() == 19
	}, 10*time.Second, 10*time.Millisecond)

	// The follower crashes, and restarts from what was on disk
	crashedKvFactory, crashedWalFactory := crashImage(t, kvOptions.DataDir, walOptions)
	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())

	fc, err = NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId,
		crashedWalFactory, crashedKvFactory, nil)
	require.NoError(t, err)
	assert.Positive(t, fc.(*followerController).wal.FirstOffset())
	assert.EqualValues(t, 19, fc.CommitOffset())

	// It applies the entries of the next term
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 2})
	require.NoError(t, err)
	stream = rpc.NewMockServerReplicateStream()
	go func() {
		_ = fc.AppendEntries(stream)
		stream.Cancel()
	}()
	stream.AddRequest(createAddRequest(t, 2, 21, map[string]string{"key-21": value}, 21))
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 21
	}, 10*time.Second, 10*time.Millisecond)
	for offset := 0; offset <= 21; offset++ {
		res, err := fc.(*followerController).db.Get(&proto.GetRequest{Key: fmt.Sprintf("key-%02d", offset)})
		require.NoError(t, err)
		assert.Equalf(t, proto.Status_OK, res.Status, "key-%02d", offset)
	}

	assert.NoError(t, fc.Close())
	assert.NoError(t, crashedKvFactory.Close())
	assert.NoError(t, crashedWalFactory.Close())
}

// crashImage copies the data directories of a running shard, and returns the
// factories to open the copies with, using walOptions apart from the directory:
// the copies hold what a crash of the node would leave on disk. The database
// runs without the Pebble WAL, so they miss the database writes that are still
// in the memtable.
func crashImage(t *testing.T, kvDataDir string, walOptions wal.FactoryOptions) (kvstore.Factory, wal.Factory) {
	t.Helper()

	kvOptions := kvstore.NewFactoryOptionsForTest(t)
	copyRunningDir(t, kvDataDir, kvOptions.DataDir)
	kvFactory, err := kvstore.NewPebbleKVFactory(kvOptions)
	require.NoError(t, err)

	crashedWalDir := t.TempDir()
	copyRunningDir(t, walOptions.BaseWalDir, crashedWalDir)
	walOptions.BaseWalDir = crashedWalDir
	return kvFactory, wal.NewWalFactory(&walOptions)
}

// copyRunningDir copies the files in src to dst. Pebble deletes its obsolete
// files in the background: a file that is gone by the time it is copied is
// skipped, as a crash right after its deletion would leave it.
func copyRunningDir(t *testing.T, src string, dst string) {
	t.Helper()

	require.NoError(t, filepath.WalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(filepath.Join(dst, relPath), 0755)
		}
		content, err := os.ReadFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, relPath), content, 0644)
	}))
}

// assertSplitTestKeys checks the keys a..f in the database of the child that
// keeps splitTestHashRange: it has the expected values of the keys in range,
// and none of the others.
func assertSplitTestKeys(t *testing.T, db database.DB, expected map[string]string) {
	t.Helper()

	for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
		res, err := db.Get(&proto.GetRequest{Key: key, IncludeValue: true})
		require.NoError(t, err)
		if value, ok := expected[key]; ok {
			assert.Equalf(t, proto.Status_OK, res.Status, "key %q", key)
			assert.Equalf(t, value, string(res.Value), "key %q", key)
		} else {
			assert.Equalf(t, proto.Status_KEY_NOT_FOUND, res.Status,
				"key %q is out of the hash range of the child, found %q", key, res.Value)
		}
	}
}

// A write request that can't be applied has no effect, like on the leader,
// which answered the client with the error: the follower must apply its entry
// and move on, instead of retrying it forever.
func TestFollower_RejectedWrite(t *testing.T) {
	for _, test := range []struct {
		name     string
		rejected *proto.WriteRequest
	}{{
		// Fewer deltas than the parts of the sequence
		name: "missing-sequence-deltas",
		rejected: &proto.WriteRequest{Puts: []*proto.PutRequest{{
			Key:              "s",
			Value:            []byte("1"),
			PartitionKey:     pb.String("s"),
			SequenceKeyDelta: []uint64{1},
		}}},
	}, {
		// Without an end, the range reaches the notification records
		name: "delete-range-without-end",
		rejected: &proto.WriteRequest{DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "a",
		}}},
	}} {
		t.Run(test.name, func(t *testing.T) {
			kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			walFactory := newTestWalFactory(t)

			// The database keeps the key sorting it was created with
			db, err := database.NewDB(constant.DefaultNamespace, 0, kvFactory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
			require.NoError(t, err)
			require.NoError(t, db.Close())

			fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, 0, walFactory, kvFactory, nil)
			require.NoError(t, err)
			_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
			require.NoError(t, err)
			_, err = fc.Truncate(&proto.TruncateRequest{
				Term:        1,
				HeadEntryId: &proto.EntryId{Term: 1, Offset: wal.InvalidOffset},
			})
			require.NoError(t, err)

			stream := rpc.NewMockServerReplicateStream()
			go func() {
				_ = fc.AppendEntries(stream)
				stream.Cancel()
			}()

			stream.AddRequest(createWriteAppend(t, 1, 0, &proto.WriteRequest{Puts: []*proto.PutRequest{{
				Key:   "a",
				Value: []byte("a"),
			}, {
				Key:              "s",
				Value:            []byte("0"),
				PartitionKey:     pb.String("s"),
				SequenceKeyDelta: []uint64{1, 1},
			}}}, wal.InvalidOffset))
			stream.AddRequest(createWriteAppend(t, 1, 1, test.rejected, 0))
			stream.AddRequest(createWriteAppend(t, 1, 2, &proto.WriteRequest{Puts: []*proto.PutRequest{{
				Key:   "b",
				Value: []byte("b"),
			}}}, 2))
			for range 3 {
				stream.GetResponse()
			}

			assert.Eventually(t, func() bool {
				return fc.CommitOffset() == 2
			}, 10*time.Second, 10*time.Millisecond)

			it, err := fc.(*followerController).db.List(&proto.ListRequest{})
			require.NoError(t, err)
			var keys []string
			for ; it.Valid(); it.Next() {
				keys = append(keys, it.Key())
			}
			assert.NoError(t, it.Close())
			assert.Equal(t, []string{"a", "b", "s-00000000000000000001-00000000000000000001"}, keys)

			// The entry was applied: after a restart, the follower doesn't go
			// through it again
			assert.NoError(t, fc.Close())
			fc, err = NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, 0, walFactory, kvFactory, nil)
			require.NoError(t, err)
			assert.EqualValues(t, 2, fc.CommitOffset())

			assert.NoError(t, fc.Close())
			assert.NoError(t, kvFactory.Close())
			assert.NoError(t, walFactory.Close())
		})
	}
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

	return createWriteAppend(t, term, offset, br, commitOffset)
}

func createWriteAppend(t *testing.T, term int64, offset int64,
	br *proto.WriteRequest,
	commitOffset int64) *proto.Append {
	t.Helper()

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

// When the leader advertises cumulative-ack support, the follower coalesces
// the acks of a sync round into a single message: ack offsets are strictly
// increasing and the last one confirms the final entry.
func TestFollower_CumulativeAcks(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	assert.NoError(t, err)
	_, err = fc.Truncate(&proto.TruncateRequest{
		Term:        1,
		HeadEntryId: &proto.EntryId{Term: 1, Offset: 0},
	})
	assert.NoError(t, err)

	stream := rpc.NewMockServerReplicateStream()
	wg := concurrent.NewWaitGroup(1)
	go func() {
		_ = fc.AppendEntries(stream)
		stream.Cancel()
		wg.Done()
	}()

	const entries = 10
	for i := 0; i < entries; i++ {
		req := createAddRequest(t, 1, int64(i), map[string]string{"k": fmt.Sprintf("%d", i)}, wal.InvalidOffset)
		req.CumulativeAcksSupported = true
		stream.AddRequest(req)
	}

	last := int64(-1)
	acks := 0
	for last < entries-1 {
		response := stream.GetResponse()
		assert.Greater(t, response.Offset, last)
		last = response.Offset
		acks++
	}
	assert.EqualValues(t, entries-1, last)
	assert.LessOrEqual(t, acks, entries)

	assert.NoError(t, fc.Close())
}

func TestFollower_NewTermRejectsUnsupportedFeature(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	assert.NoError(t, err)

	// A term pinning a feature this binary does not implement must be refused
	_, err = fc.NewTerm(&proto.NewTermRequest{
		Term: 1,
		Options: &proto.NewTermOptions{
			EnableNotifications: true,
			Features:            []proto.Feature{proto.Feature(999)},
		},
	})
	assert.ErrorIs(t, err, constant.ErrUnsupportedFeatures)
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, fc.Status())

	// The follower is still usable for a term with supported features
	res, err := fc.NewTerm(&proto.NewTermRequest{
		Term: 1,
		Options: &proto.NewTermOptions{
			EnableNotifications: true,
			Features:            []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
		},
	})
	assert.NoError(t, err)
	assert.Empty(t, res.FeaturesEnabled)
	assert.Equal(t, proto.ServingStatus_FENCED, fc.Status())
	assert.EqualValues(t, 1, fc.Term())

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestFollower_InvalidNamespaceDoesNotEscapeDataDir(t *testing.T) {
	kvOptions := kvstore.NewFactoryOptionsForTest(t)
	kvFactory, err := kvstore.NewPebbleKVFactory(kvOptions)
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	fc, err := NewFollowerController(&option.StorageOptions{}, "../escaped-ns", 7, walFactory, kvFactory, nil)
	assert.ErrorContains(t, err, "invalid path traversal sequence")
	assert.Nil(t, fc)

	// The data dir is a t.TempDir(), so its parent is the per-test temp root
	_, err = os.Stat(filepath.Join(filepath.Dir(kvOptions.DataDir), "escaped-ns"))
	assert.ErrorIs(t, err, os.ErrNotExist)

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

type failingWalFactory struct{}

func (failingWalFactory) NewWal(string, int64, wal.CommitOffsetProvider) (wal.Wal, error) {
	return nil, errors.New("wal creation failed")
}

func (failingWalFactory) Close() error {
	return nil
}

func TestFollower_ClosesDatabaseOnWalFailure(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, failingWalFactory{}, kvFactory, nil)
	assert.ErrorContains(t, err, "wal creation failed")
	assert.Nil(t, fc)

	// A leaked database would still hold the Pebble lock and fail the re-open
	walFactory := newTestWalFactory(t)
	fc, err = NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	require.NoError(t, err)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// pausingWalFactory wraps the follower WAL so that a test can pause the state
// applier right after it reads the entry at pauseOffset, before it applies it.
type pausingWalFactory struct {
	wal.Factory
	pauseOffset int64
	once        sync.Once
	paused      chan struct{} // closed when the applier has read the entry
	resume      chan struct{} // closed by the test to let the applier go on
	done        chan struct{} // closed when the applier has closed the reader
}

func (f *pausingWalFactory) NewWal(namespace string, shard int64, provider wal.CommitOffsetProvider) (wal.Wal, error) {
	w, err := f.Factory.NewWal(namespace, shard, provider)
	if err != nil {
		return nil, err
	}
	return &pausingWal{Wal: w, factory: f}, nil
}

type pausingWal struct {
	wal.Wal
	factory *pausingWalFactory
}

// NewReader is only used by the follower state applier.
func (w *pausingWal) NewReader(after int64) (wal.Reader, error) {
	r, err := w.Wal.NewReader(after)
	if err != nil {
		return nil, err
	}
	return &pausingReader{Reader: r, factory: w.factory}, nil
}

type pausingReader struct {
	wal.Reader
	factory *pausingWalFactory
	paused  bool
}

func (r *pausingReader) ReadNext() (entry *proto.LogEntry, previousCrc uint32, entryCrc uint32, err error) {
	entry, previousCrc, entryCrc, err = r.Reader.ReadNext()
	if err == nil && entry.Offset == r.factory.pauseOffset {
		r.factory.once.Do(func() {
			r.paused = true
			close(r.factory.paused)
			<-r.factory.resume
		})
	}
	return entry, previousCrc, entryCrc, err
}

func (r *pausingReader) Close() error {
	if r.paused {
		defer close(r.factory.done)
	}
	return r.Reader.Close()
}

// The state applier reads a committed entry from the WAL before taking the
// lock to apply it. A snapshot installed in between replaces the WAL and the
// database: the entry read from the previous WAL must neither be applied on
// top of the snapshot nor move the commit offset back.
func TestFollower_InstallSnapshotWhileApplyingEntries(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := &pausingWalFactory{
		Factory:     newTestWalFactory(t),
		pauseOffset: 2,
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
		done:        make(chan struct{}),
	}

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	require.NoError(t, err)
	fci := fc.(*followerController)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)

	// The follower has a backlog of entries, each overwriting the same key,
	// which a previous stream appended. The leader commits them on a new
	// stream, so the applier reads them from the WAL: it applies the first
	// two, then stops right after reading the third one.
	stream := rpc.NewMockServerReplicateStream()
	appendDone := make(chan error, 1)
	go func() {
		appendDone <- fc.AppendEntries(stream)
	}()
	for i := int64(0); i < 5; i++ {
		stream.AddRequest(createAddRequest(t, 1, i, map[string]string{"k": fmt.Sprintf("v%d", i)}, wal.InvalidOffset))
	}
	for i := int64(0); i < 5; i++ {
		assert.EqualValues(t, i, stream.GetResponse().Offset)
	}
	close(stream.Requests)
	assert.NoError(t, <-appendDone)

	stream = rpc.NewMockServerReplicateStream()
	go func() {
		appendDone <- fc.AppendEntries(stream)
	}()
	stream.AddRequest(&proto.Append{Term: 1, CommitOffset: 4})
	<-walFactory.paused

	// The leader drops the replication stream. It has moved on, overwriting
	// the key further, and sends a snapshot instead.
	close(stream.Requests)
	assert.NoError(t, <-appendDone)

	leaderKvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	leaderDb, err := database.NewDB(constant.DefaultNamespace, shardId, leaderKvFactory,
		proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		_, err = leaderDb.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
			Key:   "k",
			Value: []byte(fmt.Sprintf("v%d", i)),
		}}}, i, 0, database.NoOpCallback)
		require.NoError(t, err)
	}
	require.NoError(t, leaderDb.UpdateTerm(1, database.TermOptions{}))
	snapshot, err := leaderDb.Snapshot()
	require.NoError(t, err)

	snapshotStream := rpc.NewMockServerSendSnapshotStream()
	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		require.NoError(t, err)
		snapshotStream.AddChunk(&proto.SnapshotChunk{
			Term:       1,
			Name:       chunk.Name(),
			Content:    chunk.Content(),
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
		})
	}
	close(snapshotStream.Chunks)
	require.NoError(t, fc.InstallSnapshot(snapshotStream))
	assert.NoError(t, snapshot.Close())
	assert.NoError(t, leaderDb.Close())
	assert.NoError(t, leaderKvFactory.Close())
	assert.EqualValues(t, 9, fc.CommitOffset())

	// Let the applier go on with the entry it read before the install
	close(walFactory.resume)
	<-walFactory.done

	assert.EqualValues(t, 9, fc.CommitOffset())
	dbCommitOffset, err := fci.db.ReadCommitOffset()
	assert.NoError(t, err)
	assert.EqualValues(t, 9, dbCommitOffset)
	dbRes, err := fci.db.Get(&proto.GetRequest{Key: "k", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, []byte("v9"), dbRes.Value)

	// The follower keeps applying the entries replicated after the snapshot
	stream = rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()
	stream.AddRequest(createAddRequest(t, 1, 10, map[string]string{"k": "v10"}, 10))
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 10
	}, 10*time.Second, 10*time.Millisecond)
	dbRes, err = fci.db.Get(&proto.GetRequest{Key: "k", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, []byte("v10"), dbRes.Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// A snapshot can also leave the commit offset below the bound of the pass of
// the state applier it interrupts. The pass must not take the entries of the
// stream opened after the snapshot, which are not committed up to that bound,
// and must end, so that the follower goes on applying the next committed
// entries.
func TestFollower_InstallSnapshotBelowAppliedEntries(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := &pausingWalFactory{
		Factory:     newTestWalFactory(t),
		pauseOffset: 2,
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
		done:        make(chan struct{}),
	}

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId,
		walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)

	// The leader commits up to offset 4 the entries that a previous stream
	// appended: the applier reads them from the WAL, and stops right after
	// reading the third one
	stream := rpc.NewMockServerReplicateStream()
	appendDone := make(chan error, 1)
	go func() {
		appendDone <- fc.AppendEntries(stream)
	}()
	for i := int64(0); i < 5; i++ {
		stream.AddRequest(createAddRequest(t, 1, i, map[string]string{"k": fmt.Sprintf("v%d", i)}, wal.InvalidOffset))
		assert.EqualValues(t, i, stream.GetResponse().Offset)
	}
	close(stream.Requests)
	assert.NoError(t, <-appendDone)

	stream = rpc.NewMockServerReplicateStream()
	go func() {
		appendDone <- fc.AppendEntries(stream)
	}()
	stream.AddRequest(&proto.Append{Term: 1, CommitOffset: 4})
	<-walFactory.paused
	close(stream.Requests)
	assert.NoError(t, <-appendDone)

	// The leader sends a snapshot at offset 0 instead
	leaderKvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	leaderDb, err := database.NewDB(constant.DefaultNamespace, shardId, leaderKvFactory,
		proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)
	_, err = leaderDb.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
		Key:   "k",
		Value: []byte("s0"),
	}}}, 0, 0, database.NoOpCallback)
	require.NoError(t, err)
	require.NoError(t, leaderDb.UpdateTerm(1, database.TermOptions{}))
	snapshot, err := leaderDb.Snapshot()
	require.NoError(t, err)
	snapshotStream := rpc.NewMockServerSendSnapshotStream()
	sendSnapshot(t, snapshotStream, snapshot, 1)
	require.NoError(t, fc.InstallSnapshot(snapshotStream))
	assert.NoError(t, snapshot.Close())
	assert.NoError(t, leaderDb.Close())
	assert.NoError(t, leaderKvFactory.Close())
	assert.EqualValues(t, 0, fc.CommitOffset())

	// A new stream appends the next entry, without committing it yet
	stream = rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()
	req := createAddRequest(t, 1, 1, map[string]string{"k": "v1"}, 0)
	req.CumulativeAcksSupported = true
	stream.AddRequest(req)
	assert.EqualValues(t, 1, stream.GetResponse().Offset)

	close(walFactory.resume)
	<-walFactory.done
	assert.Never(t, func() bool {
		return fc.CommitOffset() != 0
	}, 500*time.Millisecond, 10*time.Millisecond)

	// A stuck applier would make fc.Close() hang
	stream.AddRequest(&proto.Append{Term: 1, CommitOffset: 1})
	require.Eventually(t, func() bool {
		return fc.CommitOffset() == 1
	}, 10*time.Second, 10*time.Millisecond)
	dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{Key: "k", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), dbRes.Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// countingWalFactory wraps the follower WAL to count how many times the state
// applier reads each entry.
type countingWalFactory struct {
	wal.Factory
	sync.Mutex
	reads map[int64]int
}

func (f *countingWalFactory) NewWal(namespace string, shard int64, provider wal.CommitOffsetProvider) (wal.Wal, error) {
	w, err := f.Factory.NewWal(namespace, shard, provider)
	if err != nil {
		return nil, err
	}
	return &countingWal{Wal: w, factory: f}, nil
}

func (f *countingWalFactory) readsOf(offset int64) int {
	f.Lock()
	defer f.Unlock()
	return f.reads[offset]
}

type countingWal struct {
	wal.Wal
	factory *countingWalFactory
}

// NewReader is only used by the follower state applier.
func (w *countingWal) NewReader(after int64) (wal.Reader, error) {
	r, err := w.Wal.NewReader(after)
	if err != nil {
		return nil, err
	}
	return &countingReader{Reader: r, factory: w.factory}, nil
}

type countingReader struct {
	wal.Reader
	factory *countingWalFactory
}

func (r *countingReader) ReadNext() (entry *proto.LogEntry, previousCrc uint32, entryCrc uint32, err error) {
	entry, previousCrc, entryCrc, err = r.Reader.ReadNext()
	if err == nil {
		r.factory.Lock()
		r.factory.reads[entry.Offset]++
		r.factory.Unlock()
	}
	return entry, previousCrc, entryCrc, err
}

func newCountingFollower(t *testing.T) (FollowerController, kvstore.Factory, *countingWalFactory) {
	t.Helper()
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := &countingWalFactory{
		Factory: newTestWalFactory(t),
		reads:   map[int64]int{},
	}

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId, walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)
	_, err = fc.Truncate(&proto.TruncateRequest{
		Term:        1,
		HeadEntryId: &proto.EntryId{Term: 1, Offset: wal.InvalidOffset},
	})
	require.NoError(t, err)
	return fc, kvFactory, walFactory
}

// The follower applies the entries it receives from the leader as they are,
// without reading them back from the WAL.
func TestFollower_AppliesReceivedEntriesWithoutReadingWal(t *testing.T) {
	fc, kvFactory, walFactory := newCountingFollower(t)

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	// One write at a time: each entry carries the commit offset of the entry
	// before it, which the follower applies before the next entry comes
	const entries = 10
	for i := int64(0); i < entries; i++ {
		stream.AddRequest(createAddRequest(t, 1, i, map[string]string{"k": fmt.Sprintf("v%d", i)}, i-1))
		assert.EqualValues(t, i, stream.GetResponse().Offset)
		assert.Eventually(t, func() bool {
			return fc.CommitOffset() == i-1
		}, 10*time.Second, 10*time.Millisecond)
	}
	// Then commit the last entry, with no new entry to carry the commit offset
	stream.AddRequest(&proto.Append{Term: 1, CommitOffset: entries - 1})
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == entries-1
	}, 10*time.Second, 10*time.Millisecond)

	for i := int64(0); i < entries; i++ {
		assert.Zerof(t, walFactory.readsOf(i), "reads of the entry at offset %d", i)
	}
	dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{Key: "k", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, []byte(fmt.Sprintf("v%d", entries-1)), dbRes.Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// The state applier reads from the WAL the entries that the stream doesn't
// keep, e.g. those appended by a previous stream. The WAL head is then past
// the commit offset: the state applier must stop at the commit offset, as
// reading on would only read the next entry to discard it, and the next pass
// would read it again.
func TestFollower_ReadsEachCommittedEntryOnce(t *testing.T) {
	fc, kvFactory, walFactory := newCountingFollower(t)

	const entries = 10
	stream := rpc.NewMockServerReplicateStream()
	appendDone := make(chan error, 1)
	go func() {
		appendDone <- fc.AppendEntries(stream)
	}()
	for i := int64(0); i < entries; i++ {
		stream.AddRequest(createAddRequest(t, 1, i, map[string]string{"k": fmt.Sprintf("v%d", i)}, wal.InvalidOffset))
		assert.EqualValues(t, i, stream.GetResponse().Offset)
	}
	close(stream.Requests)
	assert.NoError(t, <-appendDone)

	// A new stream commits the entries one at a time
	stream = rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()
	for i := int64(0); i < entries; i++ {
		stream.AddRequest(&proto.Append{Term: 1, CommitOffset: i})
		assert.Eventually(t, func() bool {
			return fc.CommitOffset() == i
		}, 10*time.Second, 10*time.Millisecond)
	}

	for i := int64(0); i < entries; i++ {
		assert.Equalf(t, 1, walFactory.readsOf(i), "reads of the entry at offset %d", i)
	}
	dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{Key: "k", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, []byte(fmt.Sprintf("v%d", entries-1)), dbRes.Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// The stream keeps a bounded number of entries: the state applier reads the
// entries past the bound from the WAL, once.
func TestFollower_ReadsEntriesPastAppendedBoundFromWal(t *testing.T) {
	fc, kvFactory, walFactory := newCountingFollower(t)

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()

	// The leader commits the entries only once it has sent them all
	const entries = maxAppendedEntries + 10
	for i := int64(0); i < entries; i++ {
		stream.AddRequest(createAddRequest(t, 1, i, map[string]string{"k": fmt.Sprintf("v%d", i)}, wal.InvalidOffset))
		assert.EqualValues(t, i, stream.GetResponse().Offset)
	}
	stream.AddRequest(&proto.Append{Term: 1, CommitOffset: entries - 1})
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == entries-1
	}, 10*time.Second, 10*time.Millisecond)

	for i := int64(0); i < entries; i++ {
		expectedReads := 0
		if i >= maxAppendedEntries {
			expectedReads = 1
		}
		assert.Equalf(t, expectedReads, walFactory.readsOf(i), "reads of the entry at offset %d", i)
	}
	dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{Key: "k", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, []byte(fmt.Sprintf("v%d", entries-1)), dbRes.Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// laggingSyncWalFactory wraps the follower WAL so that a test can hold back
// the offset it reports as synced, like a slow disk.
type laggingSyncWalFactory struct {
	wal.Factory
	maxSyncedOffset atomic.Int64
}

func (f *laggingSyncWalFactory) NewWal(namespace string, shard int64,
	provider wal.CommitOffsetProvider) (wal.Wal, error) {
	w, err := f.Factory.NewWal(namespace, shard, provider)
	if err != nil {
		return nil, err
	}
	return &laggingSyncWal{Wal: w, factory: f}, nil
}

type laggingSyncWal struct {
	wal.Wal
	factory *laggingSyncWalFactory
}

func (w *laggingSyncWal) LastOffset() int64 {
	return min(w.Wal.LastOffset(), w.factory.maxSyncedOffset.Load())
}

// The leader can commit an entry with the other followers before this follower
// syncs it: the follower applies the entry only once synced, as if it read it
// from the WAL.
func TestFollower_AppliesEntriesOnceSynced(t *testing.T) {
	var shardId int64
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := &laggingSyncWalFactory{Factory: newTestWalFactory(t)}
	walFactory.maxSyncedOffset.Store(0)

	fc, err := NewFollowerController(&option.StorageOptions{}, constant.DefaultNamespace, shardId,
		walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Term: 1})
	require.NoError(t, err)

	stream := rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()
	stream.AddRequest(createAddRequest(t, 1, 0, map[string]string{"k": "v0"}, wal.InvalidOffset))
	stream.AddRequest(createAddRequest(t, 1, 1, map[string]string{"k": "v1"}, 1))
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 0
	}, 10*time.Second, 10*time.Millisecond)
	assert.Never(t, func() bool {
		return fc.CommitOffset() == 1
	}, 500*time.Millisecond, 10*time.Millisecond)

	walFactory.maxSyncedOffset.Store(math.MaxInt64)
	stream.AddRequest(&proto.Append{Term: 1, CommitOffset: 1})
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 1
	}, 10*time.Second, 10*time.Millisecond)
	dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{Key: "k", IncludeValue: true})
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), dbRes.Value)

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// A new leader can truncate the entries that a stream appended, and replace
// them with its own: the state applier must apply the entries of the new
// leader, not the ones the closed stream kept. It reads from the WAL the
// entries it kept from the previous leader, and only those.
func TestFollower_AppliesEntriesReplacedAfterTruncate(t *testing.T) {
	fc, kvFactory, walFactory := newCountingFollower(t)

	stream := rpc.NewMockServerReplicateStream()
	appendDone := make(chan error, 1)
	go func() {
		appendDone <- fc.AppendEntries(stream)
	}()
	for i := int64(0); i < 5; i++ {
		stream.AddRequest(createAddRequest(t, 1, i, map[string]string{fmt.Sprintf("k%d", i): "term-1"}, wal.InvalidOffset))
		assert.EqualValues(t, i, stream.GetResponse().Offset)
	}
	close(stream.Requests)
	assert.NoError(t, <-appendDone)

	// The new leader only has the first two entries
	_, err := fc.NewTerm(&proto.NewTermRequest{Term: 2})
	require.NoError(t, err)
	_, err = fc.Truncate(&proto.TruncateRequest{
		Term:        2,
		HeadEntryId: &proto.EntryId{Term: 1, Offset: 1},
	})
	require.NoError(t, err)

	stream = rpc.NewMockServerReplicateStream()
	go func() {
		// cancelled due to fc.Close() below
		assert.ErrorIs(t, fc.AppendEntries(stream), context.Canceled)
		stream.Cancel()
	}()
	for i := int64(2); i < 5; i++ {
		stream.AddRequest(createAddRequest(t, 2, i, map[string]string{fmt.Sprintf("k%d", i): "term-2"}, 4))
		assert.EqualValues(t, i, stream.GetResponse().Offset)
	}
	assert.Eventually(t, func() bool {
		return fc.CommitOffset() == 4
	}, 10*time.Second, 10*time.Millisecond)

	for i := 0; i < 5; i++ {
		expected, expectedReads := "term-1", 1
		if i >= 2 {
			expected, expectedReads = "term-2", 0
		}
		dbRes, err := fc.(*followerController).db.Get(&proto.GetRequest{Key: fmt.Sprintf("k%d", i), IncludeValue: true})
		assert.NoError(t, err)
		assert.Equalf(t, []byte(expected), dbRes.Value, "value of k%d", i)
		assert.Equalf(t, expectedReads, walFactory.readsOf(int64(i)), "reads of the entry at offset %d", i)
	}

	assert.NoError(t, fc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}
