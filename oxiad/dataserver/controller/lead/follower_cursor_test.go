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

package lead

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/proto"
)

func TestFollowerCursor(t *testing.T) {
	var term int64 = 1
	var shard int64 = 2

	stream := rpc.NewMockRpcClient()
	ackTracker := NewQuorumAckTracker(3, wal.InvalidOffset, wal.InvalidOffset)
	kvf, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := database.NewDB(constant.DefaultNamespace, shard, kvf, proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)
	wf := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})
	w, err := wf.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	err = w.Append(&proto.LogEntry{
		Term:   1,
		Offset: 0,
		Value:  []byte("v1"),
	})
	assert.NoError(t, err)
	slog.Info("Appended entry 0 to the log")

	fc, err := NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, stream, ackTracker, w, db, wal.InvalidOffset)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, shard, fc.ShardId())
	// NOTE: lastPushed will be -1 or 0, because when FollowerCursor is initialized,
	// it will start to `streamEntries` and maybe make it advanced to 0.
	assert.True(t, func() bool {
		lastPushed := fc.LastPushed()
		return lastPushed == wal.InvalidOffset || lastPushed == 0
	}())
	assert.Equal(t, wal.InvalidOffset, fc.AckOffset())

	assert.Eventually(t, func() bool {
		return fc.LastPushed() == 0
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, wal.InvalidOffset, fc.AckOffset())

	ackTracker.AdvanceHeadOffset(0)

	assert.Equal(t, wal.InvalidOffset, fc.AckOffset())

	// The follower is acking back
	req := <-stream.AppendReqs
	assert.EqualValues(t, 1, req.Term)
	assert.Equal(t, wal.InvalidOffset, req.CommitOffset)

	stream.AckResps <- &proto.Ack{
		Offset: 0,
	}

	assert.Eventually(t, func() bool {
		return fc.AckOffset() == 0
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 0, ackTracker.CommitOffset())

	// Next entry should carry the correct commit offset
	err = w.Append(&proto.LogEntry{
		Term:   1,
		Offset: 1,
		Value:  []byte("v2"),
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 0, fc.LastPushed())
	assert.EqualValues(t, 0, fc.AckOffset())

	ackTracker.AdvanceHeadOffset(1)

	assert.Eventually(t, func() bool {
		return fc.LastPushed() == 1
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 0, fc.AckOffset())

	req = <-stream.AppendReqs
	assert.EqualValues(t, 1, req.Term)
	assert.EqualValues(t, 1, req.Entry.Term)
	assert.EqualValues(t, 1, req.Entry.Offset)
	assert.EqualValues(t, 0, req.CommitOffset)

	assert.NoError(t, fc.Close())
}

func TestFollowerCursor_SendSnapshot(t *testing.T) {
	var term int64 = 1
	var shard int64 = 2

	n := int64(10)
	stream := rpc.NewMockRpcClient()
	kvf, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := database.NewDB(constant.DefaultNamespace, shard, kvf, proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)
	wf := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})
	w, err := wf.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	// Load some entries into the db & wal
	for i := int64(0); i < n; i++ {
		wr := &proto.WriteRequest{
			Shard: &shard,
			Puts: []*proto.PutRequest{{
				Key:   fmt.Sprintf("key-%d", i),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			}},
		}
		e, _ := pb.Marshal(wrapInLogEntryValue(wr))
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:      1,
			Offset:    i,
			Value:     e,
			Timestamp: uint64(i),
		}))

		_, err := db.ProcessWrite(wr, i, uint64(i), database.NoOpCallback)
		assert.NoError(t, err)
	}

	ackTracker := NewQuorumAckTracker(3, n-1, n-1)

	fc, err := NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, stream, ackTracker, w, db, wal.InvalidOffset)
	assert.NoError(t, err)

	s := stream.SendSnapshotStream
	for req := range s.Requests {
		assert.EqualValues(t, 1, req.Term)
	}

	slog.Info("Snapshot complete")

	s.Response <- &proto.SnapshotResponse{AckOffset: n - 1}

	assert.Eventually(t, func() bool {
		return fc.AckOffset() == n-1
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, fc.Close())
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
