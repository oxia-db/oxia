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

package lead

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
)

func TestFollowerCursor_SplitMetadata(t *testing.T) {
	childRange := &proto.Int32HashRange{MinHashInclusive: 0, MaxHashInclusive: 100}
	parentRange := &proto.Int32HashRange{MinHashInclusive: 0, MaxHashInclusive: 200}
	for _, test := range []struct {
		name     string
		cursor   *followerCursor
		expected map[string]string
	}{
		{"follower", &followerCursor{}, map[string]string{}},
		{"observer without the parent range", &followerCursor{splitHashRange: childRange}, map[string]string{
			constant.MetadataSplitHashRangeMin: "0",
			constant.MetadataSplitHashRangeMax: "100",
		}},
		{"observer", &followerCursor{splitHashRange: childRange, splitParentHashRange: parentRange}, map[string]string{
			constant.MetadataSplitHashRangeMin:       "0",
			constant.MetadataSplitHashRangeMax:       "100",
			constant.MetadataSplitParentHashRangeMin: "0",
			constant.MetadataSplitParentHashRangeMax: "200",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			md, _ := metadata.FromOutgoingContext(test.cursor.withSplitMetadata(context.Background()))
			actual := map[string]string{}
			for key, values := range md {
				actual[key] = values[0]
			}
			assert.Equal(t, test.expected, actual)
		})
	}
}

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
	assert.NoError(t, w.Close())
	assert.NoError(t, wf.Close())
	assert.NoError(t, db.Close())
	assert.NoError(t, kvf.Close())
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

	// Trim the WAL: an empty follower can only be brought up to date through
	// a snapshot when the first entries are no longer available in the WAL
	reader, err := w.NewReverseReader()
	assert.NoError(t, err)
	_, _, lastCrc, err := reader.ReadNext()
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())
	assert.NoError(t, w.Clear())

	// Append one more entry so there is something to stream after the snapshot
	assert.NoError(t, w.AppendAsyncWithPreviousCrc(&proto.LogEntry{
		Term:      1,
		Offset:    n,
		Value:     []byte("post-snapshot"),
		Timestamp: uint64(n),
	}, &lastCrc))
	assert.NoError(t, w.Sync(context.Background()))

	ackTracker := NewQuorumAckTracker(3, n, n-1)

	fc, err := NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, stream, ackTracker, w, db, wal.InvalidOffset)
	assert.NoError(t, err)

	s := stream.SendSnapshotStream
	for req := range s.Requests {
		assert.EqualValues(t, 1, req.Term)
	}

	slog.Info("Snapshot complete")

	s.Response <- &proto.SnapshotResponse{AckOffset: n - 1}

	// Wait for the first Append message after the snapshot
	var firstAppend *proto.Append
	assert.Eventually(t, func() bool {
		select {
		case req := <-stream.AppendReqs:
			firstAppend = req
			return true
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)

	// The first Append after snapshot must carry the WAL CRC at the snapshot offset
	assert.NotNil(t, firstAppend.PreviousEntryCrc,
		"first Append after snapshot must have PreviousEntryCrc set")
	assert.NotZero(t, firstAppend.GetPreviousEntryCrc(),
		"first Append after snapshot must have non-zero PreviousEntryCrc")
	assert.EqualValues(t, n, firstAppend.Entry.Offset)

	assert.NoError(t, fc.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, wf.Close())
	assert.NoError(t, db.Close())
	assert.NoError(t, kvf.Close())
}

// A follower that holds every entry before the first one of the leader's wal,
// like a follower seeded from a snapshot at that offset, gets the next entries
// from the wal. Another snapshot would only wipe the data it has until it is
// installed.
func TestFollowerCursor_StreamToFollowerRightBeforeWalStart(t *testing.T) {
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

	// The entries up to n-1 are only in the database, and the wal starts at n
	for i := int64(0); i < n; i++ {
		wr := &proto.WriteRequest{
			Shard: &shard,
			Puts:  []*proto.PutRequest{{Key: fmt.Sprintf("key-%d", i), Value: []byte("value")}},
		}
		_, err := db.ProcessWrite(wr, i, uint64(i), database.NoOpCallback)
		assert.NoError(t, err)
	}
	previousCrc := uint32(1)
	assert.NoError(t, w.AppendAsyncWithPreviousCrc(&proto.LogEntry{
		Term:      1,
		Offset:    n,
		Value:     []byte("after-snapshot"),
		Timestamp: uint64(n),
	}, &previousCrc))
	assert.NoError(t, w.Sync(context.Background()))

	ackTracker := NewQuorumAckTracker(3, n, n-1)
	fc, err := NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, stream, ackTracker, w, db, n-1)
	assert.NoError(t, err)

	select {
	case req := <-stream.AppendReqs:
		assert.EqualValues(t, n, req.Entry.Offset)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the follower got no entry")
	}
	assert.Empty(t, stream.SendSnapshotStream.Requests, "the follower got a snapshot")

	// Complete a snapshot in progress: closing the cursor waits for it
	stream.SendSnapshotStream.Response <- &proto.SnapshotResponse{AckOffset: n - 1}
	assert.NoError(t, fc.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, wf.Close())
	assert.NoError(t, db.Close())
	assert.NoError(t, kvf.Close())
}

func TestFollowerCursor_StreamToEmptyFollowerWhenWalComplete(t *testing.T) {
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

	// Load some entries into the db & wal, keeping the full history in the WAL
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

	// The WAL still contains the full history: the empty follower must be
	// brought up to date by tailing the log, not with a snapshot, so that its
	// own WAL gets populated and it stays eligible for leader election
	for i := int64(0); i < n; i++ {
		select {
		case req := <-stream.AppendReqs:
			assert.EqualValues(t, 1, req.Term)
			assert.EqualValues(t, i, req.Entry.Offset)
		case <-time.After(10 * time.Second):
			t.Fatalf("did not receive the append for offset %d", i)
		}
	}
	assert.Empty(t, stream.SendSnapshotStream.Requests)

	assert.NoError(t, fc.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, wf.Close())
	assert.NoError(t, db.Close())
	assert.NoError(t, kvf.Close())
}

func TestFollowerCursor_CloseWaitsForInFlightSnapshot(t *testing.T) {
	var term int64 = 1
	var shard int64 = 2

	kvf, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := database.NewDB(constant.DefaultNamespace, shard, kvf, proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)
	wf := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})
	w, err := wf.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	wr := &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "key",
			Value: []byte("value"),
		}},
	}
	// Keep the WAL empty: the empty follower can then only be brought up to
	// date through a snapshot
	_, err = db.ProcessWrite(wr, 0, 0, database.NoOpCallback)
	assert.NoError(t, err)

	stream := newBlockingSnapshotStream()
	provider := &blockingSnapshotProvider{stream: stream}
	ackTracker := NewQuorumAckTracker(3, 0, 0)

	var fc FollowerCursor
	t.Cleanup(func() {
		stream.unblock()
		if fc != nil {
			assert.NoError(t, fc.Close())
		}
		assert.NoError(t, w.Close())
		assert.NoError(t, wf.Close())
		assert.NoError(t, db.Close())
		assert.NoError(t, kvf.Close())
	})

	fc, err = NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, provider, ackTracker, w, db, wal.InvalidOffset)
	assert.NoError(t, err)

	// The blocked Send call is after db.Snapshot() succeeds, so the cursor still
	// owns Pebble snapshot references. Close must not return before that sender
	// exits, otherwise the leader can close the DB with outstanding references.
	select {
	case <-stream.sendStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for snapshot send to start")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- fc.Close()
	}()

	select {
	case <-stream.Context().Done():
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for cursor close to cancel snapshot context")
	}

	select {
	case err := <-closeDone:
		assert.NoError(t, err)
		t.Fatal("Close returned before the snapshot sender exited")
	case <-time.After(100 * time.Millisecond):
	}

	stream.unblock()

	select {
	case err := <-closeDone:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for cursor close")
	}

	fc = nil
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

type blockingSnapshotProvider struct {
	stream *blockingSnapshotStream
}

func (*blockingSnapshotProvider) GetReplicateStream(context.Context, string, string, int64, int64) (proto.OxiaLogReplication_ReplicateClient, error) {
	return nil, errors.New("unexpected replicate stream")
}

func (p *blockingSnapshotProvider) SendSnapshot(ctx context.Context, _ string, _ string, _ int64, _ int64) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	p.stream.ctx = ctx
	return p.stream, nil
}

type blockingSnapshotStream struct {
	ctx         context.Context
	sendStarted chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

func newBlockingSnapshotStream() *blockingSnapshotStream {
	return &blockingSnapshotStream{
		sendStarted: make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (s *blockingSnapshotStream) unblock() {
	s.releaseOnce.Do(func() {
		close(s.release)
	})
}

func (s *blockingSnapshotStream) Send(*proto.SnapshotChunk) error {
	s.startOnce.Do(func() {
		close(s.sendStarted)
	})
	<-s.release
	if s.ctx != nil && s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	return context.Canceled
}

func (*blockingSnapshotStream) CloseAndRecv() (*proto.SnapshotResponse, error) {
	return nil, context.Canceled
}

func (*blockingSnapshotStream) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (*blockingSnapshotStream) Trailer() metadata.MD {
	return nil
}

func (*blockingSnapshotStream) CloseSend() error {
	return nil
}

func (s *blockingSnapshotStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (*blockingSnapshotStream) SendMsg(any) error {
	return nil
}

func (*blockingSnapshotStream) RecvMsg(any) error {
	return io.EOF
}

// An observer cursor parked at the head of the wal must advertise a commit
// offset advancement with an entry-less Append: with no more writes arriving,
// the observer (split child) would otherwise never learn the commit offset and
// the split catch-up phase would stall.
func TestObserverFollowerCursor_AdvertisesCommitOffsetAtHead(t *testing.T) {
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

	assert.NoError(t, w.Append(&proto.LogEntry{
		Term:   1,
		Offset: 0,
		Value:  []byte("v1"),
	}))

	// A regular follower acking through the quorum tracker
	regular, err := ackTracker.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	fc, err := NewObserverFollowerCursor("child-1", term, constant.DefaultNamespace, shard, stream, ackTracker,
		w, db, wal.InvalidOffset, &proto.Int32HashRange{MinHashInclusive: 0, MaxHashInclusive: 100}, nil)
	assert.NoError(t, err)

	// The observer streams entry 0 with the commit offset of that moment (-1)
	// and parks at the head of the wal
	req := <-stream.AppendReqs
	assert.EqualValues(t, 0, req.Entry.Offset)
	assert.Equal(t, wal.InvalidOffset, req.CommitOffset)

	// The quorum ack advances the commit offset with no new writes
	ackTracker.AdvanceHeadOffset(0)
	regular.Ack(0)
	assert.EqualValues(t, 0, ackTracker.CommitOffset())

	// The parked observer sends the advertisement on its own
	req = <-stream.AppendReqs
	assert.Nil(t, req.Entry)
	assert.EqualValues(t, 0, req.CommitOffset)
	assert.EqualValues(t, term, req.Term)

	// No repeated advertisement while nothing advances
	time.Sleep(100 * time.Millisecond)
	select {
	case req = <-stream.AppendReqs:
		t.Fatalf("unexpected append: %+v", req)
	default:
		// Expected. There should be nothing in the channel
	}

	assert.NoError(t, fc.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, wf.Close())
	assert.NoError(t, db.Close())
	assert.NoError(t, kvf.Close())
}
