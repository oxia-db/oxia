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
	"sync/atomic"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/failpoint"
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

	fc, err := NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, stream, ackTracker, w, db, wal.InvalidOffset, false)
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

	fc.Close()
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
	_, err = w.AppendAsyncWithPreviousCrc(&proto.LogEntry{
		Term:      1,
		Offset:    n,
		Value:     []byte("post-snapshot"),
		Timestamp: uint64(n),
	}, &lastCrc)
	assert.NoError(t, err)
	assert.NoError(t, w.Sync(context.Background()))

	ackTracker := NewQuorumAckTracker(3, n, n-1)

	fc, err := NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, stream, ackTracker, w, db, wal.InvalidOffset, false)
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

	fc.Close()
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
	_, err = w.AppendAsyncWithPreviousCrc(&proto.LogEntry{
		Term:      1,
		Offset:    n,
		Value:     []byte("after-snapshot"),
		Timestamp: uint64(n),
	}, &previousCrc)
	assert.NoError(t, err)
	assert.NoError(t, w.Sync(context.Background()))

	ackTracker := NewQuorumAckTracker(3, n, n-1)
	fc, err := NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, stream, ackTracker, w, db, n-1, false)
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
	fc.Close()
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

	fc, err := NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, stream, ackTracker, w, db, wal.InvalidOffset, false)
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

	fc.Close()
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
			fc.Close()
		}
		assert.NoError(t, w.Close())
		assert.NoError(t, wf.Close())
		assert.NoError(t, db.Close())
		assert.NoError(t, kvf.Close())
	})

	fc, err = NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, provider, ackTracker, w, db, wal.InvalidOffset, false)
	assert.NoError(t, err)

	// The blocked Send call is after db.Snapshot() succeeds, so the cursor still
	// owns Pebble snapshot references. Close must not return before that sender
	// exits, otherwise the leader can close the DB with outstanding references.
	select {
	case <-stream.sendStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for snapshot send to start")
	}

	closeDone := make(chan struct{})
	go func() {
		fc.Close()
		close(closeDone)
	}()

	select {
	case <-stream.Context().Done():
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for cursor close to cancel snapshot context")
	}

	select {
	case <-closeDone:
		t.Fatal("Close returned before the snapshot sender exited")
	case <-time.After(100 * time.Millisecond):
	}

	stream.unblock()

	select {
	case <-closeDone:
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
	// Entry 0 is committed, entry 1 is not
	ackTracker := NewQuorumAckTracker(3, 0, 0)
	kvf, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := database.NewDB(constant.DefaultNamespace, shard, kvf, proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)
	wf := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})
	w, err := wf.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	for offset := int64(0); offset <= 1; offset++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: offset,
			Value:  []byte("v1"),
		}))
	}

	// A regular follower acking through the quorum tracker
	regular, err := ackTracker.NewCursorAcker(0)
	assert.NoError(t, err)

	// The observer was seeded with a snapshot up to entry 0
	fc := NewObserverFollowerCursor("child-1", term, constant.DefaultNamespace, shard, stream, ackTracker,
		w, db, 0, &proto.Int32HashRange{MinHashInclusive: 0, MaxHashInclusive: 100}, nil)

	// The observer streams entry 1 with the commit offset of that moment (0)
	// and parks at the head of the wal
	req := <-stream.AppendReqs
	assert.EqualValues(t, 1, req.Entry.Offset)
	assert.EqualValues(t, 0, req.CommitOffset)

	// The quorum ack advances the commit offset with no new writes
	ackTracker.AdvanceHeadOffset(1)
	regular.Ack(1)
	assert.EqualValues(t, 1, ackTracker.CommitOffset())

	// The parked observer sends the advertisement on its own
	req = <-stream.AppendReqs
	assert.Nil(t, req.Entry)
	assert.EqualValues(t, 1, req.CommitOffset)
	assert.EqualValues(t, term, req.Term)

	// No repeated advertisement while nothing advances
	time.Sleep(100 * time.Millisecond)
	select {
	case req = <-stream.AppendReqs:
		t.Fatalf("unexpected append: %+v", req)
	default:
		// Expected. There should be nothing in the channel
	}

	fc.Close()
	assert.NoError(t, w.Close())
	assert.NoError(t, wf.Close())
	assert.NoError(t, db.Close())
	assert.NoError(t, kvf.Close())
}

// snapshotStreamsProvider gives each snapshot a stream of its own, and uses
// the replicate streams of a mock rpc client.
type snapshotStreamsProvider struct {
	*rpc.MockRpcClient
	snapshots chan *rpc.MockSendSnapshotClientStream
}

func (p *snapshotStreamsProvider) SendSnapshot(ctx context.Context, _ string, _ string, _ int64,
	_ int64) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	stream := rpc.NewMockSendSnapshotClientStream(ctx)
	p.snapshots <- stream
	return stream, nil
}

// A split child must start from a snapshot that holds an entry of the parent:
// the followers of the child would otherwise get the parent's entries from the
// child's wal, and apply them without the split filter. The observer of a
// leader that has committed no entry waits for one, and sends another snapshot
// if the one it sent holds no entry yet.
func TestObserverFollowerCursor_SeedsChildWithAnEntry(t *testing.T) {
	var term int64 = 1
	var shard int64 = 2

	provider := &snapshotStreamsProvider{
		MockRpcClient: rpc.NewMockRpcClient(),
		snapshots:     make(chan *rpc.MockSendSnapshotClientStream, 10),
	}
	ackTracker := NewQuorumAckTracker(3, wal.InvalidOffset, wal.InvalidOffset)
	kvf, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := database.NewDB(constant.DefaultNamespace, shard, kvf, proto.KeySortingType_HIERARCHICAL, 1*time.Hour,
		time2.SystemClock)
	assert.NoError(t, err)
	wf := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})
	w, err := wf.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	// Entry 0 is in the wal, but not committed
	assert.NoError(t, w.Append(&proto.LogEntry{
		Term:   1,
		Offset: 0,
		Value:  []byte("v1"),
	}))
	regular, err := ackTracker.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	fc := NewObserverFollowerCursor("child-1", term, constant.DefaultNamespace, shard, provider, ackTracker,
		w, db, wal.InvalidOffset, &proto.Int32HashRange{MinHashInclusive: 0, MaxHashInclusive: 100}, nil)

	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, provider.snapshots, "snapshot sent before the first commit")
	assert.Empty(t, provider.AppendReqs, "entries sent before the first commit")

	ackTracker.AdvanceHeadOffset(0)
	regular.Ack(0)

	// The first snapshot is taken before the database applies entry 0
	for _, ackOffset := range []int64{wal.InvalidOffset, 0} {
		select {
		case snapshot := <-provider.snapshots:
			for chunk := range snapshot.Requests {
				assert.EqualValues(t, term, chunk.Term)
			}
			snapshot.Response <- &proto.SnapshotResponse{AckOffset: ackOffset}
		case <-time.After(10 * time.Second):
			t.Fatal("the observer did not send a snapshot")
		}
	}

	// The observer streams the entries after the snapshot
	assert.NoError(t, w.Append(&proto.LogEntry{
		Term:   1,
		Offset: 1,
		Value:  []byte("v2"),
	}))
	ackTracker.AdvanceHeadOffset(1)
	var req *proto.Append
	for req == nil || req.Entry == nil {
		select {
		case req = <-provider.AppendReqs:
		case <-time.After(10 * time.Second):
			t.Fatal("the observer did not send entry 1")
		}
	}
	assert.EqualValues(t, 1, req.Entry.Offset)
	assert.Empty(t, provider.snapshots)

	fc.Close()
	assert.NoError(t, w.Close())
	assert.NoError(t, wf.Close())
	assert.NoError(t, db.Close())
	assert.NoError(t, kvf.Close())
}

// A follower cursor that advertises the commit offset (the term pins the
// feature) sends an entry-less Append when the commit offset advances while
// it is parked at the head of the wal, like an observer does; one that
// doesn't waits for the next entry.
func TestFollowerCursorAdvertisesCommitOffsetAtHeadWhenTheTermPinsIt(t *testing.T) {
	for _, advertise := range []bool{true, false} {
		t.Run(fmt.Sprintf("advertise=%v", advertise), func(t *testing.T) {
			var term int64 = 1
			var shard int64 = 2
			stream := rpc.NewMockRpcClient()
			ackTracker := NewQuorumAckTracker(3, 0, 0)
			kvf, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			db, err := database.NewDB(constant.DefaultNamespace, shard, kvf, proto.KeySortingType_HIERARCHICAL, 1*time.Hour,
				time2.SystemClock)
			require.NoError(t, err)
			wf := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})
			w, err := wf.NewWal(constant.DefaultNamespace, shard, nil)
			require.NoError(t, err)
			for offset := int64(0); offset <= 1; offset++ {
				require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: offset, Value: []byte("v1")}))
			}
			regular, err := ackTracker.NewCursorAcker(0)
			require.NoError(t, err)

			fc, err := NewFollowerCursor("f1", term, constant.DefaultNamespace, shard, stream, ackTracker, w, db, 0,
				advertise)
			require.NoError(t, err)
			req := <-stream.AppendReqs
			assert.EqualValues(t, 1, req.Entry.Offset)
			assert.EqualValues(t, 0, req.CommitOffset)

			ackTracker.AdvanceHeadOffset(1)
			regular.Ack(1)
			require.EqualValues(t, 1, ackTracker.CommitOffset())

			if advertise {
				req = <-stream.AppendReqs
				assert.Nil(t, req.Entry)
				assert.EqualValues(t, 1, req.CommitOffset)
			} else {
				time.Sleep(100 * time.Millisecond)
				select {
				case req = <-stream.AppendReqs:
					t.Fatalf("unexpected append: %+v", req)
				default:
				}
			}
			fc.Close()
			ackTracker.Close()
			assert.NoError(t, w.Close())
			assert.NoError(t, wf.Close())
			assert.NoError(t, db.Close())
			assert.NoError(t, kvf.Close())
		})
	}
}

func TestNewFollowerCursorFailsWithoutRoomInTheTracker(t *testing.T) {
	db, w := newCursorTestStores(t, &hookKVFactory{})

	// A replication factor of 1 has no follower
	fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, 2, rpc.NewMockRpcClient(),
		NewQuorumAckTracker(1, wal.InvalidOffset, wal.InvalidOffset), w, db, wal.InvalidOffset, false)
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
	assert.Nil(t, fc)
}

// A follower that misses entries before the first one of the leader's wal can
// only be brought up to date through a snapshot.
func TestFollowerCursorSendsSnapshotToAFollowerBehindTheWal(t *testing.T) {
	n := int64(10)
	db, w := newCursorTestStores(t, &hookKVFactory{})
	previousCrc := uint32(1)
	_, err := w.AppendAsyncWithPreviousCrc(&proto.LogEntry{Term: 1, Offset: n, Value: []byte("v")}, &previousCrc)
	require.NoError(t, err)
	require.NoError(t, w.Sync(context.Background()))
	stream := rpc.NewMockRpcClient()

	fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, 2, stream, NewQuorumAckTracker(3, n, n-1), w,
		db, n-5, false)
	require.NoError(t, err)

	select {
	case chunk := <-stream.SendSnapshotStream.Requests:
		assert.EqualValues(t, 1, chunk.Term)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the follower got no snapshot")
	}
	stream.SendSnapshotStream.Response <- &proto.SnapshotResponse{AckOffset: n - 1}
	fc.Close()
}

// A follower that reports it's not a member lost its data, e.g. after a
// clean-up: the cursor brings it up to date from the start.
func TestFollowerCursorRestartsWhenTheFollowerIsNotAMember(t *testing.T) {
	db, w := newCursorTestStores(t, &hookKVFactory{})
	appendCursorTestEntries(t, w, 2)
	notMember := &atomic.Bool{}
	notMember.Store(true)
	provider := &hookStreamProvider{MockRpcClient: rpc.NewMockRpcClient()}
	provider.getReplicateStream = func(stream proto.OxiaLogReplication_ReplicateClient) (
		proto.OxiaLogReplication_ReplicateClient, error) {
		if notMember.CompareAndSwap(true, false) {
			return nil, constant.ErrNodeIsNotMember
		}
		return stream, nil
	}

	fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, 2, provider, NewQuorumAckTracker(3, 1, 0), w, db,
		0, false)
	require.NoError(t, err)
	defer fc.Close()

	select {
	case req := <-provider.AppendReqs:
		assert.EqualValues(t, 0, req.Entry.Offset)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the follower got no entry")
	}
	assert.EqualValues(t, wal.InvalidOffset, fc.AckOffset())
}

// The cursor retries a snapshot that it fails to read.
func TestFollowerCursorRetriesTheSnapshotWhenItCantBeRead(t *testing.T) {
	for _, test := range []struct {
		name     string
		snapshot func(kv kvstore.KV) (kvstore.Snapshot, error)
	}{
		{"open", func(kvstore.KV) (kvstore.Snapshot, error) { return nil, errInjected }},
		{"chunk", func(kv kvstore.KV) (kvstore.Snapshot, error) {
			snapshot, err := kv.Snapshot()
			return failingChunkSnapshot{snapshot}, err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshots := &atomic.Int64{}
			db, w := newCursorTestStores(t, &hookKVFactory{snapshot: func(kv kvstore.KV) (kvstore.Snapshot, error) {
				snapshots.Add(1)
				return test.snapshot(kv)
			}})
			// The wal is empty: the follower can only get a snapshot
			var shard int64 = 2
			_, err := db.ProcessWrite(&proto.WriteRequest{Shard: &shard, Puts: []*proto.PutRequest{{
				Key:   "a",
				Value: []byte("a"),
			}}}, 0, 0, database.NoOpCallback)
			require.NoError(t, err)
			stream := rpc.NewMockRpcClient()

			fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, shard, stream,
				NewQuorumAckTracker(3, 0, 0), w, db, wal.InvalidOffset, false)
			require.NoError(t, err)

			assert.Eventually(t, func() bool { return snapshots.Load() >= 2 }, 10*time.Second, 10*time.Millisecond)
			fc.Close()
			assert.Empty(t, stream.SendSnapshotStream.Requests)
			assert.EqualValues(t, wal.InvalidOffset, fc.AckOffset())
		})
	}
}

// The cursor streams the entries again on a new stream when it fails to
// read or to send them.
func TestFollowerCursorRetriesTheEntriesWhenTheyCantBeStreamed(t *testing.T) {
	for _, test := range []struct {
		name      string
		newReader func(w wal.Wal, after int64) (wal.Reader, error)
		send      func(req *proto.Append) error
	}{
		{name: "open", newReader: func(wal.Wal, int64) (wal.Reader, error) { return nil, errInjected }},
		{name: "read", newReader: func(w wal.Wal, after int64) (wal.Reader, error) {
			return newFailingReader(w.NewReader(after))
		}},
		{name: "send", send: func(*proto.Append) error { return errInjected }},
		{name: "send canceled", send: func(*proto.Append) error { return status.Error(codes.Canceled, "canceled") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, w := newCursorTestStores(t, &hookKVFactory{})
			appendCursorTestEntries(t, w, 1)
			failReader := &atomic.Bool{}
			failReader.Store(test.newReader != nil)
			w = &hookWal{Wal: w, factory: &hookWalFactory{newReader: func(w wal.Wal, after int64) (wal.Reader, error) {
				if failReader.CompareAndSwap(true, false) {
					return test.newReader(w, after)
				}
				return w.NewReader(after)
			}}}
			failSend := &atomic.Bool{}
			failSend.Store(test.send != nil)
			provider := &hookStreamProvider{MockRpcClient: rpc.NewMockRpcClient()}
			provider.getReplicateStream = func(stream proto.OxiaLogReplication_ReplicateClient) (
				proto.OxiaLogReplication_ReplicateClient, error) {
				send := func(req *proto.Append) error {
					if failSend.CompareAndSwap(true, false) {
						return test.send(req)
					}
					return nil
				}
				return &hookReplicateStream{OxiaLogReplication_ReplicateClient: stream, send: send}, nil
			}

			fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, 2, provider, NewQuorumAckTracker(3, 0, 0),
				w, db, wal.InvalidOffset, false)
			require.NoError(t, err)
			defer fc.Close()

			select {
			case req := <-provider.AppendReqs:
				assert.EqualValues(t, 0, req.Entry.Offset)
			case <-time.After(10 * time.Second):
				assert.Fail(t, "the follower got no entry")
			}
			assert.EqualValues(t, 2, provider.streams.Load())
		})
	}
}

// A cursor that fails to advertise the commit offset streams the entries
// again on a new stream, along with the commit offset.
func TestFollowerCursorRetriesWhenTheCommitAdvertisementFails(t *testing.T) {
	db, w := newCursorTestStores(t, &hookKVFactory{})
	appendCursorTestEntries(t, w, 2)
	ackTracker := NewQuorumAckTracker(3, 0, 0)
	regular, err := ackTracker.NewCursorAcker(0)
	require.NoError(t, err)
	failAdvertisement := &atomic.Bool{}
	failAdvertisement.Store(true)
	provider := &hookStreamProvider{MockRpcClient: rpc.NewMockRpcClient()}
	provider.getReplicateStream = func(stream proto.OxiaLogReplication_ReplicateClient) (
		proto.OxiaLogReplication_ReplicateClient, error) {
		return &hookReplicateStream{OxiaLogReplication_ReplicateClient: stream, send: func(req *proto.Append) error {
			if req.Entry == nil && failAdvertisement.CompareAndSwap(true, false) {
				return errInjected
			}
			return nil
		}}, nil
	}

	fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, 2, provider, ackTracker, w, db, 0, true)
	require.NoError(t, err)
	defer fc.Close()
	req := <-provider.AppendReqs
	require.EqualValues(t, 1, req.Entry.Offset)

	ackTracker.AdvanceHeadOffset(1)
	regular.Ack(1)
	select {
	case req = <-provider.AppendReqs:
		assert.EqualValues(t, 1, req.Entry.GetOffset())
		assert.EqualValues(t, 1, req.CommitOffset)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the follower got no entry")
	}
	assert.False(t, failAdvertisement.Load())
	assert.EqualValues(t, 2, provider.streams.Load())
}

func TestFollowerCursorHoldsBackTheCommitAdvertisement(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	const delay = 500 * time.Millisecond
	failpoint.Enable("followerCursor.holdBackCommitAdvertisement", delay)
	t.Cleanup(func() { failpoint.Disable("followerCursor.holdBackCommitAdvertisement") })
	db, w := newCursorTestStores(t, &hookKVFactory{})
	appendCursorTestEntries(t, w, 2)
	ackTracker := NewQuorumAckTracker(3, 0, 0)
	regular, err := ackTracker.NewCursorAcker(0)
	require.NoError(t, err)
	stream := rpc.NewMockRpcClient()

	fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, 2, stream, ackTracker, w, db, 0, true)
	require.NoError(t, err)
	defer fc.Close()
	req := <-stream.AppendReqs
	require.EqualValues(t, 1, req.Entry.Offset)

	advanced := time.Now()
	ackTracker.AdvanceHeadOffset(1)
	regular.Ack(1)
	select {
	case req = <-stream.AppendReqs:
		assert.Nil(t, req.Entry)
		assert.EqualValues(t, 1, req.CommitOffset)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the follower got no commit offset")
	}
	assert.GreaterOrEqual(t, time.Since(advanced), delay)
}

// A new entry ends the hold back, and carries the commit offset.
func TestFollowerCursorSendsTheHeldBackCommitOffsetWithANewEntry(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	failpoint.Enable("followerCursor.holdBackCommitAdvertisement", time.Hour)
	t.Cleanup(func() { failpoint.Disable("followerCursor.holdBackCommitAdvertisement") })
	db, w := newCursorTestStores(t, &hookKVFactory{})
	appendCursorTestEntries(t, w, 2)
	ackTracker := NewQuorumAckTracker(3, 0, 0)
	regular, err := ackTracker.NewCursorAcker(0)
	require.NoError(t, err)
	stream := rpc.NewMockRpcClient()

	fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, 2, stream, ackTracker, w, db, 0, true)
	require.NoError(t, err)
	req := <-stream.AppendReqs
	require.EqualValues(t, 1, req.Entry.Offset)

	ackTracker.AdvanceHeadOffset(1)
	regular.Ack(1)
	appendCursorTestEntries(t, w, 3)
	ackTracker.AdvanceHeadOffset(2)
	select {
	case req = <-stream.AppendReqs:
		assert.EqualValues(t, 2, req.Entry.GetOffset())
		assert.EqualValues(t, 1, req.CommitOffset)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the follower got no entry")
	}

	// Closing the cursor ends the hold back of the next commit offset
	regular.Ack(2)
	require.EqualValues(t, 2, ackTracker.CommitOffset())
	select {
	case req = <-stream.AppendReqs:
		assert.Fail(t, "unexpected append", "%+v", req)
	case <-time.After(100 * time.Millisecond):
	}
	closed := make(chan struct{})
	go func() {
		fc.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the cursor didn't close")
	}
}

// The follower can close its side of the stream: the cursor keeps sending
// the entries.
func TestFollowerCursorKeepsStreamingWhenTheAcksEnd(t *testing.T) {
	for _, test := range []struct {
		name string
		recv func() (*proto.Ack, error)
	}{
		{"eof", func() (*proto.Ack, error) { return nil, io.EOF }},
		// What the receiver gets once the follower closed the stream
		{"closed", func() (*proto.Ack, error) { return nil, nil }}, //nolint:nilnil
	} {
		t.Run(test.name, func(t *testing.T) {
			db, w := newCursorTestStores(t, &hookKVFactory{})
			appendCursorTestEntries(t, w, 1)
			ackTracker := NewQuorumAckTracker(3, 0, 0)
			acksEnded := make(chan struct{})
			provider := &hookStreamProvider{MockRpcClient: rpc.NewMockRpcClient()}
			provider.getReplicateStream = func(stream proto.OxiaLogReplication_ReplicateClient) (
				proto.OxiaLogReplication_ReplicateClient, error) {
				recv := func() (*proto.Ack, error) {
					defer close(acksEnded)
					return test.recv()
				}
				return &hookReplicateStream{OxiaLogReplication_ReplicateClient: stream, recv: recv}, nil
			}

			fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, 2, provider, ackTracker, w, db, 0, false)
			require.NoError(t, err)
			defer fc.Close()
			<-acksEnded

			appendCursorTestEntries(t, w, 2)
			ackTracker.AdvanceHeadOffset(1)
			select {
			case req := <-provider.AppendReqs:
				assert.EqualValues(t, 1, req.Entry.Offset)
			case <-time.After(10 * time.Second):
				assert.Fail(t, "the follower got no entry")
			}
			assert.EqualValues(t, 1, provider.streams.Load())
		})
	}
}

func TestFollowerCursorLogsTheEntriesAtDebugLevel(t *testing.T) {
	logs := captureDebugLogs(t)
	db, w := newCursorTestStores(t, &hookKVFactory{})
	appendCursorTestEntries(t, w, 1)
	stream := rpc.NewMockRpcClient()

	fc, err := NewFollowerCursor("f1", 1, constant.DefaultNamespace, 2, stream, NewQuorumAckTracker(3, 0, 0), w, db,
		wal.InvalidOffset, false)
	require.NoError(t, err)
	defer fc.Close()
	<-stream.AppendReqs
	stream.AckResps <- &proto.Ack{Offset: 0}
	require.Eventually(t, func() bool { return fc.AckOffset() == 0 }, 10*time.Second, 10*time.Millisecond)

	assert.Contains(t, logs(), "Sending entries to follower")
	assert.Contains(t, logs(), "Received ack")
}

// newCursorTestStores returns the database and the wal of shard 2, on the
// Pebble store that the hooks of kvFactory can override, which get closed at
// the end of the test.
func newCursorTestStores(t *testing.T, kvFactory *hookKVFactory) (database.DB, wal.Wal) {
	t.Helper()

	pebbleFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	kvFactory.Factory = pebbleFactory
	db, err := database.NewDB(constant.DefaultNamespace, 2, kvFactory, proto.KeySortingType_HIERARCHICAL, time.Hour,
		time2.SystemClock)
	require.NoError(t, err)
	wf := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})
	w, err := wf.NewWal(constant.DefaultNamespace, 2, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, w.Close())
		assert.NoError(t, wf.Close())
		assert.NoError(t, db.Close())
		assert.NoError(t, pebbleFactory.Close())
	})
	return db, w
}

// appendCursorTestEntries appends the entries of term 1 up to count-1, from
// the last one of the wal.
func appendCursorTestEntries(t *testing.T, w wal.Wal, count int64) {
	t.Helper()

	for offset := w.LastOffset() + 1; offset < count; offset++ {
		require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: offset, Value: []byte("v")}))
	}
}

// failingChunkSnapshot fails to read its chunks.
type failingChunkSnapshot struct {
	kvstore.Snapshot
}

func (failingChunkSnapshot) Chunk() (kvstore.SnapshotChunk, error) {
	return nil, errInjected
}

// hookStreamProvider opens the replicate streams of a mock rpc client, which
// the getReplicateStream hook can replace, and counts them.
type hookStreamProvider struct {
	*rpc.MockRpcClient
	getReplicateStream func(stream proto.OxiaLogReplication_ReplicateClient) (
		proto.OxiaLogReplication_ReplicateClient, error)
	streams atomic.Int64
}

func (p *hookStreamProvider) GetReplicateStream(ctx context.Context, follower string, namespace string, shard int64,
	term int64) (proto.OxiaLogReplication_ReplicateClient, error) {
	p.streams.Add(1)
	stream, err := p.MockRpcClient.GetReplicateStream(ctx, follower, namespace, shard, term)
	if err != nil || p.getReplicateStream == nil {
		return stream, err
	}
	return p.getReplicateStream(stream)
}

// hookReplicateStream sends and receives on its stream, after the send hook
// accepted the message, and receives from the recv hook instead if set.
type hookReplicateStream struct {
	proto.OxiaLogReplication_ReplicateClient
	send func(req *proto.Append) error
	recv func() (*proto.Ack, error)
}

func (s *hookReplicateStream) Send(req *proto.Append) error {
	if s.send != nil {
		if err := s.send(req); err != nil {
			return err
		}
	}
	return s.OxiaLogReplication_ReplicateClient.Send(req)
}

func (s *hookReplicateStream) Recv() (*proto.Ack, error) {
	if s.recv != nil {
		return s.recv()
	}
	return s.OxiaLogReplication_ReplicateClient.Recv()
}
