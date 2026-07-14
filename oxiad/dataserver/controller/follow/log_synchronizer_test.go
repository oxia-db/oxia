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
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

// stubWal lets the test control the gap between the appended and the synced
// offsets, which is timing-dependent with a real wal.
type stubWal struct {
	wal.Wal
	appended  atomic.Int64
	synced    atomic.Int64
	syncCalls atomic.Int64
}

func (s *stubWal) LastOffset() int64 { return s.synced.Load() }

func (s *stubWal) AppendAsyncWithPreviousCrc(entry *proto.LogEntry, _ *uint32) error {
	s.appended.Store(entry.Offset)
	return nil
}

func (s *stubWal) Sync(context.Context) error {
	s.syncCalls.Add(1)
	s.synced.Store(s.appended.Load())
	return nil
}

// A duplicated Append must only be acknowledged up to the synced offset: the
// leader accounts acks (cumulatively) as durable. Unsynced duplicates are
// acknowledged after the fsync. All the acks are sent by the syncer
// goroutine: gRPC streams do not support concurrent Send() calls.
func TestLogSynchronizer_DuplicateAckOnlySynced(t *testing.T) {
	w := &stubWal{}
	w.appended.Store(2)
	w.synced.Store(2)

	lastAppendedOffset := &atomic.Int64{}
	lastAppendedOffset.Store(2)
	advertisedCommitOffset := &atomic.Int64{}

	stream := &syncerOnlySendStream{MockServerReplicateStream: rpc.NewMockServerReplicateStream(), t: t}
	ls := NewLogSynchronizer(LogSynchronizerParams{
		Log:                    slog.Default(),
		Namespace:              "test",
		ShardId:                0,
		Term:                   1,
		Wal:                    w,
		AdvertisedCommitOffset: advertisedCommitOffset,
		LastAppendedOffset:     lastAppendedOffset,
		WriteLatencyHisto: metric.NewLatencyHistogram("oxia_test_dup_ack",
			"test", map[string]any{}),
		StateApplierCond: make(chan struct{}, 1),
		Stream:           stream,
		OnAppend:         func() {},
	})
	defer func() {
		// Unblock the appender goroutine's Recv before closing
		stream.Cancel()
		assert.NoError(t, ls.Close())
	}()

	// A duplicate at or below the synced offset is re-acked without
	// requiring anything new to become durable. The ack is cumulative, at
	// the durable head, so that the leader can skip everything the
	// follower already has.
	stream.AddRequest(&proto.Append{
		Term:                    1,
		Entry:                   &proto.LogEntry{Term: 1, Offset: 1},
		CommitOffset:            wal.InvalidOffset,
		CumulativeAcksSupported: true,
	})
	response := stream.GetResponse()
	assert.EqualValues(t, 2, response.Offset)
	assert.EqualValues(t, 2, w.synced.Load())

	// Entries 3..4 are appended but not synced yet
	w.appended.Store(4)
	lastAppendedOffset.Store(4)

	// A duplicate above the synced offset must not be acked before the
	// fsync: the ack only comes after a sync round
	stream.AddRequest(&proto.Append{
		Term:                    1,
		Entry:                   &proto.LogEntry{Term: 1, Offset: 4},
		CommitOffset:            wal.InvalidOffset,
		CumulativeAcksSupported: true,
	})
	response = stream.GetResponse()
	assert.EqualValues(t, 4, response.Offset)
	assert.GreaterOrEqual(t, w.syncCalls.Load(), int64(1))
	assert.EqualValues(t, 4, w.synced.Load())
}

// A follower with an empty wal (lastAppendedOffset = InvalidOffset) that
// receives its first append at a non-zero offset must reject it: entries were
// lost in transit. The wal cannot catch this gap itself (an empty wal accepts
// any first offset, for the post-snapshot case), and accepting it would let
// the follower ack entries it does not have — which the leader then counts,
// cumulatively, towards the commit quorum.
func TestLogSynchronizer_RejectGapOnEmptyWal(t *testing.T) {
	w := &stubWal{}
	w.appended.Store(wal.InvalidOffset)
	w.synced.Store(wal.InvalidOffset)

	lastAppendedOffset := &atomic.Int64{}
	lastAppendedOffset.Store(wal.InvalidOffset)
	advertisedCommitOffset := &atomic.Int64{}
	advertisedCommitOffset.Store(wal.InvalidOffset)

	stream := rpc.NewMockServerReplicateStream()
	ls := NewLogSynchronizer(LogSynchronizerParams{
		Log:                    slog.Default(),
		Namespace:              "test",
		ShardId:                0,
		Term:                   1,
		Wal:                    w,
		AdvertisedCommitOffset: advertisedCommitOffset,
		LastAppendedOffset:     lastAppendedOffset,
		WriteLatencyHisto: metric.NewLatencyHistogram("oxia_test_reject_gap",
			"test", map[string]any{}),
		StateApplierCond: make(chan struct{}, 1),
		Stream:           stream,
		OnAppend:         func() {},
	})

	stream.AddRequest(&proto.Append{
		Term:                    1,
		Entry:                   &proto.LogEntry{Term: 1, Offset: 5},
		CommitOffset:            wal.InvalidOffset,
		CumulativeAcksSupported: true,
	})

	assert.ErrorIs(t, ls.Sync(), wal.ErrInvalidNextOffset)
	assert.NoError(t, ls.Close())

	// Nothing was appended and no ack was sent
	assert.EqualValues(t, wal.InvalidOffset, w.appended.Load())
	assert.EqualValues(t, wal.InvalidOffset, lastAppendedOffset.Load())
	assert.Empty(t, stream.Responses)
}

// An entry-less Append is a commit-offset advertisement (sent by the leader to
// an observer parked at the head of the wal): it must take the new commit
// offset and wake up the applier chain, without appending anything to the wal
// and without acking.
func TestLogSynchronizer_CommitOffsetAdvertisement(t *testing.T) {
	w := &stubWal{}
	w.appended.Store(0)
	w.synced.Store(0)

	lastAppendedOffset := &atomic.Int64{}
	lastAppendedOffset.Store(0)
	advertisedCommitOffset := &atomic.Int64{}
	advertisedCommitOffset.Store(wal.InvalidOffset)

	stateApplierCond := make(chan struct{}, 1)
	stream := rpc.NewMockServerReplicateStream()
	ls := NewLogSynchronizer(LogSynchronizerParams{
		Log:                    slog.Default(),
		Namespace:              "test",
		ShardId:                0,
		Term:                   1,
		Wal:                    w,
		AdvertisedCommitOffset: advertisedCommitOffset,
		LastAppendedOffset:     lastAppendedOffset,
		WriteLatencyHisto: metric.NewLatencyHistogram("oxia_test_commit_advertisement",
			"test", map[string]any{}),
		StateApplierCond: stateApplierCond,
		Stream:           stream,
		OnAppend:         func() {},
	})
	defer func() {
		// Unblock the appender goroutine's Recv before closing
		stream.Cancel()
		assert.NoError(t, ls.Close())
	}()

	stream.AddRequest(&proto.Append{
		Term:                    1,
		CommitOffset:            0,
		CumulativeAcksSupported: true,
	})

	assert.Eventually(t, func() bool {
		return advertisedCommitOffset.Load() == 0
	}, 10*time.Second, 10*time.Millisecond)

	// The state applier gets woken up (through the syncer)
	select {
	case <-stateApplierCond:
	case <-time.After(10 * time.Second):
		t.Fatal("state applier was not woken up")
	}

	// Nothing was appended and no ack was sent
	assert.EqualValues(t, 0, lastAppendedOffset.Load())
	assert.Empty(t, stream.Responses)
}
