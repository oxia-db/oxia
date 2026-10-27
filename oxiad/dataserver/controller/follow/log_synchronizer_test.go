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
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/failpoint"
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

func (s *stubWal) AppendAsyncWithPreviousCrc(entry *proto.LogEntry, _ *uint32) (uint32, error) {
	s.appended.Store(entry.Offset)
	return 0, nil
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
		ls.Close()
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

	assert.ErrorIs(t, ls.Sync(), errs.ErrInvalidArgument)
	ls.Close()

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
		ls.Close()
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

// The stream keeps each entry it appends with the chained crc that the wal
// computed for it, which the state applier records in the wal checksum gauge.
func TestLogSynchronizer_KeepsTheWalCrcOfTheEntries(t *testing.T) {
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir(), SegmentSize: 256})
	w, err := walFactory.NewWal("test", 0, nil)
	require.NoError(t, err)

	// As after a snapshot at offset 9: the crc of the leader seeds the chain of
	// the empty wal
	lastAppendedOffset := &atomic.Int64{}
	lastAppendedOffset.Store(9)
	advertisedCommitOffset := &atomic.Int64{}
	advertisedCommitOffset.Store(9)

	stream := rpc.NewMockServerReplicateStream()
	ls := NewLogSynchronizer(LogSynchronizerParams{
		Log:                    slog.Default(),
		Namespace:              "test",
		ShardId:                0,
		Term:                   1,
		Wal:                    w,
		AdvertisedCommitOffset: advertisedCommitOffset,
		LastAppendedOffset:     lastAppendedOffset,
		WriteLatencyHisto: metric.NewLatencyHistogram("oxia_test_kept_crc",
			"test", map[string]any{}),
		StateApplierCond: make(chan struct{}, 1),
		Stream:           stream,
		OnAppend:         func() {},
	})
	defer func() {
		// Unblock the appender goroutine's Recv before closing
		stream.Cancel()
		ls.Close()
		assert.NoError(t, w.Close())
		assert.NoError(t, walFactory.Close())
	}()

	// Enough entries to roll over a few segments
	previousCrc := uint32(1234)
	for offset := int64(10); offset < 50; offset++ {
		stream.AddRequest(&proto.Append{
			Term:                    1,
			Entry:                   &proto.LogEntry{Term: 1, Offset: offset, Value: []byte(fmt.Sprintf("value-%d", offset))},
			CommitOffset:            9,
			PreviousEntryCrc:        &previousCrc,
			CumulativeAcksSupported: true,
		})
		assert.EqualValues(t, offset, stream.GetResponse().Offset)
	}

	reader, err := w.NewReader(9)
	require.NoError(t, err)
	for offset := int64(10); offset < 50; offset++ {
		_, _, entryCrc, err := reader.ReadNext()
		require.NoError(t, err)
		kept, _, ok := ls.appended.take(offset)
		require.Truef(t, ok, "take of offset %d", offset)
		assert.Equalf(t, entryCrc, kept.entryCrc, "crc of the entry at offset %d", offset)
	}
	assert.NoError(t, reader.Close())
}

// A nil request ends the stream, like its end.
func TestLogSynchronizerSyncEndsOnNilRequest(t *testing.T) {
	w := &stubWal{}
	stream := rpc.NewMockServerReplicateStream()
	ls := newTestLogSynchronizer(t, w, wal.InvalidOffset, slog.Default(), stream)

	stream.AddRequest(nil)
	assert.NoError(t, ls.Sync())
	ls.Close()
}

// A failure to send an ack fails the stream, whether it acks the synced
// entries or re-acks a duplicate, as the leader doesn't support cumulative acks.
func TestLogSynchronizerSyncFailsWhenAckFails(t *testing.T) {
	for _, test := range []struct {
		name   string
		offset int64
	}{
		{"ack", 3},
		{"re-ack", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := &stubWal{}
			w.appended.Store(2)
			w.synced.Store(2)
			stream := &ackFailingStream{MockServerReplicateStream: rpc.NewMockServerReplicateStream()}
			ls := newTestLogSynchronizer(t, w, 2, slog.Default(), stream)

			stream.AddRequest(&proto.Append{
				Term:         1,
				Entry:        &proto.LogEntry{Term: 1, Offset: test.offset},
				CommitOffset: wal.InvalidOffset,
			})
			assert.ErrorContains(t, ls.Sync(), "send failed")

			stream.Cancel()
			ls.Close()
		})
	}
}

// A failure to append an entry to the wal fails the stream, without acking it.
func TestLogSynchronizerSyncFailsWhenAppendFails(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	appendErr := errors.New("append failed")
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir: t.TempDir(),
		Failpoints: failpoint.New(map[string]func() error{
			"appendAsync.marshal": func() error { return appendErr },
		}),
	})
	w, err := walFactory.NewWal("test", 0, nil)
	require.NoError(t, err)
	stream := rpc.NewMockServerReplicateStream()
	ls := newTestLogSynchronizer(t, w, wal.InvalidOffset, slog.Default(), stream)

	stream.AddRequest(&proto.Append{
		Term:         1,
		Entry:        &proto.LogEntry{Term: 1, Offset: 0, Value: []byte("v")},
		CommitOffset: wal.InvalidOffset,
	})
	assert.ErrorIs(t, ls.Sync(), appendErr)

	stream.Cancel()
	ls.Close()
	assert.EqualValues(t, wal.InvalidOffset, w.LastOffset())
	assert.Empty(t, stream.Responses)
	assert.NoError(t, w.Close())
	assert.NoError(t, walFactory.Close())
}

// The stream logs the requests it receives at the debug level.
func TestLogSynchronizerLogsRequestsAtDebugLevel(t *testing.T) {
	w := &stubWal{}
	w.appended.Store(wal.InvalidOffset)
	w.synced.Store(wal.InvalidOffset)
	logs := &logCapture{}
	stream := rpc.NewMockServerReplicateStream()
	ls := newTestLogSynchronizer(t, w, wal.InvalidOffset, slog.New(logs), stream)

	entry := &proto.Append{Term: 1, Entry: &proto.LogEntry{Term: 1, Offset: 0}, CommitOffset: wal.InvalidOffset}
	stream.AddRequest(entry)
	assert.EqualValues(t, 0, stream.GetResponse().Offset)
	stream.AddRequest(entry)
	assert.EqualValues(t, 0, stream.GetResponse().Offset)
	stream.AddRequest(&proto.Append{Term: 1, CommitOffset: 0})
	assert.Eventually(t, func() bool {
		return logs.has("Received commit-offset advertisement")
	}, 10*time.Second, 10*time.Millisecond)
	assert.True(t, logs.has("Add entry"))
	assert.True(t, logs.has("Ignoring duplicated entry"))

	stream.Cancel()
	ls.Close()
}

func newTestLogSynchronizer(t *testing.T, w wal.Wal, lastAppendedOffset int64, log *slog.Logger,
	stream proto.OxiaLogReplication_ReplicateServer) *LogSynchronizer {
	t.Helper()
	lastAppended := &atomic.Int64{}
	lastAppended.Store(lastAppendedOffset)
	advertisedCommitOffset := &atomic.Int64{}
	advertisedCommitOffset.Store(wal.InvalidOffset)
	return NewLogSynchronizer(LogSynchronizerParams{
		Log:                    log,
		Namespace:              "test",
		ShardId:                0,
		Term:                   1,
		Wal:                    w,
		AdvertisedCommitOffset: advertisedCommitOffset,
		LastAppendedOffset:     lastAppended,
		WriteLatencyHisto: metric.NewLatencyHistogram("oxia_test_log_synchronizer",
			"test", map[string]any{}),
		StateApplierCond: make(chan struct{}, 1),
		Stream:           stream,
		OnAppend:         func() {},
	})
}

// ackFailingStream fails to send any ack.
type ackFailingStream struct {
	*rpc.MockServerReplicateStream
}

func (*ackFailingStream) Send(*proto.Ack) error {
	return errors.New("send failed")
}

// logCapture records the messages of the log records, at every level.
type logCapture struct {
	mu       sync.Mutex
	messages []string
}

func (*logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, record slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, record.Message)
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *logCapture) WithGroup(string) slog.Handler { return c }

func (c *logCapture) has(message string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Contains(c.messages, message)
}
