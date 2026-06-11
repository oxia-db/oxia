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
