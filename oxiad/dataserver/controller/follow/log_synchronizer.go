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
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

type LogSynchronizerParams struct {
	Log                    *slog.Logger
	Namespace              string
	ShardId                int64
	Term                   int64
	Wal                    wal.Wal
	AdvertisedCommitOffset *atomic.Int64
	LastAppendedOffset     *atomic.Int64
	WriteLatencyHisto      metric.LatencyHistogram
	StateApplierCond       chan struct{}
	Stream                 proto.OxiaLogReplication_ReplicateServer
	OnAppend               func()
}

type LogSynchronizer struct {
	waitGroup sync.WaitGroup
	log       *slog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	term      int64
	wal       wal.Wal
	finish    chan error
	closed    atomic.Bool

	advertisedCommitOffset *atomic.Int64
	lastAppendedOffset     *atomic.Int64

	// Whether the leader advertised cumulative-ack support on the last
	// received Append. Atomic because the appender goroutine stores it while
	// the syncer goroutine reads it, and the two share no lock. The value
	// never changes within a stream in practice (one stream, one leader):
	// the per-message store is simply the stateless way to track it, and it
	// is idempotent and contention-free.
	cumulativeAcks atomic.Bool

	// Offsets of duplicated entries waiting to be re-acked. Duplicates don't
	// advance the WAL head, though the leader still needs their acks to make
	// progress after a reconnect. The acks must come from the syncer
	// goroutine: gRPC streams don't support concurrent Send() calls and the
	// syncer is already sending on this stream.
	reAckMutex   sync.Mutex
	reAckOffsets []int64

	writeLatencyHisto metric.LatencyHistogram
}

func (ls *LogSynchronizer) Sync() error {
	return <-ls.finish
}

func (ls *LogSynchronizer) SyncAndClose() error {
	return multierr.Combine(ls.Sync(), ls.Close())
}

func (ls *LogSynchronizer) IsValid() bool {
	return ls != nil && !ls.closed.Load()
}

func (ls *LogSynchronizer) Close() error {
	if !ls.closed.CompareAndSwap(false, true) {
		return nil
	}
	ls.cancel()
	channel.PushNoBlock(ls.finish, context.Canceled)

	ls.waitGroup.Wait()
	return nil
}

func (ls *LogSynchronizer) bgAppender(stream proto.OxiaLogReplication_ReplicateServer, syncCond chan struct{}, onAppend func()) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if req == nil {
			return nil
		}
		if err = ls.append0(syncCond, onAppend, req); err != nil {
			return err
		}
	}
}

func (ls *LogSynchronizer) append0(syncCond chan struct{}, onAppend func(), req *proto.Append) error {
	timer := ls.writeLatencyHisto.Timer()
	defer timer.Done()

	if req.Term != constant.I64NegativeOne && req.Term != ls.term {
		return constant.ErrInvalidTerm
	}

	ls.cumulativeAcks.Store(req.CumulativeAcksSupported)

	// This runs once per replicated entry: skip the attribute boxing entirely
	// when debug logging is off, and check the level once for both call sites.
	debugEnabled := ls.log.Enabled(context.Background(), slog.LevelDebug)
	if debugEnabled {
		ls.log.Debug(
			"Add entry",
			slog.Int64("commit-offset", req.CommitOffset),
			slog.Int64("offset", req.Entry.Offset),
		)
	}

	onAppend()

	lastAppendedOffset := ls.lastAppendedOffset.Load()
	if req.Entry.Offset <= lastAppendedOffset {
		// This was a duplicated request. We already have this entry
		if debugEnabled {
			ls.log.Debug(
				"Ignoring duplicated entry",
				slog.Int64("commit-offset", req.CommitOffset),
				slog.Int64("offset", req.Entry.Offset),
			)
		}

		// Only request a re-ack for what is already synced: the leader
		// accounts the ack, cumulatively, as durable. A duplicate that is
		// appended but not synced yet is acknowledged by the sync round
		// itself. Either way the ack is sent by the syncer goroutine: gRPC
		// streams do not support concurrent Send() calls, and the syncer is
		// already sending on this stream.
		if req.Entry.Offset <= ls.wal.LastOffset() {
			ls.requestReAck(req.Entry.Offset)
		}
		channel.PushNoBlock(syncCond, struct{}{})
		return nil
	}

	// Append the entry asynchronously, passing the previous CRC from the leader.
	// When the WAL is empty (e.g. after snapshot install), the CRC seeds the chain
	// so that the follower's CRC matches the leader's.
	if err := ls.wal.AppendAsyncWithPreviousCrc(req.GetEntry(), req.PreviousEntryCrc); err != nil {
		return err
	}

	ls.advertisedCommitOffset.Store(req.CommitOffset)
	ls.lastAppendedOffset.Store(req.Entry.Offset)

	channel.PushNoBlock(syncCond, struct{}{})
	return nil
}

// requestReAck records the offset of a duplicated entry so that the syncer
// goroutine can send the ack.
func (ls *LogSynchronizer) requestReAck(offset int64) {
	ls.reAckMutex.Lock()
	defer ls.reAckMutex.Unlock()
	ls.reAckOffsets = append(ls.reAckOffsets, offset)
}

func (ls *LogSynchronizer) takeReAcks() []int64 {
	ls.reAckMutex.Lock()
	defer ls.reAckMutex.Unlock()
	offsets := ls.reAckOffsets
	ls.reAckOffsets = nil
	return offsets
}

func (ls *LogSynchronizer) bgSyncer(stream proto.OxiaLogReplication_ReplicateServer, syncCond chan struct{}, stateApplierCond chan struct{}) error {
	for {
		select {
		case <-ls.ctx.Done():
			return ls.ctx.Err()
		case <-syncCond:
			oldHeadOffset := ls.wal.LastOffset()
			if err := ls.wal.Sync(ls.ctx); err != nil {
				return err
			}
			newHeadOffset := ls.wal.LastOffset()
			if err := ls.sendAcks(stream, oldHeadOffset, newHeadOffset); err != nil {
				return err
			}
			channel.PushNoBlock(stateApplierCond, struct{}{})
		}
	}
}

// sendAcks acknowledges the re-ack requests for duplicated entries and the
// entries synced in the last round.
func (ls *LogSynchronizer) sendAcks(stream proto.OxiaLogReplication_ReplicateServer, oldHeadOffset int64,
	newHeadOffset int64) error {
	reAcks := ls.takeReAcks()

	if ls.cumulativeAcks.Load() {
		// A single cumulative ack at the durable head covers both the
		// re-acked duplicates and the entries synced in the last round.
		// Always ack the head, not the duplicated offsets: it lets a
		// reconnecting leader skip everything the follower already has.
		if len(reAcks) == 0 && newHeadOffset <= oldHeadOffset {
			return nil
		}
		return stream.Send(&proto.Ack{Offset: newHeadOffset})
	}

	// The leader did not advertise cumulative-ack support (older version):
	// it accounts acks individually, so re-ack each duplicate and then ack
	// every entry synced in the last round
	for _, offset := range reAcks {
		if err := stream.Send(&proto.Ack{Offset: offset}); err != nil {
			return err
		}
	}
	for offset := oldHeadOffset + 1; offset <= newHeadOffset; offset++ {
		if err := stream.Send(&proto.Ack{Offset: offset}); err != nil {
			return err
		}
	}
	return nil
}

func NewLogSynchronizer(params LogSynchronizerParams) *LogSynchronizer {
	ctx, cancel := context.WithCancel(params.Stream.Context())

	ls := &LogSynchronizer{
		log:                    params.Log,
		waitGroup:              sync.WaitGroup{},
		ctx:                    ctx,
		cancel:                 cancel,
		term:                   params.Term,
		wal:                    params.Wal,
		advertisedCommitOffset: params.AdvertisedCommitOffset,
		lastAppendedOffset:     params.LastAppendedOffset,
		writeLatencyHisto:      params.WriteLatencyHisto,
		finish:                 make(chan error, 1),
	}

	syncCond := make(chan struct{}, 1)
	ls.waitGroup.Go(func() {
		process.DoWithLabels(
			ctx,
			map[string]string{
				"oxia":      "follower-log-synchronizer-appender",
				"namespace": params.Namespace,
				"shard":     fmt.Sprintf("%d", params.ShardId),
			},
			func() { //nolint:contextcheck
				channel.PushNoBlock(ls.finish, ls.bgAppender(params.Stream, syncCond, params.OnAppend))
			},
		)
	})

	ls.waitGroup.Go(func() {
		process.DoWithLabels(
			ctx,
			map[string]string{
				"oxia":      "follower-log-synchronizer-syncer",
				"namespace": params.Namespace,
				"shard":     fmt.Sprintf("%d", params.ShardId),
			},
			func() { channel.PushNoBlock(ls.finish, ls.bgSyncer(params.Stream, syncCond, params.StateApplierCond)) },
		)
	})
	return ls
}
