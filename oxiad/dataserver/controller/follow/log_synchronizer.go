package follow

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	dserror "github.com/oxia-db/oxia/oxiad/dataserver/errors"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

type LogSynchronizer struct {
	io.Closer

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

	writeLatencyHisto metric.LatencyHistogram
}

func (ls *LogSynchronizer) Sync() error {
	err := <-ls.finish
	ls.closed.Store(true)
	ls.cancel()
	ls.waitGroup.Wait()
	return err
}

func (ls *LogSynchronizer) IsValid() bool {
	return ls != nil && !ls.closed.Load()
}

func (ls *LogSynchronizer) Close() error {
	if !ls.closed.CompareAndSwap(false, true) {
		return nil
	}
	channel.PushNoBlock(ls.finish, context.Canceled)

	ls.waitGroup.Wait()
	return nil
}

func (ls *LogSynchronizer) bgAppender(stream proto.OxiaLogReplication_ReplicateServer, syncCond chan struct{}, becomeFollower func()) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if req == nil {
			return nil
		}
		if err = ls.append0(stream, syncCond, becomeFollower, req); err != nil {
			return err
		}
	}
}

func (ls *LogSynchronizer) append0(stream proto.OxiaLogReplication_ReplicateServer, syncCond chan struct{}, becomeFollower func(), req *proto.Append) error {
	timer := ls.writeLatencyHisto.Timer()
	defer timer.Done()

	if req.Term != ls.term {
		return dserror.ErrInvalidTerm
	}

	ls.log.Debug(
		"Add entry",
		slog.Int64("commit-offset", req.CommitOffset),
		slog.Int64("offset", req.Entry.Offset),
	)

	// A follower node confirms an entry to the leader
	//
	// The follower adds the entry to its log, sets the head offset
	// and updates its commit offset with the commit offset of
	// the request.
	becomeFollower()

	lastAppendedOffset := ls.lastAppendedOffset.Load()
	if req.Entry.Offset <= lastAppendedOffset {
		// This was a duplicated request. We already have this entry
		ls.log.Debug(
			"Ignoring duplicated entry",
			slog.Int64("commit-offset", req.CommitOffset),
			slog.Int64("offset", req.Entry.Offset),
		)
		if err := stream.Send(&proto.Ack{Offset: req.Entry.Offset}); err != nil {
			return err
		}
		return nil
	}

	// Append the entry asynchronously. We'll sync it in a group from the "sync" routine,
	// where the ack is then sent back
	if err := ls.wal.AppendAsync(req.GetEntry()); err != nil {
		return err
	}

	ls.advertisedCommitOffset.Store(req.CommitOffset)
	ls.lastAppendedOffset.Store(req.Entry.Offset)

	channel.PushNoBlock(syncCond, struct{}{})
	return nil
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
			// Ack all the entries that were synced in the last round
			for offset := oldHeadOffset + 1; offset <= newHeadOffset; offset++ {
				if err := stream.Send(&proto.Ack{Offset: offset}); err != nil {
					return err
				}
			}
			channel.PushNoBlock(stateApplierCond, struct{}{})
		}
	}
}

func NewLogSynchronizer(log *slog.Logger, namespace string, shardId int64, term int64, wal wal.Wal, advertisedCommitOffset *atomic.Int64, lastAppendedOffset *atomic.Int64, writeLatencyHisto metric.LatencyHistogram, stateApplierCond chan struct{}, stream proto.OxiaLogReplication_ReplicateServer, becomeFollower func()) *LogSynchronizer {
	ctx, cancel := context.WithCancel(stream.Context())

	ls := &LogSynchronizer{
		log:                    log,
		waitGroup:              sync.WaitGroup{},
		ctx:                    ctx,
		cancel:                 cancel,
		term:                   term,
		wal:                    wal,
		advertisedCommitOffset: advertisedCommitOffset,
		lastAppendedOffset:     lastAppendedOffset,
		writeLatencyHisto:      writeLatencyHisto,
		finish:                 make(chan error, 1),
	}

	syncCond := make(chan struct{})
	ls.waitGroup.Go(func() {
		process.DoWithLabels(
			ctx,
			map[string]string{
				"oxia":      "follower-log-synchronizer-appender",
				"namespace": namespace,
				"shard":     fmt.Sprintf("%d", shardId),
			},
			func() {
				channel.PushNoBlock(ls.finish, ls.bgAppender(stream, syncCond, becomeFollower))
			},
		)
	})

	ls.waitGroup.Go(func() {
		process.DoWithLabels(
			ctx,
			map[string]string{
				"oxia":      "follower-log-synchronizer-syncer",
				"namespace": namespace,
				"shard":     fmt.Sprintf("%d", shardId),
			},
			func() { channel.PushNoBlock(ls.finish, ls.bgSyncer(stream, syncCond, stateApplierCond)) },
		)
	})
	return ls
}
