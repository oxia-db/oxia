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
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	dserror "github.com/oxia-db/oxia/oxiad/dataserver/errors"
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
		if err = ls.append0(stream, syncCond, onAppend, req); err != nil {
			return err
		}
	}
}

func (ls *LogSynchronizer) append0(stream proto.OxiaLogReplication_ReplicateServer, syncCond chan struct{}, onAppend func(), req *proto.Append) error {
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

	onAppend()

	lastAppendedOffset := ls.lastAppendedOffset.Load()
	if req.Entry.Offset <= lastAppendedOffset {
		// This was a duplicated request. We already have this entry
		ls.log.Debug(
			"Ignoring duplicated entry",
			slog.Int64("commit-offset", req.CommitOffset),
			slog.Int64("offset", req.Entry.Offset),
		)
		return stream.Send(&proto.Ack{Offset: req.Entry.Offset})
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
