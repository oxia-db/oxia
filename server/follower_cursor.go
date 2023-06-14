// Copyright 2023 StreamNative, Inc.
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

package server

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"oxia/common"
	"oxia/common/metrics"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"sync"
	"sync/atomic"
	"time"
)

// ReplicateStreamProvider
// This is a provider for the ReplicateStream Grpc handler
// It's used to allow passing in a mocked version of the Grpc service
type ReplicateStreamProvider interface {
	GetReplicateStream(ctx context.Context, follower string, namespace string, shard int64) (proto.OxiaLogReplication_ReplicateClient, error)
	SendSnapshot(ctx context.Context, follower string, namespace string, shard int64) (proto.OxiaLogReplication_SendSnapshotClient, error)
}

// FollowerCursor
// The FollowerCursor represents a cursor on the leader WAL that sends entries to a specific follower and receives a
// stream of acknowledgments from that follower.
type FollowerCursor interface {
	io.Closer

	ShardId() int64

	// LastPushed
	// The last entry that was sent to this follower
	LastPushed() int64

	// AckOffset The highest entry already acknowledged by this follower
	AckOffset() int64
}

type followerCursor struct {
	sync.Mutex

	term                    int64
	follower                string
	replicateStreamProvider ReplicateStreamProvider
	stream                  proto.OxiaLogReplication_ReplicateClient

	ackTracker  QuorumAckTracker
	cursorAcker CursorAcker
	wal         wal.Wal
	db          kv.DB
	lastPushed  atomic.Int64
	ackOffset   atomic.Int64
	namespace   string
	shardId     int64

	backoff backoff.BackOff
	closed  atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
	log     zerolog.Logger

	snapshotsTransferTime     metrics.LatencyHistogram
	snapshotsStartedCounter   metrics.Counter
	snapshotsCompletedCounter metrics.Counter
	snapshotsFailedCounter    metrics.Counter
	snapshotsBytesSent        metrics.Counter
}

func NewFollowerCursor(
	follower string,
	term int64,
	namespace string,
	shardId int64,
	replicateStreamProvider ReplicateStreamProvider,
	ackTracker QuorumAckTracker,
	wal wal.Wal,
	db kv.DB,
	ackOffset int64) (FollowerCursor, error) {

	labels := map[string]any{
		"namespace": namespace,
		"shard":     shardId,
		"follower":  follower,
	}

	fc := &followerCursor{
		term:                    term,
		follower:                follower,
		ackTracker:              ackTracker,
		replicateStreamProvider: replicateStreamProvider,
		wal:                     wal,
		db:                      db,
		namespace:               namespace,
		shardId:                 shardId,

		log: log.With().
			Str("component", "follower-cursor").
			Str("namespace", namespace).
			Int64("shard", shardId).
			Int64("term", term).
			Str("follower", follower).
			Logger(),

		snapshotsTransferTime: metrics.NewLatencyHistogram("oxia_server_snapshots_transfer_time",
			"The time taken to transfer a full snapshot", labels),
		snapshotsStartedCounter: metrics.NewCounter("oxia_server_snapshots_started",
			"The number of DB snapshots started", "count", labels),
		snapshotsCompletedCounter: metrics.NewCounter("oxia_server_snapshots_completed",
			"The number of DB snapshots completed", "count", labels),
		snapshotsFailedCounter: metrics.NewCounter("oxia_server_snapshots_failed",
			"The number of DB snapshots failed", "count", labels),
		snapshotsBytesSent: metrics.NewCounter("oxia_server_snapshots_sent",
			"The amount of data sent as snapshot", metrics.Bytes, labels),
	}

	fc.ctx, fc.cancel = context.WithCancel(context.Background())
	fc.backoff = common.NewBackOff(fc.ctx)

	fc.lastPushed.Store(ackOffset)
	fc.ackOffset.Store(ackOffset)

	var err error
	if fc.cursorAcker, err = ackTracker.NewCursorAcker(ackOffset); err != nil {
		return nil, err
	}

	go common.DoWithLabels(map[string]string{
		"oxia":      "follower-cursor-send",
		"namespace": namespace,
		"shard":     fmt.Sprintf("%d", fc.shardId),
	}, func() {
		fc.run()
	})

	return fc, nil
}

func (fc *followerCursor) shouldSendSnapshot() bool {
	fc.Lock()
	defer fc.Unlock()

	ackOffset := fc.ackOffset.Load()
	walFirstOffset := fc.wal.FirstOffset()

	if ackOffset == wal.InvalidOffset && fc.ackTracker.CommitOffset() >= 0 {
		fc.log.Info().
			Int64("follower-ack-offset", ackOffset).
			Int64("leader-commit-offset", fc.ackTracker.CommitOffset()).
			Msg("Sending snapshot to empty follower")
		return true
	} else if walFirstOffset > 0 && ackOffset < walFirstOffset {
		fc.log.Info().
			Int64("follower-ack-offset", ackOffset).
			Int64("wal-first-offset", fc.wal.FirstOffset()).
			Int64("wal-last-offset", fc.wal.LastOffset()).
			Msg("The follower is behind the first available entry in the leader WAL")
		return true
	}

	// No snapshot, just tail the log
	return false
}

func (fc *followerCursor) Close() error {
	fc.closed.Store(true)
	fc.cancel()

	fc.Lock()
	defer fc.Unlock()

	if fc.stream != nil {
		return fc.stream.CloseSend()
	}

	return nil
}

func (fc *followerCursor) ShardId() int64 {
	return fc.shardId
}

func (fc *followerCursor) LastPushed() int64 {
	return fc.lastPushed.Load()
}

func (fc *followerCursor) AckOffset() int64 {
	return fc.ackOffset.Load()
}

func (fc *followerCursor) run() {
	_ = backoff.RetryNotify(fc.runOnce, fc.backoff,
		func(err error, duration time.Duration) {
			fc.log.Error().Err(err).
				Dur("retry-after", duration).
				Msg("Error while pushing entries to follower")
		})
}

func (fc *followerCursor) runOnce() error {
	if fc.shouldSendSnapshot() {
		timer := fc.snapshotsTransferTime.Timer()

		if err := fc.sendSnapshot(); err != nil {
			fc.snapshotsFailedCounter.Inc()
			return err
		}

		timer.Done()
	}

	return fc.streamEntries()
}

func (fc *followerCursor) sendSnapshot() error {
	fc.Lock()
	defer fc.Unlock()

	fc.snapshotsStartedCounter.Inc()

	ctx, cancel := context.WithCancel(fc.ctx)
	defer cancel()

	stream, err := fc.replicateStreamProvider.SendSnapshot(ctx, fc.follower, fc.namespace, fc.shardId)
	if err != nil {
		return err
	}

	snapshot, err := fc.db.Snapshot()
	if err != nil {
		return err
	}

	defer snapshot.Close()

	var chunksCount, totalSize int64
	startTime := time.Now()

	for ; snapshot.Valid(); snapshot.Next() {
		chunk, err := snapshot.Chunk()
		if err != nil {
			return err
		}
		content := chunk.Content()

		fc.log.Debug().
			Str("chunk-name", chunk.Name()).
			Int("chunk-size", len(content)).
			Msg("Sending snapshot chunk")

		if err := stream.Send(&proto.SnapshotChunk{
			Term:       fc.term,
			Name:       chunk.Name(),
			ChunkIndex: chunk.Index(),
			ChunkCount: chunk.TotalCount(),
			Content:    content,
		}); err != nil {
			return err
		}

		chunksCount++
		size := len(content)
		totalSize += int64(size)
		fc.snapshotsBytesSent.Add(size)
	}

	fc.log.Debug().
		Msg("Sent the complete snapshot, waiting for response")

	// Sent all the chunks. Wait for the follower ack
	response, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	elapsedTime := time.Since(startTime)
	throughput := float64(totalSize) / elapsedTime.Seconds()

	fc.log.Info().
		Int64("chunks-count", chunksCount).
		Str("total-size", humanize.IBytes(uint64(totalSize))).
		Stringer("elapsed-time", elapsedTime).
		Str("throughput", fmt.Sprintf("%s/s", humanize.IBytes(uint64(throughput)))).
		Int64("follower-ack-offset", response.AckOffset).
		Msg("Successfully sent snapshot to follower")
	fc.ackOffset.Store(response.AckOffset)
	fc.snapshotsCompletedCounter.Inc()
	return nil
}

func (fc *followerCursor) streamEntries() error {
	ctx, cancel := context.WithCancel(fc.ctx)
	defer cancel()

	fc.Lock()
	var err error
	if fc.stream, err = fc.replicateStreamProvider.GetReplicateStream(ctx, fc.follower, fc.namespace, fc.shardId); err != nil {
		fc.Unlock()
		return err
	}
	fc.Unlock()

	currentOffset := fc.ackOffset.Load()

	reader, err := fc.wal.NewReader(currentOffset)
	if err != nil {
		return err
	}
	defer reader.Close()

	go common.DoWithLabels(map[string]string{
		"oxia":  "follower-cursor-receive",
		"shard": fmt.Sprintf("%d", fc.shardId),
	}, func() {
		fc.receiveAcks(cancel, fc.stream)
	})

	fc.log.Info().
		Int64("ack-offset", currentOffset).
		Msg("Successfully attached cursor follower")

	for {
		if fc.closed.Load() {
			return nil
		}

		if !reader.HasNext() {
			// We have reached the head of the wal
			// Wait for more entries to be written
			if err = fc.ackTracker.WaitForHeadOffset(ctx, currentOffset+1); err != nil {
				return err
			}

			continue
		}

		le, err := reader.ReadNext()
		if err != nil {
			return err
		}

		fc.log.Debug().
			Int64("offset", le.Offset).
			Msg("Sending entries to follower")

		if err = fc.stream.Send(&proto.Append{
			Term:         fc.term,
			Entry:        le,
			CommitOffset: fc.ackTracker.CommitOffset(),
		}); err != nil {
			return err
		}

		fc.lastPushed.Store(le.Offset)
		currentOffset = le.Offset

		// Since we've made progress, we can reset the backoff to initial setting
		fc.backoff.Reset()
	}
}

func (fc *followerCursor) receiveAcks(cancel context.CancelFunc, stream proto.OxiaLogReplication_ReplicateClient) {
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			fc.log.Info().Msg("Ack stream finished")
			return
		}
		if err != nil {
			if status.Code(err) != codes.Canceled && status.Code(err) != codes.Unavailable {
				fc.log.Warn().Err(err).
					Msg("Error while receiving acks")
			}

			cancel()
			return
		}

		if res == nil {
			// Stream was closed by server side
			return
		}

		fc.log.Debug().
			Int64("offset", res.Offset).
			Msg("Received ack")
		fc.cursorAcker.Ack(res.Offset)

		fc.ackOffset.Store(res.Offset)
	}
}
