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

package dataserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/oxia-db/oxia/oxiad/dataserver/kv"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/process"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/proto"
)

// ReplicateStreamProvider
// This is a provider for the ReplicateStream Grpc handler
// It's used to allow passing in a mocked version of the Grpc service.
type ReplicateStreamProvider interface {
	GetReplicateStream(ctx context.Context, follower string, namespace string, shard int64, term int64) (proto.OxiaLogReplication_ReplicateClient, error)
	SendSnapshot(ctx context.Context, follower string, namespace string, shard int64, term int64) (proto.OxiaLogReplication_SendSnapshotClient, error)
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
	log     *slog.Logger

	snapshotsTransferTime     metric.LatencyHistogram
	snapshotsStartedCounter   metric.Counter
	snapshotsCompletedCounter metric.Counter
	snapshotsFailedCounter    metric.Counter
	snapshotsBytesSent        metric.Counter
}

func NewFollowerCursor( //nolint:revive
	follower string,
	term int64,
	namespace string,
	shardId int64,
	replicateStreamProvider ReplicateStreamProvider,
	ackTracker QuorumAckTracker,
	walObject wal.Wal,
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
		wal:                     walObject,
		db:                      db,
		namespace:               namespace,
		shardId:                 shardId,

		log: slog.With(
			slog.String("component", "follower-cursor"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
			slog.Int64("term", term),
			slog.String("follower", follower),
		),

		snapshotsTransferTime: metric.NewLatencyHistogram("oxia_server_snapshots_transfer_time",
			"The time taken to transfer a full snapshot", labels),
		snapshotsStartedCounter: metric.NewCounter("oxia_server_snapshots_started",
			"The number of DB snapshots started", "count", labels),
		snapshotsCompletedCounter: metric.NewCounter("oxia_server_snapshots_completed",
			"The number of DB snapshots completed", "count", labels),
		snapshotsFailedCounter: metric.NewCounter("oxia_server_snapshots_failed",
			"The number of DB snapshots failed", "count", labels),
		snapshotsBytesSent: metric.NewCounter("oxia_server_snapshots_sent",
			"The amount of data sent as snapshot", metric.Bytes, labels),
	}

	fc.ctx, fc.cancel = context.WithCancel(context.Background())
	fc.backoff = time2.NewBackOff(fc.ctx)

	fc.lastPushed.Store(ackOffset)
	fc.ackOffset.Store(ackOffset)

	var err error
	if fc.cursorAcker, err = ackTracker.NewCursorAcker(ackOffset); err != nil {
		return nil, err
	}

	go process.DoWithLabels(
		context.Background(),
		map[string]string{
			"oxia":      "follower-cursor-send",
			"namespace": namespace,
			"shard":     fmt.Sprintf("%d", fc.shardId),
		},
		func() {
			fc.run()
		},
	)

	return fc, nil
}

func (fc *followerCursor) shouldSendSnapshot() bool {
	fc.Lock()
	defer fc.Unlock()

	ackOffset := fc.ackOffset.Load()
	walFirstOffset := fc.wal.FirstOffset()

	if ackOffset == wal.InvalidOffset && fc.ackTracker.CommitOffset() >= 0 {
		fc.log.Info(
			"Sending snapshot to empty follower",
			slog.Int64("follower-ack-offset", ackOffset),
			slog.Int64("leader-commit-offset", fc.ackTracker.CommitOffset()),
		)
		return true
	} else if walFirstOffset > 0 && ackOffset < walFirstOffset {
		fc.log.Info(
			"The follower is behind the first available entry in the leader WAL",
			slog.Int64("follower-ack-offset", ackOffset),
			slog.Int64("wal-first-offset", fc.wal.FirstOffset()),
			slog.Int64("wal-last-offset", fc.wal.LastOffset()),
		)
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
			fc.log.Error(
				"Error while pushing entries to follower",
				slog.Any("error", err),
				slog.Duration("retry-after", duration),
			)
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

	stream, err := fc.replicateStreamProvider.SendSnapshot(ctx, fc.follower, fc.namespace, fc.shardId, fc.term)
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

		fc.log.Debug(
			"Sending snapshot chunk",
			slog.String("chunk-name", chunk.Name()),
			slog.Int("chunk-size", len(content)),
		)

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

	fc.log.Debug("Sent the complete snapshot, waiting for response")

	// Sent all the chunks. Wait for the follower ack
	response, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	elapsedTime := time.Since(startTime)
	throughput := float64(totalSize) / elapsedTime.Seconds()

	fc.log.Info(
		"Successfully sent snapshot to follower",
		slog.Int64("chunks-count", chunksCount),
		slog.String("total-size", humanize.IBytes(uint64(totalSize))),
		slog.Any("elapsed-time", elapsedTime),
		slog.String("throughput", fmt.Sprintf("%s/s", humanize.IBytes(uint64(throughput)))),
		slog.Int64("follower-ack-offset", response.AckOffset),
	)
	fc.ackOffset.Store(response.AckOffset)
	fc.snapshotsCompletedCounter.Inc()
	return nil
}

func (fc *followerCursor) streamEntriesLoop(ctx context.Context, reader wal.Reader, currentOffset int64) error {
	for {
		if fc.closed.Load() {
			return nil
		}

		if !reader.HasNext() {
			// We have reached the head of the wal
			// Wait for more entries to be written
			if err := fc.ackTracker.WaitForHeadOffset(ctx, currentOffset+1); err != nil {
				return err
			}

			continue
		}

		le, err := reader.ReadNext()
		if err != nil {
			return err
		}

		fc.log.Debug(
			"Sending entries to follower",
			slog.Int64("offset", le.Offset),
		)

		if err = fc.stream.Send(&proto.Append{
			Term:         fc.term,
			Entry:        le,
			CommitOffset: fc.ackTracker.CommitOffset(),
		}); err != nil {
			return err
		}

		fc.lastPushed.Store(le.Offset)
		currentOffset = le.Offset
	}
}

func (fc *followerCursor) streamEntries() error {
	ctx, cancel := context.WithCancel(fc.ctx)
	defer cancel()

	fc.Lock()
	var err error
	if fc.stream, err = fc.replicateStreamProvider.GetReplicateStream(ctx, fc.follower, fc.namespace, fc.shardId, fc.term); err != nil {
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

	go process.DoWithLabels(
		ctx,
		map[string]string{
			"oxia":  "follower-cursor-receive",
			"shard": fmt.Sprintf("%d", fc.shardId),
		}, func() {
			fc.receiveAcks(cancel, fc.stream)
		},
	)

	fc.log.Info(
		"Successfully attached cursor follower",
		slog.Int64("ack-offset", currentOffset),
	)

	return fc.streamEntriesLoop(ctx, reader, currentOffset)
}

func (fc *followerCursor) receiveAcks(cancel context.CancelFunc, stream proto.OxiaLogReplication_ReplicateClient) {
	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			fc.log.Info("Ack stream finished")
			return
		}
		if err != nil {
			if status.Code(err) != codes.Canceled && status.Code(err) != codes.Unavailable {
				fc.log.Warn(
					"Error while receiving acks",
					slog.Any("error", err),
				)
			}

			cancel()
			return
		}

		if res == nil {
			// Stream was closed by dataserver side
			return
		}

		fc.log.Debug(
			"Received ack",
			slog.Int64("offset", res.Offset),
		)
		fc.cursorAcker.Ack(res.Offset)

		fc.ackOffset.Store(res.Offset)

		// Since we've made progress, we can reset the backoff to initial setting
		fc.backoff.Reset()
	}
}
