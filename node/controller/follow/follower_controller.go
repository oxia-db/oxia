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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/oxia-db/oxia/node/conf"
	"github.com/oxia-db/oxia/node/db/kv"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/compare"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/node/db"
	"github.com/oxia-db/oxia/node/wal"
	"github.com/oxia-db/oxia/proto"
)

// FollowerController handles all the operations of a given shard's follower.
type FollowerController interface {
	io.Closer

	// NewTerm
	//
	// Node handles a new term request
	//
	// A node receives a new term request, fences itself and responds
	// with its head offset.
	//
	// When a node is fenced it cannot:
	// - accept any writes from a client.
	// - accept append from a leader.
	// - send any entries to followers if it was a leader.
	//
	// Any existing follow cursors are destroyed as is any state
	// regarding reconfigurations.
	NewTerm(req *proto.NewTermRequest) (*proto.NewTermResponse, error)

	// Truncate
	//
	// A node that receives a truncate request knows that it
	// has been selected as a follower. It truncates its log
	// to the indicates entry id, updates its term and changes
	// to a Follower.
	Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error)

	Replicate(stream proto.OxiaLogReplication_ReplicateServer) error

	SendSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) error

	GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
	DeleteShard(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)

	Term() int64
	CommitOffset() int64
	Status() proto.ServingStatus
}

type followerController struct {
	sync.Mutex

	namespace string
	shardId   int64
	term      int64

	// The highest commit offset advertised by the leader
	advertisedCommitOffset atomic.Int64

	// The commit offset already applied in the database
	commitOffset atomic.Int64

	// Offset of the last entry appended and not fully synced yet on the wal
	lastAppendedOffset int64

	status      proto.ServingStatus
	wal         wal.Wal
	kvFactory   kv.Factory
	db          db.DB
	termOptions db.TermOptions

	ctx              context.Context
	cancel           context.CancelFunc
	syncCond         concurrent.ConditionContext
	applyEntriesCond concurrent.ConditionContext
	applyEntriesDone chan any
	closeStreamWg    concurrent.WaitGroup
	log              *slog.Logger
	config           conf.Config

	writeLatencyHisto metric.LatencyHistogram
}

func NewFollowerController(config conf.Config, namespace string, shardId int64, wf wal.Factory, kvFactory kv.Factory) (FollowerController, error) {
	fc := &followerController{
		config:           config,
		namespace:        namespace,
		shardId:          shardId,
		kvFactory:        kvFactory,
		status:           proto.ServingStatus_NOT_MEMBER,
		closeStreamWg:    nil,
		applyEntriesDone: make(chan any),
		writeLatencyHisto: metric.NewLatencyHistogram("oxia_server_follower_write_latency",
			"Latency for write operations in the follower", metric.LabelsForShard(namespace, shardId)),
	}
	fc.ctx, fc.cancel = context.WithCancel(context.Background())
	fc.syncCond = concurrent.NewConditionContext(fc)
	fc.applyEntriesCond = concurrent.NewConditionContext(fc)

	var err error
	if fc.wal, err = wf.NewWal(namespace, shardId, fc); err != nil {
		return nil, err
	}

	fc.lastAppendedOffset = fc.wal.LastOffset()

	if fc.db, err = db.NewDB(namespace, shardId, kvFactory,
		compare.EncoderHierarchical, config.NotificationsRetentionTime, time.SystemClock); err != nil {
		return nil, err
	}

	if fc.term, fc.termOptions, err = fc.db.ReadTerm(); err != nil {
		return nil, err
	}

	if fc.term != wal.InvalidTerm {
		fc.status = proto.ServingStatus_FENCED
	}

	fc.db.EnableNotifications(fc.termOptions.NotificationsEnabled)

	commitOffset, err := fc.db.ReadCommitOffset()
	if err != nil {
		return nil, err
	}
	fc.commitOffset.Store(commitOffset)

	if fc.lastAppendedOffset == wal.InvalidOffset {
		// The wal is empty, though we have restored from snapshot
		fc.lastAppendedOffset = commitOffset
	}

	fc.setLogger()

	go process.DoWithLabels(
		fc.ctx,
		map[string]string{
			"oxia":  "follower-apply-committed-entries",
			"shard": fmt.Sprintf("%d", fc.shardId),
		},
		fc.applyAllCommittedEntries,
	)

	fc.log.Info(
		"Created follower",
		slog.Int64("head-offset", fc.lastAppendedOffset),
		slog.Int64("commit-offset", commitOffset),
	)
	return fc, nil
}

func (fc *followerController) setLogger() {
	fc.log = slog.With(
		slog.String("component", "follower-controller"),
		slog.String("namespace", fc.namespace),
		slog.Int64("shard", fc.shardId),
		slog.Int64("term", fc.term),
	)
}

func (fc *followerController) isClosed() bool {
	return fc.ctx.Err() != nil
}

func (fc *followerController) Close() error {
	fc.cancel()

	<-fc.applyEntriesDone

	fc.Lock()
	defer fc.Unlock()

	fc.log.Debug("Closing follower controller")
	return fc.close()
}

func (fc *followerController) close() error {
	var err error

	if fc.wal != nil {
		err = multierr.Append(err, fc.wal.Close())
		fc.wal = nil
	}

	if fc.db != nil {
		err = multierr.Append(err, fc.db.Close())
		fc.db = nil
	}
	return err
}

func (fc *followerController) closeStream(err error) {
	fc.Lock()
	defer fc.Unlock()

	fc.closeStreamNoMutex(err)
}

func (fc *followerController) closeStreamNoMutex(err error) {
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
		fc.log.Warn(
			"Error in handle Replicate stream",
			slog.Any("error", err),
		)
	}

	if fc.closeStreamWg != nil {
		fc.closeStreamWg.Fail(err)
		fc.closeStreamWg = nil
	}
}

func (fc *followerController) Status() proto.ServingStatus {
	fc.Lock()
	defer fc.Unlock()
	return fc.status
}

func (fc *followerController) Term() int64 {
	fc.Lock()
	defer fc.Unlock()
	return fc.term
}

func (fc *followerController) CommitOffset() int64 {
	return fc.commitOffset.Load()
}

func (fc *followerController) NewTerm(req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	if fc.isClosed() {
		return nil, constant.ErrAlreadyClosed
	}

	if req.Term < fc.term {
		fc.log.Warn(
			"Failed to fence with invalid term",
			slog.Int64("follower-term", fc.term),
			slog.Int64("new-term", req.Term),
		)
		return nil, constant.ErrInvalidTerm
	}

	if fc.db == nil {
		var err error
		if fc.db, err = db.NewDB(fc.namespace, fc.shardId, fc.kvFactory,
			compare.EncoderHierarchical, fc.config.NotificationsRetentionTime, time.SystemClock); err != nil {
			return nil, errors.Wrapf(err, "failed to reopen database")
		}
	}

	fc.termOptions = db.ToDbOption(req.Options)
	if err := fc.db.UpdateTerm(req.Term, fc.termOptions); err != nil {
		return nil, err
	}

	fc.db.EnableNotifications(fc.termOptions.NotificationsEnabled)

	fc.term = req.Term
	fc.setLogger()
	fc.status = proto.ServingStatus_FENCED
	fc.closeStreamNoMutex(nil)

	lastEntryId, err := wal.GetLastEntryIdInWal(fc.wal)
	if err != nil {
		fc.log.Warn(
			"Failed to get last",
			slog.Any("error", err),
			slog.Int64("follower-term", fc.term),
			slog.Int64("new-term", req.Term),
		)
		return nil, err
	}

	fc.log.Info(
		"Follower successfully initialized in new term",
		slog.Any("last-entry", lastEntryId),
	)
	return &proto.NewTermResponse{HeadEntryId: lastEntryId}, nil
}

func (fc *followerController) Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	if fc.isClosed() {
		return nil, constant.ErrAlreadyClosed
	}

	if fc.status != proto.ServingStatus_FENCED {
		return nil, constant.ErrInvalidStatus
	}

	if req.Term != fc.term {
		return nil, constant.ErrInvalidTerm
	}

	fc.status = proto.ServingStatus_FOLLOWER
	headOffset, err := fc.wal.TruncateLog(req.HeadEntryId.Offset)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to truncate wal. truncate-offset: %d - wal-last-offset: %d",
			req.HeadEntryId.Offset, fc.wal.LastOffset())
	}
	fc.lastAppendedOffset = headOffset

	return &proto.TruncateResponse{
		HeadEntryId: &proto.EntryId{
			Term:   req.Term,
			Offset: headOffset,
		},
	}, nil
}

func (fc *followerController) Replicate(stream proto.OxiaLogReplication_ReplicateServer) error {
	fc.Lock()
	if fc.status != proto.ServingStatus_FENCED && fc.status != proto.ServingStatus_FOLLOWER {
		fc.Unlock()
		return constant.ErrInvalidStatus
	}

	if fc.closeStreamWg != nil {
		fc.Unlock()
		return constant.ErrLeaderAlreadyConnected
	}

	closeStreamWg := concurrent.NewWaitGroup(1)
	fc.closeStreamWg = closeStreamWg
	fc.Unlock()

	go process.DoWithLabels(
		stream.Context(),
		map[string]string{
			"oxia":  "add-entries",
			"shard": fmt.Sprintf("%d", fc.shardId),
		},
		func() { fc.handleServerStream(stream) },
	)

	go process.DoWithLabels(
		stream.Context(),
		map[string]string{
			"oxia":  "add-entries-sync",
			"shard": fmt.Sprintf("%d", fc.shardId),
		},
		func() { fc.handleReplicateSync(stream) },
	)

	return closeStreamWg.Wait(fc.ctx)
}

func (fc *followerController) handleServerStream(stream proto.OxiaLogReplication_ReplicateServer) {
	for {
		if req, err := stream.Recv(); err != nil {
			fc.closeStream(err)
			return
		} else if req == nil {
			fc.closeStream(nil)
			return
		} else if err := fc.append(req, stream); err != nil {
			fc.closeStream(err)
			return
		}
	}
}

func (fc *followerController) append(req *proto.Append, stream proto.OxiaLogReplication_ReplicateServer) error {
	timer := fc.writeLatencyHisto.Timer()
	defer timer.Done()

	fc.Lock()
	defer fc.Unlock()

	if req.Term != fc.term {
		return constant.ErrInvalidTerm
	}

	fc.log.Debug(
		"Add entry",
		slog.Int64("commit-offset", req.CommitOffset),
		slog.Int64("offset", req.Entry.Offset),
	)

	// A follower node confirms an entry to the leader
	//
	// The follower adds the entry to its log, sets the head offset
	// and updates its commit offset with the commit offset of
	// the request.
	fc.status = proto.ServingStatus_FOLLOWER

	if req.Entry.Offset <= fc.lastAppendedOffset {
		// This was a duplicated request. We already have this entry
		fc.log.Debug(
			"Ignoring duplicated entry",
			slog.Int64("commit-offset", req.CommitOffset),
			slog.Int64("offset", req.Entry.Offset),
		)
		if err := stream.Send(&proto.Ack{Offset: req.Entry.Offset}); err != nil {
			fc.closeStreamNoMutex(err)
		}
		return nil
	}

	// Append the entry asynchronously. We'll sync it in a group from the "sync" routine,
	// where the ack is then sent back
	if err := fc.wal.AppendAsync(req.GetEntry()); err != nil {
		return err
	}

	fc.advertisedCommitOffset.Store(req.CommitOffset)
	fc.lastAppendedOffset = req.Entry.Offset

	// Trigger the sync
	fc.syncCond.Signal()
	return nil
}

func (fc *followerController) handleReplicateSync(stream proto.OxiaLogReplication_ReplicateServer) {
	for {
		fc.Lock()
		if err := fc.syncCond.Wait(stream.Context()); err != nil {
			fc.Unlock()
			fc.closeStream(err)
			return
		}
		fc.Unlock()

		oldHeadOffset := fc.wal.LastOffset()

		if err := fc.wal.Sync(stream.Context()); err != nil {
			fc.closeStream(err)
			return
		}

		// Ack all the entries that were synced in the last round
		newHeadOffset := fc.wal.LastOffset()
		for offset := oldHeadOffset + 1; offset <= newHeadOffset; offset++ {
			if err := stream.Send(&proto.Ack{Offset: offset}); err != nil {
				fc.closeStream(err)
				return
			}
		}

		fc.applyEntriesCond.Signal()
	}
}

func (fc *followerController) applyAllCommittedEntries() {
	for {
		fc.Lock()
		if err := fc.applyEntriesCond.Wait(fc.ctx); err != nil {
			close(fc.applyEntriesDone)
			fc.Unlock()
			return
		}
		fc.Unlock()

		maxInclusive := fc.advertisedCommitOffset.Load()
		if err := fc.processCommittedEntries(maxInclusive); err != nil {
			fc.closeStream(err)
			close(fc.applyEntriesDone)
			return
		}
	}
}

func (fc *followerController) processCommitRequest(entry *proto.LogEntry, logEntryValue *proto.LogEntryValue) error {
	for _, br := range logEntryValue.GetRequests().Writes {
		_, err := fc.db.ProcessWrite(br, entry.Offset, entry.Timestamp, db.WrapperUpdateOperationCallback)
		if err != nil {
			fc.log.Error(
				"Error applying committed entry",
				slog.Any("error", err),
			)
			return err
		}
	}

	return nil
}

func (fc *followerController) processCommittedEntriesLoop(reader wal.Reader, maxInclusive int64) error {
	logEntryValue := proto.LogEntryValueFromVTPool()
	defer logEntryValue.ReturnToVTPool()

	for reader.HasNext() {
		entry, err := reader.ReadNext()

		if errors.Is(err, wal.ErrReaderClosed) {
			fc.log.Info("Stopped reading committed entries")
			return err
		} else if err != nil {
			fc.log.Error("Error reading committed entry", slog.Any("error", err))
			return err
		}

		fc.log.Debug(
			"Reading entry",
			slog.Int64("offset", entry.Offset),
		)

		if entry.Offset > maxInclusive {
			// We read up to the max point
			return nil
		}

		logEntryValue.ResetVT()
		if err := logEntryValue.UnmarshalVT(entry.Value); err != nil {
			fc.log.Error(
				"Error unmarshalling committed entry",
				slog.Any("error", err),
			)
			return err
		}
		if err := fc.processCommitRequest(entry, logEntryValue); err != nil {
			return err
		}

		fc.commitOffset.Store(entry.Offset)
	}

	return nil
}

func (fc *followerController) processCommittedEntries(maxInclusive int64) error {
	fc.log.Debug(
		"Process committed entries",
		slog.Int64("min-exclusive", fc.commitOffset.Load()),
		slog.Int64("max-inclusive", maxInclusive),
		slog.Int64("head-offset", fc.wal.LastOffset()),
	)
	if maxInclusive <= fc.commitOffset.Load() {
		return nil
	}

	reader, err := fc.wal.NewReader(fc.commitOffset.Load())
	if err != nil {
		fc.log.Error(
			"Error opening reader used for applying committed entries",
			slog.Any("error", err),
		)
		return err
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			fc.log.Error(
				"Error closing reader used for applying committed entries",
				slog.Any("error", err),
			)
		}
	}()

	return fc.processCommittedEntriesLoop(reader, maxInclusive)
}

type MessageWithTerm interface {
	GetTerm() int64
}

func (fc *followerController) SendSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) error {
	fc.Lock()

	if fc.closeStreamWg != nil {
		fc.Unlock()
		return constant.ErrLeaderAlreadyConnected
	}

	closeStreamWg := concurrent.NewWaitGroup(1)
	fc.closeStreamWg = closeStreamWg
	fc.Unlock()

	go process.DoWithLabels(
		stream.Context(),
		map[string]string{
			"oxia":  "receive-snapshot",
			"shard": fmt.Sprintf("%d", fc.shardId),
		},
		func() { fc.handleSnapshot(stream) },
	)

	return closeStreamWg.Wait(fc.ctx)
}

func (fc *followerController) readSnapshotStream(stream proto.OxiaLogReplication_SendSnapshotServer, loader kv.SnapshotLoader) (int64, error) {
	var totalSize int64

	for {
		snapChunk, err := stream.Recv()
		switch {
		case err != nil:
			if errors.Is(err, io.EOF) {
				return totalSize, nil
			}

			fc.closeStreamNoMutex(err)
			return totalSize, err
		case snapChunk == nil:
			return totalSize, nil
		case fc.term != wal.InvalidTerm && snapChunk.Term != fc.term:
			// The follower could be left with term=-1 by a previous failed
			// attempt at sending the snapshot. It's ok to proceed in that case.
			fc.closeStreamNoMutex(constant.ErrInvalidTerm)
			return totalSize, constant.ErrInvalidTerm
		}

		fc.term = snapChunk.Term

		fc.log.Debug(
			"Applying snapshot chunk",
			slog.String("chunk-name", snapChunk.Name),
			slog.Int("chunk-size", len(snapChunk.Content)),
			slog.String("chunk-progress", fmt.Sprintf("%d/%d", snapChunk.ChunkIndex, snapChunk.ChunkCount)),
			slog.Int64("term", fc.term),
		)
		if err = loader.AddChunk(snapChunk.Name, snapChunk.ChunkIndex, snapChunk.ChunkCount, snapChunk.Content); err != nil {
			fc.closeStream(err)
			return totalSize, err
		}

		totalSize += int64(len(snapChunk.Content))
	}
}

func (fc *followerController) handleSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) {
	fc.Lock()
	defer fc.Unlock()

	// Wipe out both WAL and DB contents
	err := fc.wal.Clear()
	if err != nil {
		fc.closeStreamNoMutex(err)
		return
	}

	if fc.db != nil {
		err = fc.db.Close()
		if err != nil {
			fc.closeStreamNoMutex(err)
			return
		}

		fc.db = nil
	}

	loader, err := fc.kvFactory.NewSnapshotLoader(fc.namespace, fc.shardId)
	if err != nil {
		fc.closeStreamNoMutex(err)
		return
	}

	defer loader.Close()

	totalSize, err := fc.readSnapshotStream(stream, loader)
	if err != nil {
		return
	}

	// We have received all the files for the database
	loader.Complete()

	newDb, err := db.NewDB(fc.namespace, fc.shardId, fc.kvFactory, compare.EncoderHierarchical, fc.config.NotificationsRetentionTime, time.SystemClock)
	if err != nil {
		fc.closeStreamNoMutex(errors.Wrap(err, "failed to open database after loading snapshot"))
		return
	}

	// The new term must be persisted, to avoid rolling it back
	if err = newDb.UpdateTerm(fc.term, fc.termOptions); err != nil {
		fc.closeStreamNoMutex(errors.Wrap(err, "Failed to update term in db"))
	}

	commitOffset, err := newDb.ReadCommitOffset()
	if err != nil {
		fc.closeStreamNoMutex(errors.Wrap(err, "Failed to read committed offset in the new snapshot"))
		return
	}

	if err = stream.SendAndClose(&proto.SnapshotResponse{
		AckOffset: commitOffset,
	}); err != nil {
		fc.closeStreamNoMutex(errors.Wrap(err, "Failed to send response after processing snapshot"))
		return
	}

	fc.db = newDb
	fc.commitOffset.Store(commitOffset)
	fc.lastAppendedOffset = commitOffset
	fc.closeStreamNoMutex(nil)

	fc.log.Info(
		"Successfully applied snapshot",
		slog.Int64("term", fc.term),
		slog.Int64("snapshot-size", totalSize),
		slog.Int64("commit-offset", commitOffset),
	)
}

func (fc *followerController) GetStatus(_ *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	fc.Lock()
	defer fc.Unlock()

	return &proto.GetStatusResponse{
		Term:         fc.term,
		Status:       fc.status,
		HeadOffset:   fc.lastAppendedOffset,
		CommitOffset: fc.CommitOffset(),
	}, nil
}

func (fc *followerController) DeleteShard(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	fc.cancel()
	<-fc.applyEntriesDone

	fc.Lock()
	defer fc.Unlock()

	if request.Term < fc.term {
		fc.log.Warn("Invalid term when deleting shard",
			slog.Int64("follower-term", fc.term),
			slog.Int64("new-term", request.Term))
		_ = fc.close()
		return nil, constant.ErrInvalidTerm
	}

	fc.log.Info("Deleting shard")

	deleteWal := fc.wal
	deleteDb := fc.db

	// close the fc first
	if err := fc.close(); err != nil {
		return nil, err
	}

	// Wipe out both WAL and DB contents
	if err := multierr.Combine(
		deleteWal.Delete(),
		deleteDb.Delete(),
	); err != nil {
		return nil, err
	}

	return &proto.DeleteShardResponse{}, nil
}
