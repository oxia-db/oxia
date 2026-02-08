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

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/dataserver/controller/statemachine"
	dserror "github.com/oxia-db/oxia/oxiad/dataserver/errors"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	constant2 "github.com/oxia-db/oxia/oxiad/dataserver/constant"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
)

// FollowerController handles all the operations of a given shard's follower.
type FollowerController interface {
	io.Closer

	Term() int64
	CommitOffset() int64
	Status() proto.ServingStatus
	GetStatus(request *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
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
	Delete(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)
	AppendEntries(stream proto.OxiaLogReplication_ReplicateServer) error
	InstallSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) error
}

type followerController struct {
	rwMutex   sync.RWMutex
	waitGroup sync.WaitGroup
	log       *slog.Logger
	ctx       context.Context
	cancel    context.CancelFunc

	// Invariants: set once at construction, never modified.
	namespace      string
	shardId        int64
	kvFactory      kvstore.Factory
	storageOptions *option.StorageOptions

	// Atomic state: lock-free reads and writes.
	closed                 atomic.Bool
	status                 atomic.Int32
	term                   *atomic.Int64 // Writes MUST hold rwMutex to prevent term regression.
	commitOffset           *atomic.Int64 // The commit offset already applied in the database.
	advertisedCommitOffset *atomic.Int64 // The highest commit offset advertised by the leader.
	lastAppendedOffset     *atomic.Int64 // The offset of the last entry appended and not fully synced yet on the WAL.

	// Guarded resources: all access MUST hold rwMutex (RLock for reads, Lock for mutations).
	// db may be nil during InstallSnapshot; it is recovered on failure.
	wal             wal.Wal
	db              database.DB
	logSynchronizer *LogSynchronizer

	stateApplierCond  chan struct{}
	writeLatencyHisto metric.LatencyHistogram
}

func initDatabase(namespace string, shardId int64, newTermOptions *proto.NewTermOptions, storageOptions *option.StorageOptions,
	factory kvstore.Factory) (term int64, commitOffset int64, db database.DB, err error) {
	var to *database.TermOptions
	if newTermOptions != nil {
		tmpTo := database.ToDbOption(newTermOptions)
		to = &tmpTo
	}
	keySorting := proto.KeySortingType_UNKNOWN
	if to != nil {
		keySorting = to.KeySorting
	}
	db, err = database.NewDB(namespace, shardId, factory, keySorting, storageOptions.Notification.Retention.ToDuration(), time.SystemClock)
	if err != nil {
		return constant.I64NegativeOne, constant.I64NegativeOne, nil, err
	}
	term, dbTermOptions, err := db.ReadTerm()
	if err != nil {
		return constant.I64NegativeOne, constant.I64NegativeOne, db, err
	}
	if newTermOptions == nil {
		to = &dbTermOptions
	}
	db.EnableNotifications(to.NotificationsEnabled)

	commitOffset, err = db.ReadCommitOffset()
	if err != nil {
		return constant.I64NegativeOne, constant.I64NegativeOne, db, err
	}
	return term, commitOffset, db, nil
}

func NewFollowerController(storageOptions *option.StorageOptions, namespace string, shardId int64, wf wal.Factory, kvFactory kvstore.Factory,
	newTermOptions *proto.NewTermOptions,
) (FollowerController, error) {
	rawTerm, rawCommitOffset, db, err := initDatabase(namespace, shardId, newTermOptions, storageOptions, kvFactory)
	if err != nil {
		return nil, err
	}
	commitOffset := &atomic.Int64{}
	commitOffset.Store(rawCommitOffset)

	writeAheadLog, err := wf.NewWal(namespace, shardId, wal.NewCommitOffsetObserver(commitOffset))
	if err != nil {
		return nil, err
	}
	lastAppendedOffset := &atomic.Int64{}
	lastAppendedOffset.Store(writeAheadLog.LastOffset())

	if lastAppendedOffset.Load() == constant.I64NegativeOne {
		lastAppendedOffset.Store(rawCommitOffset)
	}
	advertisedCommitOffset := &atomic.Int64{}
	advertisedCommitOffset.Store(rawCommitOffset)

	term := &atomic.Int64{}
	term.Store(rawTerm)

	ctx, cancel := context.WithCancel(context.Background()) // todo: add parent context
	fc := &followerController{
		rwMutex:   sync.RWMutex{},
		waitGroup: sync.WaitGroup{},
		ctx:       ctx,
		cancel:    cancel,
		log: slog.With(
			slog.String("component", "follower-controller"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
			slog.Any("term", term),
		),
		storageOptions:         storageOptions,
		namespace:              namespace,
		shardId:                shardId,
		kvFactory:              kvFactory,
		term:                   term,
		commitOffset:           commitOffset,
		advertisedCommitOffset: advertisedCommitOffset,
		lastAppendedOffset:     lastAppendedOffset,
		wal:                    writeAheadLog,
		db:                     db,
		stateApplierCond:       make(chan struct{}, 1),
		writeLatencyHisto: metric.NewLatencyHistogram("oxia_server_follower_write_latency",
			"Latency for write operations in the follower", metric.LabelsForShard(namespace, shardId)),
	}

	if rawTerm != constant.I64NegativeOne {
		fc.status.Store(int32(proto.ServingStatus_FENCED))
	} else {
		fc.status.Store(int32(proto.ServingStatus_NOT_MEMBER))
	}

	fc.waitGroup.Go(func() {
		process.DoWithLabels(
			fc.ctx,
			map[string]string{
				"oxia":      "follower-state-applier",
				"namespace": namespace,
				"shard":     fmt.Sprintf("%d", fc.shardId),
			},
			fc.stateApplier,
		)
	})

	fc.log.Info("Created follower", slog.Int64("head-offset", fc.lastAppendedOffset.Load()), slog.Int64("commit-offset", commitOffset.Load()))
	return fc, nil
}

func (fc *followerController) Close() error {
	if !fc.closed.CompareAndSwap(false, true) {
		return nil
	}
	fc.log.Info("Closing follower controller")
	var err error
	defer func() {
		if err != nil {
			fc.log.Error("Follower controller closed with error", slog.Any("error", err))
			return
		}
		fc.log.Info("Follower controller closed")
	}()
	fc.cancel()
	fc.waitGroup.Wait()

	fc.rwMutex.Lock()
	defer fc.rwMutex.Unlock()
	if fc.logSynchronizer.IsValid() {
		err = multierr.Append(err, fc.logSynchronizer.Close())
	}
	return multierr.Combine(
		err,
		fc.wal.Close(),
		fc.db.Close(),
	)
}

func (fc *followerController) Status() proto.ServingStatus {
	return proto.ServingStatus(fc.status.Load())
}

func (fc *followerController) Term() int64 {
	return fc.term.Load()
}

func (fc *followerController) CommitOffset() int64 {
	return fc.commitOffset.Load()
}

func (fc *followerController) AppendEntries(stream proto.OxiaLogReplication_ReplicateServer) error {
	if fc.closed.Load() {
		return dserror.ErrResourceConflict
	}
	var synchronizer *LogSynchronizer
	var err error
	if synchronizer, err = func() (*LogSynchronizer, error) {
		fc.rwMutex.Lock()
		defer fc.rwMutex.Unlock()

		if fc.closed.Load() { // double-check
			return nil, dserror.ErrResourceConflict
		}

		if s := proto.ServingStatus(fc.status.Load()); s != proto.ServingStatus_FENCED && s != proto.ServingStatus_FOLLOWER {
			return nil, dserror.ErrInvalidStatus
		}
		if fc.logSynchronizer.IsValid() {
			return nil, dserror.ErrResourceConflict
		}
		fc.logSynchronizer = NewLogSynchronizer(LogSynchronizerParams{
			Log:                    fc.log,
			Namespace:              fc.namespace,
			ShardId:                fc.shardId,
			Term:                   fc.term.Load(),
			Wal:                    fc.wal,
			AdvertisedCommitOffset: fc.advertisedCommitOffset,
			LastAppendedOffset:     fc.lastAppendedOffset,
			WriteLatencyHisto:      fc.writeLatencyHisto,
			StateApplierCond:       fc.stateApplierCond,
			Stream:                 stream,
			OnAppend:               func() { fc.status.Store(int32(proto.ServingStatus_FOLLOWER)) },
		})
		return fc.logSynchronizer, nil
	}(); err != nil {
		return err
	}
	return synchronizer.SyncAndClose()
}
func (fc *followerController) NewTerm(req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	if fc.closed.Load() {
		return nil, dserror.ErrResourceConflict
	}
	var err error
	newTerm := req.GetTerm()
	newTermOptions := req.GetOptions()
	if newTerm < fc.term.Load() { // Allowing idempotency during negotiations
		fc.log.Warn("Failed to fence with invalid term", slog.Int64("new-term", newTerm))
		return nil, dserror.ErrInvalidTerm
	}
	fc.rwMutex.Lock()
	defer fc.rwMutex.Unlock()

	if fc.closed.Load() { // double-check
		return nil, dserror.ErrResourceConflict
	}

	if newTerm < fc.term.Load() { // double-check after lock
		return nil, dserror.ErrInvalidTerm
	}

	if fc.logSynchronizer.IsValid() {
		if err := fc.logSynchronizer.Close(); err != nil {
			return nil, errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to close log synchronizer")
		}
		fc.logSynchronizer = nil
	}

	dbOption := database.ToDbOption(newTermOptions)
	fc.db.EnableNotifications(dbOption.NotificationsEnabled)
	if err = fc.db.UpdateTerm(req.Term, dbOption); err != nil {
		return nil, errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "persistent term failed")
	}
	fc.term.Store(newTerm)
	fc.status.Store(int32(proto.ServingStatus_FENCED))
	lastEntryId, err := getLastEntryIdInWal(fc.wal) // todo: consider support it in the WAL directly
	if err != nil {
		fc.log.Warn("Failed to get last entry from WAL", slog.Any("error", err), slog.Int64("new-term", req.Term))
		return nil, errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "get last entry from WAL failed")
	}
	fc.log.Info("Follower successfully initialized in new term", slog.Any("last-entry", lastEntryId))
	return &proto.NewTermResponse{HeadEntryId: lastEntryId}, nil
}

func (fc *followerController) Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	if fc.closed.Load() {
		return nil, dserror.ErrResourceConflict
	}
	fc.rwMutex.Lock()
	defer fc.rwMutex.Unlock()

	if fc.closed.Load() { // double-check
		return nil, dserror.ErrResourceConflict
	}

	if fc.logSynchronizer.IsValid() {
		return nil, dserror.ErrResourceConflict
	}

	if proto.ServingStatus(fc.status.Load()) != proto.ServingStatus_FENCED {
		return nil, dserror.ErrInvalidStatus
	}

	newTerm := req.GetTerm()
	if newTerm != fc.term.Load() {
		return nil, dserror.ErrInvalidTerm
	}
	fc.status.Store(int32(proto.ServingStatus_FOLLOWER))
	headOffset, err := fc.wal.TruncateLog(req.HeadEntryId.Offset)
	if err != nil {
		return nil, errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err),
			"failed to truncate wal. truncate-offset: %d - wal-last-offset: %d", req.HeadEntryId.Offset, fc.wal.LastOffset())
	}
	fc.lastAppendedOffset.Store(headOffset)
	return &proto.TruncateResponse{
		HeadEntryId: &proto.EntryId{
			Term:   req.Term,
			Offset: headOffset,
		},
	}, nil
}

func (fc *followerController) stateApplier() {
	for {
		select {
		case <-fc.ctx.Done():
			return
		case <-fc.stateApplierCond:
			// todo: support retry logic
			maxInclusive := fc.advertisedCommitOffset.Load()
			if err := fc.applyCommittedEntries(maxInclusive); err != nil {
				fc.log.Error("State applier failed", slog.Any("error", err))
				return
			}
		}
	}
}

func (fc *followerController) applyCommittedEntries(maxInclusive int64) error {
	fc.log.Debug(
		"Apply committed entries",
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
		if err = statemachine.ApplyLogEntry(fc.db, entry, lead.WrapperUpdateOperationCallback); err != nil {
			return err
		}

		fc.commitOffset.Store(entry.Offset)
	}

	return nil
}

func (fc *followerController) InstallSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) error { //nolint:revive // cyclomatic complexity justified by sequential error handling
	if fc.closed.Load() {
		return dserror.ErrResourceConflict
	}
	fc.log.Info("Installing snapshot...")
	var err error
	defer func() {
		if err != nil {
			fc.log.Error("Follower controller installed snapshot with error", slog.Any("error", err))
			return
		}
	}()

	fc.rwMutex.Lock()
	defer fc.rwMutex.Unlock()

	if fc.closed.Load() { // double check
		return dserror.ErrResourceConflict
	}

	if fc.logSynchronizer.IsValid() {
		err = dserror.ErrResourceConflict
		return err
	}

	// Read the first chunk to validate the term before performing any
	// destructive operations (WAL clear, DB close). This ensures the
	// follower remains usable if the snapshot has a wrong term.
	term := fc.term.Load()
	firstChunk, err := stream.Recv()
	switch {
	case err != nil:
		if errors.Is(err, io.EOF) {
			return nil
		}
		return errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to read first snapshot chunk")
	case firstChunk == nil:
		return nil
	case term != constant.I64NegativeOne && term != firstChunk.Term:
		// The follower could be left with term=-1 by a previous failed
		// attempt at sending the snapshot. It's ok to proceed in that case.
		err = dserror.ErrInvalidTerm
		return err
	}

	if err = fc.wal.Clear(); err != nil {
		return errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to clear WAL")
	}
	oldDb := fc.db
	fc.db = nil
	if err = oldDb.Close(); err != nil {
		return errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to close Database")
	}
	// If anything below fails, recover by re-opening the database from disk
	// so the follower controller remains usable for retries.
	defer func() {
		if err != nil && fc.db == nil {
			fc.log.Warn("Recovering database after failed snapshot install", slog.Any("error", err))
			if _, _, db, initErr := initDatabase(fc.namespace, fc.shardId, nil, fc.storageOptions, fc.kvFactory); initErr == nil {
				fc.db = db
			} else {
				fc.log.Error("Failed to recover database, follower is in a broken state", slog.Any("error", initErr))
			}
		}
	}()
	var loader kvstore.SnapshotLoader
	loader, err = fc.kvFactory.NewSnapshotLoader(fc.namespace, fc.shardId)
	if err != nil {
		return errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to create snapshot loader")
	}
	defer func() {
		_ = loader.Close()
	}()

	totalSize, err := fc.loadSnapshotChunks(loader, firstChunk, stream)
	if err != nil {
		return err
	}
	loader.Complete()

	var db database.DB
	var rawTerm, rawCommitOffset int64
	if rawTerm, rawCommitOffset, db, err = initDatabase(fc.namespace, fc.shardId, nil, fc.storageOptions, fc.kvFactory); err != nil {
		return errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to initialize database")
	}
	fc.db = db
	fc.term.Store(rawTerm)
	fc.commitOffset.Store(rawCommitOffset)
	fc.lastAppendedOffset.Store(rawCommitOffset)
	fc.advertisedCommitOffset.Store(rawCommitOffset)

	if err = stream.SendAndClose(&proto.SnapshotResponse{
		AckOffset: rawCommitOffset,
	}); err != nil {
		return errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to send snapshot response")
	}
	fc.log.Info(
		"Successfully installed snapshot",
		slog.Int64("snapshot-size", totalSize),
		slog.Int64("commit-offset", rawCommitOffset),
	)
	return nil
}

func (fc *followerController) loadSnapshotChunks(loader kvstore.SnapshotLoader, firstChunk *proto.SnapshotChunk, stream proto.OxiaLogReplication_SendSnapshotServer) (int64, error) {
	fc.log.Info(
		"Applying snapshot chunk",
		slog.String("chunk-name", firstChunk.Name),
		slog.Int("chunk-size", len(firstChunk.Content)),
		slog.String("chunk-progress", fmt.Sprintf("%d/%d", firstChunk.ChunkIndex, firstChunk.ChunkCount)),
	)
	if err := loader.AddChunk(firstChunk.Name, firstChunk.ChunkIndex, firstChunk.ChunkCount, firstChunk.Content); err != nil {
		return 0, errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to add snapshot chunk")
	}
	totalSize := int64(len(firstChunk.Content))

	for {
		snapChunk, err := stream.Recv()
		switch {
		case err != nil:
			if errors.Is(err, io.EOF) {
				return totalSize, nil
			}
			return 0, errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to read snapshot chunk")
		case snapChunk == nil:
			return totalSize, nil
		}
		fc.log.Info(
			"Applying snapshot chunk",
			slog.String("chunk-name", snapChunk.Name),
			slog.Int("chunk-size", len(snapChunk.Content)),
			slog.String("chunk-progress", fmt.Sprintf("%d/%d", snapChunk.ChunkIndex, snapChunk.ChunkCount)),
		)
		if err = loader.AddChunk(snapChunk.Name, snapChunk.ChunkIndex, snapChunk.ChunkCount, snapChunk.Content); err != nil {
			return 0, errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "failed to add snapshot chunk")
		}
		totalSize += int64(len(snapChunk.Content))
	}
}

func (fc *followerController) GetStatus(_ *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	if fc.closed.Load() {
		return nil, dserror.ErrResourceConflict
	}

	return &proto.GetStatusResponse{
		Term:         fc.term.Load(),
		Status:       proto.ServingStatus(fc.status.Load()),
		HeadOffset:   fc.lastAppendedOffset.Load(),
		CommitOffset: fc.commitOffset.Load(),
	}, nil
}

func (fc *followerController) Delete(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	if fc.closed.Load() {
		return nil, dserror.ErrResourceConflict
	}
	if request.Term < fc.term.Load() {
		return nil, dserror.ErrInvalidTerm
	}
	var err error
	fc.log.Info("Deleting shard")
	defer func() {
		if err != nil {
			fc.log.Error("Follower controller deleted with error", slog.Any("error", err))
			return
		}
		fc.log.Info("Follower controller deleted")
	}()

	if err = fc.Close(); err != nil {
		return nil, errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "delete follower failed")
	}

	fc.rwMutex.Lock()
	defer fc.rwMutex.Unlock()

	if err = multierr.Combine(
		fc.wal.Delete(),
		fc.db.Delete(),
	); err != nil {
		return nil, errors.Wrapf(multierr.Combine(dserror.ErrResourceNotAvailable, err), "delete follower failed")
	}
	return &proto.DeleteShardResponse{}, nil
}

func getLastEntryIdInWal(walObject wal.Wal) (*proto.EntryId, error) {
	reader, err := walObject.NewReverseReader()
	if err != nil {
		return nil, err
	}

	if !reader.HasNext() {
		return constant2.InvalidEntryId, nil
	}

	entry, err := reader.ReadNext()
	if err != nil {
		return nil, err
	}
	return &proto.EntryId{Term: entry.Term, Offset: entry.Offset}, nil
}
