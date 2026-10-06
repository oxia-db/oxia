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
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/common/crc"
	"github.com/oxia-db/oxia/oxiad/common/feature"

	"github.com/oxia-db/oxia/oxiad/dataserver/controller/statemachine"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	commontime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/common/validation"

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

	IsFeatureEnabled(f proto.Feature) bool
	Checksum() crc.Checksum

	// SetSplitHashRange marks this follower as a split child. After loading
	// a snapshot, the database will be filtered to retain only keys within
	// the given hash range. WAL entries will also be filtered at apply time.
	// The range comes with a stream of the parent in the given term, and
	// only holds in that term: a new term clears it.
	SetSplitHashRange(hashRange *proto.HashRange, term int64)
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
	// Incremented when InstallSnapshot replaces the WAL and database content:
	// the state applier discards the entries it read before.
	snapshotGeneration int64

	stateApplierCond  chan struct{}
	writeLatencyHisto metric.LatencyHistogram
	checksumGauge     metric.SyncGauge
	walChecksumGauge  metric.SyncGauge

	// splitHashRange, when non-nil, indicates this follower is a child shard
	// in a split. The snapshot will be filtered after loading, and WAL entries
	// will be filtered at state machine apply time. Set by the streams of the
	// parent, and cleared by a new term.
	splitHashRange *proto.HashRange
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
	db, err = database.NewDB(namespace, shardId, factory, keySorting, storageOptions.Notification.Retention.ToDuration(), commontime.SystemClock)
	if err != nil {
		return constant.I64NegativeOne, constant.I64NegativeOne, nil, err
	}
	term, dbTermOptions, err := db.ReadTerm()
	if err != nil {
		return constant.I64NegativeOne, constant.I64NegativeOne, nil, multierr.Append(err, db.Close())
	}
	if newTermOptions == nil {
		to = &dbTermOptions
	}
	db.EnableNotifications(to.NotificationsEnabled)

	commitOffset, err = db.ReadCommitOffset()
	if err != nil {
		return constant.I64NegativeOne, constant.I64NegativeOne, nil, multierr.Append(err, db.Close())
	}
	return term, commitOffset, db, nil
}

func NewFollowerController(storageOptions *option.StorageOptions, namespace string, shardId int64, wf wal.Factory, kvFactory kvstore.Factory,
	newTermOptions *proto.NewTermOptions,
) (FollowerController, error) {
	if err := validation.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	rawTerm, rawCommitOffset, db, err := initDatabase(namespace, shardId, newTermOptions, storageOptions, kvFactory)
	if err != nil {
		return nil, err
	}
	commitOffset := &atomic.Int64{}
	commitOffset.Store(rawCommitOffset)
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
		),
		storageOptions:         storageOptions,
		namespace:              namespace,
		shardId:                shardId,
		kvFactory:              kvFactory,
		term:                   term,
		commitOffset:           commitOffset,
		advertisedCommitOffset: advertisedCommitOffset,
		lastAppendedOffset:     &atomic.Int64{},
		db:                     db,
		stateApplierCond:       make(chan struct{}, 1),
		writeLatencyHisto: metric.NewLatencyHistogram("oxia_server_follower_write_latency",
			"Latency for write operations in the follower", metric.LabelsForShard(namespace, shardId)),
		checksumGauge: metric.NewSyncGauge("oxia_dataserver_db_checksum",
			"The current DB checksum value", "count", metric.LabelsForShard(namespace, shardId)),
		walChecksumGauge: metric.NewSyncGauge("oxia_dataserver_wal_checksum",
			"The current WAL checksum value", "count", metric.LabelsForShard(namespace, shardId)),
	}

	// The WAL trimming gets the commit offset from the follower, which flushes
	// the database for it
	if fc.wal, err = wf.NewWal(namespace, shardId, fc); err != nil {
		cancel()
		return nil, multierr.Append(err, db.Close())
	}
	fc.lastAppendedOffset.Store(fc.wal.LastOffset())
	if fc.lastAppendedOffset.Load() == constant.I64NegativeOne {
		fc.lastAppendedOffset.Store(rawCommitOffset)
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

	fc.log.Info("Created follower", slog.Int64("term", fc.term.Load()), slog.Int64("head-offset", fc.lastAppendedOffset.Load()), slog.Int64("commit-offset", commitOffset.Load()))
	return fc, nil
}

func (fc *followerController) Close() error {
	if !fc.closed.CompareAndSwap(false, true) {
		return nil
	}
	fc.log.Info("Closing follower controller", slog.Int64("term", fc.term.Load()))
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

// FlushDatabase is called by the WAL trimming, while Close can hold the lock
// and wait for the WAL to close: it doesn't wait for the lock. When a writer
// holds it or waits for it, like a snapshot install or Close, the trimming
// deletes no segment, and retries later.
func (fc *followerController) FlushDatabase() error {
	if !fc.rwMutex.TryRLock() {
		return errors.Wrap(constant.ErrResourceConflict, "the follower is busy")
	}
	defer fc.rwMutex.RUnlock()
	return fc.db.Flush()
}

func (fc *followerController) AppendEntries(stream proto.OxiaLogReplication_ReplicateServer) error {
	if fc.closed.Load() {
		return constant.ErrResourceConflict
	}
	var synchronizer *LogSynchronizer
	var err error
	if synchronizer, err = func() (*LogSynchronizer, error) {
		fc.rwMutex.Lock()
		defer fc.rwMutex.Unlock()

		if fc.closed.Load() { // double-check
			return nil, constant.ErrResourceConflict
		}

		if s := proto.ServingStatus(fc.status.Load()); s != proto.ServingStatus_FENCED && s != proto.ServingStatus_FOLLOWER {
			if s == proto.ServingStatus_NOT_MEMBER {
				return nil, constant.ErrNodeIsNotMember
			}
			return nil, constant.ErrInvalidStatus
		}
		if fc.logSynchronizer.IsValid() {
			return nil, constant.ErrResourceConflict
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
		return nil, constant.ErrResourceConflict
	}
	var err error
	newTerm := req.GetTerm()
	newTermOptions := req.GetOptions()
	if newTerm < fc.term.Load() { // Allowing idempotency during negotiations
		fc.log.Warn("Failed to fence with invalid term", slog.Int64("current-term", fc.term.Load()), slog.Int64("new-term", newTerm))
		return nil, constant.ErrInvalidTerm
	}
	fc.rwMutex.Lock()
	defer fc.rwMutex.Unlock()

	if fc.closed.Load() { // double-check
		return nil, constant.ErrResourceConflict
	}

	if newTerm < fc.term.Load() { // double-check after lock
		return nil, constant.ErrInvalidTerm
	}

	if unsupported := feature.Unsupported(newTermOptions.GetFeatures()); len(unsupported) > 0 {
		fc.log.Error(
			"Rejecting new term: it pins features not supported by this binary",
			slog.Int64("new-term", newTerm),
			slog.Any("unsupported-features", unsupported),
		)
		return nil, errors.Wrapf(constant.ErrUnsupportedFeatures,
			"term %d pins features %v not supported by this binary", newTerm, unsupported)
	}

	if fc.logSynchronizer.IsValid() {
		if err := fc.logSynchronizer.Close(); err != nil {
			return nil, errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to close log synchronizer")
		}
		fc.logSynchronizer = nil
	}

	// The split range only holds in the parent's term. A node that still
	// observes the parent gets it again with the next stream, while one that
	// stays a follower of the child must apply the child's entries, and
	// install its snapshots, as they are. The parent's entries it replays
	// later are filtered with the split filter recorded in the database.
	fc.splitHashRange = nil

	dbOption := database.ToDbOption(newTermOptions)
	fc.db.EnableNotifications(dbOption.NotificationsEnabled)
	if err = fc.db.UpdateTerm(req.Term, dbOption); err != nil {
		return nil, errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "persistent term failed")
	}
	fc.term.Store(newTerm)
	fc.status.Store(int32(proto.ServingStatus_FENCED))
	headEntryId, err := lead.HeadEntryId(fc.wal, fc.db)
	if err != nil {
		fc.log.Warn("Failed to get the head entry", slog.Any("error", err), slog.Int64("new-term", req.Term))
		return nil, errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "get head entry failed")
	}
	fc.log.Info("Follower successfully initialized in new term", slog.Int64("term", fc.term.Load()),
		slog.Any("last-entry", headEntryId))
	return &proto.NewTermResponse{
		HeadEntryId:     headEntryId,
		FeaturesEnabled: fc.db.EnabledFeatures(),
	}, nil
}

func (fc *followerController) Truncate(req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	if fc.closed.Load() {
		return nil, constant.ErrResourceConflict
	}
	fc.rwMutex.Lock()
	defer fc.rwMutex.Unlock()

	if fc.closed.Load() { // double-check
		return nil, constant.ErrResourceConflict
	}

	if fc.logSynchronizer.IsValid() {
		return nil, constant.ErrResourceConflict
	}

	if proto.ServingStatus(fc.status.Load()) != proto.ServingStatus_FENCED {
		return nil, constant.ErrInvalidStatus
	}

	newTerm := req.GetTerm()
	if newTerm != fc.term.Load() {
		return nil, constant.ErrInvalidTerm
	}
	fc.status.Store(int32(proto.ServingStatus_FOLLOWER))
	headOffset, err := fc.wal.TruncateLog(req.HeadEntryId.Offset)
	if err != nil {
		return nil, errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err),
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
	bo := commontime.NewBackOff(fc.ctx)
	_ = backoff.RetryNotify(func() error {
		for {
			select {
			case <-fc.ctx.Done():
				return nil
			case <-fc.stateApplierCond:
			}

			maxInclusive := fc.advertisedCommitOffset.Load()
			if err := fc.applyCommittedEntries(maxInclusive); err != nil {
				return err
			}
			bo.Reset()
		}
	}, bo, func(err error, d time.Duration) {
		fc.log.Error("State applier failed, retrying",
			slog.Int64("term", fc.term.Load()),
			slog.Any("error", err),
			slog.Duration("retry-after", d),
		)
	})
}

func (fc *followerController) processCommittedEntriesLoop(reader wal.Reader, snapshotGeneration int64,
	maxInclusive int64) error {
	for reader.HasNext() {
		entry, _, entryCrc, err := reader.ReadNext()

		if errors.Is(err, wal.ErrReaderClosed) {
			fc.log.Info("Stopped reading committed entries")
			return err
		} else if err != nil {
			fc.log.Error("Error reading committed entry", slog.Any("error", err))
			return err
		}

		if fc.log.Enabled(fc.ctx, slog.LevelDebug) {
			fc.log.Debug(
				"Reading entry",
				slog.Int64("offset", entry.Offset),
			)
		}

		if entry.Offset > maxInclusive {
			// We read up to the max point
			return nil
		}

		fc.rwMutex.RLock()
		if fc.snapshotGeneration != snapshotGeneration {
			// A snapshot replaced the WAL and the database since the entry
			// was read: it must not be applied on top of the snapshot
			fc.rwMutex.RUnlock()
			fc.log.Info(
				"Discarded the committed entries read before the snapshot install",
				slog.Int64("offset", entry.Offset),
			)
			return nil
		}
		resp, err := fc.applyEntry(entry)
		fc.rwMutex.RUnlock()
		if err != nil {
			return err
		}
		fc.recordChecksums(resp, entryCrc)
		if entry.Offset == maxInclusive {
			// Stop at the max point, not at the WAL head: the next entry
			// would only be discarded, and read again by the next pass
			return nil
		}
	}

	return nil
}

// applyCommittedEntries applies the committed entries up to maxInclusive. It
// takes them from the replication stream, which keeps the entries it appends to
// the WAL, and reads back from the WAL only the ones the stream doesn't keep:
// the entries appended before it, e.g. before a restart or a reconnection, and
// those it had no room for, e.g. while catching up.
func (fc *followerController) applyCommittedEntries(maxInclusive int64) error {
	if fc.log.Enabled(fc.ctx, slog.LevelDebug) {
		fc.log.Debug(
			"Apply committed entries",
			slog.Int64("min-exclusive", fc.commitOffset.Load()),
			slog.Int64("max-inclusive", maxInclusive),
			slog.Int64("head-offset", fc.wal.LastOffset()),
		)
	}
	if maxInclusive <= fc.commitOffset.Load() {
		return nil
	}

	// The entries are applied from the WAL and database content of the same
	// snapshot generation, and only once synced in the WAL
	fc.rwMutex.RLock()
	snapshotGeneration := fc.snapshotGeneration
	maxInclusive = min(maxInclusive, fc.wal.LastOffset())
	fc.rwMutex.RUnlock()

	for fc.commitOffset.Load() < maxInclusive {
		firstAppended, err := fc.applyAppendedEntries(snapshotGeneration, maxInclusive)
		if err != nil {
			return err
		}
		// The stream doesn't keep the next entry: read it from the WAL, with
		// the ones after it up to the first entry the stream keeps
		commitOffset := fc.commitOffset.Load()
		walMaxInclusive := min(maxInclusive, firstAppended-1)
		if walMaxInclusive <= commitOffset {
			// All applied, or a snapshot replaced the WAL
			return nil
		}
		if err = fc.applyWalEntries(snapshotGeneration, walMaxInclusive); err != nil {
			return err
		}
		if fc.commitOffset.Load() == commitOffset {
			// A snapshot replaced the WAL
			return nil
		}
	}
	return nil
}

// applyAppendedEntries applies the committed entries up to maxInclusive that
// the replication stream keeps, as long as it keeps the next one. It returns
// the offset of the first entry the stream keeps after them, or math.MaxInt64
// when it keeps none.
func (fc *followerController) applyAppendedEntries(snapshotGeneration int64, maxInclusive int64) (int64, error) {
	for {
		fc.rwMutex.RLock()
		// A closed stream doesn't keep the entries: the WAL can get truncated
		// or cleared once it is closed
		if fc.snapshotGeneration != snapshotGeneration || !fc.logSynchronizer.IsValid() {
			fc.rwMutex.RUnlock()
			return math.MaxInt64, nil
		}
		offset := fc.commitOffset.Load() + 1
		if offset > maxInclusive {
			fc.rwMutex.RUnlock()
			return math.MaxInt64, nil
		}
		next, firstOffset, ok := fc.logSynchronizer.appended.take(offset)
		if !ok {
			fc.rwMutex.RUnlock()
			return firstOffset, nil
		}
		resp, err := fc.applyEntry(next.entry)
		fc.rwMutex.RUnlock()
		if err != nil {
			return 0, err
		}
		fc.recordChecksums(resp, next.entryCrc)
	}
}

// applyWalEntries applies the committed entries up to maxInclusive that it
// reads from the WAL.
func (fc *followerController) applyWalEntries(snapshotGeneration int64, maxInclusive int64) error {
	// Open the reader under the lock, so that it starts from the commit
	// offset of the WAL and database content of the same snapshot generation
	fc.rwMutex.RLock()
	if fc.snapshotGeneration != snapshotGeneration {
		fc.rwMutex.RUnlock()
		return nil
	}
	reader, err := fc.wal.NewReader(fc.commitOffset.Load())
	fc.rwMutex.RUnlock()
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

	return fc.processCommittedEntriesLoop(reader, snapshotGeneration, maxInclusive)
}

// applyEntry applies the committed entry that follows the commit offset. Must
// be called while holding the read lock: a snapshot installed after it is
// released sets its own commit offset, which must not be moved back.
func (fc *followerController) applyEntry(entry *proto.LogEntry) (statemachine.ApplyResponse, error) {
	var resp statemachine.ApplyResponse
	var err error
	if fc.splitHashRange != nil {
		resp, err = statemachine.ApplyLogEntryWithSplitFilter(fc.db, entry,
			lead.WrapperUpdateOperationCallback, fc.splitHashRange)
	} else {
		resp, err = statemachine.ApplyLogEntry(fc.db, entry, lead.WrapperUpdateOperationCallback)
	}
	if err != nil {
		return resp, err
	}
	fc.commitOffset.Store(entry.Offset)
	return resp, nil
}

func (fc *followerController) recordChecksums(resp statemachine.ApplyResponse, entryCrc uint32) {
	if resp.Checksum != nil {
		fc.checksumGauge.Record(int64(*resp.Checksum))
		fc.walChecksumGauge.Record(int64(entryCrc))
	}
}

func (fc *followerController) SetSplitHashRange(hashRange *proto.HashRange, term int64) {
	fc.rwMutex.Lock()
	defer fc.rwMutex.Unlock()
	// A stream of another term, e.g. a late one of a parent fenced since,
	// doesn't set the range again: its entries are rejected as well
	if current := fc.term.Load(); term != constant.I64NegativeOne && current != constant.I64NegativeOne && term != current {
		return
	}
	fc.splitHashRange = hashRange
}

func (fc *followerController) InstallSnapshot(stream proto.OxiaLogReplication_SendSnapshotServer) error { //nolint:revive // cyclomatic complexity justified by sequential error handling
	if fc.closed.Load() {
		return constant.ErrResourceConflict
	}
	fc.log.Info("Installing snapshot...", slog.Int64("term", fc.term.Load()))
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
		return constant.ErrResourceConflict
	}

	if fc.logSynchronizer.IsValid() {
		err = constant.ErrResourceConflict
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
		return errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to read first snapshot chunk")
	case firstChunk == nil:
		return nil
	case term != constant.I64NegativeOne && firstChunk.Term != constant.I64NegativeOne && term != firstChunk.Term:
		// The follower could be left with term=-1 by a previous failed
		// attempt at sending the snapshot. It's ok to proceed in that case.
		err = constant.ErrInvalidTerm
		return err
	}

	// From here on the WAL and the database content get replaced, even if
	// the install fails: the state applier must not apply what it read before
	fc.snapshotGeneration++
	if err = fc.wal.Clear(); err != nil {
		return errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to clear WAL")
	}
	// The database gets replaced as well: until the snapshot is installed, the
	// follower has no entries
	fc.lastAppendedOffset.Store(wal.InvalidOffset)
	fc.commitOffset.Store(wal.InvalidOffset)
	fc.advertisedCommitOffset.Store(wal.InvalidOffset)
	// If anything below fails, recover by re-opening the database from disk
	// so the follower controller remains usable for retries.
	defer func() {
		if err != nil && fc.db == nil {
			fc.log.Warn("Recovering database after failed snapshot install", slog.Any("error", err))
			_, commitOffset, db, initErr := initDatabase(fc.namespace, fc.shardId, nil, fc.storageOptions, fc.kvFactory)
			if initErr == nil {
				fc.db = db
				// The follower is left with the entries of the recovered
				// database: none if the snapshot loader wiped it already
				fc.lastAppendedOffset.Store(commitOffset)
				fc.commitOffset.Store(commitOffset)
				fc.advertisedCommitOffset.Store(commitOffset)
			} else {
				fc.log.Error("Failed to recover database, follower is in a broken state", slog.Any("error", initErr))
			}
		}
	}()
	// Pebble closes the database even when Close reports an error (e.g. leaked
	// iterators), so a failed close is recovered by re-opening it as well
	oldDb := fc.db
	fc.db = nil
	if err = oldDb.Close(); err != nil {
		return errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to close Database")
	}
	var loader kvstore.SnapshotLoader
	loader, err = fc.kvFactory.NewSnapshotLoader(fc.namespace, fc.shardId)
	if err != nil {
		return errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to create snapshot loader")
	}
	defer func() {
		_ = loader.Close()
	}()

	totalSize, err := fc.loadSnapshotChunks(loader, firstChunk, stream)
	if err != nil {
		return err
	}
	if err = loader.Complete(); err != nil {
		return errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to complete snapshot")
	}

	var db database.DB
	var rawTerm, rawCommitOffset int64
	if rawTerm, rawCommitOffset, db, err = initDatabase(fc.namespace, fc.shardId, nil, fc.storageOptions, fc.kvFactory); err != nil {
		return errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to initialize database")
	}
	fc.db = db
	fc.term.Store(rawTerm)
	fc.commitOffset.Store(rawCommitOffset)
	fc.lastAppendedOffset.Store(rawCommitOffset)
	fc.advertisedCommitOffset.Store(rawCommitOffset)

	// If this follower is a split child, filter the snapshot to only retain
	// keys within the child's hash range.
	if fc.splitHashRange != nil {
		if err = database.FilterDBForSplit(db.RawKV(), fc.splitHashRange); err != nil {
			return errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to filter snapshot for split")
		}
		// FilterDBForSplit deletes the checksum key (it's invalid after
		// filtering), so reset the in-memory cached checksum state.
		db.ResetChecksum()
		// Record the filter too, for the parent's entries that the child
		// applies other than as the follower of the parent, e.g. when it
		// replays them after a restart. Recording it flushes the database,
		// which runs without the Pebble WAL: once acked, the filtered snapshot
		// must survive a crash, as the parent doesn't send it again.
		if err = db.SetSplitFilter(&database.SplitFilter{
			MinHash:    fc.splitHashRange.GetMin(),
			MaxHash:    fc.splitHashRange.GetMax(),
			ParentTerm: rawTerm,
		}); err != nil {
			return errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to record split filter")
		}
	}

	if err = stream.SendAndClose(&proto.SnapshotResponse{
		AckOffset: rawCommitOffset,
	}); err != nil {
		return errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to send snapshot response")
	}
	fc.status.Store(int32(proto.ServingStatus_FOLLOWER))
	fc.log.Info(
		"Successfully installed snapshot",
		slog.Int64("term", fc.term.Load()),
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
		return 0, errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to add snapshot chunk")
	}
	totalSize := int64(len(firstChunk.Content))

	for {
		snapChunk, err := stream.Recv()
		switch {
		case err != nil:
			if errors.Is(err, io.EOF) {
				return totalSize, nil
			}
			return 0, errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to read snapshot chunk")
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
			return 0, errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "failed to add snapshot chunk")
		}
		totalSize += int64(len(snapChunk.Content))
	}
}

func (fc *followerController) GetStatus(_ *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	if fc.closed.Load() {
		return nil, constant.ErrResourceConflict
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
		return nil, constant.ErrResourceConflict
	}
	if request.Term < fc.term.Load() {
		return nil, constant.ErrInvalidTerm
	}
	var err error
	fc.log.Info("Deleting shard", slog.Int64("term", fc.term.Load()))
	defer func() {
		if err != nil {
			fc.log.Error("Follower controller deleted with error", slog.Any("error", err))
			return
		}
		fc.log.Info("Follower controller deleted")
	}()

	if err = fc.Close(); err != nil {
		return nil, errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "delete follower failed")
	}

	fc.rwMutex.Lock()
	defer fc.rwMutex.Unlock()

	if err = multierr.Combine(
		fc.wal.Delete(),
		fc.db.Delete(),
	); err != nil {
		return nil, errors.Wrapf(multierr.Combine(constant.ErrResourceUnavailable, err), "delete follower failed")
	}
	return &proto.DeleteShardResponse{}, nil
}

func (fc *followerController) IsFeatureEnabled(f proto.Feature) bool {
	fc.rwMutex.RLock()
	defer fc.rwMutex.RUnlock()
	return fc.db.IsFeatureEnabled(f)
}

func (fc *followerController) Checksum() crc.Checksum {
	fc.rwMutex.RLock()
	defer fc.rwMutex.RUnlock()
	return fc.db.ReadChecksum()
}
