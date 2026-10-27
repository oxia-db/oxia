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

package wal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"

	"github.com/oxia-db/oxia/oxiad/common/vfs"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/process"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/common/validation"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
)

type walFactory struct {
	options *FactoryOptions
}

func NewWalFactory(options *FactoryOptions) Factory {
	return &walFactory{
		options: options,
	}
}

func (f *walFactory) NewWal(namespace string, shard int64, commitOffsetProvider CommitOffsetProvider) (Wal, error) {
	clock := f.options.Clock
	if clock == nil {
		clock = time2.SystemClock
	}
	trimmerCheckInterval := f.options.TrimmerCheckInterval
	if trimmerCheckInterval == 0 {
		trimmerCheckInterval = DefaultCheckInterval
	}
	impl, err := newWal(namespace, shard, f.options, commitOffsetProvider, clock, trimmerCheckInterval)
	return impl, err
}

func (*walFactory) Close() error {
	return nil
}

type wal struct {
	sync.RWMutex

	// The reads hold the mutex at most to pick the segment to read from, and
	// hold the read side of truncateLock for the whole read instead. A
	// rollover, the trimming, Clear or Close leaves the reads correct: it
	// unmaps a read-write segment once the copies from it are done, and
	// releases the references to the read-only ones. TruncateLog does not: it
	// zeroes the truncated records in place, deletes the read-only segments it
	// polls, and moves the remaining entries from the read-only group to the
	// current segment. It takes the write side, before the mutex.
	truncateLock sync.RWMutex

	fs          vfs.FS
	failpoints  *failpoint.Failpoints
	walPath     string
	namespace   string
	shard       int64
	firstOffset atomic.Int64
	segmentSize uint32
	syncData    bool

	currentSegment ReadWriteSegment
	// A copy of currentSegment for the reads that pick it without the mutex
	// (see readRecord): written only by openCurrentSegment, along with
	// currentSegment, while holding the mutex. The reads only load it.
	publishedCurrentSegment atomic.Pointer[ReadWriteSegment]

	// Rolled-over segments waiting for the sync goroutine to make them
	// durable and close them (see rolloverSegment); guarded by the wal
	// mutex. They stay readable through readAtIndex until closed.
	pendingCloseSegments []ReadWriteSegment

	readOnlySegments     ReadOnlySegmentsGroup
	commitOffsetProvider CommitOffsetProvider

	// The last offset appended to the Wal. It might not yet be synced
	lastAppendedOffset atomic.Int64

	// The last offset synced in the Wal.
	lastSyncedOffset atomic.Int64

	// Incremented, while holding the lock, when Clear or TruncateLog drop
	// entries: the trimmer trims nothing once the entries it computed the trim
	// offset on got dropped (see trim)
	generation atomic.Int64

	ctx          context.Context
	cancel       context.CancelFunc
	syncRequests chan func(error)

	// Reusable serialization buffer: only accessed by appendAsync0, while
	// holding the WAL write lock
	marshalBuf []byte

	trimmer Trimmer

	appendLatency metric.LatencyHistogram
	appendBytes   metric.Counter
	readLatency   metric.LatencyHistogram
	readBytes     metric.Counter
	trimOps       metric.Counter
	readErrors    metric.Counter
	writeErrors   metric.Counter
	activeEntries metric.Gauge
	syncLatency   metric.LatencyHistogram
}

func walPath(logDir string, namespace string, shard int64) string {
	return filepath.Join(logDir, namespace, fmt.Sprint("shard-", shard))
}

func newWal(namespace string, shard int64, options *FactoryOptions, commitOffsetProvider CommitOffsetProvider,
	clock time2.Clock, trimmerCheckInterval time.Duration) (Wal, error) {
	if err := validation.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	if options.SegmentSize == 0 {
		options.SegmentSize = DefaultFactoryOptions.SegmentSize
	}

	fs := options.fs
	if fs == nil {
		fs = vfs.OSFS{}
	}

	labels := metric.LabelsForShard(namespace, shard)
	w := &wal{
		fs:                   fs,
		failpoints:           options.Failpoints,
		walPath:              walPath(options.BaseWalDir, namespace, shard),
		namespace:            namespace,
		shard:                shard,
		segmentSize:          uint32(options.SegmentSize),
		syncData:             options.SyncData,
		commitOffsetProvider: commitOffsetProvider,

		appendLatency: metric.NewLatencyHistogram("oxia_server_wal_append_latency",
			"The time it takes to append entries to the WAL", labels),
		appendBytes: metric.NewCounter("oxia_server_wal_append",
			"Bytes appended to the WAL", metric.Bytes, labels),
		readLatency: metric.NewLatencyHistogram("oxia_server_wal_read_latency",
			"The time it takes to read an entry from the WAL", labels),
		readBytes: metric.NewCounter("oxia_server_wal_read",
			"Bytes read from the WAL", metric.Bytes, labels),
		trimOps: metric.NewCounter("oxia_server_wal_trim",
			"The number of trim operations happening on the WAL", "count", labels),
		readErrors: metric.NewCounter("oxia_server_wal_read_errors",
			"The number of IO errors in the WAL read operations", "count", labels),
		writeErrors: metric.NewCounter("oxia_server_wal_write_errors",
			"The number of IO errors in the WAL read operations", "count", labels),
		syncLatency: metric.NewLatencyHistogram("oxia_server_wal_sync_latency",
			"The time it takes to fsync the wal data on disk", labels),
	}

	var err error
	if w.readOnlySegments, err = newReadOnlySegmentsGroup(w.fs, w.walPath); err != nil {
		return nil, err
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.syncRequests = make(chan func(error), 1000)

	w.activeEntries = metric.NewGauge("oxia_server_wal_entries",
		"The number of active entries in the wal", "count", labels, func() int64 {
			return w.lastSyncedOffset.Load() - w.firstOffset.Load()
		})

	if err := w.recoverWal(); err != nil {
		w.activeEntries.Unregister()
		// readOnlySegments is nil if discardTrailingSegment failed to rebuild it
		if w.readOnlySegments != nil {
			err = multierr.Append(err, w.readOnlySegments.Close())
		}
		return nil, errors.Wrapf(err, "failed to recover wal for shard %s / %d", namespace, shard)
	}

	w.trimmer = newTrimmer(namespace, shard, w, options.Retention, trimmerCheckInterval, clock, commitOffsetProvider)

	if options.SyncData {
		go process.DoWithLabels(
			w.ctx,
			map[string]string{
				"oxia":      "wal-sync",
				"namespace": namespace,
				"shard":     fmt.Sprintf("%d", shard),
			},
			w.runSync,
		)
	}

	return w, nil
}

func (t *wal) readAtIndex(index int64) (entry *proto.LogEntry, previousCrc uint32, entryCrc uint32, err error) {
	timer := t.readLatency.Timer()
	defer timer.Done()

	var val []byte
	t.truncateLock.RLock()
	val, previousCrc, entryCrc, err = t.readRecord(index)
	t.truncateLock.RUnlock()
	if err != nil {
		return nil, 0, 0, err
	}

	entry = &proto.LogEntry{}
	// The codec already copies the record out of the mmap into a private heap
	// buffer that nothing else references or mutates, so entry.Value can alias
	// it instead of being copied again. ApplyLogEntry relies on this: it
	// decodes entry.Value zero-copy, and the aliases must survive segment
	// unmap/close.
	if err = entry.UnmarshalVTUnsafe(val); err != nil {
		t.readErrors.Inc()
		return nil, 0, 0, err
	}
	t.readBytes.Add(len(val))
	return entry, previousCrc, entryCrc, err
}

// readRecord copies the record at the given index out of the segment holding
// it. Must be called while holding the read side of truncateLock.
func (t *wal) readRecord(index int64) (val []byte, previousCrc uint32, entryCrc uint32, err error) {
	// The reads tailing the wal find the entry in the current segment: they
	// read it without taking the mutex, which every append holds. A read-write
	// segment serves its entries until it gets closed, even once a rollover
	// replaced it as the current one
	if current := *t.publishedCurrentSegment.Load(); index >= current.BaseOffset() {
		if val, previousCrc, entryCrc, err = current.Read(index); err == nil {
			return val, previousCrc, entryCrc, nil
		}
	}

	val, previousCrc, entryCrc, err = t.readFromSegment(index)
	if errors.Is(err, errs.ErrWALSegmentClosed) {
		// A rollover closed the read-write segment after it got picked, once
		// it had moved to the read-only group: read it from there
		val, previousCrc, entryCrc, err = t.readFromSegment(index)
	}
	return val, previousCrc, entryCrc, err
}

// readFromSegment holds the mutex only to pick the segment holding the given
// index: opening it and copying the record happen after releasing it. A
// read-write segment keeps its mapping until the copy is done (see
// readWriteSegment.Read), and a read-only one until its reference is released.
func (t *wal) readFromSegment(index int64) (val []byte, previousCrc uint32, entryCrc uint32, err error) {
	t.RLock()
	var segment ReadOnlySegment
	if index >= t.currentSegment.BaseOffset() {
		segment = t.currentSegment
	} else if pending := t.pendingCloseSegmentFor(index); pending != nil {
		// A rolled-over segment not closed by the sync goroutine yet: read
		// it directly
		segment = pending
	}
	readOnlySegments := t.readOnlySegments
	t.RUnlock()

	if segment == nil {
		var rc object.RefCount[ReadOnlySegment]
		if rc, err = readOnlySegments.Get(index); err != nil {
			return nil, 0, 0, err
		}

		defer func(rc object.RefCount[ReadOnlySegment]) {
			err = multierr.Append(err, rc.Close())
		}(rc)
		segment = rc.Get()
	}

	if val, previousCrc, entryCrc, err = segment.Read(index); err != nil {
		if !errors.Is(err, errs.ErrWALSegmentClosed) {
			t.readErrors.Inc()
		}
		return nil, 0, 0, err
	}
	return val, previousCrc, entryCrc, nil
}

// openCurrentSegment opens the read-write segment that becomes the current
// one. On failure, the previous current segment is kept, so that the wal can
// still be closed. Must be called while holding the mutex, or before the wal
// gets shared.
func (t *wal) openCurrentSegment(baseOffset int64, lastCrc uint32) error {
	current, err := newReadWriteSegment(t.fs, t.walPath, baseOffset, t.segmentSize, lastCrc, t.commitOffsetProvider)
	if err != nil {
		return err
	}
	t.currentSegment = current
	t.publishedCurrentSegment.Store(&current)
	return nil
}

// pendingCloseSegmentFor returns the rolled-over segment containing the given
// index, when it has not been handed to the read-only group yet.
// Must be called while holding the wal lock.
func (t *wal) pendingCloseSegmentFor(index int64) ReadWriteSegment {
	for i := len(t.pendingCloseSegments) - 1; i >= 0; i-- {
		if s := t.pendingCloseSegments[i]; index >= s.BaseOffset() {
			return s
		}
	}
	return nil
}

func (t *wal) LastOffset() int64 {
	return t.lastSyncedOffset.Load()
}

func (t *wal) FirstOffset() int64 {
	return t.firstOffset.Load()
}

// trim moves the first offset to firstOffset, and deletes the segments that end
// before the one holding it, calling beforeDelete first. The trimmer computes
// firstOffset without holding the lock, on the entries of the given generation:
// once Clear or TruncateLog dropped them, it must not apply to the entries the
// wal holds instead.
func (t *wal) trim(firstOffset int64, generation int64, beforeDelete func() error) error {
	t.RLock()
	segments := t.readOnlySegments
	// Once firstOffset reaches the current segment, the segments before it are
	// all closed: firstOffset is at most the last synced offset, and a segment
	// gets closed before the offsets that follow it are synced
	currentSegment := t.currentSegment.BaseOffset()
	dropped := t.generation.Load() != generation
	t.RUnlock()
	if dropped || firstOffset <= t.firstOffset.Load() {
		return nil
	}

	if err := segments.TrimSegments(firstOffset, currentSegment, beforeDelete); err != nil {
		return err
	}

	t.Lock()
	defer t.Unlock()
	// Clear and TruncateLog can also run while the segments get deleted
	if t.generation.Load() != generation {
		return nil
	}
	t.trimOps.Inc()
	t.firstOffset.Store(firstOffset)
	return nil
}

func (t *wal) Close() error {
	t.trimmer.Close()

	t.Lock()
	defer t.Unlock()

	return t.closeWithoutLock()
}

func (t *wal) closeWithoutLock() error {
	select {
	case <-t.ctx.Done():
		return nil
	default:
		t.cancel()
		t.activeEntries.Unregister()

		return multierr.Combine(
			t.drainPendingCloseSegments(),
			t.currentSegment.Close(),
			t.readOnlySegments.Close(),
		)
	}
}

func (t *wal) isClosed() bool {
	select {
	case <-t.ctx.Done():
		return true
	default:
		return false
	}
}

func (t *wal) Append(entry *proto.LogEntry) error {
	if err := t.AppendAsync(entry); err != nil {
		return err
	}

	return t.Sync(context.Background())
}

func (t *wal) AppendAsync(entry *proto.LogEntry) error {
	t.Lock()
	defer t.Unlock()
	return t.appendAsync0(entry, nil)
}

func (t *wal) AppendAsyncWithPreviousCrc(entry *proto.LogEntry, previousCrc *uint32) (uint32, error) {
	t.Lock()
	defer t.Unlock()
	if err := t.appendAsync0(entry, previousCrc); err != nil {
		return 0, err
	}
	return t.currentSegment.LastCrc(), nil
}

func (t *wal) appendAsync0(entry *proto.LogEntry, previousCrc *uint32) error {
	timer := t.appendLatency.Timer()
	defer timer.Done()

	if t.isClosed() {
		return constant.ErrResourceUnavailable
	}

	if err := t.checkNextOffset(entry.Offset); err != nil {
		t.writeErrors.Inc()
		return err
	}

	// Serialize into the reusable buffer instead of allocating per entry:
	// val is copied into the segment before the lock is released
	marshalBuf, val, err := proto.MarshalToBuffer(t.marshalBuf, entry)
	if err = t.failpoints.Fail("appendAsync.marshal", err); err != nil {
		t.writeErrors.Inc()
		return err
	}
	t.marshalBuf = marshalBuf

	if t.lastAppendedOffset.Load() == InvalidOffset && entry.Offset != 0 && t.currentSegment.BaseOffset() == 0 {
		// The wal was cleared and we're starting from a non-initial position
		if err = t.currentSegment.Delete(); err != nil {
			t.writeErrors.Inc()
			return err
		}

		var lastCrc uint32
		if previousCrc != nil {
			lastCrc = *previousCrc
		}

		if err = t.openCurrentSegment(entry.Offset, lastCrc); err != nil {
			t.writeErrors.Inc()
			return err
		}
	}

	if err = t.currentSegment.Append(entry.Offset, val); err != nil {
		if !errors.Is(err, errs.ErrWALSegmentFull) {
			t.writeErrors.Inc()
			return err
		}
		if err = t.rolloverSegment(); err != nil {
			t.writeErrors.Inc()
			return errors.Wrap(err, "failed to rollover segment")
		}

		// After the rollover, try to append again
		if err = t.currentSegment.Append(entry.Offset, val); err != nil {
			t.writeErrors.Inc()
			return err
		}
	}

	t.lastAppendedOffset.Store(entry.Offset)
	t.firstOffset.CompareAndSwap(InvalidOffset, entry.Offset)

	t.appendBytes.Add(len(val))
	return nil
}

func (t *wal) AppendAndSync(entry *proto.LogEntry, callback func(entryCrc uint32, err error)) {
	t.Lock()
	if err := t.appendAsync0(entry, nil); err != nil {
		t.Unlock()
		callback(0, err)
		return
	}
	entryCrc := t.currentSegment.LastCrc()
	t.Unlock()

	// Enqueue outside the lock: when the sync queue is full, the backpressure
	// must only block this writer, not the WAL readers (the follower cursors
	// tailing the log).
	t.doSync(func(err error) {
		callback(entryCrc, err)
	})
}

func (t *wal) rolloverSegment() error {
	var err error
	rolled := t.currentSegment
	lastCrc := rolled.LastCrc()

	// The new segment file is created without fsync-ing it: the rollover runs
	// in the append path, while holding the WAL write lock (and, on the
	// leader, the controller lock), where the fsync would stall the whole
	// shard. The sync goroutine fsyncs the file before the first entries of
	// the segment are acknowledged (see runSync).
	if err = t.openCurrentSegment(t.lastAppendedOffset.Load()+1, lastCrc); err != nil {
		return err
	}

	if !t.syncData {
		// With sync disabled there are no sync rounds to hand the rolled-over
		// segment to: close it inline
		err = multierr.Combine(rolled.SyncFileIfNeeded(), rolled.Close())
		t.readOnlySegments.AddedNewSegment(rolled.BaseOffset())
		return err
	}

	// Hand the rolled-over segment to the sync goroutine: its unsynced tail
	// must be made durable before those offsets get acknowledged, and closing
	// it (index-file write, munmap) does not belong in the append path
	// either. The segment stays readable through readAtIndex until closed.
	t.pendingCloseSegments = append(t.pendingCloseSegments, rolled)
	return nil
}

// flushAndCloseRolledSegment is invoked by the sync goroutine for the segments
// handed over by rolloverSegment, before acknowledging their offsets. The
// flushes are safe no-ops when a cold path already closed the segment
// concurrently: Close nils the mapping while holding the flush lock, and both
// calls check it under that same lock.
func (t *wal) flushAndCloseRolledSegment(s ReadWriteSegment) error {
	if err := s.SyncFileIfNeeded(); err != nil {
		return err
	}
	if err := s.Flush(); err != nil {
		return err
	}

	// Atomically move the segment from the pending list to the read-only
	// group, so that there is no instant where readers find it in neither.
	// The removal is also the ownership gate: when a cold path (close, clear,
	// truncate) drained the segment concurrently, there is nothing left to
	// do here.
	t.Lock()
	owned := false
	for i, ps := range t.pendingCloseSegments {
		if ps == s {
			t.pendingCloseSegments = append(t.pendingCloseSegments[:i], t.pendingCloseSegments[i+1:]...)
			owned = true
			break
		}
	}
	if owned {
		t.readOnlySegments.AddedNewSegment(s.BaseOffset())
	}
	t.Unlock()
	if !owned {
		return nil
	}

	// The read-only group can open the segment even before this Close has
	// written the index file: a missing index gets rebuilt from the txn file
	// (see newReadOnlySegment)
	return s.Close()
}

// drainPendingCloseSegments makes durable and closes the rolled-over segments
// still waiting for the sync goroutine; used by the cold paths (close, clear,
// truncate). Must be called while holding the wal lock.
func (t *wal) drainPendingCloseSegments() error {
	var err error
	for _, s := range t.pendingCloseSegments {
		err = multierr.Append(err, multierr.Combine(
			s.SyncFileIfNeeded(),
			s.Flush(),
			s.Close()))
		t.readOnlySegments.AddedNewSegment(s.BaseOffset())
	}
	t.pendingCloseSegments = nil
	return err
}

func (t *wal) drainSyncRequestsChannel(callbacks []func(error)) []func(error) {
	for {
		select {
		case callback := <-t.syncRequests:
			callbacks = append(callbacks, callback)
		default:
			return callbacks
		}
	}
}

func (t *wal) runSync() {
	var callbacks []func(error)

	for {
		// Clear the slice
		callbacks = callbacks[:0]

		select {
		case <-t.ctx.Done():
			// Wal is closing, exit the go routine
			return

		case callback := <-t.syncRequests:
			// Wait for the first request
			callbacks = append(callbacks, callback)
		}

		// Clear all the other requests in the channel
		callbacks = t.drainSyncRequestsChannel(callbacks)

		t.RLock()
		segment := t.currentSegment
		lastAppendedOffset := t.lastAppendedOffset.Load()
		generation := t.generation.Load()
		pending := slices.Clone(t.pendingCloseSegments)
		t.RUnlock()

		var err error
		if len(pending) > 0 || t.lastSyncedOffset.Load() != lastAppendedOffset {
			timer := t.syncLatency.Timer()
			// The rolled-over segments must be durable and closed before the
			// acknowledgment below, which covers their tail offsets
			for _, s := range pending {
				if err = t.flushAndCloseRolledSegment(s); err != nil {
					break
				}
			}
			if err == nil {
				if err = segment.SyncFileIfNeeded(); err == nil {
					err = segment.Flush()
				}
			}
			if err != nil {
				t.writeErrors.Inc()
			} else {
				timer.Done()
				t.storeSyncedOffset(lastAppendedOffset, generation)
			}
		}

		for _, callback := range callbacks {
			callback(err)
		}
	}
}

// storeSyncedOffset stores the offset synced by a round, unless Clear or
// TruncateLog dropped entries since the round read it.
func (t *wal) storeSyncedOffset(offset int64, generation int64) {
	t.RLock()
	defer t.RUnlock()
	if t.generation.Load() == generation {
		t.lastSyncedOffset.Store(offset)
	}
}

func (t *wal) doSync(callback func(error)) {
	if !t.syncData {
		t.lastSyncedOffset.Store(t.lastAppendedOffset.Load())
		callback(nil)
		return
	}

	t.syncRequests <- callback
}

func (t *wal) Sync(ctx context.Context) error {
	wg := concurrent.NewWaitGroup(1)
	t.doSync(func(err error) {
		if err != nil {
			wg.Fail(err)
		} else {
			wg.Done()
		}
	})

	return wg.Wait(ctx)
}

func (t *wal) checkNextOffset(nextOffset int64) error {
	if nextOffset < 0 {
		return fmt.Errorf("invalid next offset. %d should be > 0", nextOffset)
	}

	lastAppendedOffset := t.lastAppendedOffset.Load()
	expectedOffset := lastAppendedOffset + 1

	if lastAppendedOffset != InvalidOffset && nextOffset != expectedOffset {
		return errors.Wrapf(errs.ErrWALInvalidNextOffset,
			"%d can not immediately follow %d", nextOffset, lastAppendedOffset)
	}
	return nil
}

func (t *wal) Clear() error {
	t.Lock()
	defer t.Unlock()

	return t.clearWithoutLock()
}

func (t *wal) clearWithoutLock() error {
	t.generation.Add(1)
	err := multierr.Combine(
		t.drainPendingCloseSegments(),
		t.currentSegment.Close(),
		t.readOnlySegments.Close(),
		t.fs.RemoveAll(t.walPath),
	)

	if err != nil {
		t.writeErrors.Inc()
		return errors.Wrap(err, "failed to clear wal")
	}

	if err = t.openCurrentSegment(0, constant.U32Zero); err != nil {
		return err
	}

	readOnlySegments, err := newReadOnlySegmentsGroup(t.fs, t.walPath)
	if err != nil {
		return err
	}
	t.readOnlySegments = readOnlySegments

	t.lastAppendedOffset.Store(InvalidOffset)
	t.lastSyncedOffset.Store(InvalidOffset)
	t.firstOffset.Store(InvalidOffset)
	return nil
}

func (t *wal) Delete() error {
	// NOTE: we must close the trimmer before closing the wal(without the lock), otherwise
	// when trimmer is doTrim, it accquire the lock and it will block forever
	if err := t.Close(); err != nil {
		return err
	}

	t.Lock()
	defer t.Unlock()

	return multierr.Combine(
		t.closeWithoutLock(),
		t.fs.RemoveAll(t.walPath),
	)
}

func (t *wal) TruncateLog(lastSafeOffset int64) (int64, error) { //nolint:revive
	if lastSafeOffset == InvalidOffset {
		if err := t.Clear(); err != nil {
			return InvalidOffset, err
		}
		return t.LastOffset(), nil
	}

	t.truncateLock.Lock()
	defer t.truncateLock.Unlock()
	t.Lock()
	defer t.Unlock()

	t.generation.Add(1)

	// Bring any rolled-over segment still pending close into the read-only
	// group, so that the truncation below sees the complete set of segments
	if err := t.drainPendingCloseSegments(); err != nil {
		return InvalidOffset, err
	}

	lastIndex := t.lastAppendedOffset.Load()
	if lastIndex == InvalidOffset {
		// The WAL is empty
		return InvalidOffset, nil
	}

	if lastSafeOffset >= t.currentSegment.BaseOffset() {
		// Truncation is only affecting the
		if err := t.currentSegment.Truncate(lastSafeOffset); err != nil {
			return InvalidOffset, err
		}
	} else {
		if err := t.currentSegment.Delete(); err != nil {
			return InvalidOffset, err
		}

		// Delete any intermediate segment and truncate to the right position
	truncateLoop:
		for {
			segment, err := t.readOnlySegments.PollHighestSegment()
			switch {
			case err != nil:
				return InvalidOffset, err
			case segment == nil:
				// There are no segments left
				if err := t.clearWithoutLock(); err != nil {
					return InvalidOffset, err
				}
				return t.LastOffset(), nil
			case lastSafeOffset >= segment.Get().BaseOffset():
				// The truncation will happen in the middle of this segment,
				// and this will also become the new current segment
				baseOffset := segment.Get().BaseOffset()
				lastCrc := segment.Get().LastCrc()
				// Close the reference exactly once: the counter decrements
				// unconditionally, so the error paths must not close it again
				if err = segment.Close(); err != nil {
					return InvalidOffset, err
				}
				if err = t.openCurrentSegment(baseOffset, lastCrc); err != nil {
					return InvalidOffset, err
				}
				if err := t.currentSegment.Truncate(lastSafeOffset); err != nil {
					return InvalidOffset, err
				}
				// Proceed to updating the last appended/synced offsets below:
				// returning from here would leave them stale, breaking the
				// next append's offset check and the readers' upper bound
				break truncateLoop
			default:
				// The entire segment can be discarded
				if err := segment.Get().Delete(); err != nil {
					err = multierr.Append(err, segment.Close())
					return InvalidOffset, err
				} else if err := segment.Close(); err != nil {
					return InvalidOffset, err
				}
			}
		}
	}

	t.lastAppendedOffset.Store(lastSafeOffset)
	t.lastSyncedOffset.Store(lastSafeOffset)
	return lastSafeOffset, nil
}

func (t *wal) recoverWal() error {
	segments, err := listAllSegments(t.fs, t.walPath)
	if err != nil {
		return err
	}

	var firstSegment, lastSegment int64
	lastCrc := constant.U32Zero
	for len(segments) > 0 {
		firstSegment = segments[0]
		lastSegment = segments[len(segments)-1]
		if firstSegment == lastSegment {
			break
		}
		if lastCrc, err = t.readOnlySegments.GetLastCrc(lastSegment); err == nil {
			break
		}
		if !errors.Is(err, errs.ErrWALEmptySegment) {
			return err
		}
		// The segment preceding lastSegment has no recoverable entries: its
		// content was lost by a crash before being synced. The entries of
		// lastSegment sit after that gap and were never acknowledged either,
		// since the sync rounds close the rolled-over segments in order,
		// before acknowledging: discard it and recover from one segment
		// further back
		if err = t.discardTrailingSegment(lastSegment); err != nil {
			return err
		}
		segments = segments[:len(segments)-1]
	}

	if err = t.openCurrentSegment(lastSegment, lastCrc); err != nil {
		return err
	}

	t.lastAppendedOffset.Store(t.currentSegment.LastOffset())
	t.lastSyncedOffset.Store(t.currentSegment.LastOffset())

	if firstSegment == lastSegment {
		if t.lastSyncedOffset.Load() >= 0 {
			t.firstOffset.Store(t.currentSegment.BaseOffset())
		} else {
			t.firstOffset.Store(InvalidOffset)
		}
	} else {
		t.firstOffset.Store(firstSegment)
	}

	return nil
}

// discardTrailingSegment removes the files of a trailing segment whose entries
// cannot be recovered, and rebuilds the read-only group over the remaining
// segments, so that the segment now becoming the last one is no longer served
// as read-only: it gets reopened as the read-write segment instead.
func (t *wal) discardTrailingSegment(baseOffset int64) error {
	slog.Warn("Discarding trailing wal segment that follows a segment with no entries.",
		slog.String("wal-path", t.walPath),
		slog.Int64("base-offset", baseOffset))

	c, err := newSegmentConfig(t.fs, t.walPath, baseOffset)
	if err != nil {
		return err
	}
	if err = multierr.Combine(
		vfs.RemoveFileIfExists(t.fs, c.idxPath),
		vfs.RemoveFileIfExists(t.fs, c.txnPath),
	); err != nil {
		return err
	}

	if err = t.readOnlySegments.Close(); err != nil {
		return err
	}
	t.readOnlySegments, err = newReadOnlySegmentsGroup(t.fs, t.walPath)
	return err
}

func listAllSegments(fs vfs.FS, walPath string) (segments []int64, err error) {
	dir, err := afero.ReadDir(fs, walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to list files in wal directory %s", walPath)
	}

	for _, entry := range dir {
		for _, _codec := range codec.SupportedCodecs {
			if matched, _ := filepath.Match("*"+_codec.GetTxnExtension(), entry.Name()); matched {
				var id int64
				if _, err := fmt.Sscanf(entry.Name(), "%d"+_codec.GetTxnExtension(), &id); err != nil {
					return nil, err
				}
				segments = append(segments, id)
			}
		}
	}

	slices.Sort(segments)
	return segments, nil
}
