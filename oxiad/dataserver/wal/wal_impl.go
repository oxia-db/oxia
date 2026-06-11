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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
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
	impl, err := newWal(namespace, shard, f.options, commitOffsetProvider, time2.SystemClock, DefaultCheckInterval)
	return impl, err
}

func (*walFactory) Close() error {
	return nil
}

type wal struct {
	sync.RWMutex
	walPath     string
	namespace   string
	shard       int64
	firstOffset atomic.Int64
	segmentSize uint32
	syncData    bool

	currentSegment ReadWriteSegment

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

	labels := metric.LabelsForShard(namespace, shard)
	w := &wal{
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
	if w.readOnlySegments, err = newReadOnlySegmentsGroup(w.walPath); err != nil {
		return nil, err
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.syncRequests = make(chan func(error), 1000)

	w.activeEntries = metric.NewGauge("oxia_server_wal_entries",
		"The number of active entries in the wal", "count", labels, func() int64 {
			return w.lastSyncedOffset.Load() - w.firstOffset.Load()
		})

	if err := w.recoverWal(); err != nil {
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
	t.RLock()
	defer t.RUnlock()

	timer := t.readLatency.Timer()
	defer timer.Done()

	var rc object.RefCount[ReadOnlySegment]
	var segment ReadOnlySegment
	if index >= t.currentSegment.BaseOffset() {
		segment = t.currentSegment
	} else if pending := t.pendingCloseSegmentFor(index); pending != nil {
		// A rolled-over segment not closed by the sync goroutine yet: it is
		// still mapped, read it directly
		segment = pending
	} else {
		rc, err = t.readOnlySegments.Get(index)
		if err != nil {
			return nil, 0, 0, err
		}

		defer func(rc object.RefCount[ReadOnlySegment]) {
			err = multierr.Append(err, rc.Close())
		}(rc)
		segment = rc.Get()
	}

	var val []byte
	if val, previousCrc, entryCrc, err = segment.Read(index); err != nil {
		t.readErrors.Inc()
		return nil, 0, 0, err
	}

	entry = &proto.LogEntry{}
	if err = entry.UnmarshalVT(val); err != nil {
		t.readErrors.Inc()
		return nil, 0, 0, err
	}
	t.readBytes.Add(len(val))
	return entry, previousCrc, entryCrc, err
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

func (t *wal) trim(firstOffset int64) error {
	if firstOffset <= t.firstOffset.Load() {
		return nil
	}

	if err := t.readOnlySegments.TrimSegments(firstOffset); err != nil {
		return err
	}

	t.trimOps.Inc()
	t.firstOffset.Store(firstOffset)
	return nil
}

func (t *wal) Close() error {
	if err := t.trimmer.Close(); err != nil {
		return err
	}

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

func (t *wal) AppendAsyncWithPreviousCrc(entry *proto.LogEntry, previousCrc *uint32) error {
	t.Lock()
	defer t.Unlock()
	return t.appendAsync0(entry, previousCrc)
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
	if err != nil {
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

		if t.currentSegment, err = newReadWriteSegment(t.walPath, entry.Offset, t.segmentSize,
			lastCrc, t.commitOffsetProvider); err != nil {
			t.writeErrors.Inc()
			return err
		}
	}

	if err = t.currentSegment.Append(entry.Offset, val); err != nil {
		if !errors.Is(err, ErrSegmentFull) {
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
	if t.currentSegment, err = newReadWriteSegment(t.walPath, t.lastAppendedOffset.Load()+1, t.segmentSize,
		lastCrc, t.commitOffsetProvider); err != nil {
		return err
	}

	if !t.syncData {
		// With sync disabled there are no sync rounds to hand the rolled-over
		// segment to: close it inline
		if err = rolled.SyncFileIfNeeded(); err != nil {
			return err
		}
		if err = rolled.Close(); err != nil {
			return err
		}
		t.readOnlySegments.AddedNewSegment(rolled.BaseOffset())
		return nil
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
		serr := multierr.Combine(
			s.SyncFileIfNeeded(),
			s.Flush(),
			s.Close())
		if serr == nil {
			t.readOnlySegments.AddedNewSegment(s.BaseOffset())
		}
		err = multierr.Append(err, serr)
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
				t.lastSyncedOffset.Store(lastAppendedOffset)
			}
		}

		for _, callback := range callbacks {
			callback(err)
		}
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
		return errors.Wrapf(ErrInvalidNextOffset,
			"%d can not immediately follow %d", nextOffset, lastAppendedOffset)
	}
	return nil
}

func (t *wal) Clear() error {
	t.Lock()
	defer t.Unlock()

	err := multierr.Combine(
		t.drainPendingCloseSegments(),
		t.currentSegment.Close(),
		t.readOnlySegments.Close(),
		os.RemoveAll(t.walPath),
	)

	if err != nil {
		t.writeErrors.Inc()
		return errors.Wrap(err, "failed to clear wal")
	}

	if t.currentSegment, err = newReadWriteSegment(t.walPath, 0, t.segmentSize,
		constant.U32Zero, t.commitOffsetProvider); err != nil {
		return err
	}

	if t.readOnlySegments, err = newReadOnlySegmentsGroup(t.walPath); err != nil {
		return err
	}

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
		os.RemoveAll(t.walPath),
	)
}

func (t *wal) TruncateLog(lastSafeOffset int64) (int64, error) { //nolint:revive
	if lastSafeOffset == InvalidOffset {
		if err := t.Clear(); err != nil {
			return InvalidOffset, err
		}
		return t.LastOffset(), nil
	}

	t.Lock()
	defer t.Unlock()

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
				if err := t.Clear(); err != nil {
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
				if t.currentSegment, err = newReadWriteSegment(t.walPath, baseOffset,
					t.segmentSize, lastCrc, t.commitOffsetProvider); err != nil {
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
	segments, err := listAllSegments(t.walPath)
	if err != nil {
		return err
	}

	var firstSegment, lastSegment int64
	var lastCrc uint32
	if len(segments) > 0 {
		firstSegment = segments[0]
		lastSegment = segments[len(segments)-1]
		if firstSegment != lastSegment {
			if lastCrc, err = t.readOnlySegments.GetLastCrc(lastSegment); err != nil {
				return err
			}
		} else {
			lastCrc = constant.U32Zero
		}
	} else {
		firstSegment = 0
		lastSegment = 0
		lastCrc = constant.U32Zero
	}

	if t.currentSegment, err = newReadWriteSegment(t.walPath, lastSegment, t.segmentSize,
		lastCrc, t.commitOffsetProvider); err != nil {
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

func listAllSegments(walPath string) (segments []int64, err error) {
	dir, err := os.ReadDir(walPath)
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
