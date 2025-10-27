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

package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/process"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/proto"
	"github.com/oxia-db/oxia/server/wal/codec"
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

	currentSegment       ReadWriteSegment
	readOnlySegments     ReadOnlySegmentsGroup
	commitOffsetProvider CommitOffsetProvider
	logEntryCache        *lru.Cache

	// The last offset appended to the Wal. It might not yet be synced
	lastAppendedOffset atomic.Int64

	// The last offset synced in the Wal.
	lastSyncedOffset atomic.Int64

	ctx          context.Context
	cancel       context.CancelFunc
	syncRequests chan func(error)

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
	cacheHits     metric.Counter
}

func walPath(logDir string, namespace string, shard int64) string {
	return filepath.Join(logDir, namespace, fmt.Sprint("shard-", shard))
}

func newWal(namespace string, shard int64, options *FactoryOptions, commitOffsetProvider CommitOffsetProvider,
	clock time2.Clock, trimmerCheckInterval time.Duration) (Wal, error) {
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

	if cache, err := lru.New(logEntryCacheSize); err == nil {
		w.cacheHits = metric.NewCounter("oxia_server_wal_cache_hits",
			"The number of log entries read from the cache", "count", labels)
		w.logEntryCache = cache
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

func (t *wal) readAtIndex(index int64) (*proto.LogEntry, error) {
	t.RLock()
	defer t.RUnlock()

	timer := t.readLatency.Timer()
	defer timer.Done()

	// Read the entry from the cache first
	if entry := t.readCache(index); entry != nil {
		return entry, nil
	}

	var err error
	var rc object.RefCount[ReadOnlySegment]
	var segment ReadOnlySegment
	if index >= t.currentSegment.BaseOffset() {
		segment = t.currentSegment
	} else {
		rc, err = t.readOnlySegments.Get(index)
		if err != nil {
			return nil, err
		}

		defer func(rc object.RefCount[ReadOnlySegment]) {
			err = multierr.Append(err, rc.Close())
		}(rc)
		segment = rc.Get()
	}

	var val []byte
	if val, err = segment.Read(index); err != nil {
		t.readErrors.Inc()
		return nil, err
	}

	entry := &proto.LogEntry{}
	if err = entry.UnmarshalVT(val); err != nil {
		t.readErrors.Inc()
		return nil, err
	}
	t.readBytes.Add(len(val))
	return entry, err
}

func (t *wal) readCache(index int64) *proto.LogEntry {
	if t.logEntryCache == nil {
		return nil
	}

	if entry, ok := t.logEntryCache.Get(index); ok {
		if e, ok := entry.(*proto.LogEntry); ok {
			t.cacheHits.Inc()
			return e.CloneVT()
		}
	}
	return nil
}

func (t *wal) writeCache(entry *proto.LogEntry) {
	if t.logEntryCache == nil {
		return
	}
	t.logEntryCache.Add(entry.Offset, entry)
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
	return t.appendAsync0(entry)
}

func (t *wal) appendAsync0(entry *proto.LogEntry) error {
	timer := t.appendLatency.Timer()
	defer timer.Done()

	if t.isClosed() {
		return constant.ErrAlreadyClosed
	}

	if err := t.checkNextOffset(entry.Offset); err != nil {
		t.writeErrors.Inc()
		return err
	}

	val, err := pb.Marshal(entry)
	if err != nil {
		t.writeErrors.Inc()
		return err
	}

	if t.lastAppendedOffset.Load() == InvalidOffset && entry.Offset != 0 && t.currentSegment.BaseOffset() == 0 {
		// The wal was cleared and we're starting from a non-initial position
		if err = t.currentSegment.Delete(); err != nil {
			t.writeErrors.Inc()
			return err
		}

		if t.currentSegment, err = newReadWriteSegment(t.walPath, entry.Offset, t.segmentSize,
			0, t.commitOffsetProvider); err != nil {
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
	// Add the entry into cache
	t.writeCache(entry)

	t.appendBytes.Add(len(val))
	return nil
}

func (t *wal) AppendAndSync(entry *proto.LogEntry, callback func(err error)) {
	t.Lock()
	defer t.Unlock()
	if err := t.appendAsync0(entry); err != nil {
		callback(err)
		return
	}
	t.doSync(callback)
}

func (t *wal) rolloverSegment() error {
	var err error
	if err = t.currentSegment.Close(); err != nil {
		return err
	}
	lastCrc := t.currentSegment.LastCrc()
	t.readOnlySegments.AddedNewSegment(t.currentSegment.BaseOffset())

	if t.currentSegment, err = newReadWriteSegment(t.walPath, t.lastAppendedOffset.Load()+1, t.segmentSize,
		lastCrc, t.commitOffsetProvider); err != nil {
		return err
	}

	return nil
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

		t.Lock()
		segment := t.currentSegment
		lastAppendedOffset := t.lastAppendedOffset.Load()
		t.Unlock()

		var err error
		if t.lastSyncedOffset.Load() != lastAppendedOffset {
			timer := t.syncLatency.Timer()
			if err = segment.Flush(); err != nil {
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
		t.currentSegment.Close(),
		t.readOnlySegments.Close(),
		os.RemoveAll(t.walPath),
	)

	if err != nil {
		t.writeErrors.Inc()
		return errors.Wrap(err, "failed to clear wal")
	}

	if t.currentSegment, err = newReadWriteSegment(t.walPath, 0, t.segmentSize,
		0, t.commitOffsetProvider); err != nil {
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
				if err = segment.Close(); err != nil {
					return InvalidOffset, err
				}
				if t.currentSegment, err = newReadWriteSegment(t.walPath, segment.Get().BaseOffset(),
					t.segmentSize, segment.Get().LastCrc(), t.commitOffsetProvider); err != nil {
					err = multierr.Append(err, segment.Close())
					return InvalidOffset, err
				}
				if err := t.currentSegment.Truncate(lastSafeOffset); err != nil {
					err = multierr.Append(err, segment.Close())
					return InvalidOffset, err
				}

				err = segment.Close()
				return lastSafeOffset, err
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
			lastCrc = 0
		}
	} else {
		firstSegment = 0
		lastSegment = 0
		lastCrc = 0
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
