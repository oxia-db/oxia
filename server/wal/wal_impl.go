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
	pb "google.golang.org/protobuf/proto"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/proto"
)

type walFactory struct {
	options *WalFactoryOptions
}

func NewWalFactory(options *WalFactoryOptions) WalFactory {
	return &walFactory{
		options: options,
	}
}

func (f *walFactory) NewWal(namespace string, shard int64, commitOffsetProvider CommitOffsetProvider) (Wal, error) {
	impl, err := newWal(namespace, shard, f.options, commitOffsetProvider, common.SystemClock, DefaultCheckInterval)
	return impl, err
}

func (f *walFactory) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type wal struct {
	sync.RWMutex
	walPath     string
	namespace   string
	shard       int64
	firstOffset atomic.Int64
	segmentSize uint32
	syncData    bool

	currentSegment   ReadWriteSegment
	readOnlySegments ReadOnlySegmentsGroup

	// The last offset appended to the Wal. It might not yet be synced
	lastAppendedOffset atomic.Int64

	// The last offset synced in the Wal.
	lastSyncedOffset atomic.Int64

	ctx         context.Context
	cancel      context.CancelFunc
	syncRequest common.ConditionContext
	syncDone    common.ConditionContext
	lastSyncErr atomic.Pointer[error] // The error from the last sync operation, if any

	trimmer Trimmer

	appendLatency metrics.LatencyHistogram
	appendBytes   metrics.Counter
	readLatency   metrics.LatencyHistogram
	readBytes     metrics.Counter
	trimOps       metrics.Counter
	readErrors    metrics.Counter
	writeErrors   metrics.Counter
	activeEntries metrics.Gauge
	syncLatency   metrics.LatencyHistogram
}

func walPath(logDir string, namespace string, shard int64) string {
	return filepath.Join(logDir, namespace, fmt.Sprint("shard-", shard))
}

func newWal(namespace string, shard int64, options *WalFactoryOptions, commitOffsetProvider CommitOffsetProvider,
	clock common.Clock, trimmerCheckInterval time.Duration) (Wal, error) {
	if options.SegmentSize == 0 {
		options.SegmentSize = DefaultWalFactoryOptions.SegmentSize
	}

	labels := metrics.LabelsForShard(namespace, shard)
	w := &wal{
		walPath:     walPath(options.BaseWalDir, namespace, shard),
		namespace:   namespace,
		shard:       shard,
		segmentSize: uint32(options.SegmentSize),
		syncData:    options.SyncData,

		appendLatency: metrics.NewLatencyHistogram("oxia_server_wal_append_latency",
			"The time it takes to append entries to the WAL", labels),
		appendBytes: metrics.NewCounter("oxia_server_wal_append",
			"Bytes appended to the WAL", metrics.Bytes, labels),
		readLatency: metrics.NewLatencyHistogram("oxia_server_wal_read_latency",
			"The time it takes to read an entry from the WAL", labels),
		readBytes: metrics.NewCounter("oxia_server_wal_read",
			"Bytes read from the WAL", metrics.Bytes, labels),
		trimOps: metrics.NewCounter("oxia_server_wal_trim",
			"The number of trim operations happening on the WAL", "count", labels),
		readErrors: metrics.NewCounter("oxia_server_wal_read_errors",
			"The number of IO errors in the WAL read operations", "count", labels),
		writeErrors: metrics.NewCounter("oxia_server_wal_write_errors",
			"The number of IO errors in the WAL read operations", "count", labels),
		syncLatency: metrics.NewLatencyHistogram("oxia_server_wal_sync_latency",
			"The time it takes to fsync the wal data on disk", labels),
	}

	var err error
	if w.readOnlySegments, err = newReadOnlySegmentsGroup(w.walPath); err != nil {
		return nil, err
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.syncRequest = common.NewConditionContext(w)
	w.syncDone = common.NewConditionContext(w)

	w.activeEntries = metrics.NewGauge("oxia_server_wal_entries",
		"The number of active entries in the wal", "count", labels, func() int64 {
			return w.lastSyncedOffset.Load() - w.firstOffset.Load()
		})

	if err := w.recoverWal(); err != nil {
		return nil, errors.Wrapf(err, "failed to recover wal for shard %s / %d", namespace, shard)
	}

	w.trimmer = newTrimmer(namespace, shard, w, options.Retention, trimmerCheckInterval, clock, commitOffsetProvider)

	if options.SyncData {
		go common.DoWithLabels(map[string]string{
			"oxia":      "wal-sync",
			"namespace": namespace,
			"shard":     fmt.Sprintf("%d", shard),
		}, w.runSync)
	}

	return w, nil
}

func (t *wal) readAtIndex(index int64) (*proto.LogEntry, error) {
	t.RLock()
	defer t.RUnlock()

	timer := t.readLatency.Timer()
	defer timer.Done()

	var err error
	var rc common.RefCount[ReadOnlySegment]
	var segment ReadOnlySegment
	if index >= t.currentSegment.BaseOffset() {
		segment = t.currentSegment
	} else {
		rc, err = t.readOnlySegments.Get(index)
		if err != nil {
			return nil, err
		}

		defer func(rc common.RefCount[ReadOnlySegment]) {
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

	return t.close()
}

func (t *wal) close() error {
	t.cancel()
	t.activeEntries.Unregister()

	return multierr.Combine(
		t.trimmer.Close(),
		t.currentSegment.Close(),
		t.readOnlySegments.Close(),
	)
}

func (t *wal) Append(entry *proto.LogEntry) error {
	if err := t.AppendAsync(entry); err != nil {
		return err
	}

	return t.Sync(context.Background())
}

func (t *wal) AppendAsync(entry *proto.LogEntry) error {
	timer := t.appendLatency.Timer()
	defer timer.Done()

	t.Lock()
	defer t.Unlock()

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

		if t.currentSegment, err = newReadWriteSegment(t.walPath, entry.Offset, t.segmentSize); err != nil {
			t.writeErrors.Inc()
			return err
		}
	}

	if !t.currentSegment.HasSpace(len(val)) {
		if err = t.rolloverSegment(); err != nil {
			t.writeErrors.Inc()
			return errors.Wrap(err, "failed to rollover segment")
		}
	}

	if err = t.currentSegment.Append(entry.Offset, val); err != nil {
		t.writeErrors.Inc()
		return err
	}
	t.lastAppendedOffset.Store(entry.Offset)
	t.firstOffset.CompareAndSwap(InvalidOffset, entry.Offset)

	t.appendBytes.Add(len(val))
	return nil
}

func (t *wal) rolloverSegment() error {
	var err error
	if err = t.currentSegment.Close(); err != nil {
		return err
	}

	t.readOnlySegments.AddedNewSegment(t.currentSegment.BaseOffset())

	if t.currentSegment, err = newReadWriteSegment(t.walPath, t.lastAppendedOffset.Load()+1, t.segmentSize); err != nil {
		return err
	}

	return nil
}

func (t *wal) runSync() {
	for {
		t.Lock()

		if err := t.syncRequest.Wait(t.ctx); err != nil {
			// Wal is closing, exit the go routine
			t.Unlock()
			return
		}

		segment := t.currentSegment
		lastAppendedOffset := t.lastAppendedOffset.Load()
		t.Unlock()

		if t.lastSyncedOffset.Load() == lastAppendedOffset {
			// We are already at the end, no need to sync
			t.syncDone.Broadcast()
			continue
		}

		timer := t.syncLatency.Timer()
		if err := segment.Flush(); err != nil {
			t.lastSyncErr.Store(&err)
			t.writeErrors.Inc()
		} else {
			timer.Done()
			t.lastSyncedOffset.Store(lastAppendedOffset)
			t.lastSyncErr.Store(nil)
		}

		t.syncDone.Broadcast()
	}
}

func (t *wal) Sync(ctx context.Context) error {
	if !t.syncData {
		t.lastSyncedOffset.Store(t.lastAppendedOffset.Load())
		return nil
	}

	t.Lock()
	defer t.Unlock()

	// Wait until the currently last appended offset is synced
	lastOffset := t.lastAppendedOffset.Load()

	for lastOffset > t.lastSyncedOffset.Load() {
		t.syncRequest.Signal()

		if err := t.syncDone.Wait(ctx); err != nil {
			return err
		}
	}

	if lastErr := t.lastSyncErr.Load(); lastErr != nil {
		return *lastErr
	}

	return nil
}

func (t *wal) checkNextOffset(nextOffset int64) error {
	if nextOffset < 0 {
		return errors.New(fmt.Sprintf("Invalid next offset. %d should be > 0", nextOffset))
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

	if t.currentSegment, err = newReadWriteSegment(t.walPath, 0, t.segmentSize); err != nil {
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
	t.Lock()
	defer t.Unlock()

	return multierr.Combine(
		t.close(),
		os.RemoveAll(t.walPath),
	)
}

func (t *wal) TruncateLog(lastSafeOffset int64) (int64, error) {
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
			if segment, err := t.readOnlySegments.PollHighestSegment(); err != nil {
				return InvalidOffset, err
			} else if segment == nil {
				// There are no segments left
				if err := t.Clear(); err != nil {
					return InvalidOffset, err
				}
				return t.LastOffset(), nil
			} else if lastSafeOffset >= segment.Get().BaseOffset() {
				// The truncation will happen in the middle of this segment,
				// and this will also become the new current segment
				if err = segment.Close(); err != nil {
					return InvalidOffset, err
				}

				if t.currentSegment, err = newReadWriteSegment(t.walPath, segment.Get().BaseOffset(), t.segmentSize); err != nil {
					err = multierr.Append(err, segment.Close())
					return InvalidOffset, err
				}
				if err := t.currentSegment.Truncate(lastSafeOffset); err != nil {
					err = multierr.Append(err, segment.Close())
					return InvalidOffset, err
				}

				err = segment.Close()
				return lastSafeOffset, err
			} else {
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
	if len(segments) > 0 {
		firstSegment = segments[0]
		lastSegment = segments[len(segments)-1]
	} else {
		firstSegment = 0
		lastSegment = 0
	}

	if t.currentSegment, err = newReadWriteSegment(t.walPath, lastSegment, t.segmentSize); err != nil {
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
		if matched, _ := filepath.Match("*"+txnExtension, entry.Name()); matched {
			var id int64
			if _, err := fmt.Sscanf(entry.Name(), "%d"+txnExtension, &id); err != nil {
				return nil, err
			}

			segments = append(segments, id)
		}
	}

	slices.Sort(segments)
	return segments, nil
}
