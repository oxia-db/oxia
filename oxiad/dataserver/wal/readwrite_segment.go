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
	"encoding/binary"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/common/vfs"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"
)

type ReadWriteSegment interface {
	ReadOnlySegment

	Append(offset int64, data []byte) error

	Truncate(lastSafeOffset int64) error

	HasSpace(l int) bool

	Flush() error

	// SyncFileIfNeeded fsyncs the segment file after its creation: the fsync
	// is deferred out of the creation path, since segments get created during
	// rollovers, in the append path, while holding the WAL write lock. It
	// must be called before the first entries of the segment are
	// acknowledged, so that the file metadata (size, directory entry) is
	// durable by then.
	SyncFileIfNeeded() error
}

type readWriteSegment struct {
	sync.RWMutex

	// flushLock guards the mapping lifecycle: Flush runs the msync, and Read
	// the record copy, under its read side, without holding the main mutex, so
	// that appends can proceed while the disk sync or the copy is in flight.
	// Only Close (unmap) needs to exclude them, through the write side.
	flushLock sync.RWMutex

	c *segmentConfig

	// Written by Append and Truncate while holding the mutex, after the record
	// and its index entry. Read loads it without taking the mutex.
	lastOffset    atomic.Int64
	lastCrc       uint32
	txnFile       vfs.File
	txnMappedFile mmap.MMap

	currentFileOffset uint32
	writingIdx        []byte

	// writingIdx extended to its capacity, for Read. Append stores it again,
	// while holding the mutex, when writingIdx moves to a larger buffer, and
	// before storing lastOffset: Read loads lastOffset first, so the index it
	// loads next holds the entry.
	readIdx atomic.Pointer[[]byte]

	segmentSize     uint32
	pendingFileSync atomic.Bool
}

func newReadWriteSegment(fs vfs.FS, basePath string, baseOffset int64, segmentSize uint32, lastCrc uint32,
	commitOffsetProvider CommitOffsetProvider) (ReadWriteSegment, error) {
	var err error
	if _, err = fs.Stat(basePath); os.IsNotExist(err) {
		if err = fs.MkdirAll(basePath, 0755); err != nil {
			return nil, errors.Wrapf(err, "failed to create wal directory %s", basePath)
		}
	}

	c, err := newSegmentConfig(fs, basePath, baseOffset)
	if err != nil {
		return nil, err
	}

	ms := &readWriteSegment{
		c:           c,
		segmentSize: segmentSize,
		lastCrc:     lastCrc,
	}

	if ms.txnFile, err = fs.OpenFile(ms.c.txnPath, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return nil, errors.Wrapf(err, "failed to open segment file %s", ms.c.txnPath)
	}

	info, err := fs.Stat(c.txnPath)
	if err != nil {
		return nil, multierr.Append(
			errors.Wrapf(err, "failed to stat segment file %s", ms.c.txnPath),
			ms.txnFile.Close())
	}
	if info.Size() < int64(segmentSize) {
		if err = initFileWithZeroes(ms.txnFile, segmentSize); err != nil {
			return nil, multierr.Append(err, ms.txnFile.Close())
		}
		ms.pendingFileSync.Store(true)
	}

	// An index file next to a segment reopened for writes belongs to a previous
	// incarnation of the segment and goes stale with the first new append:
	// remove it, so that a crash before the Close that rewrites it cannot
	// leave it lying next to newer txn data. Close writes a fresh one.
	if c.segmentExists {
		if err = vfs.RemoveFileIfExists(fs, c.idxPath); err != nil {
			return nil, multierr.Append(
				errors.Wrapf(err, "failed to remove stale segment index file %s", c.idxPath),
				ms.txnFile.Close())
		}
	}

	if ms.txnMappedFile, err = fs.Map(ms.txnFile, int(segmentSize), mmap.RDWR); err != nil {
		return nil, multierr.Append(
			errors.Wrapf(err, "failed to map segment file %s", ms.c.txnPath),
			ms.txnFile.Close())
	}

	var commitOffset *int64
	if commitOffsetProvider != nil {
		offset := commitOffsetProvider.CommitOffset()
		commitOffset = &offset
	} else {
		commitOffset = nil
	}
	initialLastCrc := ms.lastCrc
	var lastOffset int64
	if ms.writingIdx, ms.lastCrc, ms.currentFileOffset, lastOffset, err = ms.c.codec.RecoverIndex(ms.txnMappedFile,
		ms.currentFileOffset, ms.c.baseOffset, commitOffset); err != nil {
		return nil, multierr.Combine(
			errors.Wrapf(err, "failed to rebuild index for segment file %s", ms.c.txnPath),
			ms.c.fs.Unmap(&ms.txnMappedFile),
			ms.txnFile.Close())
	}
	// If the segment is empty, preserve the caller's CRC seed so that it can
	// be used as the previous CRC for the first entry appended to this segment.
	if len(ms.writingIdx) == 0 {
		ms.lastCrc = initialLastCrc
	}
	ms.publishReadIdx()
	ms.lastOffset.Store(lastOffset)
	if err = ms.zeroDiscardedTail(); err != nil {
		codec.ReturnIndexBuf(&ms.writingIdx)
		return nil, multierr.Combine(
			errors.Wrapf(err, "failed to zero the discarded tail of segment file %s", ms.c.txnPath),
			ms.c.fs.Unmap(&ms.txnMappedFile),
			ms.txnFile.Close())
	}
	return ms, nil
}

// zeroDiscardedTail zeroes what the recovery left past the recovered entries
// when it stopped at a record it did not accept: a corrupted record past the
// commit offset is discarded along with everything after it, but its bytes,
// and the intact records following it, are still in the file. A later
// recovery would read those records back as valid entries, as soon as new
// appends end right where one of them starts. Like Truncate, it syncs the
// zeroes, here before any new entry gets appended.
func (ms *readWriteSegment) zeroDiscardedTail() error {
	tail := ms.txnMappedFile[ms.currentFileOffset:]
	headerSlot := tail[:min(len(tail), int(ms.c.codec.GetHeaderSize()))]
	if !slices.ContainsFunc(headerSlot, isNonZero) {
		// The recovery reached the end of the written data. Intact records
		// past a record whose header never reached the disk would go unnoticed
		// here: finding them takes scanning the whole segment on every reopen
		return nil
	}

	slog.Warn("Zeroing the discarded data past the last recovered entry of the wal segment.",
		slog.String("path", ms.c.txnPath),
		slog.Int64("last-entry-offset", ms.lastOffset.Load()),
		slog.Uint64("file-offset", uint64(ms.currentFileOffset)))
	// Only write the non-zero bytes: the pages that never held data stay
	// untouched, instead of getting allocated in the sparse segment file
	for i, b := range tail {
		if b != 0 {
			tail[i] = 0
		}
	}
	return ms.Flush()
}

func isNonZero(b byte) bool {
	return b != 0
}

func (ms *readWriteSegment) LastCrc() uint32 {
	ms.RLock()
	defer ms.RUnlock()
	return ms.lastCrc
}

func (ms *readWriteSegment) BaseOffset() int64 {
	return ms.c.baseOffset
}

func (ms *readWriteSegment) LastOffset() int64 {
	return ms.lastOffset.Load()
}

// Read does not take the mutex, so that the appends do not wait for it: they
// only write past the records it can find in the index, and publish them
// through lastOffset. It holds the read side of flushLock instead, so that
// Close does not unmap the segment under the copy, and returns
// errs.ErrAlreadyClosed once the segment is closed. Truncate must not run
// concurrently.
func (ms *readWriteSegment) Read(offset int64) (payload []byte, previousCrc uint32, payloadCrc uint32, err error) {
	ms.flushLock.RLock()
	defer ms.flushLock.RUnlock()
	if ms.txnMappedFile == nil {
		return nil, 0, 0, errors.Wrap(errs.ErrAlreadyClosed, "segment")
	}
	if offset < ms.c.baseOffset || offset > ms.lastOffset.Load() {
		return nil, 0, 0, errors.Wrap(errs.ErrInvalidArgument, "offset out of bounds")
	}

	fileReadOffset := fileOffset(*ms.readIdx.Load(), ms.c.baseOffset, offset)
	if payload, previousCrc, payloadCrc, err = ms.c.codec.ReadRecordWithValidation(ms.txnMappedFile, fileReadOffset); err != nil {
		if errors.Is(err, errs.ErrDataCorrupted) {
			return nil, 0, 0, errors.Wrapf(err, "read record failed. entryOffset: %d", offset)
		}
		return nil, 0, 0, err
	}
	return payload, previousCrc, payloadCrc, nil
}

func (ms *readWriteSegment) HasSpace(l int) bool {
	return ms.currentFileOffset+ms.c.codec.GetHeaderSize()+uint32(l) <= ms.segmentSize
}

func (ms *readWriteSegment) Append(offset int64, data []byte) error {
	ms.Lock()
	defer ms.Unlock()

	if len(data) == 0 {
		return errs.ErrWALEmptyPayload
	}
	if !ms.HasSpace(len(data)) {
		return errs.ErrWALSegmentFull
	}
	if offset != ms.lastOffset.Load()+1 {
		return errors.Wrap(errs.ErrInvalidArgument, "invalid next offset in wal")
	}

	fOffset := ms.currentFileOffset
	var recordSize uint32
	recordSize, ms.lastCrc = ms.c.codec.WriteRecord(ms.txnMappedFile, fOffset, ms.lastCrc, data)
	ms.currentFileOffset += recordSize
	idxCap := cap(ms.writingIdx)
	ms.writingIdx = binary.BigEndian.AppendUint32(ms.writingIdx, fOffset)
	if cap(ms.writingIdx) != idxCap {
		ms.publishReadIdx()
	}
	ms.lastOffset.Store(offset)
	return nil
}

func (ms *readWriteSegment) publishReadIdx() {
	readIdx := ms.writingIdx[:cap(ms.writingIdx)]
	ms.readIdx.Store(&readIdx)
}

// Flush msyncs the mapped file without holding the segment mutex: appends and
// reads are not blocked for the duration of the disk sync. Records written
// concurrently with the msync may reach the disk torn, but they have not been
// acknowledged yet and are discarded by the CRC validation in RecoverIndex.
func (ms *readWriteSegment) Flush() error {
	ms.flushLock.RLock()
	defer ms.flushLock.RUnlock()
	if ms.txnMappedFile == nil {
		return nil
	}
	return ms.c.fs.Flush(ms.txnMappedFile)
}

// SyncFileIfNeeded is invoked by the WAL sync goroutine before Flush and, as a
// fallback for segments that fill up before any sync round has run, by the
// rollover before closing the segment.
func (ms *readWriteSegment) SyncFileIfNeeded() error {
	if !ms.pendingFileSync.Load() {
		return nil
	}

	ms.flushLock.RLock()
	defer ms.flushLock.RUnlock()
	if ms.txnMappedFile == nil {
		return nil
	}
	if err := ms.txnFile.Sync(); err != nil {
		return err
	}
	ms.pendingFileSync.Store(false)
	return nil
}

func (*readWriteSegment) OpenTimestamp() time.Time {
	return time.Now()
}

func (ms *readWriteSegment) Close() error {
	ms.Lock()
	defer ms.Unlock()
	ms.flushLock.Lock()
	defer ms.flushLock.Unlock()

	if ms.txnMappedFile == nil {
		// Already closed: TruncateLog clears the wal after having deleted
		// (and thus closed) the current segment
		return nil
	}

	err := multierr.Combine(
		ms.c.fs.Unmap(&ms.txnMappedFile),
		ms.txnFile.Close(),
	)
	if len(ms.writingIdx) > 0 {
		err = multierr.Append(err, ms.c.codec.WriteIndex(ms.c.fs, ms.c.idxPath, ms.writingIdx))
	}
	codec.ReturnIndexBuf(&ms.writingIdx)
	return err
}

func (ms *readWriteSegment) Delete() error {
	return multierr.Combine(
		ms.Close(),
		vfs.RemoveFileIfExists(ms.c.fs, ms.c.idxPath),
		vfs.RemoveFileIfExists(ms.c.fs, ms.c.txnPath),
	)
}

func (ms *readWriteSegment) Truncate(lastSafeOffset int64) error {
	if lastSafeOffset < ms.c.baseOffset || lastSafeOffset > ms.lastOffset.Load() {
		return errors.Wrap(errs.ErrInvalidArgument, "offset out of bounds")
	}

	// Write zeroes in the section to clear
	fileLastSafeOffset := fileOffset(ms.writingIdx, ms.c.baseOffset, lastSafeOffset)
	payloadSize, _, lastSafeCrc, err := ms.c.codec.ReadHeaderWithValidation(ms.txnMappedFile, fileLastSafeOffset)
	if err != nil {
		return err
	}
	fileEndOffset := fileLastSafeOffset + ms.c.codec.GetHeaderSize() + payloadSize
	for i := fileEndOffset; i < ms.currentFileOffset; i++ {
		ms.txnMappedFile[i] = 0
	}

	// Truncate the index
	ms.writingIdx = ms.writingIdx[:4*(lastSafeOffset-ms.c.baseOffset+1)]
	ms.currentFileOffset = fileEndOffset
	ms.lastOffset.Store(lastSafeOffset)
	// The next append must chain from the crc of the record at the truncation
	// point, not from the one of the last truncated record
	ms.lastCrc = lastSafeCrc
	return ms.Flush()
}

// initFileWithZeroes extends the file to its full segment size, without
// fsync-ing it: the file metadata is made durable by SyncFileIfNeeded before
// the first entries of the segment get acknowledged.
func initFileWithZeroes(f vfs.File, size uint32) error {
	if _, err := f.Seek(int64(size), 0); err != nil {
		return err
	}

	if _, err := f.Write([]byte{0x00}); err != nil {
		return err
	}

	return nil
}
