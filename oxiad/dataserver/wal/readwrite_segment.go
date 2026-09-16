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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

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

	// flushLock guards the mapping lifecycle: Flush runs the msync under its
	// read side, without holding the main mutex, so that appends and reads can
	// proceed while the disk sync is in flight. Only Close (unmap) needs to
	// exclude an in-flight msync, through the write side.
	flushLock sync.RWMutex

	c *segmentConfig

	lastOffset    int64
	lastCrc       uint32
	txnFile       *os.File
	txnMappedFile mmap.MMap

	currentFileOffset uint32
	writingIdx        []byte

	segmentSize     uint32
	pendingFileSync atomic.Bool
}

func newReadWriteSegment(basePath string, baseOffset int64, segmentSize uint32, lastCrc uint32,
	commitOffsetProvider CommitOffsetProvider) (ReadWriteSegment, error) {
	var err error
	if _, err = os.Stat(basePath); os.IsNotExist(err) {
		if err = os.MkdirAll(basePath, 0755); err != nil {
			return nil, errors.Wrapf(err, "failed to create wal directory %s", basePath)
		}
	}

	c, err := newSegmentConfig(basePath, baseOffset)
	if err != nil {
		return nil, err
	}

	ms := &readWriteSegment{
		c:           c,
		segmentSize: segmentSize,
		lastCrc:     lastCrc,
	}

	if ms.txnFile, err = os.OpenFile(ms.c.txnPath, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return nil, errors.Wrapf(err, "failed to open segment file %s", ms.c.txnPath)
	}

	if !c.segmentExists {
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
		if err = codec.RemoveFileIfExists(c.idxPath); err != nil {
			return nil, multierr.Append(
				errors.Wrapf(err, "failed to remove stale segment index file %s", c.idxPath),
				ms.txnFile.Close())
		}
	}

	if ms.txnMappedFile, err = mmap.MapRegion(ms.txnFile, int(segmentSize), mmap.RDWR, 0, 0); err != nil {
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
	if ms.writingIdx, ms.lastCrc, ms.currentFileOffset, ms.lastOffset, err = ms.c.codec.RecoverIndex(ms.txnMappedFile,
		ms.currentFileOffset, ms.c.baseOffset, commitOffset); err != nil {
		return nil, multierr.Combine(
			errors.Wrapf(err, "failed to rebuild index for segment file %s", ms.c.txnPath),
			ms.txnMappedFile.Unmap(),
			ms.txnFile.Close())
	}
	// If the segment is empty, preserve the caller's CRC seed so that it can
	// be used as the previous CRC for the first entry appended to this segment.
	if len(ms.writingIdx) == 0 {
		ms.lastCrc = initialLastCrc
	}
	return ms, nil
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
	ms.RLock()
	defer ms.RUnlock()

	return ms.lastOffset
}

func (ms *readWriteSegment) Read(offset int64) (payload []byte, previousCrc uint32, payloadCrc uint32, err error) {
	ms.RLock()
	defer ms.RUnlock()
	if offset < ms.c.baseOffset || offset > ms.lastOffset {
		return nil, 0, 0, codec.ErrOffsetOutOfBounds
	}

	fileReadOffset := fileOffset(ms.writingIdx, ms.c.baseOffset, offset)
	if payload, previousCrc, payloadCrc, err = ms.c.codec.ReadRecordWithValidation(ms.txnMappedFile, fileReadOffset); err != nil {
		if errors.Is(err, codec.ErrDataCorrupted) {
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
		return codec.ErrEmptyPayload
	}
	if !ms.HasSpace(len(data)) {
		return ErrSegmentFull
	}
	if offset != ms.lastOffset+1 {
		return ErrInvalidNextOffset
	}

	fOffset := ms.currentFileOffset
	var recordSize uint32
	recordSize, ms.lastCrc = ms.c.codec.WriteRecord(ms.txnMappedFile, fOffset, ms.lastCrc, data)
	ms.currentFileOffset += recordSize
	ms.lastOffset = offset
	ms.writingIdx = binary.BigEndian.AppendUint32(ms.writingIdx, fOffset)
	return nil
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
	return ms.txnMappedFile.Flush()
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
		ms.txnMappedFile.Unmap(),
		ms.txnFile.Close(),
	)
	if len(ms.writingIdx) > 0 {
		err = multierr.Append(err, ms.c.codec.WriteIndex(ms.c.idxPath, ms.writingIdx))
	}
	codec.ReturnIndexBuf(&ms.writingIdx)
	return err
}

func (ms *readWriteSegment) Delete() error {
	return multierr.Combine(
		ms.Close(),
		codec.RemoveFileIfExists(ms.c.idxPath),
		os.Remove(ms.c.txnPath),
	)
}

func (ms *readWriteSegment) Truncate(lastSafeOffset int64) error {
	if lastSafeOffset < ms.c.baseOffset || lastSafeOffset > ms.lastOffset {
		return codec.ErrOffsetOutOfBounds
	}

	// Write zeroes in the section to clear
	fileLastSafeOffset := fileOffset(ms.writingIdx, ms.c.baseOffset, lastSafeOffset)
	var recordSize uint32
	var err error
	if recordSize, err = ms.c.codec.GetRecordSize(ms.txnMappedFile, fileLastSafeOffset); err != nil {
		return err
	}
	fileEndOffset := fileLastSafeOffset + recordSize
	for i := fileEndOffset; i < ms.currentFileOffset; i++ {
		ms.txnMappedFile[i] = 0
	}

	// Truncate the index
	ms.writingIdx = ms.writingIdx[:4*(lastSafeOffset-ms.c.baseOffset+1)]
	ms.currentFileOffset = fileEndOffset
	ms.lastOffset = lastSafeOffset
	return ms.Flush()
}

// initFileWithZeroes extends the file to its full segment size, without
// fsync-ing it: the file metadata is made durable by SyncFileIfNeeded before
// the first entries of the segment get acknowledged.
func initFileWithZeroes(f *os.File, size uint32) error {
	if _, err := f.Seek(int64(size), 0); err != nil {
		return err
	}

	if _, err := f.Write([]byte{0x00}); err != nil {
		return err
	}

	return nil
}
