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
	"encoding/binary"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/dataserver/wal/codec"
)

type ReadWriteSegment interface {
	ReadOnlySegment

	Append(offset int64, data []byte) error

	Truncate(lastSafeOffset int64) error

	HasSpace(l int) bool

	Flush() error
}

type readWriteSegment struct {
	sync.RWMutex

	c *segmentConfig

	lastOffset    int64
	lastCrc       uint32
	txnFile       *os.File
	txnMappedFile mmap.MMap

	currentFileOffset uint32
	writingIdx        []byte

	segmentSize uint32
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
			return nil, err
		}
	}

	if ms.txnMappedFile, err = mmap.MapRegion(ms.txnFile, int(segmentSize), mmap.RDWR, 0, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to map segment file %s", ms.c.txnPath)
	}

	var commitOffset *int64
	if commitOffsetProvider != nil {
		offset := commitOffsetProvider.CommitOffset()
		commitOffset = &offset
	} else {
		commitOffset = nil
	}
	if ms.writingIdx, ms.lastCrc, ms.currentFileOffset, ms.lastOffset, err = ms.c.codec.RecoverIndex(ms.txnMappedFile,
		ms.currentFileOffset, ms.c.baseOffset, commitOffset); err != nil {
		return nil, errors.Wrapf(err, "failed to rebuild index for segment file %s", ms.c.txnPath)
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

func (ms *readWriteSegment) Read(offset int64) ([]byte, error) {
	ms.Lock()
	defer ms.Unlock()
	if offset < ms.c.baseOffset || offset > ms.lastOffset {
		return nil, codec.ErrOffsetOutOfBounds
	}

	fileReadOffset := fileOffset(ms.writingIdx, ms.c.baseOffset, offset)
	var payload []byte
	var err error
	if payload, err = ms.c.codec.ReadRecordWithValidation(ms.txnMappedFile, fileReadOffset); err != nil {
		if errors.Is(err, codec.ErrDataCorrupted) {
			return nil, errors.Wrapf(err, "read record failed. entryOffset: %d", offset)
		}
		return nil, err
	}
	return payload, nil
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

func (ms *readWriteSegment) Flush() error {
	return ms.txnMappedFile.Flush()
}

func (*readWriteSegment) OpenTimestamp() time.Time {
	return time.Now()
}

func (ms *readWriteSegment) Close() error {
	ms.Lock()
	defer ms.Unlock()

	err := multierr.Combine(
		ms.txnMappedFile.Unmap(),
		ms.txnFile.Close(),
		// Write index file
		ms.c.codec.WriteIndex(ms.c.idxPath, ms.writingIdx),
	)
	codec.ReturnIndexBuf(&ms.writingIdx)
	return err
}

func (ms *readWriteSegment) Delete() error {
	return multierr.Combine(
		ms.Close(),
		os.Remove(ms.c.txnPath),
		os.Remove(ms.c.idxPath),
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

func initFileWithZeroes(f *os.File, size uint32) error {
	if _, err := f.Seek(int64(size), 0); err != nil {
		return err
	}

	if _, err := f.Write([]byte{0x00}); err != nil {
		return err
	}

	return f.Sync()
}
