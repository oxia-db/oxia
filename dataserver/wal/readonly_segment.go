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
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/dataserver/wal/codec"
)

func segmentPath(basePath string, firstOffset int64) string {
	return filepath.Join(basePath, fmt.Sprintf("%d", firstOffset))
}

func fileOffset(idx []byte, firstOffset, offset int64) uint32 {
	return codec.ReadInt(idx, uint32((offset-firstOffset)*4))
}

type ReadOnlySegment interface {
	io.Closer

	BaseOffset() int64
	LastOffset() int64

	LastCrc() uint32

	Read(offset int64) ([]byte, error)

	Delete() error

	OpenTimestamp() time.Time
}

type readOnlySegment struct {
	c *segmentConfig

	lastOffset int64
	lastCrc    uint32
	closed     bool

	txnFile *os.File
	reader  *codec.FileReader

	// Index file maps a logical "offset" to a physical file offset within the wal segment
	idx           []byte
	openTimestamp time.Time
}

func newReadOnlySegment(basePath string, baseOffset int64) (ReadOnlySegment, error) {
	c, err := newSegmentConfig(basePath, baseOffset)
	if err != nil {
		return nil, err
	}

	ms := &readOnlySegment{
		c:             c,
		openTimestamp: time.Now(),
	}

	if ms.txnFile, err = os.OpenFile(ms.c.txnPath, os.O_RDONLY, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to open segment txn file %s", ms.c.txnPath)
	}
	fileStat, err := ms.txnFile.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to stat segment txn file %s", ms.c.txnPath)
	}
	ms.reader = codec.NewFileReader(ms.txnFile, fileStat)

	if ms.idx, err = ms.c.codec.ReadIndex(ms.c.idxPath); err != nil {
		if !errors.Is(err, codec.ErrDataCorrupted) {
			return nil, errors.Wrapf(err, "failed to decode segment index file %s", ms.c.idxPath)
		}
		slog.Warn("The segment index file is corrupted and the index is being rebuilt.", slog.String("path", ms.c.idxPath))
		// recover from txn
		if ms.idx, _, _, _, err = ms.c.codec.RecoverIndex0(ms.reader, 0,
			ms.c.baseOffset, nil); err != nil {
			slog.Error("The segment index file rebuild failed.", slog.String("path", ms.c.idxPath))
			return nil, errors.Wrapf(err, "failed to rebuild segment index file %s", ms.c.idxPath)
		}
		slog.Info("The segment index file has been rebuilt.", slog.String("path", ms.c.idxPath))
		if err := ms.c.codec.WriteIndex(ms.c.idxPath, ms.idx); err != nil {
			slog.Warn("write recovered segment index failed. it can continue work but will retry writing after restart.",
				slog.String("path", ms.c.idxPath))
		}
	}

	ms.lastOffset = ms.c.baseOffset + int64(len(ms.idx)/4-1)

	// recover the last crc
	fo := fileOffset(ms.idx, ms.c.baseOffset, ms.lastOffset)
	if _, _, ms.lastCrc, err = ms.c.codec.ReadHeaderWithValidation0(ms.reader, fo); err != nil {
		return nil, err
	}
	return ms, nil
}

func (ms *readOnlySegment) LastCrc() uint32 {
	return ms.lastCrc
}

func (ms *readOnlySegment) BaseOffset() int64 {
	return ms.c.baseOffset
}

func (ms *readOnlySegment) LastOffset() int64 {
	return ms.lastOffset
}

func (ms *readOnlySegment) Read(offset int64) ([]byte, error) {
	if offset < ms.c.baseOffset || offset > ms.lastOffset {
		return nil, codec.ErrOffsetOutOfBounds
	}
	fileReadOffset := fileOffset(ms.idx, ms.c.baseOffset, offset)
	var payload []byte
	var err error
	if payload, err = ms.c.codec.ReadRecordWithValidation0(ms.reader, fileReadOffset); err != nil {
		if errors.Is(err, codec.ErrDataCorrupted) {
			return nil, errors.Wrapf(err, "read record failed. entryOffset: %d", offset)
		}
		return nil, err
	}
	return payload, nil
}

func (ms *readOnlySegment) Close() error {
	if ms.closed {
		return nil
	}

	ms.closed = true
	return multierr.Combine(
		ms.txnFile.Close(),
	)
}

func (ms *readOnlySegment) Delete() error {
	return multierr.Combine(
		ms.Close(),
		os.Remove(ms.c.txnPath),
		os.Remove(ms.c.idxPath),
	)
}

func (ms *readOnlySegment) OpenTimestamp() time.Time {
	return ms.openTimestamp
}

const (
	maxReadOnlySegmentsInCacheCount = 5
	maxReadOnlySegmentsInCacheTime  = 5 * time.Minute
)
