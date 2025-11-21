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

package codec

import (
	"encoding/binary"
	"io"
	"log/slog"
	"os"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/dataserver/util/crc"
)

// Txn File:
// +--------------+-------------------+-------------+--------------+
// | Size(4Bytes) | PreviousCRC(4Bytes) | CRC(4Bytes) | Payload(...) |
// +--------------+-------------------+-------------+--------------+
// Size: 			Length of the payload data
// PreviousCRC: 	32bit hash computed over the previous payload using CRC.
// CRC:				32bit hash computed over the previous and the current payload using CRC. CRC(n) = CRC( DATAn, CRCn-1 )
// Payload: 		Byte stream as long as specified by the payload size.

// Idx File:
// +--------------+-----------+-----------+-----+
// | CRC(4Bytes)  | Index(4B) | Index(4B) | ... |
// +--------------+-----------+-----------+-----+
// CRC: 	 		32bit hash computed over the payload using CRC.
// Index:    		The file offset index.
var _ Codec = &V2{}

const v2PayloadSizeLen uint32 = 4
const v2PreviousCrcLen uint32 = 4
const v2PayloadCrcLen uint32 = 4

const v2IndexCrcLen uint32 = 4

const v2TxnExtension = ".txnx"
const v2IdxExtension = ".idxx"

var v2 = &V2{
	Metadata{
		TxnExtension:  v2TxnExtension,
		IdxExtension:  v2IdxExtension,
		HeaderSize:    v2PayloadSizeLen + v2PreviousCrcLen + v2PayloadCrcLen,
		IdxHeaderSize: v2IndexCrcLen,
	},
}

type V2 struct {
	Metadata
}

func (v *V2) GetIdxExtension() string {
	return v.IdxExtension
}

func (v *V2) GetTxnExtension() string {
	return v.TxnExtension
}

func (v *V2) GetHeaderSize() uint32 {
	return v.HeaderSize
}

func (v *V2) GetRecordSize(buf []byte, startFileOffset uint32) (uint32, error) {
	var payloadSize uint32
	var err error
	if payloadSize, _, _, err = v.ReadHeaderWithValidation(buf, startFileOffset); err != nil {
		return 0, err
	}
	return v.HeaderSize + payloadSize, nil
}

func (v *V2) ReadRecordWithValidation(buf []byte, startFileOffset uint32) (payload []byte, err error) {
	var payloadSize uint32
	if payloadSize, _, _, err = v.ReadHeaderWithValidation(buf, startFileOffset); err != nil {
		return nil, err
	}
	payload = make([]byte, payloadSize)
	payloadStartFileOffset := startFileOffset + v.HeaderSize
	copy(payload, buf[payloadStartFileOffset:payloadStartFileOffset+payloadSize])
	return payload, nil
}

func (v *V2) ReadHeaderWithValidation(buf []byte, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error) {
	bufSize := uint32(len(buf))
	if startFileOffset >= bufSize {
		return payloadSize, previousCrc, payloadCrc,
			errors.Wrapf(ErrOffsetOutOfBounds, "expected payload size: %d. actual buf size: %d ",
				startFileOffset+v2PayloadSizeLen, bufSize)
	}

	var headerOffset uint32
	payloadSize = ReadInt(buf, startFileOffset)
	headerOffset += v2PayloadSizeLen

	// It shouldn't happen when normal reading
	if payloadSize == 0 {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrEmptyPayload, "unexpected empty payload")
	}

	expectSize := payloadSize + v.HeaderSize
	// overflow checking
	actualBufSize := bufSize - startFileOffset
	if expectSize > actualBufSize {
		return payloadSize, previousCrc, payloadCrc,
			errors.Wrapf(ErrOffsetOutOfBounds, "expected payload size: %d. actual buf size: %d ", expectSize, bufSize)
	}

	previousCrc = ReadInt(buf, startFileOffset+headerOffset)
	headerOffset += v2PreviousCrcLen
	payloadCrc = ReadInt(buf, startFileOffset+headerOffset)
	headerOffset += v2PayloadCrcLen

	payloadStartFileOffset := startFileOffset + headerOffset
	payloadSlice := buf[payloadStartFileOffset : payloadStartFileOffset+payloadSize]

	if expectedCrc := crc.Checksum(previousCrc).Update(payloadSlice).Value(); expectedCrc != payloadCrc {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrDataCorrupted,
			" expected crc: %d; actual crc: %d", expectedCrc, payloadCrc)
	}

	return payloadSize, previousCrc, payloadCrc, nil
}

func (*V2) WriteRecord(buf []byte, startOffset uint32, previousCrc uint32, payload []byte) (recordSize uint32, payloadCrc uint32) {
	payloadSize := uint32(len(payload))

	var headerOffset uint32
	binary.BigEndian.PutUint32(buf[startOffset:], payloadSize)
	headerOffset += v2PayloadSizeLen

	binary.BigEndian.PutUint32(buf[startOffset+headerOffset:], previousCrc)
	headerOffset += v2PreviousCrcLen
	payloadCrc = crc.Checksum(previousCrc).Update(payload).Value()
	binary.BigEndian.PutUint32(buf[startOffset+headerOffset:], payloadCrc)
	headerOffset += v2PayloadCrcLen

	copy(buf[startOffset+headerOffset:], payload)
	return headerOffset + payloadSize, payloadCrc
}

func (v *V2) WriteIndex(path string, index []byte) error {
	idxFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to open index file %s", path)
	}
	buf := make([]byte, uint32(len(index))+v.GetIndexHeaderSize())
	indexCrc := crc.Checksum(0).Update(index).Value()
	binary.BigEndian.PutUint32(buf[0:], indexCrc)
	copy(buf[v.GetIndexHeaderSize():], index)
	if _, err = idxFile.Write(buf); err != nil {
		return errors.Wrapf(err, "failed write index file %s", path)
	}
	return idxFile.Close()
}

func (v *V2) ReadIndex(path string) ([]byte, error) {
	var idFile *os.File
	var err error
	if idFile, err = os.OpenFile(path, os.O_RDONLY, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to open segment index file %s", path)
	}
	var indexBuf []byte
	if indexBuf, err = io.ReadAll(idFile); err != nil {
		return nil, multierr.Combine(
			errors.Wrapf(err, "failed to read segment index file %s", path),
			idFile.Close())
	}
	if err = idFile.Close(); err != nil {
		return nil, errors.Wrapf(err, "failed to close segment index file %s", path)
	}
	expectedCrc := ReadInt(indexBuf, 0)
	actualCrc := crc.Checksum(0).Update(indexBuf[v.GetIndexHeaderSize():]).Value()
	if expectedCrc != actualCrc {
		return nil, errors.Wrapf(ErrDataCorrupted,
			" expected crc: %d; actual crc: %d", expectedCrc, actualCrc)
	}
	index := make([]byte, uint32(len(indexBuf))-v.GetIndexHeaderSize())
	copy(index, indexBuf[v.GetIndexHeaderSize():])
	return index, nil
}

func (v *V2) RecoverIndex(buf []byte, startFileOffset uint32, baseEntryOffset int64,
	commitOffset *int64) (index []byte, lastCrc uint32,
	newFileOffset uint32, lastEntryOffset int64, err error) {
	maxSize := uint32(len(buf))
	newFileOffset = startFileOffset
	currentEntryOffset := baseEntryOffset

	index = BorrowEmptyIndexBuf()

	for newFileOffset+v.HeaderSize <= maxSize {
		var payloadSize uint32
		var payloadCrc uint32
		var err error
		if payloadSize, _, payloadCrc, err = v.ReadHeaderWithValidation(buf, newFileOffset); err != nil {
			if errors.Is(err, ErrEmptyPayload) {
				// we might read the end of the segment.
				break
			}
			// data corruption
			if errors.Is(err, ErrOffsetOutOfBounds) || errors.Is(err, ErrDataCorrupted) {
				if commitOffset != nil && currentEntryOffset > *commitOffset {
					// uncommitted data corruption, simply discard it
					slog.Warn("discard the corrupted uncommitted data.",
						slog.Int64("entryId", currentEntryOffset), slog.Any("error", err))
					break
				}
				return nil, 0, 0, 0, errors.Wrapf(err, "entryOffset: %d", currentEntryOffset)
			}
			return nil, 0, 0, 0, err
		}
		lastCrc = payloadCrc
		index = binary.BigEndian.AppendUint32(index, newFileOffset)
		newFileOffset += v.GetHeaderSize() + payloadSize
		currentEntryOffset++
	}
	return index, lastCrc, newFileOffset, currentEntryOffset - 1, nil
}

func (v *V2) GetIndexHeaderSize() uint32 {
	return v.IdxHeaderSize
}
