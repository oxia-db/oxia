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
	"os"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

// Txn File:
// +--------------+--------------+
// | Size(4 Bytes) | Payload(...) |
// +--------------+--------------+
// Size: 			Length of the payload data
// Payload: 		Byte stream as long as specified by the payload size.

// Idx File:
// +----------------+----------------+----------------+
// | Index(4 Bytes) | Index(4 Bytes) | 	... 		  |
// +----------------+----------------+----------------+
// Index: 			The file offset index.
var _ Codec = &V1{}

const v1PayloadSizeLen uint32 = 4
const v1TxnExtension = ".txn"
const v1IdxExtension = ".idx"

var v1 = &V1{
	Metadata{
		TxnExtension:  v1TxnExtension,
		IdxExtension:  v1IdxExtension,
		HeaderSize:    v1PayloadSizeLen,
		IdxHeaderSize: 0,
	},
}

type V1 struct {
	Metadata
}

func (v *V1) GetIdxExtension() string {
	return v.IdxExtension
}

func (v *V1) GetTxnExtension() string {
	return v.TxnExtension
}

func (v *V1) GetHeaderSize() uint32 {
	return v.HeaderSize
}

func (v *V1) ReadRecordWithValidation(buf []byte, startFileOffset uint32) (payload []byte, err error) {
	var payloadSize uint32
	if payloadSize, _, _, err = v.ReadHeaderWithValidation(buf, startFileOffset); err != nil {
		return nil, err
	}
	payload = make([]byte, payloadSize)
	payloadStartFileOffset := startFileOffset + v.HeaderSize
	copy(payload, buf[payloadStartFileOffset:payloadStartFileOffset+payloadSize])
	return payload, nil
}

func (v *V1) ReadRecordWithValidation0(reader *FileReader, startFileOffset uint32) (payload []byte, err error) {
	var payloadSize uint32
	if payloadSize, _, _, err = v.ReadHeaderWithValidation0(reader, startFileOffset); err != nil {
		return nil, err
	}
	payload = make([]byte, payloadSize)
	payloadStartFileOffset := startFileOffset + v.HeaderSize
	_, err = reader.ReadAt(payload, int64(payloadStartFileOffset))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read payload from file")
	}
	return payload, nil
}

func (v *V1) GetRecordSize(buf []byte, startFileOffset uint32) (payloadSize uint32, err error) {
	if payloadSize, _, _, err = v.ReadHeaderWithValidation(buf, startFileOffset); err != nil {
		return 0, err
	}
	return v.HeaderSize + payloadSize, nil
}

func (v *V1) ReadHeaderWithValidation(buf []byte, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error) {
	bufSize := uint32(len(buf))
	if startFileOffset >= bufSize {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrOffsetOutOfBounds,
			"expected payload size: %d. actual buf size: %d ", startFileOffset+v1PayloadSizeLen, bufSize)
	}

	payloadSize = ReadInt(buf, startFileOffset)
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
	return payloadSize, previousCrc, payloadCrc, nil
}

func (v *V1) ReadHeaderWithValidation0(reader *FileReader, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error) {
	bufSize := uint32(reader.Size())
	if startFileOffset >= bufSize {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrOffsetOutOfBounds,
			"expected payload size: %d. actual file size: %d ", startFileOffset+v1PayloadSizeLen, bufSize)
	}

	payloadSize, err = ReadUint32(reader, startFileOffset)
	if err != nil {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(err, "fail to read payload size")
	}
	// It shouldn't happen when normal reading
	if payloadSize == 0 {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrEmptyPayload, "unexpected empty payload")
	}
	expectSize := payloadSize + v.HeaderSize
	// overflow checking
	actualBufSize := bufSize - startFileOffset
	if expectSize > actualBufSize {
		return payloadSize, previousCrc, payloadCrc,
			errors.Wrapf(ErrOffsetOutOfBounds, "expected payload size: %d. actual file size: %d ", expectSize, bufSize)
	}
	return payloadSize, previousCrc, payloadCrc, nil
}

func (*V1) WriteRecord(buf []byte, startOffset uint32, _ uint32, payload []byte) (recordSize uint32, payloadCrc uint32) {
	payloadSize := uint32(len(payload))

	var headerOffset uint32
	binary.BigEndian.PutUint32(buf[startOffset:], payloadSize)
	headerOffset += v1PayloadSizeLen

	copy(buf[startOffset+headerOffset:], payload)
	return headerOffset + payloadSize, payloadCrc
}

func (*V1) WriteIndex(path string, index []byte) error {
	idxFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to open index file %s", path)
	}

	if _, err = idxFile.Write(index); err != nil {
		return errors.Wrapf(err, "failed write index file %s", path)
	}
	return idxFile.Close()
}

func (*V1) ReadIndex(path string) ([]byte, error) {
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
	return indexBuf, nil
}

func (v *V1) GetIndexHeaderSize() uint32 {
	return v.IdxHeaderSize
}

func (v *V1) RecoverIndex(buf []byte, startFileOffset uint32, baseEntryOffset int64,
	_ *int64) (index []byte, lastCrc uint32, newFileOffset uint32, lastEntryOffset int64, err error) {
	maxSize := uint32(len(buf))
	newFileOffset = startFileOffset
	index = BorrowEmptyIndexBuf()
	currentEntryOffset := baseEntryOffset

	for newFileOffset < maxSize {
		var payloadSize uint32
		var err error
		if payloadSize, _, _, err = v.ReadHeaderWithValidation(buf, newFileOffset); err != nil {
			if errors.Is(err, ErrEmptyPayload) || errors.Is(err, ErrOffsetOutOfBounds) {
				// we might read the end of the segment.
				break
			}
			return nil, 0, 0, 0, err
		}
		index = binary.BigEndian.AppendUint32(index, newFileOffset)
		newFileOffset += v.GetHeaderSize() + payloadSize
		currentEntryOffset++
	}

	return index, lastCrc, newFileOffset, currentEntryOffset - 1, nil
}

func (v *V1) RecoverIndex0(reader *FileReader, startFileOffset uint32, baseEntryOffset int64,
	_ *int64) (index []byte, lastCrc uint32, newFileOffset uint32, lastEntryOffset int64, err error) {
	maxSize := uint32(reader.Size())
	newFileOffset = startFileOffset
	index = BorrowEmptyIndexBuf()
	currentEntryOffset := baseEntryOffset

	for newFileOffset < maxSize {
		var payloadSize uint32
		var err error
		if payloadSize, _, _, err = v.ReadHeaderWithValidation0(reader, newFileOffset); err != nil {
			if errors.Is(err, ErrEmptyPayload) || errors.Is(err, ErrOffsetOutOfBounds) {
				// we might read the end of the segment.
				break
			}
			return nil, 0, 0, 0, err
		}
		index = binary.BigEndian.AppendUint32(index, newFileOffset)
		newFileOffset += v.GetHeaderSize() + payloadSize
		currentEntryOffset++
	}

	return index, lastCrc, newFileOffset, currentEntryOffset - 1, nil
}
