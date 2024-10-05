// Copyright 2024 StreamNative, Inc.
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

	"github.com/pkg/errors"
)

// +--------------+--------------+
// | Size(4Bytes) | Payload(...) |
// +--------------+--------------+
// Size: 			Length of the payload data
// Payload: 		Byte stream as long as specified by the payload size.
var _ Codec = V1{}

const v1PayloadSizeLen uint32 = 4
const v1TxnExtension = ".txn"
const v1IdxExtension = ".idx"

var v1 = &V1{
	Metadata{
		TxnExtension: v1TxnExtension,
		IdxExtension: v1IdxExtension,
		HeaderSize:   v1PayloadSizeLen,
	},
}

type V1 struct {
	Metadata
}

func (v V1) GetIdxExtension() string {
	return v.IdxExtension
}

func (v V1) GetTxnExtension() string {
	return v.TxnExtension
}

func (v V1) GetHeaderSize() uint32 {
	return v.HeaderSize
}

func (v V1) ReadRecordWithValidation(buf []byte, startFileOffset uint32) (payload []byte, err error) {
	var payloadSize uint32
	if payloadSize, _, _, err = v.ReadHeaderWithValidation(buf, startFileOffset); err != nil {
		return nil, err
	}
	payload = make([]byte, payloadSize)
	payloadStartFileOffset := startFileOffset + v.HeaderSize
	copy(payload, buf[payloadStartFileOffset:payloadStartFileOffset+payloadSize])
	return payload, nil
}

func (v V1) GetRecordSize(buf []byte, startFileOffset uint32) (payloadSize uint32, err error) {
	if payloadSize, _, _, err = v.ReadHeaderWithValidation(buf, startFileOffset); err != nil {
		return 0, err
	}
	return v.HeaderSize + payloadSize, nil
}

func (v V1) ReadHeaderWithValidation(buf []byte, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error) {
	bufSize := uint32(len(buf))
	if startFileOffset >= bufSize {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrOffsetOutOfBounds,
			"expected payload size: %d. actual buf size: %d ", startFileOffset+v1PayloadSizeLen, bufSize)
	}

	var headerOffset uint32
	payloadSize = ReadInt(buf, startFileOffset)
	headerOffset += v1PayloadSizeLen
	// It shouldn't happen when normal reading
	if payloadSize == 0 {
		return payloadSize, previousCrc, payloadCrc, errors.Wrapf(ErrEmptyPayload, "unexpected empty payload")
	}
	expectSize := payloadSize + v.HeaderSize
	// overflow checking
	actualBufSize := bufSize - (startFileOffset + headerOffset)
	if expectSize > actualBufSize {
		return payloadSize, previousCrc, payloadCrc,
			errors.Wrapf(ErrOffsetOutOfBounds, "expected payload size: %d. actual buf size: %d ", expectSize, bufSize)
	}
	return payloadSize, previousCrc, payloadCrc, nil
}

func (V1) WriteRecord(buf []byte, startOffset uint32, _ uint32, payload []byte) (recordSize uint32, payloadCrc uint32) {
	payloadSize := uint32(len(payload))

	var headerOffset uint32
	binary.BigEndian.PutUint32(buf[startOffset:], payloadSize)
	headerOffset += v1PayloadSizeLen

	copy(buf[startOffset+headerOffset:], payload)
	return headerOffset + payloadSize, payloadCrc
}
