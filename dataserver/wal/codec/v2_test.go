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
	"bytes"
	"encoding/binary"
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/dataserver/util/crc"
)

func TestV2_GetHeaderSize(t *testing.T) {
	assert.EqualValues(t, v2.GetHeaderSize(), 12)
}

func TestV2_Codec(t *testing.T) {
	buf := make([]byte, 100)
	payload := []byte{1}
	recordSize, _ := v2.WriteRecord(buf, 0, 0, payload)
	assert.EqualValues(t, recordSize, 13)
	getRecordSize, err := v2.GetRecordSize(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, getRecordSize, recordSize)
	payloadSize, previousCrc, payloadCrc, err := v2.ReadHeaderWithValidation(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, previousCrc, 0)
	expectedPayloadCrc := crc.Checksum(0).Update(payload).Value()
	assert.EqualValues(t, expectedPayloadCrc, payloadCrc)
	assert.EqualValues(t, recordSize-(v2PayloadSizeLen+v2PreviousCrcLen+v2PayloadCrcLen), payloadSize)

	getPayload, err := v2.ReadRecordWithValidation(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload, getPayload)
}

func TestV2_Crc(t *testing.T) {
	buf := make([]byte, 1000)

	var entriesToOffset []uint32
	offset := uint32(0)
	previousCrc := uint32(0)
	for i := 0; i < 10; i++ {
		recordSize, payloadCrc := v2.WriteRecord(buf, offset, previousCrc, []byte{byte(i)})
		entriesToOffset = append(entriesToOffset, offset)
		previousCrc = payloadCrc
		offset += recordSize
	}

	// crc validation
	expectedChecksum := uint32(0)
	for index := range len(entriesToOffset) {
		startFOffset := entriesToOffset[index]
		_, previousCrc, payloadCrc, err := v2.ReadHeaderWithValidation(buf, startFOffset)
		assert.NoError(t, err)
		assert.EqualValues(t, expectedChecksum, previousCrc)
		expectedPayloadChecksum := crc.Checksum(expectedChecksum).Update([]byte{byte(index)}).Value()
		assert.EqualValues(t, expectedPayloadChecksum, payloadCrc)
		expectedChecksum = expectedPayloadChecksum
	}

	for index := range len(entriesToOffset) {
		startFOffset := entriesToOffset[index]
		payload, err := v2.ReadRecordWithValidation(buf, startFOffset)
		assert.NoError(t, err)
		assert.EqualValues(t, []byte{byte(index)}, payload)
	}
}

func TestV2_CrcConsistency(t *testing.T) {
	buf1 := make([]byte, 1000)
	buf2 := make([]byte, 1000)
	offset := uint32(0)
	previousCrc := uint32(0)

	// load buf-1
	var entriesToOffset1 []uint32

	for i := 0; i < 10; i++ {
		recordSize, payloadCrc := v2.WriteRecord(buf1, offset, previousCrc, []byte{byte(i)})
		entriesToOffset1 = append(entriesToOffset1, offset)
		previousCrc = payloadCrc
		offset += recordSize
	}

	// reload variables
	offset = uint32(0)
	previousCrc = uint32(0)

	// load buf-2
	var entriesToOffset2 []uint32
	for i := 0; i < 10; i++ {
		recordSize, payloadCrc := v2.WriteRecord(buf2, offset, previousCrc, []byte{byte(i)})
		entriesToOffset2 = append(entriesToOffset2, offset)
		previousCrc = payloadCrc
		offset += recordSize
	}

	lastEntryOffset1 := entriesToOffset1[len(entriesToOffset1)-1]
	lastEntryOffset2 := entriesToOffset2[len(entriesToOffset2)-1]

	_, _, payloadCrc1, err := v2.ReadHeaderWithValidation(buf1, lastEntryOffset1)
	assert.NoError(t, err)
	_, _, payloadCrc2, err := v2.ReadHeaderWithValidation(buf2, lastEntryOffset2)
	assert.NoError(t, err)
	assert.EqualValues(t, payloadCrc1, payloadCrc2)
}

func TestV2_DeviatingCrc(t *testing.T) {
	buf1 := make([]byte, 1000)
	buf2 := make([]byte, 1000)
	deviatingIndex := 5
	offset := uint32(0)
	previousCrc := uint32(0)

	// load buf-1
	var entriesToOffset1 []uint32

	for i := 0; i < 10; i++ {
		recordSize, payloadCrc := v2.WriteRecord(buf1, offset, previousCrc, []byte{byte(i)})
		entriesToOffset1 = append(entriesToOffset1, offset)
		previousCrc = payloadCrc
		offset += recordSize
	}

	// reload variables
	offset = uint32(0)
	previousCrc = uint32(0)

	// load buf-2
	var entriesToOffset2 []uint32
	for i := 0; i < 10; i++ {
		payload := []byte{byte(i)}
		if i == deviatingIndex {
			payload = []byte{128}
		}
		recordSize, payloadCrc := v2.WriteRecord(buf2, offset, previousCrc, payload)
		entriesToOffset2 = append(entriesToOffset2, offset)
		previousCrc = payloadCrc
		offset += recordSize
	}

	lastEntryOffset1 := entriesToOffset1[len(entriesToOffset1)-1]
	lastEntryOffset2 := entriesToOffset2[len(entriesToOffset2)-1]

	_, _, payloadCrc1, err := v2.ReadHeaderWithValidation(buf1, lastEntryOffset1)
	assert.NoError(t, err)
	_, _, payloadCrc2, err := v2.ReadHeaderWithValidation(buf2, lastEntryOffset2)
	assert.NoError(t, err)
	assert.NotEqualValues(t, payloadCrc1, payloadCrc2)

	assert.EqualValues(t, len(entriesToOffset1), len(entriesToOffset2))
	var actualDeviatingIndex int
	// find the deviating index
	for index := range len(entriesToOffset1) {
		fOffset := entriesToOffset1[index]
		_, _, payloadCrc1, err := v2.ReadHeaderWithValidation(buf1, fOffset)
		assert.NoError(t, err)
		fOffset = entriesToOffset2[index]
		_, _, payloadCrc2, err := v2.ReadHeaderWithValidation(buf2, fOffset)
		assert.NoError(t, err)
		if payloadCrc1 == payloadCrc2 {
			continue
		}
		actualDeviatingIndex = index
		break
	}
	assert.EqualValues(t, deviatingIndex, actualDeviatingIndex)
}

func TestV2_BreakingPoint_Size(t *testing.T) {
	buf := make([]byte, 100)

	v2.WriteRecord(buf, 0, 0, []byte{1})
	binary.BigEndian.PutUint32(buf, 123123)

	_, _, _, err := v2.ReadHeaderWithValidation(buf, 0)
	assert.ErrorIs(t, err, ErrOffsetOutOfBounds)

	_, err = v2.ReadRecordWithValidation(buf, 0)
	assert.ErrorIs(t, err, ErrOffsetOutOfBounds)
}

func TestV2_BreakingPoint_PreviousCrc(t *testing.T) {
	buf := make([]byte, 100)

	v2.WriteRecord(buf, 0, 0, []byte{1})
	binary.BigEndian.PutUint32(buf[v2PayloadSizeLen:], 123123)

	_, _, _, err := v2.ReadHeaderWithValidation(buf, 0)
	assert.ErrorIs(t, err, ErrDataCorrupted)

	_, err = v2.ReadRecordWithValidation(buf, 0)
	assert.ErrorIs(t, err, ErrDataCorrupted)
}

func TestV2_BreakingPoint_PayloadCrc(t *testing.T) {
	buf := make([]byte, 100)

	v2.WriteRecord(buf, 0, 0, []byte{1})
	binary.BigEndian.PutUint32(buf[v2PayloadSizeLen+v2PreviousCrcLen:], 123123)

	_, _, _, err := v2.ReadHeaderWithValidation(buf, 0)
	assert.ErrorIs(t, err, ErrDataCorrupted)

	_, err = v2.ReadRecordWithValidation(buf, 0)
	assert.ErrorIs(t, err, ErrDataCorrupted)
}

func TestV2_BreakingPoint_Payload(t *testing.T) {
	buf := make([]byte, 100)

	v2.WriteRecord(buf, 0, 0, []byte{1})
	binary.BigEndian.PutUint32(buf[v2.HeaderSize:], 1231242)

	_, _, _, err := v2.ReadHeaderWithValidation(buf, 0)
	assert.ErrorIs(t, err, ErrDataCorrupted)

	_, err = v2.ReadRecordWithValidation(buf, 0)
	assert.ErrorIs(t, err, ErrDataCorrupted)
}

func TestV2_WriteReadIndex(t *testing.T) {
	dir := os.TempDir()
	fileName := "0"
	elementsNum := 5
	indexBuf := make([]byte, uint32(elementsNum*4)+v2.GetIndexHeaderSize())
	for i := 0; i < elementsNum; i++ {
		binary.BigEndian.PutUint32(indexBuf[i*4:], uint32(i))
	}
	p := path.Join(dir, fileName+v2.GetIdxExtension())
	err := v2.WriteIndex(p, indexBuf)
	assert.NoError(t, err)
	index, err := v2.ReadIndex(p)
	assert.NoError(t, err)
	for i := 0; i < elementsNum; i++ {
		idx := ReadInt(index, uint32(i*4))
		assert.EqualValues(t, idx, i)
	}
}

func TestV2_RecoverIndex(t *testing.T) {
	elementsNum := 5

	buf := make([]byte, 200)
	var payloads [][]byte
	for i := 0; i < elementsNum; i++ {
		payload, err := uuid.New().MarshalBinary()
		assert.NoError(t, err)
		payloads = append(payloads, payload)
	}

	fOffset := uint32(0)
	for i := 0; i < elementsNum; i++ {
		recordSize, _ := v2.WriteRecord(buf, fOffset, 0, payloads[i])
		fOffset += recordSize
	}

	index, _, newFileOffset, lastEntryOffset, err := v2.RecoverIndex(buf, 0, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, lastEntryOffset, 4)
	assert.EqualValues(t, fOffset, newFileOffset)
	for i := 0; i < elementsNum; i++ {
		fOffset := ReadInt(index, uint32(i*4))
		payload, err := v2.ReadRecordWithValidation(buf, fOffset)
		assert.NoError(t, err)
		assert.EqualValues(t, payloads[i], payload)
	}
}

func TestV2_IndexBroken(t *testing.T) {
	dir := os.TempDir()
	fileName := "0"
	elementsNum := 5
	indexBuf := make([]byte, uint32(elementsNum*4)+v2.GetIndexHeaderSize())
	for i := 0; i < elementsNum; i++ {
		binary.BigEndian.PutUint32(indexBuf[i*4:], uint32(i))
	}
	p := path.Join(dir, fileName+v2.GetIdxExtension())
	err := v2.WriteIndex(p, indexBuf)
	assert.NoError(t, err)

	// inject wrong data
	file, err := os.OpenFile(p, os.O_RDWR, 0644)
	assert.NoError(t, err)
	defer file.Close()
	faultData, err := uuid.New().MarshalBinary()
	assert.NoError(t, err)
	_, err = file.WriteAt(faultData, 0)
	assert.NoError(t, err)

	_, err = v2.ReadIndex(p)
	assert.ErrorIs(t, err, ErrDataCorrupted)
}

func TestV2_ReadWithValidation(t *testing.T) {
	buf := make([]byte, 15)
	payloadSize := uint32(len(buf)) - v2.HeaderSize
	payload := bytes.Repeat([]byte("A"), int(payloadSize))
	_, wPayloadCrc := v2.WriteRecord(buf, 0, 0, payload)
	_, _, rPayloadCrc, err := v2.ReadHeaderWithValidation(buf, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, wPayloadCrc, rPayloadCrc)
}

func TestV2_RecoveryWithNotEnoughBuf(t *testing.T) {
	buf := make([]byte, 16)
	payloadSize := uint32(len(buf)) - v2.HeaderSize - 1
	payload := bytes.Repeat([]byte("A"), int(payloadSize))
	_, wPayloadCrc := v2.WriteRecord(buf, 0, 0, payload)
	_, rLastCrc, _, entryOffset, err := v2.RecoverIndex(buf, 0, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, wPayloadCrc, rLastCrc)
	assert.EqualValues(t, entryOffset, 0)
}

// TestV2_ReadRecordWithValidation0 tests reading records using FileReader.
func TestV2_ReadRecordWithValidation0(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "TestV2_ReadRecordWithValidation0.txn")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Prepare test data
	buf := make([]byte, 100)
	payload := []byte{1, 2, 3, 4, 5}
	recordSize, _ := v2.WriteRecord(buf, 0, 0, payload)

	// Write to file
	_, err = tmpFile.Write(buf[:recordSize])
	assert.NoError(t, err)

	// Create FileReader
	fileStat, err := tmpFile.Stat()
	assert.NoError(t, err)
	reader := NewFileReader(tmpFile, fileStat)

	// Test reading record
	getPayload, err := v2.ReadRecordWithValidation0(reader, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload, getPayload)
}

// TestV2_ReadHeaderWithValidation0 tests reading record headers using FileReader.
func TestV2_ReadHeaderWithValidation0(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "TestV2_ReadHeaderWithValidation0.txn")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Prepare test data
	buf := make([]byte, 100)
	payload := []byte{1, 2, 3}
	recordSize, expectedPayloadCrc := v2.WriteRecord(buf, 0, 0, payload)

	// Write to file
	_, err = tmpFile.Write(buf[:recordSize])
	assert.NoError(t, err)

	// Create FileReader
	fileStat, err := tmpFile.Stat()
	assert.NoError(t, err)
	reader := NewFileReader(tmpFile, fileStat)

	// Test reading record header
	payloadSize, previousCrc, payloadCrc, err := v2.ReadHeaderWithValidation0(reader, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, previousCrc)
	assert.EqualValues(t, expectedPayloadCrc, payloadCrc)
	assert.EqualValues(t, len(payload), payloadSize)
}

// TestV2_ReadRecordWithValidation0_WithCrc tests CRC chain validation.
func TestV2_ReadRecordWithValidation0_WithCrc(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "TestV2_ReadRecordWithValidation0_WithCrc.txn")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Prepare multiple records data
	buf := make([]byte, 1000)
	var fileOffsets []uint32
	currentOffset := uint32(0)
	previousCrc := uint32(0)

	// Write multiple records
	for i := 0; i < 5; i++ {
		payload := []byte{byte(i)}
		recordSize, payloadCrc := v2.WriteRecord(buf, currentOffset, previousCrc, payload)
		fileOffsets = append(fileOffsets, currentOffset)
		previousCrc = payloadCrc
		currentOffset += recordSize
	}

	// Write to file
	_, err = tmpFile.Write(buf[:currentOffset])
	assert.NoError(t, err)

	// Create FileReader
	fileStat, err := tmpFile.Stat()
	assert.NoError(t, err)
	reader := NewFileReader(tmpFile, fileStat)

	// Validate each record
	expectedChecksum := uint32(0)
	for i, offset := range fileOffsets {
		// Read record header
		_, prevCrc, payloadCrc, err := v2.ReadHeaderWithValidation0(reader, offset)
		assert.NoError(t, err)
		assert.EqualValues(t, expectedChecksum, prevCrc)

		// Calculate expected CRC
		expectedPayloadCrc := crc.Checksum(expectedChecksum).Update([]byte{byte(i)}).Value()
		assert.EqualValues(t, expectedPayloadCrc, payloadCrc)
		expectedChecksum = expectedPayloadCrc

		// Read full record
		payload, err := v2.ReadRecordWithValidation0(reader, offset)
		assert.NoError(t, err)
		assert.EqualValues(t, []byte{byte(i)}, payload)
	}
}

// TestV2_ReadRecordWithValidation0_ErrorCases tests error scenarios.
func TestV2_ReadRecordWithValidation0_ErrorCases(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "test_read_record_errors_*.txn")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Prepare test data with sufficient buffer
	buf := make([]byte, 100)
	payload := []byte{1, 2, 3, 4, 5}
	recordSize, _ := v2.WriteRecord(buf, 0, 0, payload)

	// Write to file
	_, err = tmpFile.Write(buf[:recordSize])
	assert.NoError(t, err)

	// Test cases
	testCases := []struct {
		name        string
		modifyFunc  func([]byte)
		expectError error
	}{
		{
			name: "corrupted payload size",
			modifyFunc: func(buf []byte) {
				if len(buf) >= 4 {
					binary.BigEndian.PutUint32(buf, 123123) // invalid payload size
				}
			},
			expectError: ErrOffsetOutOfBounds,
		},
		{
			name: "corrupted previous CRC",
			modifyFunc: func(buf []byte) {
				if len(buf) >= int(v2PayloadSizeLen+4) {
					binary.BigEndian.PutUint32(buf[v2PayloadSizeLen:], 123123)
				}
			},
			expectError: ErrDataCorrupted,
		},
		{
			name: "corrupted payload CRC",
			modifyFunc: func(buf []byte) {
				if len(buf) >= int(v2PayloadSizeLen+v2PreviousCrcLen+4) {
					binary.BigEndian.PutUint32(buf[v2PayloadSizeLen+v2PreviousCrcLen:], 123123)
				}
			},
			expectError: ErrDataCorrupted,
		},
		{
			name: "corrupted payload data",
			modifyFunc: func(buf []byte) {
				if len(buf) >= int(v2.HeaderSize+4) {
					binary.BigEndian.PutUint32(buf[v2.HeaderSize:], 1231242)
				}
			},
			expectError: ErrDataCorrupted,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset file
			err = tmpFile.Truncate(0)
			assert.NoError(t, err)
			_, err = tmpFile.Seek(0, 0)
			assert.NoError(t, err)

			// Write original data
			_, err = tmpFile.Write(buf[:recordSize])
			assert.NoError(t, err)

			// Read file content for modification
			fileContent := make([]byte, recordSize)
			_, err = tmpFile.ReadAt(fileContent, 0)
			assert.NoError(t, err)

			// Modify data to create errors
			tc.modifyFunc(fileContent)
			_, err = tmpFile.WriteAt(fileContent, 0)
			assert.NoError(t, err)

			// Create new FileReader
			fileStat, err := tmpFile.Stat()
			assert.NoError(t, err)
			reader := NewFileReader(tmpFile, fileStat)

			// Verify read failure
			_, _, _, err = v2.ReadHeaderWithValidation0(reader, 0)
			assert.ErrorIs(t, err, tc.expectError)

			_, err = v2.ReadRecordWithValidation0(reader, 0)
			assert.ErrorIs(t, err, tc.expectError)
		})
	}
}

// TestV2_RecoverIndex0 tests index recovery using FileReader.
func TestV2_RecoverIndex0(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "TestV2_RecoverIndex0.txn")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Prepare test data
	buf := make([]byte, 200)
	elementsNum := 5
	var payloads [][]byte
	for i := 0; i < elementsNum; i++ {
		payload, err := uuid.New().MarshalBinary()
		assert.NoError(t, err)
		payloads = append(payloads, payload)
	}

	// Write multiple records
	fOffset := uint32(0)
	for i := 0; i < elementsNum; i++ {
		recordSize, _ := v2.WriteRecord(buf, fOffset, 0, payloads[i])
		fOffset += recordSize
	}

	// Write to file
	_, err = tmpFile.Write(buf[:fOffset])
	assert.NoError(t, err)

	// Create FileReader
	fileStat, err := tmpFile.Stat()
	assert.NoError(t, err)
	reader := NewFileReader(tmpFile, fileStat)

	// Recover index
	index, _, newFileOffset, lastEntryOffset, err := v2.RecoverIndex0(reader, 0, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, lastEntryOffset, int64(elementsNum-1))
	assert.EqualValues(t, fOffset, newFileOffset)

	// Verify recovered index
	for i := 0; i < elementsNum; i++ {
		fOffset := ReadInt(index, uint32(i*4))
		payload, err := v2.ReadRecordWithValidation0(reader, fOffset)
		assert.NoError(t, err)
		assert.EqualValues(t, payloads[i], payload)
	}
}

// TestV2_RecoverIndex0_InsufficientData tests insufficient data scenario.
func TestV2_RecoverIndex0_InsufficientData(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "TestV2_RecoverIndex0_InsufficientData.txn")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Prepare data that's insufficient for a complete record
	buf := make([]byte, 16)
	payloadSize := uint32(len(buf)) - v2.HeaderSize - 1 // 1 byte less than complete record
	payload := bytes.Repeat([]byte("A"), int(payloadSize))
	_, wPayloadCrc := v2.WriteRecord(buf, 0, 0, payload)

	// Write to file (incomplete record)
	_, err = tmpFile.Write(buf[:len(buf)-1])
	assert.NoError(t, err)

	// Create FileReader
	fileStat, err := tmpFile.Stat()
	assert.NoError(t, err)
	reader := NewFileReader(tmpFile, fileStat)

	// Index recovery should succeed but only process valid data
	_, rLastCrc, _, entryOffset, err := v2.RecoverIndex0(reader, 0, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, wPayloadCrc, rLastCrc)
	assert.EqualValues(t, entryOffset, 0)
}

// TestV2_RecoverIndex0_CorruptedData tests corrupted data scenario.
func TestV2_RecoverIndex0_CorruptedData(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "TestV2_RecoverIndex0_CorruptedData.txn")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Prepare mixed data: valid records + corrupted data
	buf := make([]byte, 300)

	// Write some valid records
	var validPayloads [][]byte
	fOffset := uint32(0)
	for i := 0; i < 3; i++ {
		payload := []byte{byte(i)}
		recordSize, _ := v2.WriteRecord(buf, fOffset, 0, payload)
		validPayloads = append(validPayloads, payload)
		fOffset += recordSize
	}

	// Add corrupted data
	copy(buf[fOffset:], []byte{0xFF, 0xFF, 0xFF, 0xFF}) // Invalid payload size

	// Write to file
	_, err = tmpFile.Write(buf[:fOffset+10]) // Include partial corrupted data
	assert.NoError(t, err)

	// Create FileReader
	fileStat, err := tmpFile.Stat()
	assert.NoError(t, err)
	reader := NewFileReader(tmpFile, fileStat)

	// Index recovery should succeed but only process valid records
	index, _, _, lastEntryOffset, err := v2.RecoverIndex0(reader, 0, 0, nil)
	assert.NoError(t, err)

	// Should only recover up to last valid record
	assert.EqualValues(t, 2, lastEntryOffset) // 3 valid records, index starts from 0

	// Verify recovered valid records
	for i := 0; i <= int(lastEntryOffset); i++ {
		fOffset := ReadInt(index, uint32(i*4))
		payload, err := v2.ReadRecordWithValidation0(reader, fOffset)
		assert.NoError(t, err)
		assert.EqualValues(t, validPayloads[i], payload)
	}
}

// TestV2_FileReader_EdgeCases tests FileReader edge cases.
func TestV2_FileReader_EdgeCases(t *testing.T) {
	// Test empty file
	tmpFile, err := os.CreateTemp("", ".txn")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	fileStat, err := tmpFile.Stat()
	assert.NoError(t, err)
	reader := NewFileReader(tmpFile, fileStat)

	// Reading from empty file should fail
	_, _, _, err = v2.ReadHeaderWithValidation0(reader, 0)
	assert.Error(t, err)

	_, err = v2.ReadRecordWithValidation0(reader, 0)
	assert.Error(t, err)

	// Recovering index from empty file should return empty index
	index, lastCrc, newOffset, lastEntry, err := v2.RecoverIndex0(reader, 0, 0, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), lastCrc)
	assert.Equal(t, uint32(0), newOffset)
	assert.Equal(t, int64(-1), lastEntry) // Should be -1 when no valid records
	assert.NotNil(t, index)
}

// TestV2_FileReader_Size tests FileReader Size method.
func TestV2_FileReader_Size(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "TestV2_FileReader_Size.txn")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Write some data
	data := []byte{1, 2, 3, 4, 5}
	_, err = tmpFile.Write(data)
	assert.NoError(t, err)

	fileStat, err := tmpFile.Stat()
	assert.NoError(t, err)
	reader := NewFileReader(tmpFile, fileStat)

	assert.Equal(t, fileStat.Size(), reader.Size())
}
