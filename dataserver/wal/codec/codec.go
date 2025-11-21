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
	"os"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrOffsetOutOfBounds = errors.New("oxia: offset out of bounds")
	ErrEmptyPayload      = errors.New("oxia: empty payload")
	ErrDataCorrupted     = errors.New("oxia: data corrupted")
)

type Metadata struct {
	TxnExtension  string
	IdxExtension  string
	HeaderSize    uint32
	IdxHeaderSize uint32
}

type FileReader struct {
	file  *os.File
	stats os.FileInfo
}

func NewFileReader(file *os.File, stats os.FileInfo) *FileReader {
	return &FileReader{
		file:  file,
		stats: stats,
	}
}

func (f *FileReader) ReadAt(b []byte, off int64) (n int, err error) {
	return f.file.ReadAt(b, off)
}

func (f *FileReader) Size() int64 {
	return f.stats.Size()
}

type Codec interface {
	// GetHeaderSize returns the fixed size of the header in bytes
	// for each record. This value is used to understand where the
	// payload starts after the header.
	GetHeaderSize() uint32

	// GetIdxExtension returns the index file extension. THis value is used to help compatible with
	// multiple versions for index file.
	GetIdxExtension() string

	// GetTxnExtension returns the txn file extension. THis value is used to help compatible with
	// multiple versions for txn file.
	GetTxnExtension() string

	// GetRecordSize returns the size of the record in bytes which includes the header.
	GetRecordSize(buf []byte, startFileOffset uint32) (payloadSize uint32, err error)

	// ReadRecordWithValidation reads a record starting at the specified
	// file offset in the buffer. It also validates the record's integrity
	// (e.g., CRC checks) before returning the payload.
	//
	// Parameters:
	// - buf: The buffer containing the data to read from.
	// - startFileOffset: The file offset to start reading from.
	//
	// Returns:
	// - payload: The actual data (payload) of the record.
	// - err: Error if any issues occur during reading or validation.
	ReadRecordWithValidation(buf []byte, startFileOffset uint32) (payload []byte, err error)

	ReadRecordWithValidation0(reader *FileReader, startFileOffset uint32) (payload []byte, err error)

	// ReadHeaderWithValidation reads the header of a record at the specified
	// offset and validates the integrity of the header data (e.g., CRC checks).
	//
	// Parameters:
	// - buf: The buffer containing the data to read from.
	// - startFileOffset: The file offset to start reading from.
	//
	// Returns:
	// - payloadSize: The size of the payload.
	// - previousCrc: The CRC value of the previous record.
	// - payloadCrc: The CRC value of the current payload.
	// - err: Error if any issues occur during reading or validation.
	ReadHeaderWithValidation(buf []byte, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error)

	ReadHeaderWithValidation0(reader *FileReader, startFileOffset uint32) (payloadSize uint32, previousCrc uint32, payloadCrc uint32, err error)

	// WriteRecord writes a record to the buffer, starting at the specified
	// offset, and includes a header with metadata like CRC.
	//
	// Parameters:
	// - buf: The buffer where the record will be written.
	// - startFileOffset: The file offset to start reading from.
	// - previousCrc: The CRC value of the previous record to maintain consistency.
	// - payload: The actual data (payload) to write as part of the record.
	//
	// Returns:
	// - recordSize: The total size of the written record, including the header.
	// - payloadCrc: The CRC value of the written payload.
	WriteRecord(buf []byte, startFileOffset uint32, previousCrc uint32, payload []byte) (recordSize uint32, payloadCrc uint32)

	// GetIndexHeaderSize returns the size of the index header in bytes.
	// The header size is typically a fixed value representing the metadata at the
	// beginning of an index file.
	GetIndexHeaderSize() uint32

	// WriteIndex writes the provided index data to the specified file path.
	// Parameters:
	// - path: is the location where the index file will be written.
	// - index: is the byte slice that contains the index data.
	// Returns an error if the file cannot be written or if any I/O issues occur.
	WriteIndex(path string, index []byte) error

	// ReadIndex reads the index data from the specified file path.
	// Parameters
	// - path is the location of the index file to be read.
	// Returns the index data as a byte slice and an error if any I/O issues occur.
	ReadIndex(path string) ([]byte, error)

	// RecoverIndex attempts to recover the index from a txn byte buffer.
	//
	// Parameters:
	//   - buf: the byte slice containing the raw data.
	//   - startFileOffset: the starting file offset from which recovery begins.
	//   - baseEntryOffset: the base offset for the index entries, used to adjust entry offsets.
	//   - commitOffset: a pointer to the commit offset, which is using for auto-discard uncommitted corruption data
	//
	// Returns:
	//   - index: the recovered index data as a byte slice.
	//   - lastCrc: the CRC of the last valid entry in the index, used to verify data corruption.
	//   - newFileOffset: the new file offset after recovery, indicating where the next data should be written.
	//   - lastEntryOffset: the offset of the last valid entry in the recovered index.
	//   - err: an error if the recovery process encounters issues, such as data corruption or invalid entries.
	RecoverIndex(buf []byte, startFileOffset uint32, baseEntryOffset int64, commitOffset *int64) (index []byte,
		lastCrc uint32, newFileOffset uint32, lastEntryOffset int64, err error)

	RecoverIndex0(reader *FileReader, startFileOffset uint32, baseEntryOffset int64, commitOffset *int64) (index []byte,
		lastCrc uint32, newFileOffset uint32, lastEntryOffset int64, err error)
}

// The latest codec.
var latestCodec = v2
var SupportedCodecs = []Codec{latestCodec, v1} // the latest codec should be always first element

// GetOrCreate checks if a file with the specified extension exists at the basePath to support compatible with
// the old codec versions.
func GetOrCreate(basePath string) (_codec Codec, exist bool, err error) {
	_codec = latestCodec
	fullPath := basePath + _codec.GetTxnExtension()
	candidateCodecs := SupportedCodecs[1:] // pop the latest version
	for {
		if _, err := os.Stat(fullPath); err != nil {
			if !os.IsNotExist(err) {
				// unexpected behaviour
				return nil, false, nil
			}
			if len(candidateCodecs) == 0 {
				// complete recursive check, go back to the latest txn extension
				return latestCodec, false, nil
			}
			// fallback to previousVersion and check again.
			_codec = candidateCodecs[0]
			// pop
			candidateCodecs = candidateCodecs[1:]
			fullPath = basePath + _codec.GetTxnExtension()
			continue
		}
		return _codec, true, nil
	}
}

// ReadInt read unsigned int from buf with big endian.
func ReadInt(b []byte, offset uint32) uint32 {
	return binary.BigEndian.Uint32(b[offset : offset+4])
}

func ReadUint32(reader *FileReader, offset uint32) (uint32, error) {
	buf := make([]byte, 4)
	_, err := reader.ReadAt(buf, int64(offset))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to read uint32 at offset %d", offset)
	}
	return binary.BigEndian.Uint32(buf), nil
}

func ReadHeader(reader *FileReader, startFileOffset uint32, headerSize uint32) ([]byte, error) {
	headerBuf := make([]byte, headerSize)
	_, err := reader.ReadAt(headerBuf, int64(startFileOffset))
	if err != nil {
		return nil, errors.Wrapf(err, "fail to read header")
	}
	return headerBuf, nil
}

// Index buf.
var bufferPool = sync.Pool{}

const initialIndexBufferCapacity = 16 * 1024

func BorrowEmptyIndexBuf() []byte {
	if pooledBuffer, ok := bufferPool.Get().(*[]byte); ok {
		return (*pooledBuffer)[:0]
	}
	// Start with empty slice, though with some initial capacity
	return make([]byte, 0, initialIndexBufferCapacity)
}

func ReturnIndexBuf(buf *[]byte) {
	bufferPool.Put(buf)
}
