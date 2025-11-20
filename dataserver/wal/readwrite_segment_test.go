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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/dataserver/wal/codec"
)

func TestReadWriteSegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)

	assert.EqualValues(t, 0, rw.BaseOffset())
	assert.EqualValues(t, -1, rw.LastOffset())

	assert.NoError(t, rw.Append(0, []byte("entry-0")))
	assert.EqualValues(t, 0, rw.BaseOffset())
	assert.EqualValues(t, 0, rw.LastOffset())

	assert.NoError(t, rw.Flush())

	assert.NoError(t, rw.Append(1, []byte("entry-1")))
	assert.EqualValues(t, 0, rw.BaseOffset())
	assert.EqualValues(t, 1, rw.LastOffset())

	assert.NoError(t, rw.Close())

	// Re-open and recover the segment
	rw, err = newReadWriteSegment(path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, rw.BaseOffset())
	assert.EqualValues(t, 1, rw.LastOffset())

	data, err := rw.Read(0)
	assert.NoError(t, err)
	assert.Equal(t, "entry-0", string(data))

	data, err = rw.Read(1)
	assert.NoError(t, err)
	assert.Equal(t, "entry-1", string(data))

	assert.NoError(t, rw.Close())
}

func TestReadWriteSegment_NonZero(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(path, 5, 128*1024, 0, nil)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 4, rw.LastOffset())

	assert.NoError(t, rw.Append(5, []byte("entry-0")))
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 5, rw.LastOffset())

	assert.NoError(t, rw.Flush())

	assert.NoError(t, rw.Append(6, []byte("entry-1")))
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())

	assert.ErrorIs(t, rw.Append(4, []byte("entry-4")), ErrInvalidNextOffset)
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())

	assert.ErrorIs(t, rw.Append(8, []byte("entry-8")), ErrInvalidNextOffset)
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())

	assert.NoError(t, rw.Close())

	// Re-open and recover the segment
	rw, err = newReadWriteSegment(path, 5, 128*1024, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())
}

func TestReadWriteSegment_HasSpace(t *testing.T) {
	rw, err := newReadWriteSegment(t.TempDir(), 0, 1024, 0, nil)
	assert.NoError(t, err)
	segment := rw.(*readWriteSegment)
	headerSize := int(segment.c.codec.GetHeaderSize())

	assert.True(t, rw.HasSpace(10))
	assert.False(t, rw.HasSpace(1024))
	assert.True(t, rw.HasSpace(1024-headerSize))
	assert.False(t, rw.HasSpace(1021))

	assert.NoError(t, rw.Append(0, make([]byte, 100)))
	assert.True(t, rw.HasSpace(10))
	assert.False(t, rw.HasSpace(1020))
	assert.False(t, rw.HasSpace(1020-100))
	assert.True(t, rw.HasSpace(1024-100-headerSize*2))
}

type ConfigurableCommitOffsetProvider struct {
	commitOffset int64
}

func (c ConfigurableCommitOffsetProvider) CommitOffset() int64 {
	return c.commitOffset
}

func TestReadWriteSegment_BrokenUncommittedData_ErrOffsetOutOfBounds(t *testing.T) {
	commitOffsetProvider := ConfigurableCommitOffsetProvider{}

	dir := t.TempDir()
	// basic functionality test
	rw, err := newReadWriteSegment(dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	payload1 := []byte("entry-0")
	assert.NoError(t, rw.Append(0, payload1))
	payload2 := []byte("entry-1")
	assert.NoError(t, rw.Append(1, payload2))
	payload3 := []byte("entry-2")
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload1, err := rw.Read(0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload1, actualPayload1)
	actualPayload2, err := rw.Read(1)
	assert.NoError(t, err)
	assert.EqualValues(t, payload2, actualPayload2)
	actualPayload3, err := rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	// move commit offset to 1
	commitOffsetProvider.commitOffset = 1

	// inject payload size failure to trigger ErrOffsetOutOfBounds
	rwSegment := rw.(*readWriteSegment)
	fso := fileOffset(rwSegment.writingIdx, 0, 2)
	binary.BigEndian.PutUint32(rwSegment.txnMappedFile[fso:], 9999999)

	// close the segment
	rwSegment.Close()

	// recover the rw segment
	rw, err = newReadWriteSegment(dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, rw.LastOffset())

	// test functionality
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload3, err = rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	rw.Close()
}

func TestReadWriteSegment_BrokenCommittedData_ErrOffsetOutOfBounds(t *testing.T) {
	commitOffsetProvider := ConfigurableCommitOffsetProvider{}

	dir := t.TempDir()
	// basic functionality test
	rw, err := newReadWriteSegment(dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	payload1 := []byte("entry-0")
	assert.NoError(t, rw.Append(0, payload1))
	payload2 := []byte("entry-1")
	assert.NoError(t, rw.Append(1, payload2))
	payload3 := []byte("entry-2")
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload1, err := rw.Read(0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload1, actualPayload1)
	actualPayload2, err := rw.Read(1)
	assert.NoError(t, err)
	assert.EqualValues(t, payload2, actualPayload2)
	actualPayload3, err := rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	// move commit offset to 2
	commitOffsetProvider.commitOffset = 2

	// inject payload size failure to trigger ErrOffsetOutOfBounds
	rwSegment := rw.(*readWriteSegment)
	fso := fileOffset(rwSegment.writingIdx, 0, 2)
	binary.BigEndian.PutUint32(rwSegment.txnMappedFile[fso:], 9999999)

	// close the segment
	rwSegment.Close()

	// recover the rw segment
	_, err = newReadWriteSegment(dir, 0, 1024, 0, commitOffsetProvider)
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)
}

func TestReadWriteSegment_BrokenUncommittedData_ErrDataCorrupted(t *testing.T) {
	commitOffsetProvider := ConfigurableCommitOffsetProvider{}

	dir := t.TempDir()
	// basic functionality test
	rw, err := newReadWriteSegment(dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	payload1 := []byte("entry-0")
	assert.NoError(t, rw.Append(0, payload1))
	payload2 := []byte("entry-1")
	assert.NoError(t, rw.Append(1, payload2))
	payload3 := []byte("entry-2")
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload1, err := rw.Read(0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload1, actualPayload1)
	actualPayload2, err := rw.Read(1)
	assert.NoError(t, err)
	assert.EqualValues(t, payload2, actualPayload2)
	actualPayload3, err := rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	// move commit offset to 1
	commitOffsetProvider.commitOffset = 1

	// inject payload size failure to trigger ErrOffsetOutOfBounds
	rwSegment := rw.(*readWriteSegment)
	fso := fileOffset(rwSegment.writingIdx, 0, 2)
	binary.BigEndian.PutUint32(rwSegment.txnMappedFile[fso+4:], 9999999)

	// close the segment
	rwSegment.Close()

	// recover the rw segment
	rw, err = newReadWriteSegment(dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, rw.LastOffset())

	// test functionality
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload3, err = rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	rw.Close()
}

func TestReadWriteSegment_BrokenCommittedData_ErrDataCorrupted(t *testing.T) {
	commitOffsetProvider := ConfigurableCommitOffsetProvider{}

	dir := t.TempDir()
	// basic functionality test
	rw, err := newReadWriteSegment(dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	payload1 := []byte("entry-0")
	assert.NoError(t, rw.Append(0, payload1))
	payload2 := []byte("entry-1")
	assert.NoError(t, rw.Append(1, payload2))
	payload3 := []byte("entry-2")
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload1, err := rw.Read(0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload1, actualPayload1)
	actualPayload2, err := rw.Read(1)
	assert.NoError(t, err)
	assert.EqualValues(t, payload2, actualPayload2)
	actualPayload3, err := rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	// move commit offset to 2
	commitOffsetProvider.commitOffset = 2

	// inject payload size failure to trigger ErrOffsetOutOfBounds
	rwSegment := rw.(*readWriteSegment)
	fso := fileOffset(rwSegment.writingIdx, 0, 2)
	binary.BigEndian.PutUint32(rwSegment.txnMappedFile[fso+4:], 9999999)

	// close the segment
	rwSegment.Close()

	// recover the rw segment
	_, err = newReadWriteSegment(dir, 0, 1024, 0, commitOffsetProvider)
	assert.ErrorIs(t, err, codec.ErrDataCorrupted)
}

func TestSegmentAppendShouldNotPanic(t *testing.T) {
	basePath := t.TempDir()
	rw, err := newReadWriteSegment(basePath, 0, 1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 51; i++ {
		err := rw.Append(i, fmt.Appendf(nil, "entry-%d", i))
		assert.NoError(t, err)
	}

	err = rw.Append(51, fmt.Appendf(nil, "entry-%d", 51))
	assert.ErrorIs(t, err, ErrSegmentFull)
}
