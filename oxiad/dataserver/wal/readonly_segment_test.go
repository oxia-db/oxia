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
	"fmt"
	"os"
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/common/vfs"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"
)

func TestReadOnlySegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(vfs.OSFS{}, path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	assert.NoError(t, rw.Close())

	ro, err := newReadOnlySegment(vfs.OSFS{}, path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, _, _, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	data, _, _, err := ro.Read(100)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)

	data, _, _, err = ro.Read(-1)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)

	assert.NoError(t, ro.Close())
}

// An empty index file must trigger an index rebuild, not be trusted (it used
// to panic with a slice out of bounds).
func TestRO_auto_recover_empty_index(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(vfs.OSFS{}, path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	rwSegment := rw.(*readWriteSegment)
	assert.NoError(t, rw.Close())

	// Replace the index with a valid empty one, as written by the close of a
	// previous, then-empty incarnation of the segment
	assert.NoError(t, os.Remove(rwSegment.c.idxPath))
	assert.NoError(t, rwSegment.c.codec.WriteIndex(vfs.OSFS{}, rwSegment.c.idxPath, nil))

	ro, err := newReadOnlySegment(vfs.OSFS{}, path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, _, _, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	assert.NoError(t, ro.Close())
}

// A segment whose txn file holds no entries cannot be opened for reads: it
// must fail with ErrEmptySegment (it used to panic with a slice out of
// bounds), regardless of whether an index file is present.
func TestRO_EmptySegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(vfs.OSFS{}, path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	rwSegment := rw.(*readWriteSegment)
	assert.NoError(t, rw.Close())

	_, err = os.Stat(rwSegment.c.idxPath)
	assert.ErrorIs(t, err, os.ErrNotExist)

	ro, err := newReadOnlySegment(vfs.OSFS{}, path, 0)
	assert.Nil(t, ro)
	assert.ErrorIs(t, err, ErrEmptySegment)
}

func TestRO_auto_recover_broken_index(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(vfs.OSFS{}, path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		assert.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	rwSegment := rw.(*readWriteSegment)
	assert.NoError(t, rw.Close())

	idxPath := rwSegment.c.idxPath
	// inject wrong data
	file, err := os.OpenFile(idxPath, os.O_RDWR, 0644)
	assert.NoError(t, err)
	defer file.Close()
	faultData, err := uuid.New().MarshalBinary()
	assert.NoError(t, err)
	_, err = file.WriteAt(faultData, 0)
	assert.NoError(t, err)

	ro, err := newReadOnlySegment(vfs.OSFS{}, path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, ro.BaseOffset())
	assert.EqualValues(t, 9, ro.LastOffset())

	for i := int64(0); i < 10; i++ {
		data, _, _, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}

	data, _, _, err := ro.Read(100)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)

	data, _, _, err = ro.Read(-1)
	assert.Nil(t, data)
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)

	assert.NoError(t, ro.Close())
}

func TestReadOnlySegmentNewFailsWhenTxnFileIsMissing(t *testing.T) {
	_, err := newReadOnlySegment(vfs.OSFS{}, t.TempDir(), 0)
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.ErrorContains(t, err, "failed to open segment txn file")
}

func TestReadOnlySegmentNewFailsWhenTxnFileCannotBeStatted(t *testing.T) {
	dir := t.TempDir()
	c, _ := writeSegment(t, dir, 3)
	fs := newFaultFS()
	fs.failNth(opStat, c.txnPath, 1)

	_, err := newReadOnlySegment(fs, dir, 0)
	assert.ErrorIs(t, err, errInjected)
}

func TestReadOnlySegmentNewFailsWhenTxnFileIsEmpty(t *testing.T) {
	dir := t.TempDir()
	c, err := newSegmentConfig(vfs.OSFS{}, dir, 0)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(c.txnPath, nil, 0o644))

	_, err = newReadOnlySegment(vfs.OSFS{}, dir, 0)
	assert.ErrorContains(t, err, "failed to map segment txn file")
}

func TestReadOnlySegmentNewFailsWhenIndexIsUnreadable(t *testing.T) {
	dir := t.TempDir()
	c, _ := writeSegment(t, dir, 3)
	require.NoError(t, os.Remove(c.idxPath))
	require.NoError(t, os.Mkdir(c.idxPath, 0o755))

	_, err := newReadOnlySegment(vfs.OSFS{}, dir, 0)
	assert.ErrorContains(t, err, "failed to decode segment index file")
}

func TestReadOnlySegmentNewFailsWhenIndexCannotBeClosed(t *testing.T) {
	for _, cd := range codec.SupportedCodecs {
		t.Run(cd.GetIdxExtension(), func(t *testing.T) {
			dir := t.TempDir()
			// An existing txn file with the extension of a codec selects it
			require.NoError(t, os.WriteFile(segmentPath(dir, 0)+cd.GetTxnExtension(), make([]byte, 1024), 0o644))
			c, _ := writeSegment(t, dir, 3)
			fs := newFaultFS()
			fs.failNth(opClose, c.idxPath, 1)

			_, err := newReadOnlySegment(fs, dir, 0)
			assert.ErrorIs(t, err, errInjected)
			assert.ErrorContains(t, err, "failed to close segment index file")
		})
	}
}

func TestReadOnlySegmentNewFailsWhenIndexRebuildFindsCorruption(t *testing.T) {
	dir := t.TempDir()
	c, offsets := writeSegment(t, dir, 3)
	require.NoError(t, os.Remove(c.idxPath))
	corruptPayload(t, c, offsets[1])

	_, err := newReadOnlySegment(vfs.OSFS{}, dir, 0)
	assert.ErrorIs(t, err, errs.ErrDataCorrupted)
	assert.ErrorContains(t, err, "failed to rebuild segment index file")
}

// The index rebuilt from the txn file serves the reads even when it cannot be
// written back: the next open rebuilds it again.
func TestReadOnlySegmentNewServesRebuiltIndexWhenItCannotBeWritten(t *testing.T) {
	dir := t.TempDir()
	c, _ := writeSegment(t, dir, 3)
	require.NoError(t, os.Remove(c.idxPath))
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(dir, 0o700)) })

	ro, err := newReadOnlySegment(vfs.OSFS{}, dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ro.Close()) })

	assert.NoFileExists(t, c.idxPath)
	assert.EqualValues(t, 2, ro.LastOffset())
	for i := int64(0); i < 3; i++ {
		data, _, _, err := ro.Read(i)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(data))
	}
}

func TestReadOnlySegmentNewFailsWhenLastRecordIsCorrupted(t *testing.T) {
	dir := t.TempDir()
	c, offsets := writeSegment(t, dir, 3)
	corruptPayload(t, c, offsets[2])

	_, err := newReadOnlySegment(vfs.OSFS{}, dir, 0)
	assert.ErrorIs(t, err, errs.ErrDataCorrupted)
}

func TestReadOnlySegmentReadReportsCorruptedRecord(t *testing.T) {
	dir := t.TempDir()
	c, offsets := writeSegment(t, dir, 3)
	corruptPayload(t, c, offsets[1])

	ro, err := newReadOnlySegment(vfs.OSFS{}, dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ro.Close()) })

	_, _, _, err = ro.Read(1)
	assert.ErrorIs(t, err, errs.ErrDataCorrupted)
	assert.ErrorContains(t, err, "entryOffset: 1")
}

func TestReadOnlySegmentReadReportsEmptyRecord(t *testing.T) {
	dir := t.TempDir()
	c, offsets := writeSegment(t, dir, 3)
	// Point the index entry of offset 1 to the zeroed tail of the txn file
	idx := make([]byte, 0, 12)
	for _, o := range []uint32{offsets[0], 1000, offsets[2]} {
		idx = binary.BigEndian.AppendUint32(idx, o)
	}
	require.NoError(t, c.codec.WriteIndex(vfs.OSFS{}, c.idxPath, idx))

	ro, err := newReadOnlySegment(vfs.OSFS{}, dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ro.Close()) })

	_, _, _, err = ro.Read(1)
	assert.ErrorIs(t, err, codec.ErrEmptyPayload)
}

// writeSegment writes a closed segment of 1KB at base offset 0 holding the
// entries "entry-<i>", and returns its config and the file offsets of the
// records.
func writeSegment(t *testing.T, dir string, entries int) (*segmentConfig, []uint32) {
	t.Helper()
	rw, err := newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, nil)
	require.NoError(t, err)
	for i := int64(0); i < int64(entries); i++ {
		require.NoError(t, rw.Append(i, []byte(fmt.Sprintf("entry-%d", i))))
	}
	rwSegment := rw.(*readWriteSegment)
	offsets := make([]uint32, entries)
	for i := range offsets {
		offsets[i] = fileOffset(rwSegment.writingIdx, 0, int64(i))
	}
	require.NoError(t, rw.Close())
	return rwSegment.c, offsets
}

// corruptPayload flips the first payload byte of the record at the given file
// offset of the txn file.
func corruptPayload(t *testing.T, c *segmentConfig, recordOffset uint32) {
	t.Helper()
	f, err := os.OpenFile(c.txnPath, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	b := make([]byte, 1)
	payloadOffset := int64(recordOffset + c.codec.GetHeaderSize())
	_, err = f.ReadAt(b, payloadOffset)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b, payloadOffset)
	require.NoError(t, err)
}
