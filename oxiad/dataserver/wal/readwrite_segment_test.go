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
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/common/vfs"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"
)

func TestReadWriteSegment(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(vfs.OSFS{}, path, 0, 128*1024, 0, nil)
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
	rw, err = newReadWriteSegment(vfs.OSFS{}, path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, rw.BaseOffset())
	assert.EqualValues(t, 1, rw.LastOffset())

	data, _, _, err := rw.Read(0)
	assert.NoError(t, err)
	assert.Equal(t, "entry-0", string(data))

	data, _, _, err = rw.Read(1)
	assert.NoError(t, err)
	assert.Equal(t, "entry-1", string(data))

	assert.NoError(t, rw.Close())
}

func TestReadWriteSegment_NonZero(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(vfs.OSFS{}, path, 5, 128*1024, 0, nil)
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

	assert.ErrorIs(t, rw.Append(4, []byte("entry-4")), errs.ErrInvalidArgument)
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())

	assert.ErrorIs(t, rw.Append(8, []byte("entry-8")), errs.ErrInvalidArgument)
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())

	assert.NoError(t, rw.Close())

	// Re-open and recover the segment
	rw, err = newReadWriteSegment(vfs.OSFS{}, path, 5, 128*1024, 0, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, rw.BaseOffset())
	assert.EqualValues(t, 6, rw.LastOffset())
}

func TestReadWriteSegment_HasSpace(t *testing.T) {
	rw, err := newReadWriteSegment(vfs.OSFS{}, t.TempDir(), 0, 1024, 0, nil)
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

func (ConfigurableCommitOffsetProvider) FlushDatabase() error {
	return nil
}

func TestReadWriteSegment_BrokenUncommittedData_ErrOffsetOutOfBounds(t *testing.T) {
	commitOffsetProvider := ConfigurableCommitOffsetProvider{}

	dir := t.TempDir()
	// basic functionality test
	rw, err := newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	payload1 := []byte("entry-0")
	assert.NoError(t, rw.Append(0, payload1))
	payload2 := []byte("entry-1")
	assert.NoError(t, rw.Append(1, payload2))
	payload3 := []byte("entry-2")
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload1, _, _, err := rw.Read(0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload1, actualPayload1)
	actualPayload2, _, _, err := rw.Read(1)
	assert.NoError(t, err)
	assert.EqualValues(t, payload2, actualPayload2)
	actualPayload3, _, _, err := rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	// move commit offset to 1
	commitOffsetProvider.commitOffset = 1

	// inject payload size failure to trigger errs.ErrInvalidArgument
	rwSegment := rw.(*readWriteSegment)
	fso := fileOffset(rwSegment.writingIdx, 0, 2)
	binary.BigEndian.PutUint32(rwSegment.txnMappedFile[fso:], 9999999)

	// close the segment
	rwSegment.Close()

	// recover the rw segment
	rw, err = newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, rw.LastOffset())

	// test functionality
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload3, _, _, err = rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	rw.Close()
}

func TestReadWriteSegment_BrokenCommittedData_ErrOffsetOutOfBounds(t *testing.T) {
	commitOffsetProvider := ConfigurableCommitOffsetProvider{}

	dir := t.TempDir()
	// basic functionality test
	rw, err := newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	payload1 := []byte("entry-0")
	assert.NoError(t, rw.Append(0, payload1))
	payload2 := []byte("entry-1")
	assert.NoError(t, rw.Append(1, payload2))
	payload3 := []byte("entry-2")
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload1, _, _, err := rw.Read(0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload1, actualPayload1)
	actualPayload2, _, _, err := rw.Read(1)
	assert.NoError(t, err)
	assert.EqualValues(t, payload2, actualPayload2)
	actualPayload3, _, _, err := rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	// move commit offset to 2
	commitOffsetProvider.commitOffset = 2

	// inject payload size failure to trigger errs.ErrInvalidArgument
	rwSegment := rw.(*readWriteSegment)
	fso := fileOffset(rwSegment.writingIdx, 0, 2)
	binary.BigEndian.PutUint32(rwSegment.txnMappedFile[fso:], 9999999)

	// close the segment
	rwSegment.Close()

	// recover the rw segment
	_, err = newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
}

func TestReadWriteSegment_BrokenUncommittedData_ErrDataCorrupted(t *testing.T) {
	commitOffsetProvider := ConfigurableCommitOffsetProvider{}

	dir := t.TempDir()
	// basic functionality test
	rw, err := newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	payload1 := []byte("entry-0")
	assert.NoError(t, rw.Append(0, payload1))
	payload2 := []byte("entry-1")
	assert.NoError(t, rw.Append(1, payload2))
	payload3 := []byte("entry-2")
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload1, _, _, err := rw.Read(0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload1, actualPayload1)
	actualPayload2, _, _, err := rw.Read(1)
	assert.NoError(t, err)
	assert.EqualValues(t, payload2, actualPayload2)
	actualPayload3, _, _, err := rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	// move commit offset to 1
	commitOffsetProvider.commitOffset = 1

	// inject payload size failure to trigger errs.ErrInvalidArgument
	rwSegment := rw.(*readWriteSegment)
	fso := fileOffset(rwSegment.writingIdx, 0, 2)
	binary.BigEndian.PutUint32(rwSegment.txnMappedFile[fso+4:], 9999999)

	// close the segment
	rwSegment.Close()

	// recover the rw segment
	rw, err = newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, rw.LastOffset())

	// test functionality
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload3, _, _, err = rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	rw.Close()
}

func TestReadWriteSegment_BrokenCommittedData_ErrDataCorrupted(t *testing.T) {
	commitOffsetProvider := ConfigurableCommitOffsetProvider{}

	dir := t.TempDir()
	// basic functionality test
	rw, err := newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
	assert.NoError(t, err)
	payload1 := []byte("entry-0")
	assert.NoError(t, rw.Append(0, payload1))
	payload2 := []byte("entry-1")
	assert.NoError(t, rw.Append(1, payload2))
	payload3 := []byte("entry-2")
	assert.NoError(t, rw.Append(2, payload3))
	actualPayload1, _, _, err := rw.Read(0)
	assert.NoError(t, err)
	assert.EqualValues(t, payload1, actualPayload1)
	actualPayload2, _, _, err := rw.Read(1)
	assert.NoError(t, err)
	assert.EqualValues(t, payload2, actualPayload2)
	actualPayload3, _, _, err := rw.Read(2)
	assert.NoError(t, err)
	assert.EqualValues(t, payload3, actualPayload3)

	// move commit offset to 2
	commitOffsetProvider.commitOffset = 2

	// inject payload size failure to trigger errs.ErrInvalidArgument
	rwSegment := rw.(*readWriteSegment)
	fso := fileOffset(rwSegment.writingIdx, 0, 2)
	binary.BigEndian.PutUint32(rwSegment.txnMappedFile[fso+4:], 9999999)

	// close the segment
	rwSegment.Close()

	// recover the rw segment
	_, err = newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
	assert.ErrorIs(t, err, errs.ErrDataCorrupted)
}

// The recovery discards a corrupted record past the commit offset along with
// everything after it: the discarded records must not come back when new
// appends end right where one of them starts, as a new entry of the same size
// replacing the corrupted one does.
func TestReadWriteSegment_DiscardedDataNotResurrected(t *testing.T) {
	corruptPayload := func(s *readWriteSegment, recordOffset uint32) {
		s.txnMappedFile[recordOffset+s.c.codec.GetHeaderSize()] ^= 0xFF
	}
	corruptSize := func(s *readWriteSegment, recordOffset uint32) {
		binary.BigEndian.PutUint32(s.txnMappedFile[recordOffset:], 9999999)
	}

	for _, test := range []struct {
		name    string
		codecV1 bool
		corrupt func(s *readWriteSegment, recordOffset uint32)
	}{
		{"corrupted payload", false, corruptPayload},
		{"corrupted size", false, corruptSize},
		// The v1 codec has no crc: it only rejects a record overflowing the
		// segment, and takes it for the end of the segment
		{"v1 corrupted size", true, corruptSize},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if test.codecV1 {
				// An existing txn file with the v1 extension selects the v1 codec
				assert.NoError(t, os.WriteFile(segmentPath(dir, 0)+".txn", make([]byte, 1024), 0644))
			}
			commitOffsetProvider := ConfigurableCommitOffsetProvider{commitOffset: 2}

			rw, err := newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
			assert.NoError(t, err)
			for i := int64(0); i <= 5; i++ {
				assert.NoError(t, rw.Append(i, fmt.Appendf(nil, "old-entry-%d", i)))
			}

			// A torn write of the uncommitted entry 3, followed by intact entries
			rwSegment := rw.(*readWriteSegment)
			test.corrupt(rwSegment, fileOffset(rwSegment.writingIdx, 0, 3))
			assert.NoError(t, rw.Close())

			rw, err = newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, rw.LastOffset())
			rwSegment = rw.(*readWriteSegment)
			assert.Equal(t, -1, slices.IndexFunc(rwSegment.txnMappedFile[rwSegment.currentFileOffset:], isNonZero))

			assert.NoError(t, rw.Append(3, []byte("new-entry-3")))
			assert.NoError(t, rw.Close())

			rw, err = newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, commitOffsetProvider)
			assert.NoError(t, err)
			assert.EqualValues(t, 3, rw.LastOffset())
			payload, _, _, err := rw.Read(3)
			assert.NoError(t, err)
			assert.Equal(t, "new-entry-3", string(payload))
			assert.NoError(t, rw.Close())
		})
	}
}

func TestSegmentAppendShouldNotPanic(t *testing.T) {
	basePath := t.TempDir()
	rw, err := newReadWriteSegment(vfs.OSFS{}, basePath, 0, 1024, 0, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 51; i++ {
		err := rw.Append(i, fmt.Appendf(nil, "entry-%d", i))
		assert.NoError(t, err)
	}

	err = rw.Append(51, fmt.Appendf(nil, "entry-%d", 51))
	assert.ErrorIs(t, err, errs.ErrSegmentFull)
}

// Appends, reads and flushes run concurrently by design: the flusher (WAL sync
// goroutine) must not block appends or tailing reads for the duration of the
// msync, and the data must stay consistent throughout.
func TestReadWriteSegment_ConcurrentAppendReadFlush(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(vfs.OSFS{}, path, 0, 10*1024*1024, 0, nil)
	assert.NoError(t, err)

	const entries = 5_000
	done := make(chan struct{})
	flusherDone := make(chan struct{})
	readerDone := make(chan struct{})

	go func() {
		defer close(flusherDone)
		for {
			select {
			case <-done:
				return
			default:
				assert.NoError(t, rw.Flush())
				// Yield between iterations: keeps the loop from pegging a core
				// while still maximizing the append/read/flush interleaving
				runtime.Gosched()
			}
		}
	}()

	go func() {
		defer close(readerDone)
		for {
			select {
			case <-done:
				return
			default:
				last := rw.LastOffset()
				if last >= 0 {
					payload, _, _, err := rw.Read(last)
					assert.NoError(t, err)
					assert.Equal(t, fmt.Sprintf("entry-%d", last), string(payload))
				}
				runtime.Gosched()
			}
		}
	}()

	for i := 0; i < entries; i++ {
		assert.NoError(t, rw.Append(int64(i), []byte(fmt.Sprintf("entry-%d", i))))
	}

	close(done)
	<-flusherDone
	<-readerDone

	for i := 0; i < entries; i++ {
		payload, _, _, err := rw.Read(int64(i))
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(payload))
	}

	assert.NoError(t, rw.Close())
}

// Reopening a segment for writes removes the index file left by its previous
// incarnation: the index goes stale with the first new append, and a crash
// before the Close that rewrites it would leave it next to newer txn data.
func TestReadWriteSegment_ReopenRemovesStaleIndex(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(vfs.OSFS{}, path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	idxPath := rw.(*readWriteSegment).c.idxPath
	assert.NoError(t, rw.Append(0, []byte("entry-0")))
	assert.NoError(t, rw.Close())

	_, err = os.Stat(idxPath)
	assert.NoError(t, err)

	rw, err = newReadWriteSegment(vfs.OSFS{}, path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	_, err = os.Stat(idxPath)
	assert.ErrorIs(t, err, os.ErrNotExist)

	assert.NoError(t, rw.Append(1, []byte("entry-1")))
	assert.NoError(t, rw.Close())

	// The close wrote a fresh index for the appended entries
	ro, err := newReadOnlySegment(vfs.OSFS{}, path, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, ro.LastOffset())
	assert.NoError(t, ro.Close())
}

// Closing the segment must wait for an in-flight msync: the mapping cannot be
// unmapped under it.
func TestReadWriteSegment_CloseWithConcurrentFlush(t *testing.T) {
	path := t.TempDir()

	rw, err := newReadWriteSegment(vfs.OSFS{}, path, 0, 128*1024, 0, nil)
	assert.NoError(t, err)
	assert.NoError(t, rw.Append(0, []byte("entry-0")))

	flusherDone := make(chan struct{})
	go func() {
		defer close(flusherDone)
		for i := 0; i < 1_000; i++ {
			// After Close, Flush must keep returning nil instead of msyncing
			// an unmapped region
			assert.NoError(t, rw.Flush())
		}
	}()

	assert.NoError(t, rw.Close())
	<-flusherDone
}

// Truncate must move the last crc back to the record at the truncation point:
// the next append chains from it, not from a truncated record. The v1 codec
// has no crc, and keeps reporting zero.
func TestReadWriteSegment_TruncateResetsLastCrc(t *testing.T) {
	for _, test := range []struct {
		name    string
		codecV1 bool
	}{
		{"v2", false},
		{"v1", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if test.codecV1 {
				// An existing txn file with the v1 extension selects the v1 codec
				assert.NoError(t, os.WriteFile(segmentPath(dir, 0)+".txn", make([]byte, 1024), 0644))
			}

			rw, err := newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, nil)
			assert.NoError(t, err)
			for i := int64(0); i <= 5; i++ {
				assert.NoError(t, rw.Append(i, fmt.Appendf(nil, "entry-%d", i)))
			}
			_, _, crc2, err := rw.Read(2)
			assert.NoError(t, err)
			if test.codecV1 {
				assert.Zero(t, crc2)
			} else {
				assert.NotZero(t, crc2)
			}

			assert.NoError(t, rw.Truncate(2))
			assert.EqualValues(t, 2, rw.LastOffset())
			assert.Equal(t, crc2, rw.LastCrc())
			assert.NoError(t, rw.Append(3, []byte("new-entry-3")))
			assert.NoError(t, rw.Close())

			rw, err = newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, nil)
			assert.NoError(t, err)
			assert.EqualValues(t, 3, rw.LastOffset())
			payload, previousCrc, _, err := rw.Read(3)
			assert.NoError(t, err)
			assert.Equal(t, "new-entry-3", string(payload))
			assert.Equal(t, crc2, previousCrc)
			assert.NoError(t, rw.Close())
		})
	}
}

// Read copies the record without holding the segment mutex: the appends
// proceed while the copy is in flight, while Close waits for it before
// unmapping the segment.
func TestReadWriteSegmentReadConcurrentWithAppendAndClose(t *testing.T) {
	rw, err := newReadWriteSegment(vfs.OSFS{}, t.TempDir(), 0, 128*1024, 0, nil)
	require.NoError(t, err)
	require.NoError(t, rw.Append(0, []byte("entry-0")))
	hook, reading, release := blockFirstCall()
	defer release()
	hookRecordReads(rw, hook)

	readDone := make(chan error, 1)
	go func() {
		payload, _, _, err := rw.Read(0)
		if err == nil && string(payload) != "entry-0" {
			err = fmt.Errorf("unexpected payload %q", payload)
		}
		readDone <- err
	}()
	<-reading

	appendDone := make(chan error, 1)
	go func() { appendDone <- rw.Append(1, []byte("entry-1")) }()
	select {
	case err := <-appendDone:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the append waited for the read")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- rw.Close() }()
	assert.Never(t, func() bool { return len(closeDone) > 0 }, 100*time.Millisecond, 10*time.Millisecond,
		"the segment got closed under the read")

	release()
	assert.NoError(t, <-readDone)
	assert.NoError(t, <-closeDone)

	_, _, _, err = rw.Read(1)
	assert.ErrorIs(t, err, errs.ErrAlreadyClosed)
}

func TestReadWriteSegmentNewFailsWhenWalDirectoryCannotBeCreated(t *testing.T) {
	parent := readOnlyDir(t)

	_, err := newReadWriteSegment(vfs.OSFS{}, filepath.Join(parent, "wal"), 0, 1024, 0, nil)
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.ErrorContains(t, err, "failed to create wal directory")
}

func TestReadWriteSegmentNewFailsWhenSegmentFileCannotBeCreated(t *testing.T) {
	dir := readOnlyDir(t)

	_, err := newReadWriteSegment(vfs.OSFS{}, dir, 0, 1024, 0, nil)
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.ErrorContains(t, err, "failed to open segment file")
}

func TestReadWriteSegmentNewFailsOnFilesystemErrors(t *testing.T) {
	// The stat that picks the codec, the extension of the new file to the
	// segment size, and its mapping
	for _, op := range []fsOp{opStat, opSeek, opWrite, opMap} {
		t.Run(string(op), func(t *testing.T) {
			dir := t.TempDir()
			fs := newFaultFS()
			fs.failNth(op, txnPath(dir, 0), 1)

			_, err := newReadWriteSegment(fs, dir, 0, 1024, 0, nil)
			assert.ErrorIs(t, err, errInjected)
		})
	}
}

func TestReadWriteSegmentNewFailsWhenSegmentFileCannotBeStatted(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	// The first stat picks the codec
	fs.failNth(opStat, txnPath(dir, 0), 2)

	_, err := newReadWriteSegment(fs, dir, 0, 1024, 0, nil)
	assert.ErrorIs(t, err, errInjected)
	assert.ErrorContains(t, err, "failed to stat segment file")
}

func TestReadWriteSegmentNewFailsWhenDiscardedTailCannotBeZeroed(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	commitOffsetProvider := ConfigurableCommitOffsetProvider{commitOffset: 1}
	rw, err := newReadWriteSegment(fs, dir, 0, 1024, 0, commitOffsetProvider)
	require.NoError(t, err)
	for i := int64(0); i <= 3; i++ {
		require.NoError(t, rw.Append(i, fmt.Appendf(nil, "entry-%d", i)))
	}
	// A torn write of the uncommitted entry 2, followed by the intact entry 3
	rwSegment := rw.(*readWriteSegment)
	rwSegment.txnMappedFile[fileOffset(rwSegment.writingIdx, 0, 2)+rwSegment.c.codec.GetHeaderSize()] ^= 0xFF
	require.NoError(t, rw.Close())
	fs.failNth(opFlush, txnPath(dir, 0), 1)

	_, err = newReadWriteSegment(fs, dir, 0, 1024, 0, commitOffsetProvider)
	assert.ErrorIs(t, err, errInjected)
	assert.ErrorContains(t, err, "failed to zero the discarded tail")
}

func TestReadWriteSegmentReadReportsCorruptedRecord(t *testing.T) {
	rw, err := newReadWriteSegment(vfs.OSFS{}, t.TempDir(), 0, 1024, 0, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, rw.Close()) })
	require.NoError(t, rw.Append(0, []byte("entry-0")))

	rwSegment := rw.(*readWriteSegment)
	payloadOffset := fileOffset(rwSegment.writingIdx, 0, 0) + rwSegment.c.codec.GetHeaderSize()
	rwSegment.txnMappedFile[payloadOffset] ^= 0xff

	_, _, _, err = rw.Read(0)
	assert.ErrorIs(t, err, errs.ErrDataCorrupted)
	assert.ErrorContains(t, err, "entryOffset: 0")
}

func TestReadWriteSegmentReadReportsEmptyRecord(t *testing.T) {
	rw, err := newReadWriteSegment(vfs.OSFS{}, t.TempDir(), 0, 1024, 0, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, rw.Close()) })
	require.NoError(t, rw.Append(0, []byte("entry-0")))

	rwSegment := rw.(*readWriteSegment)
	binary.BigEndian.PutUint32(rwSegment.txnMappedFile[fileOffset(rwSegment.writingIdx, 0, 0):], 0)

	_, _, _, err = rw.Read(0)
	assert.ErrorIs(t, err, errs.ErrEmptyPayload)
}

func TestReadWriteSegmentAppendRejectsEmptyPayload(t *testing.T) {
	rw, err := newReadWriteSegment(vfs.OSFS{}, t.TempDir(), 0, 1024, 0, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, rw.Close()) })

	assert.ErrorIs(t, rw.Append(0, nil), errs.ErrEmptyPayload)
	assert.EqualValues(t, InvalidOffset, rw.LastOffset())
}

func TestReadWriteSegmentSyncFileIfNeededSkipsClosedSegment(t *testing.T) {
	rw, err := newReadWriteSegment(vfs.OSFS{}, t.TempDir(), 0, 1024, 0, nil)
	require.NoError(t, err)
	rwSegment := rw.(*readWriteSegment)
	require.True(t, rwSegment.pendingFileSync.Load())
	require.NoError(t, rw.Close())

	assert.NoError(t, rw.SyncFileIfNeeded())
	assert.True(t, rwSegment.pendingFileSync.Load())
}

func TestReadWriteSegmentSyncFileIfNeededReportsSyncFailure(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	rw, err := newReadWriteSegment(fs, dir, 0, 1024, 0, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, rw.Close()) })
	fs.failNth(opSync, txnPath(dir, 0), 1)

	assert.ErrorIs(t, rw.SyncFileIfNeeded(), errInjected)
	// The next call syncs the file again
	assert.True(t, rw.(*readWriteSegment).pendingFileSync.Load())
	assert.NoError(t, rw.SyncFileIfNeeded())
	assert.False(t, rw.(*readWriteSegment).pendingFileSync.Load())
}

func TestReadWriteSegmentCloseReportsIndexWriteFailure(t *testing.T) {
	for _, c := range codec.SupportedCodecs {
		t.Run(c.GetTxnExtension(), func(t *testing.T) {
			dir := t.TempDir()
			// An existing txn file with the extension of a codec selects it
			require.NoError(t, os.WriteFile(segmentPath(dir, 0)+c.GetTxnExtension(), make([]byte, 1024), 0o644))
			fs := newFaultFS()
			rw, err := newReadWriteSegment(fs, dir, 0, 1024, 0, nil)
			require.NoError(t, err)
			require.NoError(t, rw.Append(0, []byte("entry-0")))
			idxPath := rw.(*readWriteSegment).c.idxPath
			fs.failNth(opWrite, idxPath, 1)

			assert.ErrorIs(t, rw.Close(), errInjected)
		})
	}
}

func TestReadWriteSegmentOpenTimestampIsCurrentTime(t *testing.T) {
	rw, err := newReadWriteSegment(vfs.OSFS{}, t.TempDir(), 0, 1024, 0, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, rw.Close()) })

	before := time.Now()
	ts := rw.OpenTimestamp()
	assert.False(t, ts.Before(before))
	assert.False(t, ts.After(time.Now()))
}

func TestReadWriteSegmentTruncateRejectsOffsetOutsideSegment(t *testing.T) {
	rw, err := newReadWriteSegment(vfs.OSFS{}, t.TempDir(), 10, 1024, 0, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, rw.Close()) })
	require.NoError(t, rw.Append(10, []byte("entry-10")))

	assert.ErrorIs(t, rw.Truncate(9), errs.ErrInvalidArgument)
	assert.ErrorIs(t, rw.Truncate(11), errs.ErrInvalidArgument)
	assert.EqualValues(t, 10, rw.LastOffset())
}

func TestReadWriteSegmentTruncateFailsOnCorruptedRecord(t *testing.T) {
	rw, err := newReadWriteSegment(vfs.OSFS{}, t.TempDir(), 0, 1024, 0, nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, rw.Close()) })
	require.NoError(t, rw.Append(0, []byte("entry-0")))
	require.NoError(t, rw.Append(1, []byte("entry-1")))

	rwSegment := rw.(*readWriteSegment)
	payloadOffset := fileOffset(rwSegment.writingIdx, 0, 0) + rwSegment.c.codec.GetHeaderSize()
	rwSegment.txnMappedFile[payloadOffset] ^= 0xff

	assert.ErrorIs(t, rw.Truncate(0), errs.ErrDataCorrupted)
	assert.EqualValues(t, 1, rw.LastOffset())
}

// hookedCodec runs a hook before each record read.
type hookedCodec struct {
	codec.Codec
	readRecordHook func()
}

func (c *hookedCodec) ReadRecordWithValidation(buf []byte, startFileOffset uint32) (
	payload []byte, previousCrc uint32, payloadCrc uint32, err error) {
	if c.readRecordHook != nil {
		c.readRecordHook()
	}
	return c.Codec.ReadRecordWithValidation(buf, startFileOffset)
}

// hookRecordReads runs the hook in the reads of the segment, right before they
// copy the record.
func hookRecordReads(segment ReadWriteSegment, hook func()) {
	rw := segment.(*readWriteSegment)
	rw.c.codec = &hookedCodec{Codec: rw.c.codec, readRecordHook: hook}
}

// blockFirstCall returns a hook that stops its first call until release, and
// closes blocked once that call stops. The later calls go through.
func blockFirstCall() (hook func(), blocked <-chan struct{}, release func()) {
	blockedCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var called atomic.Bool
	var releaseOnce sync.Once
	hook = func() {
		if called.CompareAndSwap(false, true) {
			close(blockedCh)
			<-releaseCh
		}
	}
	return hook, blockedCh, func() { releaseOnce.Do(func() { close(releaseCh) }) }
}

// readOnlyDir returns a directory in which no file can be created, and makes
// it writable again for the cleanup of the test.
func readOnlyDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(dir, 0o700)) })
	return dir
}
