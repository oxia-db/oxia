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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/vfs"
)

func TestReadOnlySegmentsGroupTrimSegments(t *testing.T) {
	t.Run("when txnFile is not exists", func(t *testing.T) {
		walFactory := NewWalFactory(&FactoryOptions{
			BaseWalDir:  t.TempDir(),
			Retention:   1 * time.Hour,
			SegmentSize: 128,
			SyncData:    true,
		})
		w, err := walFactory.NewWal("test", 1, nil)
		assert.NoError(t, err)
		for i := int64(0); i < 1000; i++ {
			assert.NoError(t, w.Append(&proto.LogEntry{
				Term:   1,
				Offset: i,
				Value:  fmt.Appendf(nil, "test-%d", i),
			}))
		}
		walBasePath := w.(*wal).walPath
		readOnlySegments, err := newReadOnlySegmentsGroup(vfs.OSFS{}, walBasePath)
		assert.NoError(t, err)

		// Ensure newReadOnlySegment will return an NotExists error
		err = os.Remove(filepath.Join(walBasePath, "0.txnx"))
		assert.NoError(t, err)

		err = readOnlySegments.TrimSegments(6, w.(*wal).currentSegment.BaseOffset(), func() error { return nil })
		assert.NoError(t, err)
	})
}

// The trim of a segment that a reader holds removes its files, and releases
// only the cache reference: the reader keeps reading the records until it
// closes its own.
func TestReadOnlySegmentsGroupTrimSegmentsKeepsHeldSegmentReadable(t *testing.T) {
	walFactory := NewWalFactory(&FactoryOptions{
		BaseWalDir:  t.TempDir(),
		Retention:   1 * time.Hour,
		SegmentSize: 128,
	})
	w, err := walFactory.NewWal("test", 1, nil)
	assert.NoError(t, err)
	for i := int64(0); i < 20; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: i,
			Value:  fmt.Appendf(nil, "test-%d", i),
		}))
	}
	walBasePath := w.(*wal).walPath
	assert.NoError(t, w.Close())
	assert.NoError(t, walFactory.Close())

	segments, err := listAllSegments(vfs.OSFS{}, walBasePath)
	assert.NoError(t, err)
	readOnlySegments, err := newReadOnlySegmentsGroup(vfs.OSFS{}, walBasePath)
	assert.NoError(t, err)
	c, err := newSegmentConfig(vfs.OSFS{}, walBasePath, 0)
	assert.NoError(t, err)

	rc, err := readOnlySegments.Get(0)
	assert.NoError(t, err)
	segment := rc.Get()
	err = readOnlySegments.TrimSegments(segment.LastOffset()+1, segments[len(segments)-1], func() error { return nil })
	assert.NoError(t, err)
	assert.NoFileExists(t, c.txnPath)
	assert.NoFileExists(t, c.idxPath)

	for offset := int64(0); offset <= segment.LastOffset(); offset++ {
		payload, _, _, err := segment.Read(offset)
		if !assert.NoError(t, err, "offset %d", offset) {
			break
		}
		entry := &proto.LogEntry{}
		assert.NoError(t, entry.UnmarshalVT(payload))
		assert.Equal(t, fmt.Sprintf("test-%d", offset), string(entry.Value))
	}

	// The last reference closes the segment, exactly once
	assert.NoError(t, rc.Close())
	assert.EqualValues(t, 0, rc.RefCnt())
	assert.NoError(t, readOnlySegments.Close())
}

// A trim offset at the base offset of the current segment deletes every
// segment of the group, while the current segment, once a rollover adds it to
// the group, is kept.
func TestReadOnlySegmentsGroupTrimSegmentsAtCurrentSegmentDeletesAllSegments(t *testing.T) {
	walBasePath, segments := writeClosedWal(t)
	require.GreaterOrEqual(t, len(segments), 3)
	current := segments[len(segments)-1]
	group, err := newReadOnlySegmentsGroup(vfs.OSFS{}, walBasePath)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, group.Close()) })

	require.NoError(t, group.TrimSegments(current, current, func() error { return nil }))
	for _, s := range segments[:len(segments)-1] {
		assert.NoFileExists(t, txnPath(walBasePath, s))
	}
	_, err = group.Get(current - 1)
	assert.ErrorIs(t, err, errs.ErrOffsetOverflow)

	group.AddedNewSegment(current)
	require.NoError(t, group.TrimSegments(current+1, current, func() error { return nil }))
	assert.FileExists(t, txnPath(walBasePath, current))
}

func TestReadOnlySegmentsGroupNewFailsWhenPathIsFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "wal")
	require.NoError(t, os.WriteFile(p, nil, 0o644))

	_, err := newReadOnlySegmentsGroup(vfs.OSFS{}, p)
	assert.ErrorContains(t, err, "failed to list files in wal directory")
}

// A segment evicted from the cache as soon as Get opened it gets closed by
// GetLastCrc, which reports the failure.
func TestReadOnlySegmentsGroupGetLastCrcFailsWhenEvictedSegmentCannotBeClosed(t *testing.T) {
	fs := newFaultFS()
	w := newFaultWal(t, fs, false)
	appendEntries(t, w, 1, 0, 59, "v")
	require.NoError(t, w.Close())
	segments, err := listAllSegments(fs, w.walPath)
	require.NoError(t, err)
	// The last segment is the read-write one
	require.Greater(t, len(segments), maxReadOnlySegmentsInCacheCount+1)
	group, err := newReadOnlySegmentsGroup(fs, w.walPath)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, group.Close()) })
	for _, s := range segments[1 : maxReadOnlySegmentsInCacheCount+1] {
		rc, err := group.Get(s)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
	}
	fs.failNth(opClose, txnPath(w.walPath, segments[0]), 1)

	_, err = group.GetLastCrc(segments[0])
	assert.ErrorIs(t, err, errInjected)
}

func TestReadOnlySegmentsGroupGetFailsBeforeFirstSegment(t *testing.T) {
	walBasePath, _ := writeClosedWal(t)
	group, err := newReadOnlySegmentsGroup(vfs.OSFS{}, walBasePath)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, group.Close()) })

	_, err = group.Get(-1)
	assert.ErrorIs(t, err, errs.ErrOffsetOverflow)
}

func TestReadOnlySegmentsGroupGetReleasesSegmentWhenEvictionFails(t *testing.T) {
	walBasePath, segments := writeClosedWal(t)
	require.GreaterOrEqual(t, len(segments), 2)
	g, err := newReadOnlySegmentsGroup(vfs.OSFS{}, walBasePath)
	require.NoError(t, err)
	group := g.(*readOnlySegmentsGroup)

	rc, err := group.Get(segments[0])
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	closeErr := errors.New("close failure")
	group.openSegments.Put(segments[0], object.NewRefCount[ReadOnlySegment](
		&failingCloseSegment{ReadOnlySegment: rc.Get(), closeErr: closeErr}))

	res, err := group.Get(segments[1])
	assert.ErrorIs(t, err, closeErr)
	assert.Nil(t, res)
	cached, ok := group.openSegments.Get(segments[1])
	require.True(t, ok)
	assert.EqualValues(t, 1, cached.RefCnt())

	require.NoError(t, group.Close())
	assert.True(t, cached.Get().(*readOnlySegment).closed)
}

func TestReadOnlySegmentsGroupGetEvictsExpiredSegments(t *testing.T) {
	walBasePath, segments := writeClosedWal(t)
	require.GreaterOrEqual(t, len(segments), 3)
	group, err := newReadOnlySegmentsGroup(vfs.OSFS{}, walBasePath)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, group.Close()) })

	rc, err := group.Get(segments[0])
	require.NoError(t, err)
	rc.Get().(*readOnlySegment).openTimestamp = time.Now().Add(-2 * maxReadOnlySegmentsInCacheTime)
	require.NoError(t, rc.Close())

	rc, err = group.Get(segments[1])
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	openSegments := group.(*readOnlySegmentsGroup).openSegments
	assert.Equal(t, []int64{segments[1]}, openSegments.Keys())
}

func TestReadOnlySegmentsGroupTrimSegmentsReportsUnremovableFiles(t *testing.T) {
	walBasePath, segments := writeClosedWal(t)
	require.GreaterOrEqual(t, len(segments), 3)
	c, err := newSegmentConfig(vfs.OSFS{}, walBasePath, segments[0])
	require.NoError(t, err)
	// A non-empty directory in place of the index file cannot be removed
	require.NoError(t, os.Remove(c.idxPath))
	require.NoError(t, os.MkdirAll(filepath.Join(c.idxPath, "child"), 0o755))

	group, err := newReadOnlySegmentsGroup(vfs.OSFS{}, walBasePath)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, group.Close()) })

	err = group.TrimSegments(segments[1], segments[len(segments)-1], func() error { return nil })
	assert.Error(t, err)
	assert.NoFileExists(t, c.txnPath)
	_, err = group.Get(segments[0])
	assert.ErrorIs(t, err, errs.ErrOffsetOverflow)
}

// A segment that cannot be statted is left on the disk: the trimming goes on
// with the next ones.
func TestReadOnlySegmentsGroupTrimSegmentsReportsUnstattableSegment(t *testing.T) {
	walBasePath, segments := writeClosedWal(t)
	require.GreaterOrEqual(t, len(segments), 3)
	fs := newFaultFS()
	group, err := newReadOnlySegmentsGroup(fs, walBasePath)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, group.Close()) })
	fs.failNth(opStat, txnPath(walBasePath, segments[0]), 1)

	err = group.TrimSegments(segments[2], segments[len(segments)-1], func() error { return nil })
	assert.ErrorIs(t, err, errInjected)
	assert.FileExists(t, txnPath(walBasePath, segments[0]))
	assert.NoFileExists(t, txnPath(walBasePath, segments[1]))
}

func TestReadOnlySegmentsGroupPollHighestSegmentFailsWhenTxnFileIsMissing(t *testing.T) {
	walBasePath, segments := writeClosedWal(t)
	require.GreaterOrEqual(t, len(segments), 2)
	highest := segments[len(segments)-2]
	c, err := newSegmentConfig(vfs.OSFS{}, walBasePath, highest)
	require.NoError(t, err)

	group, err := newReadOnlySegmentsGroup(vfs.OSFS{}, walBasePath)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, group.Close()) })
	require.NoError(t, os.Remove(c.txnPath))

	_, err = group.PollHighestSegment()
	assert.ErrorIs(t, err, os.ErrNotExist)
}

// writeClosedWal writes a closed wal of several segments, and returns its path
// and the base offsets of its segments.
func writeClosedWal(t *testing.T) (string, []int64) {
	t.Helper()
	walFactory := NewWalFactory(&FactoryOptions{
		BaseWalDir:  t.TempDir(),
		Retention:   1 * time.Hour,
		SegmentSize: 128,
	})
	w, err := walFactory.NewWal("test", 1, nil)
	require.NoError(t, err)
	for i := int64(0); i < 20; i++ {
		require.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: i,
			Value:  fmt.Appendf(nil, "test-%d", i),
		}))
	}
	walBasePath := w.(*wal).walPath
	require.NoError(t, w.Close())
	require.NoError(t, walFactory.Close())

	segments, err := listAllSegments(vfs.OSFS{}, walBasePath)
	require.NoError(t, err)
	return walBasePath, segments
}

// failingCloseSegment is a segment that expired from the cache, and fails to
// close.
type failingCloseSegment struct {
	ReadOnlySegment
	closeErr error
}

func (*failingCloseSegment) OpenTimestamp() time.Time { return time.Time{} }

func (s *failingCloseSegment) Close() error {
	return multierr.Append(s.ReadOnlySegment.Close(), s.closeErr)
}
