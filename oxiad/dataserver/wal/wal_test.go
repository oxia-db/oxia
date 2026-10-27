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
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/common/vfs"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/object"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
)

const shard = int64(100)

func NewTestWalFactory(t *testing.T) Factory {
	t.Helper()

	dir := t.TempDir()
	return NewWalFactory(&FactoryOptions{
		BaseWalDir:  dir,
		Retention:   1 * time.Hour,
		SegmentSize: 128 * 1024,
		SyncData:    true,
	})
}

func createWal(t *testing.T) (Factory, Wal) {
	t.Helper()

	f := NewTestWalFactory(t)
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	return f, w
}

func assertReaderReads(t *testing.T, r Reader, entries []string) {
	t.Helper()

	for i := 0; i < len(entries); i++ {
		assert.True(t, r.HasNext())
		e, _, _, err := r.ReadNext()
		assert.NoError(t, err)
		assert.Equal(t, entries[i], string(e.Value))
	}
	assert.False(t, r.HasNext())
}

func assertReaderReadsEventually(t *testing.T, r Reader, entries []string) chan error {
	t.Helper()

	ch := make(chan error)
	go func() {
		for i := 0; i < len(entries); i++ {
			assert.Eventually(t, r.HasNext,
				100*time.Millisecond,
				10*time.Millisecond,
				fmt.Sprintf("did not read all expected entries: only read %d/%d", i, len(entries)))
			e, _, _, err := r.ReadNext()
			if err != nil {
				ch <- err
				return
			}
			if entries[i] != string(e.Value) {
				ch <- errors.Errorf("entry #%d not equal. Expected '%s', got '%s'", i, entries[i], string(e.Value))
			}
		}
		ch <- nil
	}()
	return ch
}

func TestFactoryNewWal(t *testing.T) {
	f, w := createWal(t)
	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	assert.False(t, rr.HasNext())

	assert.NoError(t, rr.Close())
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assert.False(t, fr.HasNext())
	assert.NoError(t, fr.Close())
	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func TestAppend(t *testing.T) {
	f, w := createWal(t)

	// Append entries
	input := []string{"A", "B", "C"}
	for i, s := range input {
		err := w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(s),
		})
		assert.NoError(t, err)
	}

	// Read entries backwards
	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	assertReaderReads(t, rr, []string{"C", "B", "A"})
	assert.NoError(t, rr.Close())

	// Read with forward reader from beginning
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, fr, input)
	assert.NoError(t, fr.Close())

	// Read with forward reader from the middle
	fr, err = w.NewReader(1)
	assert.NoError(t, err)
	assertReaderReads(t, fr, []string{"C"})
	assert.NoError(t, fr.Close())

	// Read with forward reader waiting for new entries
	fr, err = w.NewReader(0)
	assert.NoError(t, err)
	ch := assertReaderReadsEventually(t, fr, []string{"B", "C", "D"})

	err = w.Append(&proto.LogEntry{
		Term:   1,
		Offset: int64(3),
		Value:  []byte("D"),
	})
	assert.NoError(t, err)
	assert.NoError(t, <-ch)

	assert.NoError(t, fr.Close())

	// Append invalid offset
	err = w.Append(&proto.LogEntry{
		Term:   1,
		Offset: int64(88),
		Value:  []byte("E"),
	})
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func TestAppendAsync(t *testing.T) {
	f, w := createWal(t)

	// Append entries
	input := []string{"A", "B", "C"}
	for i, s := range input {
		err := w.AppendAsync(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(s),
		})
		assert.NoError(t, err)
	}

	assert.Equal(t, InvalidOffset, w.LastOffset())

	// Read with forward reader from beginning
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assert.False(t, fr.HasNext())

	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	assert.False(t, rr.HasNext())

	assert.NoError(t, w.Sync(context.Background()))

	assert.EqualValues(t, 2, w.LastOffset())

	fr, err = w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assert.True(t, fr.HasNext())

	rr, err = w.NewReverseReader()
	assert.NoError(t, err)
	assert.True(t, rr.HasNext())

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func TestRollover(t *testing.T) {
	f, w := createWal(t)

	// Append entries
	for i := 0; i < 300; i++ {
		value := make([]byte, 1024)
		copy(value, fmt.Sprintf("entry-%d", i))

		err := w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  value,
		})
		assert.NoError(t, err)
	}

	// Read entries backwards
	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	for i := 299; i >= 0; i-- {
		assert.True(t, rr.HasNext())
		entry, _, _, err := rr.ReadNext()
		assert.NoError(t, err)

		value := make([]byte, 1024)
		copy(value, fmt.Sprintf("entry-%d", i))
		assert.Equal(t, value, entry.Value)
	}
	assert.NoError(t, rr.Close())

	// Read with forward reader from beginning
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	for i := 0; i < 300; i++ {
		assert.True(t, fr.HasNext())
		entry, _, _, err := fr.ReadNext()
		assert.NoError(t, err)

		value := make([]byte, 1024)
		copy(value, fmt.Sprintf("entry-%d", i))
		assert.Equal(t, value, entry.Value)
	}

	assert.NoError(t, fr.Close())

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func TestTruncate(t *testing.T) {
	f, w := createWal(t)

	// Append entries
	input := []string{"A", "B", "C", "D", "E"}
	for i, s := range input {
		err := w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(s),
		})
		assert.NoError(t, err)
	}

	headIndex, err := w.TruncateLog(2)
	assert.NoError(t, err)

	assert.EqualValues(t, 2, headIndex)

	// Close and Reopen the wal to ensure truncate is persistent
	err = w.Close()
	assert.NoError(t, err)

	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	// Read with forward reader from beginning
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, fr, input[:3])
	assert.NoError(t, fr.Close())

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func TestTruncateClear(t *testing.T) {
	f, w := createWal(t)

	assert.Equal(t, InvalidOffset, w.FirstOffset())
	assert.Equal(t, InvalidOffset, w.LastOffset())

	err := w.Append(&proto.LogEntry{Term: 2, Offset: 3})
	assert.NoError(t, err)
	err = w.Append(&proto.LogEntry{Term: 2, Offset: 4})
	assert.NoError(t, err)

	assert.Equal(t, int64(3), w.FirstOffset())
	assert.Equal(t, int64(4), w.LastOffset())

	lastOffset, err := w.TruncateLog(InvalidOffset)

	assert.Equal(t, InvalidOffset, lastOffset)
	assert.NoError(t, err)

	assert.Equal(t, InvalidOffset, w.FirstOffset())
	assert.Equal(t, InvalidOffset, w.LastOffset())

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func TestReopen(t *testing.T) {
	f, w := createWal(t)

	// Append entries
	input := []string{"A", "B", "C", "D", "E"}
	for i, s := range input {
		err := w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(s),
		})
		assert.NoError(t, err)
	}

	err := w.Close()
	assert.NoError(t, err)

	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	// Read with forward reader from beginning
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, fr, input)
	assert.NoError(t, fr.Close())

	err = w.Close()
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func TestClear(t *testing.T) {
	f, w := createWal(t)

	assert.EqualValues(t, InvalidOffset, w.FirstOffset())
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	for i := 0; i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}

	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	assert.NoError(t, w.Clear())

	assert.EqualValues(t, InvalidOffset, w.FirstOffset())
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	for i := 250; i < 300; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))

		assert.EqualValues(t, 250, w.FirstOffset())
		assert.EqualValues(t, i, w.LastOffset())
	}

	// Test forward reader
	r, err := w.NewReader(249)
	assert.NoError(t, err)

	for i := 250; i < 300; i++ {
		assert.True(t, r.HasNext())
		le, _, _, err := r.ReadNext()
		assert.NoError(t, err)

		assert.EqualValues(t, i, le.Offset)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(le.Value))
	}

	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())

	// Test reverse reader
	r, err = w.NewReverseReader()
	assert.NoError(t, err)

	for i := 299; i >= 250; i-- {
		assert.True(t, r.HasNext())
		le, _, _, err := r.ReadNext()
		assert.NoError(t, err)

		assert.EqualValues(t, i, le.Offset)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(le.Value))
	}

	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestTrim(t *testing.T) {
	f, w := createWal(t)

	assert.EqualValues(t, InvalidOffset, w.FirstOffset())
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	for i := 0; i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}

	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	assert.NoError(t, w.(*wal).trim(50, w.(*wal).generation.Load(), func() error { return nil }))

	assert.EqualValues(t, 50, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	// Test forward reader
	r, err := w.NewReader(49)
	assert.NoError(t, err)

	for i := 50; i < 100; i++ {
		assert.True(t, r.HasNext())
		le, _, _, err := r.ReadNext()
		assert.NoError(t, err)

		assert.EqualValues(t, i, le.Offset)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(le.Value))
	}

	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())

	// Test reverse reader
	r, err = w.NewReverseReader()
	assert.NoError(t, err)

	for i := 99; i >= 50; i-- {
		assert.True(t, r.HasNext())
		le, _, _, err := r.ReadNext()
		assert.NoError(t, err)

		assert.EqualValues(t, i, le.Offset)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(le.Value))
	}

	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())

	// Test reading a trimmed offset
	r, err = w.NewReader(48)
	assert.ErrorIs(t, err, errs.ErrNotFound)
	assert.Nil(t, r)

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// Once the trim offset reaches the current segment, every read-only segment
// ends before it: the trimming deletes them all, the last one included.
func TestWalTrimInCurrentSegmentDeletesAllReadOnlySegments(t *testing.T) {
	options := &FactoryOptions{BaseWalDir: t.TempDir(), SegmentSize: 128, SyncData: true}
	w, err := newWal(constant.DefaultNamespace, shard, options, nil, time2.SystemClock, time.Hour)
	require.NoError(t, err)
	appendEntries(t, w, 1, 0, 19, "entry")
	impl := w.(*wal)
	segments, err := listAllSegments(vfs.OSFS{}, impl.walPath)
	require.NoError(t, err)
	require.Greater(t, len(segments), 2)
	current := segments[len(segments)-1]
	require.Less(t, current, w.LastOffset())

	trimOffset := current + 1
	require.NoError(t, impl.trim(trimOffset, impl.generation.Load(), func() error { return nil }))
	assert.EqualValues(t, trimOffset, w.FirstOffset())
	for _, s := range segments[:len(segments)-1] {
		c, err := newSegmentConfig(vfs.OSFS{}, impl.walPath, s)
		require.NoError(t, err)
		assert.NoFileExists(t, c.txnPath)
		assert.NoFileExists(t, c.idxPath)
	}

	assertReadsFrom := func(w Wal, firstOffset int64) {
		t.Helper()
		r, err := w.NewReader(firstOffset - 1)
		require.NoError(t, err)
		for offset := firstOffset; offset <= 19; offset++ {
			require.True(t, r.HasNext())
			entry, _, _, err := r.ReadNext()
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("entry-%d", offset), string(entry.Value))
		}
		assert.False(t, r.HasNext())
		assert.NoError(t, r.Close())
	}
	assertReadsFrom(w, trimOffset)
	require.NoError(t, w.Close())

	// After a restart, the wal starts from the current segment
	w, err = newWal(constant.DefaultNamespace, shard, options, nil, time2.SystemClock, time.Hour)
	require.NoError(t, err)
	assert.EqualValues(t, current, w.FirstOffset())
	assert.EqualValues(t, 19, w.LastOffset())
	assertReadsFrom(w, current)
	assert.NoError(t, w.Close())
}

func TestDelete(t *testing.T) {
	f, w := createWal(t)

	for i := 0; i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}

	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, 99, w.LastOffset())

	assert.NoError(t, w.Delete())

	w, err := f.NewWal(constant.DefaultNamespace, 1, nil)
	assert.NoError(t, err)

	assert.EqualValues(t, InvalidOffset, w.FirstOffset())
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestReaderReadNext(t *testing.T) {
	f, w := createWal(t)

	c := int64(100)
	for i := int64(0); i < c; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: i,
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}

	reader, err := w.NewReader(c - 2)
	assert.NoError(t, err)
	entry, _, _, err := reader.ReadNext()
	assert.NoError(t, err)
	assert.Equal(t, &proto.LogEntry{
		Term:   1,
		Offset: 99,
		Value:  []byte("entry-99"),
	}, entry)
	entry2, _, _, err := reader.ReadNext()
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
	assert.Nil(t, entry2)

	assert.NoError(t, reader.Close())
	assert.NoError(t, f.Close())
}

// readCrcsViaReader reads all entries from the WAL using a forward reader
// and returns the previousCrc and entryCrc for each offset.
func readCrcsViaReader(t *testing.T, w Wal) (previousCrcs, entryCrcs map[int64]uint32) {
	t.Helper()
	previousCrcs = make(map[int64]uint32)
	entryCrcs = make(map[int64]uint32)
	r, err := w.NewReader(w.FirstOffset() - 1)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, r.Close()) }()
	for r.HasNext() {
		entry, prevCrc, entryCrc, err := r.ReadNext()
		assert.NoError(t, err)
		previousCrcs[entry.Offset] = prevCrc
		entryCrcs[entry.Offset] = entryCrc
	}
	return previousCrcs, entryCrcs
}

func TestReadNextReturnsCrc(t *testing.T) {
	f, w := createWal(t)

	n := 10
	for i := 0; i < n; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}

	_, entryCrcs := readCrcsViaReader(t, w)

	// Each entry should have a non-zero CRC, and CRCs should differ between offsets
	var prevCrc uint32
	for i := 0; i < n; i++ {
		crc := entryCrcs[int64(i)]
		assert.NotZero(t, crc, "CRC at offset %d should be non-zero", i)
		if i > 0 {
			assert.NotEqual(t, prevCrc, crc, "CRC should differ between offsets %d and %d", i-1, i)
		}
		prevCrc = crc
	}

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestAppendAsyncWithPreviousCrc(t *testing.T) {
	// Create a WAL and append entries 0-19 continuously
	f1, w1 := createWal(t)
	for i := 0; i < 20; i++ {
		assert.NoError(t, w1.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}
	_, w1Crcs := readCrcsViaReader(t, w1)
	expectedCrc := w1Crcs[19]

	// Create another WAL: append 0-9, record CRC at 9, clear, then use
	// AppendAsyncWithPreviousCrc to seed CRC chain and append 10-19
	f2, w2 := createWal(t)
	for i := 0; i < 10; i++ {
		assert.NoError(t, w2.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}
	_, w2Crcs := readCrcsViaReader(t, w2)
	crcAt9 := w2Crcs[9]

	assert.NoError(t, w2.Clear())

	// First entry after clear uses AppendAsyncWithPreviousCrc to seed the CRC chain
	crcAt10, err := w2.AppendAsyncWithPreviousCrc(&proto.LogEntry{
		Term:   1,
		Offset: 10,
		Value:  []byte("entry-10"),
	}, &crcAt9)
	assert.NoError(t, err)
	assert.NoError(t, w2.Sync(context.Background()))

	for i := 11; i < 20; i++ {
		assert.NoError(t, w2.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(fmt.Sprintf("entry-%d", i)),
		}))
	}
	_, w2CrcsAfter := readCrcsViaReader(t, w2)
	actualCrc := w2CrcsAfter[19]

	// The CRC at offset 19 must match between the continuous WAL and
	// the WAL that was cleared+reseeded at offset 9.
	assert.Equal(t, expectedCrc, actualCrc,
		"CRC chain must match after clear+AppendAsyncWithPreviousCrc")
	// The append returns the CRC that the WAL chained for the entry
	assert.Equal(t, w1Crcs[10], crcAt10)
	assert.Equal(t, w2CrcsAfter[10], crcAt10)

	assert.NoError(t, w1.Close())
	assert.NoError(t, f1.Close())
	assert.NoError(t, w2.Close())
	assert.NoError(t, f2.Close())
}

func appendEntries(t *testing.T, w Wal, term, firstOffset, lastOffset int64, payload string) {
	t.Helper()
	for i := firstOffset; i <= lastOffset; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: term, Offset: i, Value: []byte(fmt.Sprintf("%s-%d", payload, i))}))
	}
}

// assertCrcChain checks that every entry of the wal carries the crc of the
// entry before it as its previous crc, and returns the crc of the last entry.
func assertCrcChain(t *testing.T, w Wal) uint32 {
	t.Helper()
	previousCrcs, entryCrcs := readCrcsViaReader(t, w)
	for offset := w.FirstOffset() + 1; offset <= w.LastOffset(); offset++ {
		assert.Equal(t, entryCrcs[offset-1], previousCrcs[offset], "crc chain broken at offset %d", offset)
	}
	return entryCrcs[w.LastOffset()]
}

// The entries appended after a truncation must chain from the last entry kept:
// their crcs must match the ones of a wal that never held the truncated
// entries, as a follower's must keep matching the leader's after it truncated
// its uncommitted entries.
func TestWal_TruncateKeepsCrcChain(t *testing.T) {
	for _, test := range []struct {
		name         string
		payloadSize  int
		olderSegment bool
	}{
		{"current segment", 10, false},
		// The truncation reopens an older segment for writes, and deletes the
		// segments after it
		{"older segment", 1024, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := strings.Repeat("x", test.payloadSize)

			fExpected, wExpected := createWal(t)
			appendEntries(t, wExpected, 1, 0, 50, payload)
			appendEntries(t, wExpected, 2, 51, 59, payload)
			expectedCrc := assertCrcChain(t, wExpected)
			assert.NoError(t, wExpected.Close())
			assert.NoError(t, fExpected.Close())

			f, w := createWal(t)
			appendEntries(t, w, 1, 0, 299, payload)
			// Make sure that the truncation point is in the intended segment
			walImpl := w.(*wal)
			walImpl.RLock()
			assert.Equal(t, test.olderSegment, walImpl.currentSegment.BaseOffset() > 50)
			walImpl.RUnlock()

			headOffset, err := w.TruncateLog(50)
			assert.NoError(t, err)
			assert.EqualValues(t, 50, headOffset)
			appendEntries(t, w, 2, 51, 59, payload)
			assert.Equal(t, expectedCrc, assertCrcChain(t, w))
			assert.NoError(t, w.Close())

			// The chain written to the segment files survives a reopen
			w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
			assert.NoError(t, err)
			assert.EqualValues(t, 59, w.LastOffset())
			assert.Equal(t, expectedCrc, assertCrcChain(t, w))
			assert.NoError(t, w.Close())
			assert.NoError(t, f.Close())
		})
	}
}

// With sync disabled there are no sync rounds: every rollover exercises the
// fallback fsync of a still-pending segment file, and the data must stay
// intact across rollovers and a reopen.
func TestWal_RolloverWithoutSyncRounds(t *testing.T) {
	f := NewWalFactory(&FactoryOptions{
		BaseWalDir:  t.TempDir(),
		Retention:   1 * time.Hour,
		SegmentSize: 128 * 1024,
		SyncData:    false,
	})
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	var entries []string
	payload := strings.Repeat("x", 1024)
	for i := 0; i < 1_000; i++ {
		value := fmt.Sprintf("%s-%d", payload, i)
		entries = append(entries, value)
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(value),
		}))
	}

	r, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())

	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 999, w.LastOffset())

	r, err = w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// Segments created by rollovers defer the file fsync to the sync goroutine:
// entries must stay intact across multiple rollovers, synced acknowledgments
// and a wal reopen.
func TestWal_RolloverWithDeferredFileSync(t *testing.T) {
	f, w := createWal(t)

	// Write enough to roll over several times (128KiB segments)
	var entries []string
	payload := strings.Repeat("x", 1024)
	for i := 0; i < 1_000; i++ {
		value := fmt.Sprintf("%s-%d", payload, i)
		entries = append(entries, value)
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(i),
			Value:  []byte(value),
		}))
	}
	assert.EqualValues(t, 999, w.LastOffset())

	r, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())

	// Reopen: the rolled-over segment files must recover correctly
	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 999, w.LastOffset())

	r, err = w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// Rolled-over segments are made durable and closed by the sync goroutine:
// until then their entries stay readable through the wal, and a sync round
// drains them into the read-only group before acknowledging.
func TestWal_RolloverKeepsPendingSegmentsReadable(t *testing.T) {
	f, w := createWal(t)
	walImpl := w.(*wal)

	payload := strings.Repeat("x", 1024)
	var entries []string

	// Synced entries filling most of the first segment (128 KiB)
	for i := 0; i < 100; i++ {
		value := fmt.Sprintf("%s-%d", payload, i)
		entries = append(entries, value)
		assert.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: int64(i), Value: []byte(value)}))
	}

	// Async appends crossing the segment boundary: no sync round runs, so the
	// rolled-over segment stays pending
	for i := 100; i < 150; i++ {
		value := fmt.Sprintf("%s-%d", payload, i)
		entries = append(entries, value)
		assert.NoError(t, w.AppendAsync(&proto.LogEntry{Term: 1, Offset: int64(i), Value: []byte(value)}))
	}

	walImpl.RLock()
	pendingSegments := len(walImpl.pendingCloseSegments)
	walImpl.RUnlock()
	assert.Greater(t, pendingSegments, 0)

	// The synced entries living in the pending segment are still readable
	r, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries[:100])
	assert.NoError(t, r.Close())

	// A sync round drains the pending segments before acknowledging
	assert.NoError(t, w.Sync(context.Background()))
	walImpl.RLock()
	pendingSegments = len(walImpl.pendingCloseSegments)
	walImpl.RUnlock()
	assert.Equal(t, 0, pendingSegments)

	r, err = w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())

	// Reopen: recovery across the drained segments
	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 149, w.LastOffset())
	r, err = w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// Truncation drains the pending-close segments first, so that it operates on
// the complete set of segments.
func TestWal_TruncateWithPendingCloseSegments(t *testing.T) {
	f, w := createWal(t)
	walImpl := w.(*wal)

	payload := strings.Repeat("x", 1024)
	for i := 0; i < 100; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 1, Offset: int64(i), Value: []byte(fmt.Sprintf("%s-%d", payload, i))}))
	}
	for i := 100; i < 150; i++ {
		assert.NoError(t, w.AppendAsync(&proto.LogEntry{
			Term: 1, Offset: int64(i), Value: []byte(fmt.Sprintf("%s-%d", payload, i))}))
	}

	walImpl.RLock()
	pendingSegments := len(walImpl.pendingCloseSegments)
	walImpl.RUnlock()
	assert.Greater(t, pendingSegments, 0)

	headOffset, err := w.TruncateLog(80)
	assert.NoError(t, err)
	assert.EqualValues(t, 80, headOffset)
	assert.EqualValues(t, 80, w.LastOffset())

	walImpl.RLock()
	pendingSegments = len(walImpl.pendingCloseSegments)
	walImpl.RUnlock()
	assert.Equal(t, 0, pendingSegments)

	r, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	for i := 0; i <= 80; i++ {
		assert.True(t, r.HasNext())
		e, _, _, err := r.ReadNext()
		assert.NoError(t, err)
		assert.EqualValues(t, i, e.Offset)
	}
	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// A missing segment index file — the state left behind when the process
// crashes between a rollover and the sync round that closes the rolled-over
// segment — gets rebuilt from the txn file on open.
func TestWal_ReadOnlySegmentMissingIndex(t *testing.T) {
	dir := t.TempDir()
	f := NewWalFactory(&FactoryOptions{
		BaseWalDir:  dir,
		Retention:   1 * time.Hour,
		SegmentSize: 128 * 1024,
		SyncData:    true,
	})
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	payload := strings.Repeat("x", 1024)
	var entries []string
	for i := 0; i < 300; i++ {
		value := fmt.Sprintf("%s-%d", payload, i)
		entries = append(entries, value)
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 1, Offset: int64(i), Value: []byte(value)}))
	}
	assert.NoError(t, w.Close())

	// Simulate the crash window: drop every segment index file
	idxFiles, err := filepath.Glob(filepath.Join(walPath(dir, constant.DefaultNamespace, shard), "*.idx*"))
	assert.NoError(t, err)
	assert.NotEmpty(t, idxFiles)
	for _, idx := range idxFiles {
		assert.NoError(t, os.Remove(idx))
	}

	// Reopen: the indexes are rebuilt from the txn files
	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 299, w.LastOffset())

	r, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// A power loss can leave a rolled-over segment file with none of its content
// on disk (all zeroes, no index): recovery must discard the trailing segments
// sitting after the gap — their entries were never acknowledged — and resume
// from the last segment with recoverable entries (it used to panic with a
// slice out of bounds).
func TestWal_RecoverAfterLostRolledOverSegment(t *testing.T) {
	dir := t.TempDir()
	f := NewWalFactory(&FactoryOptions{
		BaseWalDir:  dir,
		Retention:   1 * time.Hour,
		SegmentSize: 128 * 1024,
		SyncData:    true,
	})
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	payload := strings.Repeat("x", 1024)
	var entries []string
	for i := 0; i < 300; i++ {
		value := fmt.Sprintf("%s-%d", payload, i)
		entries = append(entries, value)
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 1, Offset: int64(i), Value: []byte(value)}))
	}
	assert.NoError(t, w.Close())

	basePath := walPath(dir, constant.DefaultNamespace, shard)
	segments, err := listAllSegments(vfs.OSFS{}, basePath)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(segments), 3)

	// Simulate the power loss: the second-to-last segment never reached the
	// disk (all zeroes, no index), while the last segment kept its content
	lost := segments[len(segments)-2]
	lostConfig, err := newSegmentConfig(vfs.OSFS{}, basePath, lost)
	assert.NoError(t, err)
	info, err := os.Stat(lostConfig.txnPath)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(lostConfig.txnPath, make([]byte, info.Size()), 0644))
	assert.NoError(t, os.Remove(lostConfig.idxPath))
	lastConfig, err := newSegmentConfig(vfs.OSFS{}, basePath, segments[len(segments)-1])
	assert.NoError(t, err)
	assert.NoError(t, os.Remove(lastConfig.idxPath))

	// Reopen: everything before the lost segment is recovered, the trailing
	// segment after the gap is discarded
	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, w.FirstOffset())
	assert.EqualValues(t, lost-1, w.LastOffset())
	_, err = os.Stat(lastConfig.txnPath)
	assert.ErrorIs(t, err, os.ErrNotExist)

	// The wal accepts appends continuing right after the recovered offsets
	entries = entries[:lost]
	for i := lost; i < lost+10; i++ {
		value := fmt.Sprintf("recovered-%d", i)
		entries = append(entries, value)
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 2, Offset: i, Value: []byte(value)}))
	}

	r, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())

	// And everything survives another reopen
	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, lost+9, w.LastOffset())
	r, err = w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// A power loss wiping the content of every segment leaves the wal with no
// recoverable entries: it must reopen empty, positioned at its first segment.
func TestWal_RecoverAfterAllSegmentsContentLost(t *testing.T) {
	dir := t.TempDir()
	f := NewWalFactory(&FactoryOptions{
		BaseWalDir:  dir,
		Retention:   1 * time.Hour,
		SegmentSize: 128 * 1024,
		SyncData:    true,
	})
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	payload := strings.Repeat("x", 1024)
	for i := 0; i < 300; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 1, Offset: int64(i), Value: []byte(fmt.Sprintf("%s-%d", payload, i))}))
	}
	assert.NoError(t, w.Close())

	basePath := walPath(dir, constant.DefaultNamespace, shard)
	segments, err := listAllSegments(vfs.OSFS{}, basePath)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(segments), 3)
	for _, segment := range segments {
		c, err := newSegmentConfig(vfs.OSFS{}, basePath, segment)
		assert.NoError(t, err)
		info, err := os.Stat(c.txnPath)
		assert.NoError(t, err)
		assert.NoError(t, os.WriteFile(c.txnPath, make([]byte, info.Size()), 0644))
		assert.NoError(t, vfs.RemoveFileIfExists(vfs.OSFS{}, c.idxPath))
	}

	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	assert.Equal(t, InvalidOffset, w.FirstOffset())
	assert.Equal(t, InvalidOffset, w.LastOffset())

	// Only the first segment file survives, and the wal is appendable from
	// scratch
	remaining, err := listAllSegments(vfs.OSFS{}, basePath)
	assert.NoError(t, err)
	assert.Equal(t, []int64{0}, remaining)

	var entries []string
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf("entry-%d", i)
		entries = append(entries, value)
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 2, Offset: int64(i), Value: []byte(value)}))
	}
	r, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, entries)
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// The recovery discards a torn entry past the commit offset along with the
// intact ones after it: when the next term rewrites that entry with one of the
// same size, a later recovery must not bring the discarded entries of the
// older term back after it.
func TestWal_DiscardedEntriesNotResurrected(t *testing.T) {
	f := NewTestWalFactory(t)
	commitOffsetProvider := ConfigurableCommitOffsetProvider{commitOffset: 2}
	w, err := f.NewWal(constant.DefaultNamespace, shard, commitOffsetProvider)
	assert.NoError(t, err)
	for i := int64(0); i <= 5; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 1, Offset: i, Value: fmt.Appendf(nil, "term-1-entry-%d", i)}))
	}

	// A power loss tears the uncommitted entry 3, while 4 and 5 reach the disk
	rw := w.(*wal).currentSegment.(*readWriteSegment)
	rw.txnMappedFile[fileOffset(rw.writingIdx, 0, 3)+rw.c.codec.GetHeaderSize()] ^= 0xFF
	assert.NoError(t, w.Close())

	w, err = f.NewWal(constant.DefaultNamespace, shard, commitOffsetProvider)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, w.LastOffset())
	assert.NoError(t, w.Append(&proto.LogEntry{
		Term: 2, Offset: 3, Value: []byte("term-2-entry-3")}))
	assert.NoError(t, w.Close())

	w, err = f.NewWal(constant.DefaultNamespace, shard, commitOffsetProvider)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, w.LastOffset())
	r, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, r, []string{"term-1-entry-0", "term-1-entry-1", "term-1-entry-2", "term-2-entry-3"})
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// A failed recovery must release what the wal set up before it: a leaked
// entries gauge would keep reporting the shard, and pin the wal in memory.
func TestWal_RecoveryFailureUnregistersGauge(t *testing.T) {
	// Swap in an SDK meter so the registered gauges can be read back.
	previous := metric.GetMeter()
	reader := sdkmetric.NewManualReader()
	metric.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	defer metric.SetMeter(previous)

	dir := t.TempDir()
	f := NewWalFactory(&FactoryOptions{
		BaseWalDir:  dir,
		Retention:   1 * time.Hour,
		SegmentSize: 128 * 1024,
		SyncData:    true,
	})
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	payload := strings.Repeat("x", 1024)
	for i := 0; i < 300; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 1, Offset: int64(i), Value: []byte(fmt.Sprintf("%s-%d", payload, i))}))
	}
	assert.NoError(t, w.Close())

	// Fail the recovery after it opened a read-only segment for the last
	// crc: the last segment is reopened for writes, and its stale index
	// cannot be removed
	basePath := walPath(dir, constant.DefaultNamespace, shard)
	segments, err := listAllSegments(vfs.OSFS{}, basePath)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(segments), 2)
	lastConfig, err := newSegmentConfig(vfs.OSFS{}, basePath, segments[len(segments)-1])
	assert.NoError(t, err)
	assert.NoError(t, vfs.RemoveFileIfExists(vfs.OSFS{}, lastConfig.idxPath))
	assert.NoError(t, os.MkdirAll(filepath.Join(lastConfig.idxPath, "not-empty"), 0755))

	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.ErrorContains(t, err, "failed to remove stale segment index file")
	assert.Nil(t, w)

	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			assert.NotEqual(t, "oxia_server_wal_entries", m.Name)
		}
	}
	assert.NoError(t, f.Close())
}

// Truncating to an offset below all the retained segments clears the wal:
// the clear used to re-acquire the wal lock already held by TruncateLog,
// deadlocking the wal permanently (this test would time out).
func TestWal_TruncateBelowAllSegments(t *testing.T) {
	f, w := createWal(t)

	// Leave the wal with segments whose base offsets are all above the
	// truncation target: start from a non-initial offset, as a follower
	// does after its wal got cleared, and roll over at least once
	assert.NoError(t, w.Clear())
	payload := strings.Repeat("x", 1024)
	for i := 100; i < 300; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 1, Offset: int64(i), Value: []byte(fmt.Sprintf("%s-%d", payload, i))}))
	}

	headOffset, err := w.TruncateLog(50)
	assert.NoError(t, err)
	assert.Equal(t, InvalidOffset, headOffset)
	assert.Equal(t, InvalidOffset, w.FirstOffset())
	assert.Equal(t, InvalidOffset, w.LastOffset())

	// The cleared wal must be usable, appendable from right after the
	// truncation point
	for i := 51; i < 55; i++ {
		assert.NoError(t, w.Append(&proto.LogEntry{
			Term: 2, Offset: int64(i), Value: []byte(fmt.Sprintf("entry-%d", i))}))
	}
	assert.EqualValues(t, 51, w.FirstOffset())
	assert.EqualValues(t, 54, w.LastOffset())

	r, err := w.NewReader(50)
	assert.NoError(t, err)
	for i := 51; i < 55; i++ {
		assert.True(t, r.HasNext())
		e, _, _, err := r.ReadNext()
		assert.NoError(t, err)
		assert.EqualValues(t, i, e.Offset)
	}
	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// The segments that a truncation deletes, or reopens for writes, must leave
// the read-only segments cache. The reads of the offsets appended again after
// the truncation used to be served by the cached segments: the deleted ones,
// closed, failed every read, and the reopened one read the new entries through
// the index of the truncated ones, failing or returning the entries of other
// offsets.
func TestWal_TruncateEvictsCachedSegments(t *testing.T) {
	f := NewWalFactory(&FactoryOptions{
		BaseWalDir:  t.TempDir(),
		Retention:   1 * time.Hour,
		SegmentSize: 1024,
	})
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	group := w.(*wal).readOnlySegments.(*readOnlySegmentsGroup)

	appendEntries(t, w, 1, 0, 99, "old")

	// Cache the read-only segment holding the truncation point, and one after
	// it
	for _, offset := range []int64{0, 50} {
		r, err := w.NewReader(offset - 1)
		assert.NoError(t, err)
		_, _, _, err = r.ReadNext()
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
	}
	group.Lock()
	cached := group.openSegments.Values()
	group.Unlock()
	if assert.Len(t, cached, 2) {
		assert.Greater(t, cached[1].Get().BaseOffset(), int64(30))
	}

	headOffset, err := w.TruncateLog(30)
	assert.NoError(t, err)
	assert.EqualValues(t, 30, headOffset)
	// The truncation releases the cache references: the segments get closed,
	// exactly once
	for _, s := range cached {
		assert.EqualValues(t, 0, s.RefCnt())
	}

	// Bigger entries than the truncated ones, rolling the reopened segment
	// over
	appendEntries(t, w, 2, 31, 130, "new-term")

	r, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	for i := int64(0); i <= 130; i++ {
		e, _, _, err := r.ReadNext()
		if !assert.NoError(t, err, "offset %d", i) {
			break
		}
		assert.EqualValues(t, i, e.Offset)
	}
	assert.False(t, r.HasNext())
	assert.NoError(t, r.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// Each entry read records one read-latency sample, whichever way the reader
// goes: the forward reader used to time the read a second time around
// readAtIndex.
func TestWal_ReadLatencyRecordedOncePerEntry(t *testing.T) {
	// Read the histograms through their producer, as the Prometheus exporter
	// does.
	reader := sdkmetric.NewManualReader(sdkmetric.WithProducer(metric.HistogramProducer))
	sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	readLatencyCount := func() uint64 {
		var rm metricdata.ResourceMetrics
		assert.NoError(t, reader.Collect(context.Background(), &rm))
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != "oxia_server_wal_read_latency" {
					continue
				}
				histo, ok := m.Data.(metricdata.Histogram[float64])
				assert.True(t, ok, "unexpected data: %#v", m.Data)
				var total uint64
				for _, dp := range histo.DataPoints {
					total += dp.Count
				}
				return total
			}
		}
		return 0
	}

	f, w := createWal(t)

	input := []string{"A", "B", "C"}
	for i, s := range input {
		assert.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: int64(i), Value: []byte(s)}))
	}

	before := readLatencyCount()
	fr, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	assertReaderReads(t, fr, input)
	assert.NoError(t, fr.Close())
	assert.EqualValues(t, len(input), readLatencyCount()-before)

	before = readLatencyCount()
	rr, err := w.NewReverseReader()
	assert.NoError(t, err)
	assertReaderReads(t, rr, []string{"C", "B", "A"})
	assert.NoError(t, rr.Close())
	assert.EqualValues(t, len(input), readLatencyCount()-before)

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// A read holds the wal mutex at most to pick the segment holding the entry:
// the appends do not wait for it to open the segment, or to copy the record.
func TestWalReadAtIndexDoesNotBlockAppends(t *testing.T) {
	for _, test := range []struct {
		name   string
		offset int64
		// hookRead installs the hook in the step of the read to block
		hookRead func(t *testing.T, w *wal, hook func())
	}{
		{"current segment copy", 140, func(_ *testing.T, w *wal, hook func()) {
			hookRecordReads(w.currentSegment, hook)
		}},
		{"rolled-over segment copy", 50, func(_ *testing.T, w *wal, hook func()) {
			hookRecordReads(w.pendingCloseSegments[0], hook)
		}},
		{"read-only segment open", 50, func(t *testing.T, w *wal, hook func()) {
			t.Helper()
			// Close the rolled-over segment, moving it to the read-only group
			require.NoError(t, w.Sync(context.Background()))
			w.Lock()
			defer w.Unlock()
			w.readOnlySegments = &hookedSegmentsGroup{ReadOnlySegmentsGroup: w.readOnlySegments, getHook: hook}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, w := createWal(t)
			appendRolledOver(t, w)
			hook, blocked, release := blockFirstCall()
			defer release()
			test.hookRead(t, w.(*wal), hook)

			readDone := make(chan *proto.LogEntry, 1)
			go func() {
				entry, err := readEntry(w, test.offset)
				assert.NoError(t, err)
				readDone <- entry
			}()
			<-blocked

			appendDone := make(chan error, 1)
			go func() {
				appendDone <- w.AppendAsync(&proto.LogEntry{Term: 1, Offset: 150, Value: []byte("entry-150")})
			}()
			select {
			case err := <-appendDone:
				assert.NoError(t, err)
			case <-time.After(5 * time.Second):
				require.FailNow(t, "the append waited for the read")
			}

			release()
			if entry := <-readDone; assert.NotNil(t, entry) {
				assert.EqualValues(t, test.offset, entry.Offset)
			}
			assert.NoError(t, w.Close())
			assert.NoError(t, f.Close())
		})
	}
}

// A read can find closed the read-write segment it picked: a rollover moved it
// to the read-only group meanwhile, where the read finds the entry.
func TestWalReadAtIndexRetriesSegmentClosedByRollover(t *testing.T) {
	for _, test := range []struct {
		name string
		// Whether the segment got rolled over before the read picked it
		pending bool
	}{
		{"current segment", false},
		{"rolled-over segment", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, w := createWal(t)
			impl := w.(*wal)
			payload := strings.Repeat("x", 1024)
			appendEntries(t, w, 1, 0, 99, payload)

			hook, reading, release := blockFirstCall()
			defer release()
			impl.Lock()
			var segment ReadWriteSegment = &hookedReadWriteSegment{ReadWriteSegment: impl.currentSegment, readHook: hook}
			impl.currentSegment = segment
			impl.publishedCurrentSegment.Store(&segment)
			impl.Unlock()

			roll := func() {
				for i := int64(100); i < 150; i++ {
					require.NoError(t, w.AppendAsync(&proto.LogEntry{
						Term: 1, Offset: i, Value: []byte(fmt.Sprintf("%s-%d", payload, i))}))
				}
			}
			if test.pending {
				roll()
			}

			readDone := make(chan *proto.LogEntry, 1)
			go func() {
				entry, err := readEntry(w, 50)
				assert.NoError(t, err)
				readDone <- entry
			}()
			<-reading

			if !test.pending {
				roll()
			}
			// The sync round closes the rolled-over segment
			require.NoError(t, w.Sync(context.Background()))
			_, _, _, err := segment.Read(50)
			require.ErrorIs(t, err, errs.ErrAlreadyClosed)

			release()
			if entry := <-readDone; assert.NotNil(t, entry) {
				assert.EqualValues(t, 50, entry.Offset)
				assert.Equal(t, fmt.Sprintf("%s-%d", payload, 50), string(entry.Value))
			}
			assert.NoError(t, w.Close())
			assert.NoError(t, f.Close())
		})
	}
}

// Tailing and random reads run while the appends roll the segments over, and
// the sync rounds close them: every read returns the entry it asked for.
func TestWalReadAtIndexConcurrentWithAppends(t *testing.T) {
	f := NewWalFactory(&FactoryOptions{
		BaseWalDir:  t.TempDir(),
		Retention:   time.Hour,
		SegmentSize: 16 * 1024,
		SyncData:    true,
	})
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)

	const entries = 5_000
	value := func(offset int64) string { return fmt.Sprintf("%s-%d", strings.Repeat("x", 100), offset) }

	var done atomic.Bool
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			r, err := w.NewReader(InvalidOffset)
			require.NoError(t, err)
			defer r.Close()
			for offset := int64(0); offset < entries; offset++ {
				for !r.HasNext() {
					runtime.Gosched()
				}
				entry, _, _, err := r.ReadNext()
				if !assert.NoError(t, err) {
					return
				}
				assert.Equal(t, value(offset), string(entry.Value))
			}
		})
	}
	wg.Go(func() {
		for !done.Load() {
			lastOffset := w.LastOffset()
			if lastOffset == InvalidOffset {
				runtime.Gosched()
				continue
			}
			offset := rand.Int64N(lastOffset + 1)
			entry, err := readEntry(w, offset)
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, value(offset), string(entry.Value))
		}
	})

	for offset := int64(0); offset < entries; offset++ {
		entry := &proto.LogEntry{Term: 1, Offset: offset, Value: []byte(value(offset))}
		// Sync every few entries only: the rolled-over segments stay pending
		// close in between
		if offset%10 == 9 {
			require.NoError(t, w.Append(entry))
		} else {
			require.NoError(t, w.AppendAsync(entry))
		}
	}
	require.NoError(t, w.Sync(context.Background()))
	done.Store(true)
	wg.Wait()

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// TruncateLog waits for the reads in flight: it zeroes the truncated records in
// place.
func TestWalTruncateLogWaitsForReads(t *testing.T) {
	f, w := createWal(t)
	appendEntries(t, w, 1, 0, 99, "entry")
	hook, reading, release := blockFirstCall()
	defer release()
	hookRecordReads(w.(*wal).currentSegment, hook)

	readDone := make(chan *proto.LogEntry, 1)
	go func() {
		entry, err := readEntry(w, 50)
		assert.NoError(t, err)
		readDone <- entry
	}()
	<-reading

	truncateDone := make(chan error, 1)
	go func() {
		_, err := w.TruncateLog(30)
		truncateDone <- err
	}()
	assert.Never(t, func() bool { return len(truncateDone) > 0 }, 100*time.Millisecond, 10*time.Millisecond,
		"the truncation did not wait for the read")

	release()
	if entry := <-readDone; assert.NotNil(t, entry) {
		assert.Equal(t, "entry-50", string(entry.Value))
	}
	assert.NoError(t, <-truncateDone)
	assert.EqualValues(t, 30, w.LastOffset())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestFactoryNewWalRejectsInvalidNamespace(t *testing.T) {
	f := NewTestWalFactory(t)

	_, err := f.NewWal("../escape", shard, nil)
	assert.ErrorContains(t, err, "path traversal")
}

func TestFactoryNewWalFailsWhenWalPathIsFile(t *testing.T) {
	dir := t.TempDir()
	p := walPath(dir, constant.DefaultNamespace, shard)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, nil, 0o644))
	f := NewWalFactory(&FactoryOptions{BaseWalDir: dir})

	_, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.ErrorContains(t, err, "failed to list files in wal directory")
}

func TestFactoryNewWalFailsOnMalformedSegmentFileName(t *testing.T) {
	dir := t.TempDir()
	p := walPath(dir, constant.DefaultNamespace, shard)
	require.NoError(t, os.MkdirAll(p, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(p, "segment"+codec.SupportedCodecs[0].GetTxnExtension()), nil, 0o644))
	f := NewWalFactory(&FactoryOptions{BaseWalDir: dir})

	_, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.Error(t, err)
}

func TestFactoryNewWalFailsWhenPrecedingSegmentIsCorrupted(t *testing.T) {
	f, w := newSmallWal(t)
	appendEntries(t, w, 1, 0, 19, "v")
	require.NoError(t, w.Close())
	segments, err := listAllSegments(vfs.OSFS{}, w.walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segments), 2)

	// Corrupt the last record of the segment preceding the read-write one,
	// whose crc the recovery chains the next appends from
	preceding := segments[len(segments)-2]
	c, err := newSegmentConfig(vfs.OSFS{}, w.walPath, preceding)
	require.NoError(t, err)
	idx, err := c.codec.ReadIndex(vfs.OSFS{}, c.idxPath)
	require.NoError(t, err)
	corruptPayload(t, c, codec.ReadInt(idx, uint32(len(idx)-4)))

	_, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.ErrorIs(t, err, errs.ErrDataCorrupted)
}

func TestFactoryNewWalFailsWhenTrailingSegmentCannotBeDiscarded(t *testing.T) {
	f, w := newSmallWal(t)
	appendEntries(t, w, 1, 0, 19, "v")
	require.NoError(t, w.Close())
	segments, err := listAllSegments(vfs.OSFS{}, w.walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segments), 2)

	// The segment preceding the read-write one lost its content, which makes
	// the recovery discard the read-write one
	preceding, err := newSegmentConfig(vfs.OSFS{}, w.walPath, segments[len(segments)-2])
	require.NoError(t, err)
	require.NoError(t, os.Remove(preceding.idxPath))
	info, err := os.Stat(preceding.txnPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(preceding.txnPath, make([]byte, info.Size()), 0o644))
	// A non-empty directory in place of its index file cannot be removed
	trailing, err := newSegmentConfig(vfs.OSFS{}, w.walPath, segments[len(segments)-1])
	require.NoError(t, err)
	require.NoError(t, vfs.RemoveFileIfExists(vfs.OSFS{}, trailing.idxPath))
	require.NoError(t, os.MkdirAll(filepath.Join(trailing.idxPath, "child"), 0o755))

	_, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.ErrorContains(t, err, "failed to recover wal")
	assert.DirExists(t, trailing.idxPath)
}

func TestWalRecoverWalFailsWhenReadOnlySegmentsCannotBeClosed(t *testing.T) {
	f, w := newSmallWal(t)
	appendEntries(t, w, 1, 0, 29, "v")
	require.NoError(t, w.Close())
	require.NoError(t, f.Close())
	segments, err := listAllSegments(vfs.OSFS{}, w.walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segments), 3)

	// The segment preceding the read-write one lost its content, which makes
	// the recovery discard the read-write one
	preceding, err := newSegmentConfig(vfs.OSFS{}, w.walPath, segments[len(segments)-2])
	require.NoError(t, err)
	require.NoError(t, os.Remove(preceding.idxPath))
	info, err := os.Stat(preceding.txnPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(preceding.txnPath, make([]byte, info.Size()), 0o644))
	// The first segment is open, and fails to close
	group, err := newReadOnlySegmentsGroup(vfs.OSFS{}, w.walPath)
	require.NoError(t, err)
	rc, err := group.Get(segments[0])
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	closeErr := errors.New("close failure")
	group.(*readOnlySegmentsGroup).openSegments.Put(segments[0], object.NewRefCount[ReadOnlySegment](
		&failingCloseSegment{ReadOnlySegment: rc.Get(), closeErr: closeErr}))

	recovering := &wal{fs: vfs.OSFS{}, walPath: w.walPath, readOnlySegments: group}
	assert.ErrorIs(t, recovering.recoverWal(), closeErr)
	assert.NoFileExists(t, txnPath(w.walPath, segments[len(segments)-1]))
}

func TestWalAppendRejectsNegativeOffset(t *testing.T) {
	f, w := createWal(t)

	err := w.AppendAsync(&proto.LogEntry{Term: 1, Offset: -1, Value: []byte("v")})
	assert.ErrorContains(t, err, "invalid next offset")
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

// A log entry with all its fields at their zero value serializes to no bytes,
// which a segment cannot hold.
func TestWalAppendRejectsEmptyEntry(t *testing.T) {
	f, w := createWal(t)

	assert.ErrorIs(t, w.AppendAsync(&proto.LogEntry{}), errs.ErrEmptyPayload)
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalAppendFailsToSerializeEntry(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	failpointErr := errors.New("failpoint")
	options := &FactoryOptions{BaseWalDir: t.TempDir(), Failpoints: failpoint.New(map[string]func() error{
		"appendAsync.marshal": func() error { return failpointErr },
	})}
	w, err := newWal(constant.DefaultNamespace, shard, options, nil, time2.SystemClock, time.Hour)
	require.NoError(t, err)

	assert.ErrorIs(t, w.AppendAsync(&proto.LogEntry{Term: 1, Offset: 0, Value: []byte("v")}), failpointErr)
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	assert.NoError(t, w.Close())
}

func TestWalAppendRejectsEntryLargerThanSegment(t *testing.T) {
	f, w := newSmallWal(t)
	appendEntries(t, w, 1, 0, 0, "v")

	err := w.Append(&proto.LogEntry{Term: 1, Offset: 1, Value: make([]byte, 256)})
	assert.ErrorIs(t, err, errs.ErrSegmentFull)
	assert.EqualValues(t, 0, w.LastOffset())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalAppendFailsWhenClearedSegmentCannotBeDeleted(t *testing.T) {
	f, w := newSmallWal(t)
	require.NoError(t, w.Clear())
	require.NoError(t, os.Chmod(w.walPath, 0o500))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(w.walPath, 0o700)) })

	err := w.Append(&proto.LogEntry{Term: 1, Offset: 5, Value: []byte("v")})
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalAppendAndSyncReportsAppendFailure(t *testing.T) {
	f, w := createWal(t)
	require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: 0, Value: []byte("v")}))

	done := make(chan error, 1)
	w.AppendAndSync(&proto.LogEntry{Term: 1, Offset: 5, Value: []byte("v")}, func(_ uint32, err error) {
		done <- err
	})
	assert.ErrorIs(t, <-done, errs.ErrInvalidArgument)
	assert.EqualValues(t, 0, w.LastOffset())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalAppendAsyncWithPreviousCrcRejectsWrongOffset(t *testing.T) {
	f, w := createWal(t)
	require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: 0, Value: []byte("v")}))
	previousCrc := uint32(1)

	crc, err := w.AppendAsyncWithPreviousCrc(&proto.LogEntry{Term: 1, Offset: 5, Value: []byte("v")}, &previousCrc)
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
	assert.Zero(t, crc)
	assert.EqualValues(t, 0, w.LastOffset())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalAppendAsyncWithPreviousCrcFailsWhenClosed(t *testing.T) {
	f, w := createWal(t)
	require.NoError(t, w.Close())
	previousCrc := uint32(1)

	crc, err := w.AppendAsyncWithPreviousCrc(&proto.LogEntry{Term: 1, Offset: 0, Value: []byte("v")}, &previousCrc)
	assert.ErrorIs(t, err, constant.ErrResourceUnavailable)
	assert.Zero(t, crc)

	assert.NoError(t, f.Close())
}

func TestWalReadAtIndexFailsOnUndecodableEntry(t *testing.T) {
	options := &FactoryOptions{BaseWalDir: t.TempDir(), SegmentSize: 1024}
	f := NewWalFactory(options)
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)
	require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: 0, Value: []byte("v")}))
	require.NoError(t, w.Close())

	// Append a record that is not a log entry: a truncated varint
	segment, err := newReadWriteSegment(vfs.OSFS{}, w.(*wal).walPath, 0, 1024, 0, nil)
	require.NoError(t, err)
	require.NoError(t, segment.Append(1, []byte{0xff}))
	require.NoError(t, segment.Close())

	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, w.LastOffset())

	entry, err := readEntry(w, 0)
	require.NoError(t, err)
	assert.Equal(t, "v", string(entry.Value))
	_, err = readEntry(w, 1)
	assert.Error(t, err)

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalReadAtIndexFailsWhenSegmentFileIsMissing(t *testing.T) {
	f := NewWalFactory(&FactoryOptions{BaseWalDir: t.TempDir(), SegmentSize: 128})
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)
	for i := int64(0); i < 20; i++ {
		require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: i, Value: fmt.Appendf(nil, "v-%d", i)}))
	}
	c, err := newSegmentConfig(vfs.OSFS{}, w.(*wal).walPath, 0)
	require.NoError(t, err)
	require.NoError(t, os.Remove(c.txnPath))

	_, err = readEntry(w, 0)
	assert.ErrorIs(t, err, os.ErrNotExist)
	entry, err := readEntry(w, 19)
	require.NoError(t, err)
	assert.Equal(t, "v-19", string(entry.Value))

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalTruncateLogRejectsOffsetPastLastEntry(t *testing.T) {
	f, w := createWal(t)
	appendEntries(t, w, 1, 0, 9, "v")

	_, err := w.TruncateLog(20)
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
	assert.EqualValues(t, 9, w.LastOffset())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalTruncateLogFailsWhenWalDirectoryCannotBeRemoved(t *testing.T) {
	f, w := newSmallWal(t)
	appendEntries(t, w, 1, 0, 2, "v")
	parent := filepath.Dir(w.walPath)
	require.NoError(t, os.Chmod(parent, 0o500))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(parent, 0o700)) })

	_, err := w.TruncateLog(InvalidOffset)
	assert.ErrorContains(t, err, "failed to clear wal")
	assert.DirExists(t, w.walPath)

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalTruncateLogFailsWhenClearingBelowAllSegmentsFails(t *testing.T) {
	f, w := newSmallWal(t)
	require.NoError(t, w.Clear())
	appendEntries(t, w, 1, 100, 119, "v")
	parent := filepath.Dir(w.walPath)
	require.NoError(t, os.Chmod(parent, 0o500))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(parent, 0o700)) })

	_, err := w.TruncateLog(50)
	assert.ErrorContains(t, err, "failed to clear wal")
	assert.DirExists(t, w.walPath)

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalTruncateLogFailsWhenCurrentSegmentCannotBeDeleted(t *testing.T) {
	f, w := newSmallWal(t)
	appendEntries(t, w, 1, 0, 19, "v")
	current, err := newSegmentConfig(vfs.OSFS{}, w.walPath, w.currentSegment.BaseOffset())
	require.NoError(t, err)
	require.NoError(t, os.Chmod(w.walPath, 0o500))
	t.Cleanup(func() { assert.NoError(t, os.Chmod(w.walPath, 0o700)) })

	_, err = w.TruncateLog(0)
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.FileExists(t, current.txnPath)

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalTruncateLogFailsWhenSegmentFileIsMissing(t *testing.T) {
	f, w := newSmallWal(t)
	appendEntries(t, w, 1, 0, 19, "v")
	segments, err := listAllSegments(vfs.OSFS{}, w.walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segments), 2)
	highestReadOnly := segments[len(segments)-2]
	c, err := newSegmentConfig(vfs.OSFS{}, w.walPath, highestReadOnly)
	require.NoError(t, err)
	require.NoError(t, os.Remove(c.txnPath))

	_, err = w.TruncateLog(highestReadOnly)
	assert.ErrorIs(t, err, os.ErrNotExist)

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestReaderReadNextFailsWhenClosed(t *testing.T) {
	f, w := createWal(t)
	appendEntries(t, w, 1, 0, 2, "v")

	forward, err := w.NewReader(InvalidOffset)
	require.NoError(t, err)
	reverse, err := w.NewReverseReader()
	require.NoError(t, err)
	for _, r := range []Reader{forward, reverse} {
		require.True(t, r.HasNext())
		require.NoError(t, r.Close())

		assert.False(t, r.HasNext())
		_, _, _, err = r.ReadNext()
		assert.ErrorIs(t, err, errs.ErrAlreadyClosed)
	}

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestReverseReaderReadNextFailsOnEmptyWal(t *testing.T) {
	f, w := createWal(t)

	r, err := w.NewReverseReader()
	require.NoError(t, err)
	_, _, _, err = r.ReadNext()
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
	assert.NoError(t, r.Close())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalAppendRetriesSegmentAfterClearThatCannotBeOpened(t *testing.T) {
	fs := newFaultFS()
	w := newFaultWal(t, fs, false)
	fs.failNth(opOpenFile, txnPath(w.walPath, 5), 1)

	err := w.AppendAsync(&proto.LogEntry{Term: 1, Offset: 5, Value: []byte("v")})
	assert.ErrorIs(t, err, errInjected)
	assert.EqualValues(t, InvalidOffset, w.LastOffset())

	require.NoError(t, w.AppendAsync(&proto.LogEntry{Term: 1, Offset: 5, Value: []byte("v")}))
	entry, err := readEntry(w, 5)
	require.NoError(t, err)
	assert.EqualValues(t, 5, entry.Offset)

	assert.NoError(t, w.Close())
}

func TestWalAppendFailsWhenRolledSegmentCannotBeClosed(t *testing.T) {
	for _, op := range []fsOp{opSync, opClose} {
		t.Run(string(op), func(t *testing.T) {
			fs := newFaultFS()
			// Without sync rounds, the rollover closes the rolled segment itself
			w := newFaultWal(t, fs, false)
			fs.failNth(op, txnPath(w.walPath, 0), 1)

			var err error
			offset := int64(0)
			for ; offset < 100; offset++ {
				if err = w.AppendAsync(&proto.LogEntry{Term: 1, Offset: offset, Value: []byte("v")}); err != nil {
					break
				}
			}
			assert.ErrorIs(t, err, errInjected)
			assert.ErrorContains(t, err, "failed to rollover segment")

			require.NoError(t, w.AppendAsync(&proto.LogEntry{Term: 1, Offset: offset, Value: []byte("v")}))
			for o := int64(0); o <= offset; o++ {
				entry, err := readEntry(w, o)
				require.NoError(t, err)
				assert.EqualValues(t, o, entry.Offset)
			}

			assert.NoError(t, w.Close())
		})
	}
}

func TestWalAppendRetriesRolloverAfterSegmentCreationFailure(t *testing.T) {
	w, _, offset := failSegmentCreationOnRollover(t)

	require.NoError(t, w.AppendAsync(&proto.LogEntry{Term: 1, Offset: offset, Value: []byte("v")}))
	for o := int64(0); o <= offset; o++ {
		entry, err := readEntry(w, o)
		require.NoError(t, err)
		assert.EqualValues(t, o, entry.Offset)
	}

	assert.NoError(t, w.Close())
}

func TestWalRecoversSegmentThatFailedToBeCreated(t *testing.T) {
	w, options, offset := failSegmentCreationOnRollover(t)
	require.NoError(t, w.Close())

	reopened, err := newWal(constant.DefaultNamespace, shard, options, nil, time2.SystemClock, time.Hour)
	require.NoError(t, err)
	assert.EqualValues(t, offset-1, reopened.LastOffset())

	require.NoError(t, reopened.AppendAsync(&proto.LogEntry{Term: 1, Offset: offset, Value: []byte("v")}))
	for o := int64(0); o <= offset; o++ {
		entry, err := readEntry(reopened, o)
		require.NoError(t, err)
		assert.EqualValues(t, o, entry.Offset)
	}

	assert.NoError(t, reopened.Close())
}

func TestWalSyncReportsRolledSegmentFailure(t *testing.T) {
	for _, op := range []fsOp{opSync, opFlush} {
		t.Run(string(op), func(t *testing.T) {
			fs := newFaultFS()
			w := newFaultWal(t, fs, true)
			appendAsyncUntilRolledOver(t, w)
			fs.failNth(op, txnPath(w.walPath, 0), 1)

			assert.ErrorIs(t, w.Sync(context.Background()), errInjected)
			assert.EqualValues(t, InvalidOffset, w.LastOffset())

			// The next round makes the rolled segment durable
			assert.NoError(t, w.Sync(context.Background()))
			assert.EqualValues(t, w.lastAppendedOffset.Load(), w.LastOffset())
			assert.NoError(t, w.Close())
		})
	}
}

// A cold path draining the rolled segment while the sync round flushes it
// closes it: the round leaves it alone.
func TestWalSyncSkipsRolledSegmentDrainedByClose(t *testing.T) {
	fs := newFaultFS()
	w := newFaultWal(t, fs, true)
	appendAsyncUntilRolledOver(t, w)

	rolled := txnPath(w.walPath, 0)
	flushing := make(chan struct{})
	draining := make(chan struct{})
	release := make(chan struct{})
	var flushes atomic.Int32
	fs.setHook(func(op fsOp, name string) error {
		if op == opFlush && name == rolled {
			switch flushes.Add(1) {
			case 1:
				// The sync round
				close(flushing)
				<-release
			case 2:
				// Close, which holds the wal lock from now on
				close(draining)
			default:
			}
		}
		return nil
	})

	syncErr := make(chan error, 1)
	go func() { syncErr <- w.Sync(context.Background()) }()
	<-flushing
	closeErr := make(chan error, 1)
	go func() { closeErr <- w.Close() }()
	<-draining
	close(release)

	assert.NoError(t, <-closeErr)
	assert.NoError(t, <-syncErr)
	assert.EqualValues(t, 2, flushes.Load())
}

func TestWalSyncDoesNotUndoConcurrentTruncateLog(t *testing.T) {
	fs := newFaultFS()
	w := newFaultWal(t, fs, true)
	appendEntries(t, w, 1, 0, 1, "v")
	require.NoError(t, w.AppendAsync(&proto.LogEntry{Term: 1, Offset: 2, Value: []byte("v")}))

	current := txnPath(w.walPath, 0)
	flushing := make(chan struct{})
	release := make(chan struct{})
	var flushes atomic.Int32
	fs.setHook(func(op fsOp, name string) error {
		if op == opFlush && name == current && flushes.Add(1) == 1 {
			close(flushing)
			<-release
		}
		return nil
	})

	syncErr := make(chan error, 1)
	go func() { syncErr <- w.Sync(context.Background()) }()
	<-flushing
	lastOffset, err := w.TruncateLog(1)
	require.NoError(t, err)
	assert.EqualValues(t, 1, lastOffset)
	close(release)

	assert.NoError(t, <-syncErr)
	assert.EqualValues(t, 1, w.LastOffset())
	assert.NoError(t, w.Close())
}

func TestWalClearFailsWhenSegmentCannotBeReopened(t *testing.T) {
	fs := newFaultFS()
	w := newFaultWal(t, fs, false)
	appendEntries(t, w, 1, 0, 2, "v")
	fs.failNth(opOpenFile, txnPath(w.walPath, 0), 1)

	assert.ErrorIs(t, w.Clear(), errInjected)

	assert.NoError(t, w.Close())
}

func TestWalClearFailsWhenWalDirectoryCannotBeListed(t *testing.T) {
	fs := newFaultFS()
	w := newFaultWal(t, fs, false)
	appendEntries(t, w, 1, 0, 2, "v")
	fs.failNth(opReadDir, w.walPath, 1)

	assert.ErrorIs(t, w.Clear(), errInjected)

	assert.NoError(t, w.Close())
}

func TestWalDeleteFailsWhenSegmentCannotBeClosed(t *testing.T) {
	fs := newFaultFS()
	w := newFaultWal(t, fs, false)
	appendEntries(t, w, 1, 0, 2, "v")
	fs.failNth(opClose, txnPath(w.walPath, 0), 1)

	assert.ErrorIs(t, w.Delete(), errInjected)
	assert.DirExists(t, w.walPath)
}

func TestWalTruncateLogFailsWhenRolledSegmentCannotBeSynced(t *testing.T) {
	fs := newFaultFS()
	w := newFaultWal(t, fs, true)
	appendAsyncUntilRolledOver(t, w)
	fs.failNth(opSync, txnPath(w.walPath, 0), 1)

	_, err := w.TruncateLog(0)
	assert.ErrorIs(t, err, errInjected)

	entry, err := readEntry(w, 0)
	require.NoError(t, err)
	assert.EqualValues(t, 0, entry.Offset)

	assert.NoError(t, w.Close())
}

func TestWalTruncateLogFailsOnFilesystemErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		op   fsOp
		// The truncation goes into the segment truncateFromEnd segments from
		// the end of the wal (the current one being 1), and the operation
		// fails on the one failFromEnd segments from the end
		truncateFromEnd int
		failFromEnd     int
	}{
		{"close-truncated-segment", opClose, 2, 2},
		{"truncate-reopened-segment", opFlush, 2, 2},
		{"delete-discarded-segment", opRemove, 3, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := newFaultFS()
			w := newFaultWal(t, fs, false)
			appendEntries(t, w, 1, 0, 29, "v")
			segments, err := listAllSegments(fs, w.walPath)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(segments), 3)
			fs.failNth(test.op, txnPath(w.walPath, segments[len(segments)-test.failFromEnd]), 1)

			_, err = w.TruncateLog(segments[len(segments)-test.truncateFromEnd])
			assert.ErrorIs(t, err, errInjected)

			assert.NoError(t, w.Close())
		})
	}
}

func TestWalTruncateLogFailsWhenDiscardedSegmentCannotBeClosed(t *testing.T) {
	f, w := newSmallWal(t)
	appendEntries(t, w, 1, 0, 29, "v")
	segments, err := listAllSegments(vfs.OSFS{}, w.walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segments), 3)
	closeErr := errors.New("close failure")
	var polls atomic.Int32
	w.Lock()
	w.readOnlySegments = &hookedSegmentsGroup{
		ReadOnlySegmentsGroup: w.readOnlySegments,
		pollHook: func(segment object.RefCount[ReadOnlySegment]) object.RefCount[ReadOnlySegment] {
			if polls.Add(1) > 1 {
				return segment
			}
			return object.NewRefCount[ReadOnlySegment](
				&failingCloseSegment{ReadOnlySegment: segment.Get(), closeErr: closeErr})
		},
	}
	w.Unlock()

	_, err = w.TruncateLog(0)
	assert.ErrorIs(t, err, closeErr)
	assert.NoFileExists(t, txnPath(w.walPath, segments[len(segments)-2]))

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestFactoryNewWalFailsWhenWalDirectoryCannotBeListed(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(walPath(dir, constant.DefaultNamespace, shard), 0755))
	fs := newFaultFS()
	// The second listing, by the recovery, after the one of the read-only group
	fs.failNth(opReadDir, walPath(dir, constant.DefaultNamespace, shard), 2)

	options := &FactoryOptions{BaseWalDir: dir, SegmentSize: 128, fs: fs}
	_, err := newWal(constant.DefaultNamespace, shard, options, nil, time2.SystemClock, time.Hour)
	assert.ErrorIs(t, err, errInjected)
}

func TestFactoryNewWalFailsWhenTrailingSegmentCannotBeStatted(t *testing.T) {
	fs := newFaultFS()
	w := newFaultWal(t, fs, false)
	appendEntries(t, w, 1, 0, 19, "v")
	require.NoError(t, w.Close())
	segments, err := listAllSegments(fs, w.walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segments), 2)

	// The segment preceding the read-write one lost its content, which makes
	// the recovery discard the read-write one
	preceding, err := newSegmentConfig(fs, w.walPath, segments[len(segments)-2])
	require.NoError(t, err)
	require.NoError(t, os.Remove(preceding.idxPath))
	require.NoError(t, os.Truncate(preceding.txnPath, 0))
	require.NoError(t, os.Truncate(preceding.txnPath, 128))
	fs.failNth(opStat, txnPath(w.walPath, segments[len(segments)-1]), 1)

	options := &FactoryOptions{BaseWalDir: filepath.Dir(filepath.Dir(w.walPath)), SegmentSize: 128, fs: fs}
	_, err = newWal(constant.DefaultNamespace, shard, options, nil, time2.SystemClock, time.Hour)
	assert.ErrorIs(t, err, errInjected)
	assert.FileExists(t, txnPath(w.walPath, segments[len(segments)-1]))
}

// hookedReadWriteSegment runs a hook before each read of the segment.
type hookedReadWriteSegment struct {
	ReadWriteSegment
	readHook func()
}

func (s *hookedReadWriteSegment) Read(offset int64) (payload []byte, previousCrc uint32, payloadCrc uint32, err error) {
	if s.readHook != nil {
		s.readHook()
	}
	return s.ReadWriteSegment.Read(offset)
}

// appendRolledOver leaves the wal with the entries 0..149, of 1 KiB: the
// first segment (128 KiB) holds the synced ones up to 99, and the async ones
// cross into the second segment, leaving the first one pending close.
func appendRolledOver(t *testing.T, w Wal) {
	t.Helper()
	appendEntries(t, w, 1, 0, 99, strings.Repeat("x", 1024))
	for i := int64(100); i < 150; i++ {
		require.NoError(t, w.AppendAsync(&proto.LogEntry{
			Term: 1, Offset: i, Value: []byte(fmt.Sprintf("%s-%d", strings.Repeat("x", 1024), i))}))
	}
	impl := w.(*wal)
	impl.RLock()
	defer impl.RUnlock()
	require.Len(t, impl.pendingCloseSegments, 1)
}

// newSmallWal returns a wal with segments of 128 bytes, and without sync
// rounds.
func newSmallWal(t *testing.T) (Factory, *wal) {
	t.Helper()
	f := NewWalFactory(&FactoryOptions{BaseWalDir: t.TempDir(), SegmentSize: 128})
	w, err := f.NewWal(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)
	return f, w.(*wal)
}

func readEntry(w Wal, offset int64) (*proto.LogEntry, error) {
	r, err := w.NewReader(offset - 1)
	if err != nil {
		return nil, err
	}
	entry, _, _, err := r.ReadNext()
	return entry, multierr.Append(err, r.Close())
}

func TestWalRolloverFailureKeepsWalUsable(t *testing.T) {
	f, w := createWal(t)
	walImpl := w.(*wal)

	// Fill the current segment (128KiB) with the wal directory read-only,
	// so that the rollover fails to create the next segment
	require.NoError(t, os.Chmod(walImpl.walPath, 0o500))
	t.Cleanup(func() { _ = os.Chmod(walImpl.walPath, 0o700) })
	payload := []byte(strings.Repeat("x", 1024))
	var offset int64
	var err error
	for ; offset < 1_000; offset++ {
		if err = w.Append(&proto.LogEntry{Term: 1, Offset: offset, Value: payload}); err != nil {
			break
		}
	}
	assert.ErrorContains(t, err, "failed to rollover segment")

	// The full segment stays current, and the next append rolls over again
	require.NoError(t, os.Chmod(walImpl.walPath, 0o700))
	assert.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: offset, Value: payload}))
	assert.EqualValues(t, offset, w.LastOffset())

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}

func TestWalTruncateLogReopenFailureKeepsWalClosable(t *testing.T) {
	f, w := createWal(t)
	walImpl := w.(*wal)

	payload := []byte(strings.Repeat("x", 1024))
	for i := int64(0); i < 300; i++ {
		require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: i, Value: payload}))
	}
	segments, err := listAllSegments(vfs.OSFS{}, walImpl.walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segments), 2)

	// Truncating into the read-only segment reopens it for writing, which
	// fails on a read-only file
	target := segments[len(segments)-2]
	c, err := newSegmentConfig(vfs.OSFS{}, walImpl.walPath, target)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(c.txnPath, 0o400))
	t.Cleanup(func() { _ = os.Chmod(c.txnPath, 0o600) })

	_, err = w.TruncateLog(target)
	assert.Error(t, err)

	assert.NotPanics(t, func() { _ = w.Close() })
	assert.NoError(t, f.Close())
}

// fsOp is an operation of faultFS, or of a file it opened.
type fsOp string

const (
	opStat      fsOp = "stat"
	opReadDir   fsOp = "readDir"
	opMkdirAll  fsOp = "mkdirAll"
	opOpenFile  fsOp = "openFile"
	opRemove    fsOp = "remove"
	opRemoveAll fsOp = "removeAll"
	opMap       fsOp = "map"
	opFlush     fsOp = "flush"
	opUnmap     fsOp = "unmap"
	opRead      fsOp = "read"
	opWrite     fsOp = "write"
	opSeek      fsOp = "seek"
	opSync      fsOp = "sync"
	opClose     fsOp = "close"
)

var errInjected = errors.New("injected filesystem failure")

// faultFS runs the operations on the operating system, after passing them to
// the hook along with the path of their file (of the mapped file, for a
// mapping): an error of the hook fails the operation. Close and Unmap release
// the resource before failing, as close(2) does. The wal goroutines call it
// concurrently with the test that sets the hook: mu guards the hook and the
// mappings.
type faultFS struct {
	vfs.OSFS

	mu       sync.Mutex
	hook     func(op fsOp, name string) error
	mappings map[*byte]string
}

func newFaultFS() *faultFS {
	return &faultFS{mappings: map[*byte]string{}}
}

func (fs *faultFS) setHook(hook func(op fsOp, name string) error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.hook = hook
}

// failNth fails the nth next call of op on the file at name with errInjected.
func (fs *faultFS) failNth(op fsOp, name string, n int32) {
	var calls atomic.Int32
	fs.setHook(func(o fsOp, oName string) error {
		if o == op && oName == name && calls.Add(1) == n {
			return errInjected
		}
		return nil
	})
}

func (fs *faultFS) check(op fsOp, name string) error {
	fs.mu.Lock()
	hook := fs.hook
	fs.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(op, name)
}

func (fs *faultFS) Stat(name string) (os.FileInfo, error) {
	if err := fs.check(opStat, name); err != nil {
		return nil, err
	}
	return fs.OSFS.Stat(name)
}

func (fs *faultFS) MkdirAll(path string, perm os.FileMode) error {
	if err := fs.check(opMkdirAll, path); err != nil {
		return err
	}
	return fs.OSFS.MkdirAll(path, perm)
}

func (fs *faultFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if err := fs.check(opOpenFile, name); err != nil {
		return nil, err
	}
	f, err := fs.OSFS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: f, fs: fs, name: name}, nil
}

func (fs *faultFS) Open(name string) (afero.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *faultFS) Remove(name string) error {
	if err := fs.check(opRemove, name); err != nil {
		return err
	}
	return fs.OSFS.Remove(name)
}

func (fs *faultFS) RemoveAll(path string) error {
	if err := fs.check(opRemoveAll, path); err != nil {
		return err
	}
	return fs.OSFS.RemoveAll(path)
}

func (fs *faultFS) Map(f vfs.File, length int, prot int) (mmap.MMap, error) {
	file := f.(*faultFile)
	if err := fs.check(opMap, file.name); err != nil {
		return nil, err
	}
	m, err := fs.OSFS.Map(file.File, length, prot)
	if err == nil && len(m) > 0 {
		fs.mu.Lock()
		fs.mappings[&m[0]] = file.name
		fs.mu.Unlock()
	}
	return m, err
}

func (fs *faultFS) Flush(m mmap.MMap) error {
	if err := fs.check(opFlush, fs.mappedFile(m)); err != nil {
		return err
	}
	return fs.OSFS.Flush(m)
}

func (fs *faultFS) Unmap(m *mmap.MMap) error {
	name := fs.mappedFile(*m)
	if len(*m) > 0 {
		fs.mu.Lock()
		delete(fs.mappings, &(*m)[0])
		fs.mu.Unlock()
	}
	return multierr.Append(fs.OSFS.Unmap(m), fs.check(opUnmap, name))
}

func (fs *faultFS) mappedFile(m mmap.MMap) string {
	if len(m) == 0 {
		return ""
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.mappings[&m[0]]
}

// faultFile is a file opened by faultFS.
type faultFile struct {
	vfs.File
	fs   *faultFS
	name string
}

func (f *faultFile) Read(p []byte) (int, error) {
	if err := f.fs.check(opRead, f.name); err != nil {
		return 0, err
	}
	return f.File.Read(p)
}

func (f *faultFile) Readdir(count int) ([]os.FileInfo, error) {
	if err := f.fs.check(opReadDir, f.name); err != nil {
		return nil, err
	}
	return f.File.Readdir(count)
}

func (f *faultFile) Write(p []byte) (int, error) {
	if err := f.fs.check(opWrite, f.name); err != nil {
		return 0, err
	}
	return f.File.Write(p)
}

func (f *faultFile) Seek(offset int64, whence int) (int64, error) {
	if err := f.fs.check(opSeek, f.name); err != nil {
		return 0, err
	}
	return f.File.Seek(offset, whence)
}

func (f *faultFile) Sync() error {
	if err := f.fs.check(opSync, f.name); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f *faultFile) Close() error {
	return multierr.Append(f.File.Close(), f.fs.check(opClose, f.name))
}

// newFaultWal returns a wal on fs, with segments of 128 bytes.
func newFaultWal(t *testing.T, fs *faultFS, syncData bool) *wal {
	t.Helper()
	options := &FactoryOptions{BaseWalDir: t.TempDir(), SegmentSize: 128, SyncData: syncData, fs: fs}
	w, err := newWal(constant.DefaultNamespace, shard, options, nil, time2.SystemClock, time.Hour)
	require.NoError(t, err)
	return w.(*wal)
}

// appendAsyncUntilRolledOver appends entries to the wal without syncing them,
// up to the first rollover.
func appendAsyncUntilRolledOver(t *testing.T, w *wal) {
	t.Helper()
	for offset := w.lastAppendedOffset.Load() + 1; w.currentSegment.BaseOffset() == 0; offset++ {
		require.NoError(t, w.AppendAsync(&proto.LogEntry{Term: 1, Offset: offset, Value: []byte("v")}))
	}
}

// failSegmentCreationOnRollover appends entries to a wal with segments of 128
// bytes, failing the extension of the next segment file to its size, and
// returns the wal, its options and the offset of the append that failed.
func failSegmentCreationOnRollover(t *testing.T) (Wal, *FactoryOptions, int64) {
	t.Helper()
	fs := newFaultFS()
	options := &FactoryOptions{BaseWalDir: t.TempDir(), SegmentSize: 128, fs: fs}
	w, err := newWal(constant.DefaultNamespace, shard, options, nil, time2.SystemClock, time.Hour)
	require.NoError(t, err)
	walPath := w.(*wal).walPath
	firstSegment := txnPath(walPath, 0)
	var writes atomic.Int32
	fs.setHook(func(op fsOp, name string) error {
		if op == opWrite && name != firstSegment && strings.HasSuffix(name, codec.SupportedCodecs[0].GetTxnExtension()) &&
			writes.Add(1) == 1 {
			return errInjected
		}
		return nil
	})

	offset := int64(0)
	for ; offset < 100; offset++ {
		if err = w.AppendAsync(&proto.LogEntry{Term: 1, Offset: offset, Value: []byte("v")}); err != nil {
			break
		}
	}
	require.ErrorIs(t, err, errInjected)
	require.ErrorContains(t, err, "failed to rollover segment")
	require.FileExists(t, txnPath(walPath, offset))
	return w, options, offset
}

// txnPath returns the path of the txn file of the segment at baseOffset in dir.
func txnPath(dir string, baseOffset int64) string {
	return segmentPath(dir, baseOffset) + codec.SupportedCodecs[0].GetTxnExtension()
}
