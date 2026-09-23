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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"

	"github.com/oxia-db/oxia/common/constant"

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
	assert.ErrorIs(t, err, ErrInvalidNextOffset)

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

	assert.NoError(t, w.(*wal).trim(50))

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
	assert.ErrorIs(t, err, ErrEntryNotFound)
	assert.Nil(t, r)

	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
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
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)
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
	assert.NoError(t, w2.AppendAsyncWithPreviousCrc(&proto.LogEntry{
		Term:   1,
		Offset: 10,
		Value:  []byte("entry-10"),
	}, &crcAt9))
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

	assert.NoError(t, w1.Close())
	assert.NoError(t, f1.Close())
	assert.NoError(t, w2.Close())
	assert.NoError(t, f2.Close())
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
	segments, err := listAllSegments(basePath)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(segments), 3)

	// Simulate the power loss: the second-to-last segment never reached the
	// disk (all zeroes, no index), while the last segment kept its content
	lost := segments[len(segments)-2]
	lostConfig, err := newSegmentConfig(basePath, lost)
	assert.NoError(t, err)
	info, err := os.Stat(lostConfig.txnPath)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(lostConfig.txnPath, make([]byte, info.Size()), 0644))
	assert.NoError(t, os.Remove(lostConfig.idxPath))
	lastConfig, err := newSegmentConfig(basePath, segments[len(segments)-1])
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
	segments, err := listAllSegments(basePath)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(segments), 3)
	for _, segment := range segments {
		c, err := newSegmentConfig(basePath, segment)
		assert.NoError(t, err)
		info, err := os.Stat(c.txnPath)
		assert.NoError(t, err)
		assert.NoError(t, os.WriteFile(c.txnPath, make([]byte, info.Size()), 0644))
		assert.NoError(t, codec.RemoveFileIfExists(c.idxPath))
	}

	w, err = f.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	assert.Equal(t, InvalidOffset, w.FirstOffset())
	assert.Equal(t, InvalidOffset, w.LastOffset())

	// Only the first segment file survives, and the wal is appendable from
	// scratch
	remaining, err := listAllSegments(basePath)
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
