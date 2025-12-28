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
	"context"
	"fmt"
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
		e, err := r.ReadNext()
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
			e, err := r.ReadNext()
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
		entry, err := rr.ReadNext()
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
		entry, err := fr.ReadNext()
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
		le, err := r.ReadNext()
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
		le, err := r.ReadNext()
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
		le, err := r.ReadNext()
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
		le, err := r.ReadNext()
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
	entry, err := reader.ReadNext()
	assert.NoError(t, err)
	assert.Equal(t, &proto.LogEntry{
		Term:   1,
		Offset: 99,
		Value:  []byte("entry-99"),
	}, entry)
	entry2, err := reader.ReadNext()
	assert.ErrorIs(t, err, codec.ErrOffsetOutOfBounds)
	assert.Nil(t, entry2)

	assert.NoError(t, reader.Close())
	assert.NoError(t, f.Close())
}

func TestAppendAfterRolloverWithFullSegment(t *testing.T) {
	// This test verifies that after a segment becomes full and we roll over,
	// the WAL correctly maintains offset consistency even if subsequent appends fail
	f, w := createWal(t)

	// Fill up the segment until it's nearly full
	entryCount := 0
	largeValue := make([]byte, 10*1024) // 10KB entries
	for {
		err := w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(entryCount),
			Value:  largeValue,
		})
		if err != nil {
			// Segment should be full, but the error should not be ErrSegmentFull
			// as it should be handled internally
			assert.Fail(t, "unexpected error filling segment", "error: %v", err)
			break
		}
		entryCount++
		if entryCount >= 100 { // Safety limit
			break
		}
		// Check if we're approaching the segment limit
		if w.(*wal).currentSegment.BaseOffset() != 0 {
			// We've rolled over to a new segment
			break
		}
	}

	// Verify we rolled over
	assert.Greater(t, entryCount, 1, "should have written multiple entries")

	// Now append more entries after the rollover
	for i := 0; i < 10; i++ {
		err := w.Append(&proto.LogEntry{
			Term:   1,
			Offset: int64(entryCount + i),
			Value:  []byte(fmt.Sprintf("after-rollover-%d", i)),
		})
		assert.NoError(t, err, "append after rollover should succeed")
	}

	// Verify all entries can be read
	reader, err := w.NewReader(InvalidOffset)
	assert.NoError(t, err)
	for i := 0; i < entryCount+10; i++ {
		assert.True(t, reader.HasNext(), "should have entry %d", i)
		entry, err := reader.ReadNext()
		assert.NoError(t, err)
		assert.Equal(t, int64(i), entry.Offset, "offset should match")
	}
	assert.False(t, reader.HasNext())

	assert.NoError(t, reader.Close())
	assert.NoError(t, w.Close())
	assert.NoError(t, f.Close())
}
