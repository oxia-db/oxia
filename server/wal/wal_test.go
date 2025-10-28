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

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/proto"
	"github.com/oxia-db/oxia/server/wal/codec"
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

func TestLogEntryCache_NewLogEntryCache(t *testing.T) {
	cache := NewLogEntryCache(1024)
	assert.NotNil(t, cache)
	assert.Equal(t, 1024, cache.cacheSize)
	assert.Equal(t, 0, cache.cacheUsed)
	assert.NotNil(t, cache.index)
	assert.NotNil(t, cache.fifo)
}

func TestLogEntryCache_AddAndGet(t *testing.T) {
	cache := NewLogEntryCache(1024)

	// Test adding and getting an entry
	entry := &proto.LogEntry{
		Term:   1,
		Offset: 100,
		Value:  []byte("test data"),
	}

	cache.Add(100, entry, len(entry.Value))

	// Test getting existing entry
	retrieved, ok := cache.Get(100)
	assert.True(t, ok)
	assert.Equal(t, entry.Term, retrieved.Term)
	assert.Equal(t, entry.Offset, retrieved.Offset)
	assert.Equal(t, entry.Value, retrieved.Value)

	// Test getting non-existing entry
	_, ok = cache.Get(200)
	assert.False(t, ok)
}

func TestLogEntryCache_DuplicateAdd(t *testing.T) {
	cache := NewLogEntryCache(1024)

	entry1 := &proto.LogEntry{
		Term:   1,
		Offset: 100,
		Value:  []byte("test data 1"),
	}

	entry2 := &proto.LogEntry{
		Term:   2,
		Offset: 100,
		Value:  []byte("test data 2 "),
	}

	// Add the same offset twice
	cache.Add(100, entry1, len(entry1.Value))
	firstLen := cache.cacheUsed

	cache.Add(100, entry2, len(entry2.Value))
	secondLen := cache.cacheUsed

	// Cache size should not change when adding duplicate
	assert.Equal(t, firstLen, secondLen)

	// Should still return the first entry
	retrieved, ok := cache.Get(100)
	assert.True(t, ok)
	assert.Equal(t, entry1, retrieved)
}

func TestLogEntryCache_ValueLengthGreaterThanCacheSize(t *testing.T) {
	size := 5
	cache := NewLogEntryCache(size)
	entry := &proto.LogEntry{
		Offset: 100,
		Value:  make([]byte, size+1),
	}

	cache.Add(entry.Offset, entry, len(entry.Value))
	assert.Equal(t, 0, cache.cacheUsed)
}

func TestLogEntryCache_CacheSizeLimit(t *testing.T) {
	cacheSize := 100
	cache := NewLogEntryCache(cacheSize)

	// Add entries until cache is full
	for i := 0; i < 20; i++ {
		entry := &proto.LogEntry{
			Term:   int64(i),
			Offset: int64(i),
			Value:  make([]byte, 10), // 10 bytes each
		}
		cache.Add(int64(i), entry, len(entry.Value))
	}

	// Cache used should not exceed cacheSize
	assert.True(t, cache.cacheUsed <= cacheSize)

	// Older entries should have been evicted
	_, ok := cache.Get(0)
	assert.False(t, ok) // First entry should be evicted
}

func TestLogEntryCache_FIFORemoval(t *testing.T) {
	cacheSize := 50
	cache := NewLogEntryCache(cacheSize)

	// Add 3 entries
	entries := make([]*proto.LogEntry, 3)
	for i := 0; i < 3; i++ {
		entries[i] = &proto.LogEntry{
			Term:   int64(i),
			Offset: int64(i),
			Value:  []byte("data"),
		}
		cache.Add(int64(i), entries[i], len(entries[i].Value))
	}

	// Add a large entry that forces eviction
	largeEntry := &proto.LogEntry{
		Term:   4,
		Offset: 4,
		Value:  make([]byte, 45), // This should evict first 2 entries
	}
	cache.Add(4, largeEntry, len(largeEntry.Value))

	// Check that first entries were evicted (FIFO)
	_, ok := cache.Get(0)
	assert.False(t, ok)

	_, ok = cache.Get(1)
	assert.False(t, ok)

	// Check that last entry is still there
	_, ok = cache.Get(2)
	assert.True(t, ok)

	// Check that new entry is there
	_, ok = cache.Get(4)
	assert.True(t, ok)
}

func TestLogEntryCache_EmptyCache(t *testing.T) {
	cache := NewLogEntryCache(1024)

	// Test getting from empty cache
	_, ok := cache.Get(1)
	assert.False(t, ok)

	// Test ensureCacheSize on empty cache
	cache.ensureCacheSize(100)
	assert.Equal(t, 0, cache.cacheUsed)
}

func TestLogEntryCache_ConcurrentAccess(t *testing.T) {
	cache := NewLogEntryCache(1024)

	// This test mainly checks that there are no panics or race conditions
	done := make(chan bool)

	// Start multiple goroutines for concurrent access
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 1000; j++ {
				entry := &proto.LogEntry{
					Term:   int64(j),
					Offset: int64(j),
					Value:  []byte("concurrent test"),
				}
				cache.Add(int64(id*1000+j), entry, len(entry.Value))
				cache.Get(int64(id*1000 + j))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
