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
	"container/list"
	"context"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/proto"
)

const (
	logEntryCacheSizeInBytes int = 2 * 1024 * 1024 // 2MB
)

var (
	ErrEntryNotFound     = errors.New("oxia: entry not found")
	ErrReaderClosed      = errors.New("oxia: reader already closed")
	ErrInvalidNextOffset = errors.New("oxia: invalid next offset in wal")
	ErrSegmentFull       = errors.New("oxia: current segment is full")

	InvalidTerm   int64 = -1
	InvalidOffset int64 = -1
)

type FactoryOptions struct {
	BaseWalDir  string
	Retention   time.Duration
	SegmentSize int32
	SyncData    bool
}

var DefaultFactoryOptions = &FactoryOptions{
	BaseWalDir:  "data/wal",
	Retention:   1 * time.Hour,
	SegmentSize: 64 * 1024 * 1024,
	SyncData:    true,
}

type Factory interface {
	io.Closer
	NewWal(namespace string, shard int64, provider CommitOffsetProvider) (Wal, error)
}

// Reader reads the Wal sequentially. It is not synchronized itself.
type Reader interface {
	io.Closer
	// ReadNext returns the next entry in the log according to the Reader's direction.
	// If a forward/reverse WalReader has passed the end/beginning of the log, it returns [ErrorEntryNotFound].
	// To avoid this error, use HasNext.
	ReadNext() (*proto.LogEntry, error)
	// HasNext returns true if there is an entry to read.
	HasNext() bool
}

type Wal interface {
	io.Closer
	// Append writes an entry to the end of the log.
	// The wal is synced when Append returns
	Append(entry *proto.LogEntry) error

	// AppendAsync an entry without syncing the WAL
	// Caller should use Sync to make the entry visible
	AppendAsync(entry *proto.LogEntry) error

	// AppendAndSync an entry and forces the sync on the WAL
	// The operation is perfomed in background and the callback is
	// triggered when it's completed
	AppendAndSync(entry *proto.LogEntry, callback func(err error))

	// Sync flushes all the entries in the wal to disk
	Sync(ctx context.Context) error

	// TruncateLog removes entries from the end of the log that have an ID greater than lastSafeEntry.
	TruncateLog(lastSafeEntry int64) (int64, error)

	// NewReader returns a new WalReader to traverse the log from the entry after `after` towards the log end
	NewReader(after int64) (Reader, error)
	// NewReverseReader returns a new WalReader to traverse the log from the last entry towards the beginning
	NewReverseReader() (Reader, error)

	// LastOffset Return the offset of the last entry committed to the WAL
	// Return InvalidOffset if the WAL is empty
	LastOffset() int64

	// FirstOffset Return the offset of the first valid entry that is present in the WAL
	// Return InvalidOffset if the WAL is empty
	FirstOffset() int64

	// Clear removes all the entries in the WAL
	Clear() error

	// Delete all the files and directories of the wal
	Delete() error
}

type cacheValue struct {
	value  *proto.LogEntry
	length int
}

// LogEntryCache is a FIFO cache for log entries
type LogEntryCache struct {
	sync.RWMutex
	// Map to store offset to cacheValue mapping for O(1) lookup
	index map[int64]*cacheValue
	// List to maintain insertion order for FIFO (only storing offsets)
	fifo      *list.List
	cacheSize int
	cacheUsed int
}

func NewLogEntryCache(cacheSize int) *LogEntryCache {
	return &LogEntryCache{
		index:     make(map[int64]*cacheValue, 1024),
		fifo:      list.New(),
		cacheSize: cacheSize,
		cacheUsed: 0,
	}
}

func (t *LogEntryCache) Get(offset int64) (*proto.LogEntry, bool) {
	t.RLock()
	defer t.RUnlock()

	if ce, ok := t.index[offset]; ok {
		return ce.value.CloneVT(), true
	}
	return nil, false
}

func (t *LogEntryCache) Add(offset int64, entry *proto.LogEntry, length int) {
	t.Lock()
	defer t.Unlock()

	// If offset already exists, skip update
	if _, exists := t.index[offset]; !exists {
		// Ensure enough space
		t.ensureCacheSize(length)
		if t.cacheUsed+length > t.cacheSize {
			return
		}
		// Add new element to the end of the queue
		t.fifo.PushBack(offset)
		// Record in index
		t.index[offset] = &cacheValue{
			value:  entry,
			length: length,
		}
		t.cacheUsed += length
	}
}

func (t *LogEntryCache) ensureCacheSize(length int) {
	// Remove the oldest elements (FIFO order) until there is enough space
	for t.fifo.Len() > 0 && t.cacheUsed+length > t.cacheSize {
		// Get the first element (earliest inserted)
		oldest := t.fifo.Front()
		if oldest == nil {
			return
		}

		// Remove from list
		t.fifo.Remove(oldest)

		// Remove from map
		offset := oldest.Value.(int64) //nolint
		cv := t.index[offset]
		delete(t.index, offset)

		// Update used size
		t.cacheUsed -= cv.length
	}
}
