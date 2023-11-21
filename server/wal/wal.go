// Copyright 2023 StreamNative, Inc.
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
	"github.com/pkg/errors"
	"github.com/streamnative/oxia/proto"
	"io"
	"time"
)

var (
	ErrorEntryNotFound     = errors.New("oxia: entry not found")
	ErrorOffsetOutOfBounds = errors.New("oxia: offset out of bounds")
	ErrorReaderClosed      = errors.New("oxia: reader already closed")
	ErrorInvalidNextOffset = errors.New("oxia: invalid next offset in wal")

	InvalidTerm   int64 = -1
	InvalidOffset int64 = -1
)

type WalFactoryOptions struct {
	BaseWalDir  string
	Retention   time.Duration
	SegmentSize int32
	SyncData    bool
}

var DefaultWalFactoryOptions = &WalFactoryOptions{
	BaseWalDir:  "data/wal",
	Retention:   1 * time.Hour,
	SegmentSize: 64 * 1024 * 1024,
	SyncData:    true,
}

type WalFactory interface {
	io.Closer
	NewWal(namespace string, shard int64, provider CommitOffsetProvider) (Wal, error)
}

// WalReader reads the Wal sequentially. It is not synchronized itself.
type WalReader interface {
	io.Closer
	// ReadNext returns the next entry in the log according to the Reader's direction.
	// If a forward/reverse WalReader has passed the end/beginning of the log, it returns [ErrorEntryNotFound]. To avoid this error, use HasNext.
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

	// Sync flushes all the entries in the wal to disk
	Sync(ctx context.Context) error

	// TruncateLog removes entries from the end of the log that have an ID greater than lastSafeEntry.
	TruncateLog(lastSafeEntry int64) (int64, error)

	// NewReader returns a new WalReader to traverse the log from the entry after `after` towards the log end
	NewReader(after int64) (WalReader, error)
	// NewReverseReader returns a new WalReader to traverse the log from the last entry towards the beginning
	NewReverseReader() (WalReader, error)

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
