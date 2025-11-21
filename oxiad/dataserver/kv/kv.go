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

package kv

import (
	"io"
	"testing"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/compare"

	"github.com/oxia-db/oxia/proto"
)

var (
	ErrKeyNotFound             = errors.New("oxia: key not found")
	MaxSnapshotChunkSize int64 = 1024 * 1024 // bytes

)

type WriteBatch interface {
	io.Closer

	Put(key string, value []byte) error
	Delete(key string) error
	Get(key string) ([]byte, io.Closer, error)
	FindLower(key string) (lowerKey string, err error)

	DeleteRange(lowerBound, upperBound string) error
	KeyRangeScan(lowerBound, upperBound string) (KeyIterator, error)
	RangeScan(lowerBound, upperBound string) (KeyValueIterator, error)

	// Count is the number of transactions that are currently in the batch
	Count() int

	// Size of all the transactions that are currently in the batch
	Size() int

	Commit() error
}

type KeyIterator interface {
	io.Closer

	Valid() bool
	Key() string
	Prev() bool
	Next() bool

	SeekGE(key string) bool
	SeekLT(key string) bool
}

type ReverseKeyIterator interface {
	io.Closer

	Valid() bool
	Key() string
	Prev() bool
}

type KeyValueIterator interface {
	KeyIterator

	Value() ([]byte, error)
}

type SnapshotChunk interface {
	Name() string
	Index() int32
	TotalCount() int32
	Content() []byte
}

type Snapshot interface {
	io.Closer

	BasePath() string

	Valid() bool
	Chunk() (SnapshotChunk, error)
	Next() bool
}

type SnapshotLoader interface {
	io.Closer

	AddChunk(fileName string, chunkIndex int32, chunkCount int32, content []byte) error

	// Complete signals that the snapshot is now complete
	Complete()
}

type ComparisonType proto.KeyComparisonType

const (
	ComparisonEqual ComparisonType = iota
	ComparisonFloor
	ComparisonCeiling
	ComparisonLower
	ComparisonHigher
)

type KV interface {
	io.Closer

	NewWriteBatch() WriteBatch

	Get(key string, comparisonType ComparisonType, opts IteratorOpts) (storedKey string, value []byte, closer io.Closer, err error)

	KeyRangeScan(lowerBound, upperBound string, opts IteratorOpts) (KeyIterator, error)
	KeyRangeScanReverse(lowerBound, upperBound string, opts IteratorOpts) (ReverseKeyIterator, error)
	KeyIterator(opts IteratorOpts) (KeyIterator, error)

	RangeScan(lowerBound, upperBound string, opts IteratorOpts) (KeyValueIterator, error)

	Snapshot() (Snapshot, error)

	Flush() error

	Delete() error
}
type FactoryOptions struct {
	DataDir     string
	CacheSizeMB int64
	UseWAL      bool
	SyncData    bool
}

var DefaultFactoryOptions = &FactoryOptions{
	DataDir:     "data",
	CacheSizeMB: 100,
	UseWAL:      true,
	SyncData:    true,
}

func (fo *FactoryOptions) EnsureDefaults() {
	if fo.DataDir == "" {
		fo.DataDir = DefaultFactoryOptions.DataDir
	}

	if fo.CacheSizeMB == 0 {
		fo.CacheSizeMB = DefaultFactoryOptions.CacheSizeMB
	}
}

func NewFactoryOptionsForTest(t *testing.T) *FactoryOptions {
	t.Helper()
	return &FactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 1,
		UseWAL:      false,
		SyncData:    false,
	}
}

type Factory interface {
	io.Closer

	NewKV(namespace string, shardId int64, keyEncoder compare.Encoder) (KV, error)

	NewSnapshotLoader(namespace string, shardId int64) (SnapshotLoader, error)
}

type IteratorOpts struct {
	IncludeInternalKeys bool
}

var (
	NoInternalKeys   = IteratorOpts{IncludeInternalKeys: false}
	ShowInternalKeys = IteratorOpts{IncludeInternalKeys: true}
)
