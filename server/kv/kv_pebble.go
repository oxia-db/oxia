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

package kv

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/bloom"

	"github.com/streamnative/oxia/common/compare"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/metrics"
)

var (
	OxiaSlashSpanComparer = &pebble.Comparer{
		Compare:            compare.CompareWithSlash,
		Equal:              pebble.DefaultComparer.Equal,
		AbbreviatedKey:     pebble.DefaultComparer.AbbreviatedKey,
		FormatKey:          pebble.DefaultComparer.FormatKey,
		FormatValue:        pebble.DefaultComparer.FormatValue,
		Separator:          pebble.DefaultComparer.Separator,
		Split:              pebble.DefaultComparer.Split,
		Successor:          pebble.DefaultComparer.Successor,
		ImmediateSuccessor: pebble.DefaultComparer.ImmediateSuccessor,
		Name:               "oxia-slash-spans",
	}
)

type PebbleFactory struct {
	dataDir string
	cache   *pebble.Cache
	options *FactoryOptions

	gaugeCacheSize metrics.Gauge
}

func NewPebbleKVFactory(options *FactoryOptions) (Factory, error) {
	if options == nil {
		options = DefaultFactoryOptions
	}
	cacheSizeMB := options.CacheSizeMB
	if cacheSizeMB == 0 {
		cacheSizeMB = DefaultFactoryOptions.CacheSizeMB
	}

	dataDir := options.DataDir
	if dataDir == "" {
		dataDir = DefaultFactoryOptions.DataDir
	}

	cache := pebble.NewCache(cacheSizeMB * 1024 * 1024)

	pf := &PebbleFactory{
		dataDir: dataDir,
		options: options,

		// Share a single cache instance across the databases for all the shards
		cache: cache,

		gaugeCacheSize: metrics.NewGauge("oxia_server_kv_pebble_max_cache_size",
			"The max size configured for the Pebble block cache in bytes",
			metrics.Bytes, map[string]any{}, func() int64 {
				return options.CacheSizeMB * 1024 * 1024
			}),
	}

	// Cleanup leftover snapshots from previous runs
	if err := pf.cleanupSnapshots(); err != nil {
		return nil, errors.Wrap(err, "failed to delete database snapshots")
	}

	return pf, nil
}

func (p *PebbleFactory) cleanupSnapshots() error {
	snapshotsPath := filepath.Join(p.dataDir, "snapshots")
	_, err := os.Stat(snapshotsPath)

	if err == nil {
		return os.RemoveAll(snapshotsPath)
	} else if os.IsNotExist(err) {
		// Snapshot directory does not exist, nothing to do
		return nil
	}

	return err
}

func (p *PebbleFactory) Close() error {
	p.gaugeCacheSize.Unregister()
	p.cache.Unref()
	return nil
}

func (p *PebbleFactory) NewKV(namespace string, shardId int64) (KV, error) {
	return newKVPebble(p, namespace, shardId)
}

func (p *PebbleFactory) NewSnapshotLoader(namespace string, shardId int64) (SnapshotLoader, error) {
	return newPebbleSnapshotLoader(p, namespace, shardId)
}

func (p *PebbleFactory) getKVPath(namespace string, shard int64) string {
	if namespace == "" {
		slog.Warn(
			"Missing namespace when getting KV path",
			slog.Int64("shard", shard),
		)
		os.Exit(1)
	}

	return filepath.Join(p.dataDir, namespace, fmt.Sprint("shard-", shard))
}

type Pebble struct {
	factory         *PebbleFactory
	namespace       string
	shardId         int64
	dataDir         string
	db              *pebble.DB
	snapshotCounter atomic.Int64

	dbMetrics          func() *pebble.Metrics
	gauges             []metrics.Gauge
	batchCommitLatency metrics.LatencyHistogram

	writeBytes  metrics.Counter
	writeCount  metrics.Counter
	readBytes   metrics.Counter
	readCount   metrics.Counter
	readLatency metrics.LatencyHistogram
	writeErrors metrics.Counter
	readErrors  metrics.Counter

	batchSizeHisto  metrics.Histogram
	batchCountHisto metrics.Histogram
}

func newKVPebble(factory *PebbleFactory, namespace string, shardId int64) (KV, error) {
	labels := metrics.LabelsForShard(namespace, shardId)
	pb := &Pebble{
		factory:   factory,
		namespace: namespace,
		shardId:   shardId,
		dataDir:   factory.dataDir,

		batchCommitLatency: metrics.NewLatencyHistogram("oxia_server_kv_batch_commit_latency",
			"The latency for committing a batch into the database", labels),
		readLatency: metrics.NewLatencyHistogram("oxia_server_kv_read_latency",
			"The latency for reading a value from the database", labels),
		writeBytes: metrics.NewCounter("oxia_server_kv_write",
			"The amount of bytes written into the database", metrics.Bytes, labels),
		writeCount: metrics.NewCounter("oxia_server_kv_write_ops",
			"The amount of write operations", "count", labels),
		readBytes: metrics.NewCounter("oxia_server_kv_read",
			"The amount of bytes read from the database", metrics.Bytes, labels),
		readCount: metrics.NewCounter("oxia_server_kv_write_ops",
			"The amount of write operations", "count", labels),
		writeErrors: metrics.NewCounter("oxia_server_kv_write_errors",
			"The count of write operations errors", "count", labels),
		readErrors: metrics.NewCounter("oxia_server_kv_read_errors",
			"The count of read operations errors", "count", labels),

		batchSizeHisto: metrics.NewBytesHistogram("oxia_server_kv_batch_size",
			"The size in bytes for a given batch", labels),
		batchCountHisto: metrics.NewCountHistogram("oxia_server_kv_batch_count",
			"The number of operations in a given batch", labels),
	}

	pbOptions := &pebble.Options{
		Cache:        factory.cache,
		Comparer:     OxiaSlashSpanComparer,
		MemTableSize: 32 * 1024 * 1024,
		Levels: []pebble.LevelOptions{
			{
				BlockSize:      64 * 1024,
				Compression:    pebble.NoCompression,
				TargetFileSize: 32 * 1024 * 1024,
				FilterPolicy:   bloom.FilterPolicy(10),
				FilterType:     pebble.TableFilter,
			}, {
				BlockSize:      64 * 1024,
				Compression:    pebble.ZstdCompression,
				TargetFileSize: 64 * 1024 * 1024,
				FilterPolicy:   bloom.FilterPolicy(10),
				FilterType:     pebble.TableFilter,
			},
		},
		FS:         vfs.Default,
		DisableWAL: true,
		Logger: &pebbleLogger{
			slog.With(
				slog.String("component", "pebble"),
				slog.Int64("shard", shardId),
			),
		},

		FormatMajorVersion: pebble.FormatNewest,
	}

	if factory.options.InMemory {
		pbOptions.FS = vfs.NewMem()
	}

	dbPath := factory.getKVPath(namespace, shardId)
	db, err := pebble.Open(dbPath, pbOptions)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open database at %s", dbPath)
	}

	pb.db = db

	// Cache the calls to db.Metrics() which are common to all the gauges
	pb.dbMetrics = common.Memoize(func() *pebble.Metrics {
		return pb.db.Metrics()
	}, 5*time.Second)

	pb.gauges = []metrics.Gauge{
		metrics.NewGauge("oxia_server_kv_pebble_block_cache_used",
			"The size of the block cache used by a given db shard",
			metrics.Bytes, labels, func() int64 {
				return pb.dbMetrics().BlockCache.Size
			}),
		metrics.NewGauge("oxia_server_kv_pebble_block_cache_hits",
			"The number of hits in the block cache",
			"count", labels, func() int64 {
				return pb.dbMetrics().BlockCache.Hits
			}),
		metrics.NewGauge("oxia_server_kv_pebble_block_cache_misses",
			"The number of misses in the block cache",
			"count", labels, func() int64 {
				return pb.dbMetrics().BlockCache.Misses
			}),
		metrics.NewGauge("oxia_server_kv_pebble_read_iterators",
			"The number of iterators open",
			"value", labels, func() int64 {
				return pb.dbMetrics().TableIters
			}),

		metrics.NewGauge("oxia_server_kv_pebble_compactions_total",
			"The number of compactions operations",
			"count", labels, func() int64 {
				return pb.dbMetrics().Compact.Count
			}),
		metrics.NewGauge("oxia_server_kv_pebble_compaction_debt",
			"The estimated number of bytes that need to be compacted",
			metrics.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().Compact.EstimatedDebt)
			}),
		metrics.NewGauge("oxia_server_kv_pebble_flush_total",
			"The total number of db flushes",
			"count", labels, func() int64 {
				return pb.dbMetrics().Flush.Count
			}),
		metrics.NewGauge("oxia_server_kv_pebble_flush",
			"The total amount of bytes flushed into the db",
			metrics.Bytes, labels, func() int64 {
				return pb.dbMetrics().Flush.WriteThroughput.Bytes
			}),
		metrics.NewGauge("oxia_server_kv_pebble_memtable_size",
			"The size of the memtable",
			metrics.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().MemTable.Size)
			}),

		metrics.NewGauge("oxia_server_kv_pebble_disk_space",
			"The total size of all the db files",
			metrics.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().DiskSpaceUsage())
			}),
		metrics.NewGauge("oxia_server_kv_pebble_num_files_total",
			"The total number of files for the db",
			"count", labels, func() int64 {
				return pb.dbMetrics().Total().NumFiles
			}),
		metrics.NewGauge("oxia_server_kv_pebble_read",
			"The total amount of bytes read at this db level",
			metrics.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().Total().BytesRead)
			}),
		metrics.NewGauge("oxia_server_kv_pebble_write_amplification_percent",
			"The total amount of bytes read at this db level",
			"percent", labels, func() int64 {
				t := pb.dbMetrics().Total()
				return int64(t.WriteAmp() * 100)
			}),
	}

	// Add the per-LSM level metrics
	for i := 0; i < 7; i++ {
		level := i
		labels := map[string]any{
			"shard": shardId,
			"level": level,
		}

		pb.gauges = append(pb.gauges,
			metrics.NewGauge("oxia_server_kv_pebble_per_level_num_files",
				"The total number of files at this db level",
				"count", labels, func() int64 {
					return pb.dbMetrics().Levels[level].NumFiles
				}),
			metrics.NewGauge("oxia_server_kv_pebble_per_level_size",
				"The total size in bytes of the files at this db level",
				metrics.Bytes, labels, func() int64 {
					return pb.dbMetrics().Levels[level].Size
				}),
			metrics.NewGauge("oxia_server_kv_pebble_per_level_read",
				"The total amount of bytes read at this db level",
				metrics.Bytes, labels, func() int64 {
					return int64(pb.dbMetrics().Levels[level].BytesRead)
				}),
		)
	}

	return pb, nil
}

func (p *Pebble) Close() error {
	for _, g := range p.gauges {
		g.Unregister()
	}

	if err := p.db.Flush(); err != nil {
		return err
	}
	return p.db.Close()
}

func (p *Pebble) Delete() error {
	return multierr.Combine(
		p.Close(),
		os.RemoveAll(p.factory.getKVPath(p.namespace, p.shardId)),
	)
}

func (p *Pebble) Flush() error {
	return p.db.Flush()
}

func (p *Pebble) NewWriteBatch() WriteBatch {
	return &PebbleBatch{p: p, b: p.db.NewIndexedBatch()}
}

func (p *Pebble) getFloor(key string) (returnedKey string, value []byte, closer io.Closer, err error) {
	// There is no <= comparison in Pebble
	// We have to first check for == and then for <
	value, closer, err = p.db.Get([]byte(key))
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return "", nil, nil, err
	}

	if err == nil {
		// We found record with key ==
		return key, value, closer, nil
	}

	// Do < search
	return p.getLower(key)
}

func (p *Pebble) getCeiling(key string) (returnedKey string, value []byte, closer io.Closer, err error) {
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(key),
	})
	if err != nil {
		return "", nil, nil, err
	}

	if !it.First() {
		return "", nil, nil, multierr.Combine(it.Close(), pebble.ErrNotFound)
	}

	returnedKey = string(it.Key())
	value, err = it.ValueAndErr()
	return returnedKey, value, it, err
}

func (p *Pebble) getLower(key string) (returnedKey string, value []byte, closer io.Closer, err error) {
	it, err := p.db.NewIter(&pebble.IterOptions{
		UpperBound: []byte(key),
	})
	if err != nil {
		return "", nil, nil, err
	}

	if !it.Last() {
		return "", nil, nil, multierr.Combine(it.Close(), pebble.ErrNotFound)
	}

	returnedKey = string(it.Key())
	value, err = it.ValueAndErr()
	return returnedKey, value, it, err
}

func (p *Pebble) getHigher(key string) (returnedKey string, value []byte, closer io.Closer, err error) {
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(key),
	})
	if err != nil {
		return "", nil, nil, err
	}

	// The iterator might be positioned exactly on the key. Since we're looking for strict `x > y` comparison,
	// we will have to skip to the next record
	it.First()
	if !it.First() {
		return "", nil, nil, multierr.Combine(it.Close(), pebble.ErrNotFound)
	}

	returnedKey = string(it.Key())
	if returnedKey == key {
		// We found the same key, skip it
		if !it.Next() {
			return "", nil, nil, multierr.Combine(it.Close(), pebble.ErrNotFound)
		}
		returnedKey = string(it.Key())
	}

	value, err = it.ValueAndErr()
	return returnedKey, value, it, err
}

func (p *Pebble) Get(key string, comparisonType ComparisonType) (returnedKey string, value []byte, closer io.Closer, err error) {
	switch comparisonType {
	case ComparisonEqual:
		value, closer, err = p.db.Get([]byte(key))
		if err == nil {
			returnedKey = key
		}
	case ComparisonFloor:
		returnedKey, value, closer, err = p.getFloor(key)
	case ComparisonCeiling:
		returnedKey, value, closer, err = p.getCeiling(key)
	case ComparisonLower:
		returnedKey, value, closer, err = p.getLower(key)
	case ComparisonHigher:
		returnedKey, value, closer, err = p.getHigher(key)
	}

	if errors.Is(err, pebble.ErrNotFound) {
		err = ErrKeyNotFound
	} else if err != nil {
		p.readErrors.Inc()
	}
	return returnedKey, value, closer, err
}

func (p *Pebble) KeyRangeScan(lowerBound, upperBound string) (KeyIterator, error) {
	return p.RangeScan(lowerBound, upperBound)
}

func (p *Pebble) KeyRangeScanReverse(lowerBound, upperBound string) (ReverseKeyIterator, error) {
	opts := &pebble.IterOptions{}
	if lowerBound != "" {
		opts.LowerBound = []byte(lowerBound)
	}
	if upperBound != "" {
		opts.UpperBound = []byte(upperBound)
	}
	pbit, err := p.db.NewIter(opts)
	if err != nil {
		return nil, err
	}
	pbit.Last()
	return &PebbleReverseIterator{p, pbit}, nil
}

func (p *Pebble) RangeScan(lowerBound, upperBound string) (KeyValueIterator, error) {
	opts := &pebble.IterOptions{}
	if lowerBound != "" {
		opts.LowerBound = []byte(lowerBound)
	}
	if upperBound != "" {
		opts.UpperBound = []byte(upperBound)
	}
	pbit, err := p.db.NewIter(opts)
	if err != nil {
		return nil, err
	}

	pbit.First()
	return &PebbleIterator{p, pbit}, nil
}

func (p *Pebble) Snapshot() (Snapshot, error) {
	return newPebbleSnapshot(p)
}

// Batch wrapper methods

type PebbleBatch struct {
	p *Pebble
	b *pebble.Batch
}

func (b *PebbleBatch) Count() int {
	return int(b.b.Count())
}

func (b *PebbleBatch) Size() int {
	return b.b.Len()
}

func (b *PebbleBatch) DeleteRange(lowerBound, upperBound string) error {
	return b.b.DeleteRange([]byte(lowerBound), []byte(upperBound), pebble.NoSync)
}

func (b *PebbleBatch) KeyRangeScan(lowerBound, upperBound string) (KeyIterator, error) {
	return b.RangeScan(lowerBound, upperBound)
}

func (b *PebbleBatch) RangeScan(lowerBound, upperBound string) (KeyValueIterator, error) {
	pbit, err := b.b.NewIter(&pebble.IterOptions{
		LowerBound: []byte(lowerBound),
		UpperBound: []byte(upperBound),
	})
	if err != nil {
		return nil, err
	}
	pbit.SeekGE([]byte(lowerBound))
	return &PebbleIterator{b.p, pbit}, nil
}

func (b *PebbleBatch) Close() error {
	return b.b.Close()
}

func (b *PebbleBatch) Put(key string, value []byte) error {
	err := b.b.Set([]byte(key), value, pebble.NoSync)
	if err != nil {
		b.p.writeErrors.Inc()
	}
	return err
}

func (b *PebbleBatch) Delete(key string) error {
	err := b.b.Delete([]byte(key), pebble.NoSync)
	if err != nil {
		b.p.writeErrors.Inc()
	}
	return err
}

func (b *PebbleBatch) Get(key string) ([]byte, io.Closer, error) {
	value, closer, err := b.b.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		err = ErrKeyNotFound
	} else if err != nil {
		b.p.readErrors.Inc()
	}
	return value, closer, err
}

func (b *PebbleBatch) FindLower(key string) (lowerKey string, err error) {
	it, err := b.b.NewIter(&pebble.IterOptions{
		UpperBound: []byte(key),
	})
	if err != nil {
		return "", err
	}

	if !it.Last() {
		return "", multierr.Combine(it.Close(), ErrKeyNotFound)
	}

	lowerKey = string(it.Key())
	return lowerKey, it.Close()
}

func (b *PebbleBatch) Commit() error {
	b.p.writeCount.Add(b.Count())
	b.p.writeBytes.Add(b.Size())
	b.p.batchCountHisto.Record(b.Count())
	b.p.batchSizeHisto.Record(b.Size())

	timer := b.p.batchCommitLatency.Timer()
	defer timer.Done()

	err := b.b.Commit(pebble.NoSync)
	if err != nil {
		b.p.writeErrors.Inc()
	}
	return err
}

// Iterator wrapper methods

type PebbleIterator struct {
	p  *Pebble
	pi *pebble.Iterator
}

func (p *PebbleIterator) Close() error {
	return p.pi.Close()
}

func (p *PebbleIterator) Valid() bool {
	return p.pi.Valid()
}

func (p *PebbleIterator) Key() string {
	return string(p.pi.Key())
}

func (p *PebbleIterator) Next() bool {
	return p.pi.Next()
}

func (p *PebbleIterator) Value() ([]byte, error) {
	res, err := p.pi.ValueAndErr()
	if err != nil {
		p.p.readErrors.Inc()
	}
	return res, err
}

// Iterator wrapper methods

type PebbleReverseIterator struct {
	p  *Pebble
	pi *pebble.Iterator
}

func (p *PebbleReverseIterator) Close() error {
	return p.pi.Close()
}

func (p *PebbleReverseIterator) Valid() bool {
	return p.pi.Valid()
}

func (p *PebbleReverseIterator) Key() string {
	return string(p.pi.Key())
}

func (p *PebbleReverseIterator) Prev() bool {
	return p.pi.Prev()
}

func (p *PebbleReverseIterator) Value() ([]byte, error) {
	res, err := p.pi.ValueAndErr()
	if err != nil {
		p.p.readErrors.Inc()
	}
	return res, err
}

type pebbleSnapshotLoader struct {
	pf        *PebbleFactory
	namespace string
	shard     int64
	dbPath    string
	complete  bool
	file      *os.File
}

func newPebbleSnapshotLoader(pf *PebbleFactory, namespace string, shard int64) (SnapshotLoader, error) {
	sl := &pebbleSnapshotLoader{
		pf:        pf,
		namespace: namespace,
		shard:     shard,
		dbPath:    pf.getKVPath(namespace, shard),
	}

	if err := os.RemoveAll(sl.dbPath); err != nil {
		return nil, errors.Wrap(err, "failed to remove existing database")
	}

	if err := os.MkdirAll(sl.dbPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create database dir")
	}

	return sl, nil
}

func (sl *pebbleSnapshotLoader) Close() error {
	if sl.complete {
		return nil
	}

	// If we failed to successfully load, remove all intermediate files
	return os.RemoveAll(sl.dbPath)
}

func (sl *pebbleSnapshotLoader) AddChunk(fileName string, chunkIndex int32, chunkCount int32, content []byte) error {
	var err error
	if chunkIndex == 0 {
		if sl.file != nil {
			return errors.Errorf("Inconsistent snapshot: previous file not finished")
		}
		sl.file, err = os.OpenFile(filepath.Join(sl.dbPath, fileName), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
	}
	for len(content) > 0 {
		w, err := sl.file.Write(content)
		if err != nil {
			return err
		}
		content = content[w:]
	}
	if chunkIndex == chunkCount-1 {
		err = sl.file.Close()
		sl.file = nil
		if err != nil {
			return err
		}
	}

	return nil
}

func (sl *pebbleSnapshotLoader) Load() (KV, error) {
	return newKVPebble(sl.pf, sl.namespace, sl.shard)
}

func (sl *pebbleSnapshotLoader) Complete() {
	sl.complete = true
}
