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

package kvstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/common/crc"

	"github.com/oxia-db/oxia/common/proto"

	"github.com/oxia-db/oxia/common/cache"

	"github.com/oxia-db/oxia/common/compare"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/validation"
)

func AbbreviatedKeyDisableSlash(key []byte) uint64 {
	slashPosition := bytes.IndexByte(key, '/')
	if slashPosition != -1 {
		return math.MaxUint64
	}
	return pebble.DefaultComparer.AbbreviatedKey(key)
}

var (
	OxiaSlashSpanComparer = &pebble.Comparer{
		Compare:            compare.CompareWithSlash,
		Equal:              pebble.DefaultComparer.Equal,
		AbbreviatedKey:     AbbreviatedKeyDisableSlash,
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

	gaugeCacheSize metric.Gauge
}

func NewPebbleKVFactory(options *FactoryOptions) (Factory, error) {
	if options == nil {
		options = DefaultFactoryOptions
	}
	options.EnsureDefaults()

	blockCache := pebble.NewCache(options.CacheSizeMB * 1024 * 1024)

	pf := &PebbleFactory{
		dataDir: options.DataDir,
		options: options,

		// Share a single cache instance across the databases for all the shards
		cache: blockCache,

		gaugeCacheSize: metric.NewGauge("oxia_server_kv_pebble_max_cache_size",
			"The max size configured for the Pebble block cache in bytes",
			metric.Bytes, map[string]any{}, func() int64 {
				return options.CacheSizeMB * 1024 * 1024
			}),
	}

	// Cleanup leftover snapshots from previous runs
	if err := pf.cleanupSnapshots(); err != nil {
		return nil, errors.Wrap(err, "failed to delete database snapshots")
	}

	return pf, nil
}

// The snapshots are left behind by a previous run: the "snapshots" name is
// reserved, so nothing else can be in there.
func (p *PebbleFactory) cleanupSnapshots() error {
	return os.RemoveAll(filepath.Join(p.dataDir, validation.KeywordNamespaceSnapshots))
}

func (p *PebbleFactory) Close() error {
	p.gaugeCacheSize.Unregister()
	p.cache.Unref()
	return nil
}

func (p *PebbleFactory) NewKV(namespace string, shardId int64, keySorting proto.KeySortingType) (KV, error) {
	return newKVPebble(p, namespace, shardId, keySorting, p.options.KvTrap)
}

func (p *PebbleFactory) NewSnapshotLoader(namespace string, shardId int64) (SnapshotLoader, error) {
	return newPebbleSnapshotLoader(p, vfs.Default, namespace, shardId)
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
	ctx             context.Context
	cancel          context.CancelFunc
	factory         *PebbleFactory
	namespace       string
	shardId         int64
	dbPath          string
	db              *pebble.DB
	snapshotCounter atomic.Int64
	writeOptions    *pebble.WriteOptions

	keyEncoder compare.Encoder

	dbMetrics          func() *pebble.Metrics
	gauges             []metric.Gauge
	batchCommitLatency metric.LatencyHistogram

	writeBytes  metric.Counter
	writeCount  metric.Counter
	readBytes   metric.Counter
	readCount   metric.Counter
	readLatency metric.LatencyHistogram
	writeErrors metric.Counter
	readErrors  metric.Counter

	batchSizeHisto  metric.Histogram
	batchCountHisto metric.Histogram

	kvTrap *KvTrap
}

func newKVPebble(factory *PebbleFactory, namespace string, shardId int64, keySorting proto.KeySortingType, trap *KvTrap) (KV, error) {
	if err := validation.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	labels := metric.LabelsForShard(namespace, shardId)
	pb := &Pebble{
		ctx:       ctx,
		cancel:    cancelFunc,
		factory:   factory,
		namespace: namespace,
		shardId:   shardId,
		dbPath:    factory.getKVPath(namespace, shardId),
		kvTrap:    trap,

		batchCommitLatency: metric.NewLatencyHistogram("oxia_server_kv_batch_commit_latency",
			"The latency for committing a batch into the database", labels),
		readLatency: metric.NewLatencyHistogram("oxia_server_kv_read_latency",
			"The latency for reading a value from the database", labels),
		writeBytes: metric.NewCounter("oxia_server_kv_write",
			"The amount of bytes written into the database", metric.Bytes, labels),
		writeCount: metric.NewCounter("oxia_server_kv_write_ops",
			"The amount of write operations", "count", labels),
		readBytes: metric.NewCounter("oxia_server_kv_read",
			"The amount of bytes read from the database", metric.Bytes, labels),
		readCount: metric.NewCounter("oxia_server_kv_read_ops",
			"The amount of read operations", "count", labels),
		writeErrors: metric.NewCounter("oxia_server_kv_write_errors",
			"The count of write operations errors", "count", labels),
		readErrors: metric.NewCounter("oxia_server_kv_read_errors",
			"The count of read operations errors", "count", labels),

		batchSizeHisto: metric.NewBytesHistogram("oxia_server_kv_batch_size",
			"The size in bytes for a given batch", labels),
		batchCountHisto: metric.NewCountHistogram("oxia_server_kv_batch_count",
			"The number of operations in a given batch", labels),
	}

	var err error
	if pb.keyEncoder, err = getKeyEncoder(pb.dbPath, keySorting); err != nil {
		return nil, err
	}

	levelOptions := [7]pebble.LevelOptions{}
	levelOptions[0] = pebble.LevelOptions{
		BlockSize: 64 * 1024,
		Compression: func() *sstable.CompressionProfile {
			return sstable.NoCompression
		},
		FilterPolicy: bloom.FilterPolicy(10),
	}

	for i := 1; i < len(levelOptions); i++ {
		levelOptions[i] = pebble.LevelOptions{
			BlockSize: 64 * 1024,
			// Snappy rather than zstd: compactions take half the CPU, and a
			// block cache miss decompresses faster, for about a quarter more
			// disk space
			Compression: func() *sstable.CompressionProfile {
				return sstable.SnappyCompression
			},
			FilterPolicy: bloom.FilterPolicy(10),
		}
	}

	log := slog.With(
		slog.String("component", "pebble"),
		slog.Int64("shard", shardId),
	)
	pbOptions := &pebble.Options{
		Cache:      factory.cache,
		Levels:     levelOptions,
		FS:         vfs.Default,
		DisableWAL: !factory.options.UseWAL,
		Logger:     &pebbleLogger{log},

		FormatMajorVersion: pebble.FormatVirtualSSTables,
	}

	pebbleConv := newPebbleDbConversion(log, pb.dbPath, pb.kvTrap)
	if err := pebbleConv.checkConvertDB(pb.keyEncoder); err != nil {
		return nil, errors.Wrap(err, "failed to convert db")
	}

	if err := createMarker(pb.dbPath, pb.keyEncoder.Name()); err != nil {
		return nil, errors.Wrap(err, "failed to create marker")
	}

	db, err := pebble.Open(pb.dbPath, pbOptions)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open database at %s", pb.dbPath)
	}

	pb.db = db

	if factory.options.SyncData {
		pb.writeOptions = pebble.Sync
	} else {
		pb.writeOptions = pebble.NoSync
	}

	// Cache the calls to db.Metrics() which are common to all the gauges
	pb.dbMetrics = cache.Memoize(func() *pebble.Metrics {
		return pb.db.Metrics()
	}, 5*time.Second)

	pb.gauges = []metric.Gauge{
		metric.NewGauge("oxia_server_kv_pebble_block_cache_used",
			"The size of the block cache used by a given db shard",
			metric.Bytes, labels, func() int64 {
				return pb.dbMetrics().BlockCache.Size
			}),
		metric.NewGauge("oxia_server_kv_pebble_block_cache_hits",
			"The number of hits in the block cache",
			"count", labels, func() int64 {
				return pb.dbMetrics().BlockCache.Hits
			}),
		metric.NewGauge("oxia_server_kv_pebble_block_cache_misses",
			"The number of misses in the block cache",
			"count", labels, func() int64 {
				return pb.dbMetrics().BlockCache.Misses
			}),
		metric.NewGauge("oxia_server_kv_pebble_read_iterators",
			"The number of iterators open",
			"value", labels, func() int64 {
				return pb.dbMetrics().TableIters
			}),

		metric.NewGauge("oxia_server_kv_pebble_compactions_total",
			"The number of compactions operations",
			"count", labels, func() int64 {
				return pb.dbMetrics().Compact.Count
			}),
		metric.NewGauge("oxia_server_kv_pebble_compaction_debt",
			"The estimated number of bytes that need to be compacted",
			metric.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().Compact.EstimatedDebt)
			}),
		metric.NewGauge("oxia_server_kv_pebble_flush_total",
			"The total number of db flushes",
			"count", labels, func() int64 {
				return pb.dbMetrics().Flush.Count
			}),
		metric.NewGauge("oxia_server_kv_pebble_flush",
			"The total amount of bytes flushed into the db",
			metric.Bytes, labels, func() int64 {
				return pb.dbMetrics().Flush.WriteThroughput.Bytes
			}),
		metric.NewGauge("oxia_server_kv_pebble_memtable_size",
			"The size of the memtable",
			metric.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().MemTable.Size)
			}),

		metric.NewGauge("oxia_server_kv_pebble_disk_space",
			"The total size of all the db files",
			metric.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().DiskSpaceUsage())
			}),
		metric.NewGauge("oxia_server_kv_pebble_num_files_total",
			"The total number of files for the db",
			"count", labels, func() int64 {
				return pb.dbMetrics().Total().TablesCount
			}),
		metric.NewGauge("oxia_server_kv_pebble_read",
			"The total amount of bytes read at this db level",
			metric.Bytes, labels, func() int64 {
				return int64(pb.dbMetrics().Total().TableBytesRead)
			}),
		metric.NewGauge("oxia_server_kv_pebble_write_amplification_percent",
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
			metric.NewGauge("oxia_server_kv_pebble_per_level_num_files",
				"The total number of files at this db level",
				"count", labels, func() int64 {
					return pb.dbMetrics().Levels[level].TablesCount
				}),
			metric.NewGauge("oxia_server_kv_pebble_per_level_size",
				"The total size in bytes of the files at this db level",
				metric.Bytes, labels, func() int64 {
					return pb.dbMetrics().Levels[level].TablesSize
				}),
			metric.NewGauge("oxia_server_kv_pebble_per_level_read",
				"The total amount of bytes read at this db level",
				metric.Bytes, labels, func() int64 {
					return int64(pb.dbMetrics().Levels[level].TableBytesRead)
				}),
		)
	}

	return pb, nil
}

func (p *Pebble) Close() error {
	select {
	case <-p.ctx.Done():
		return nil
	default:
		p.cancel()
		for _, g := range p.gauges {
			g.Unregister()
		}

		if err := p.db.Flush(); err != nil {
			return err
		}
		return p.db.Close()
	}
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

func (p *Pebble) DiskSpaceUsage() uint64 {
	return p.db.Metrics().DiskSpaceUsage()
}

func (p *Pebble) NewWriteBatch() WriteBatch {
	return &PebbleBatch{p: p, b: p.db.NewIndexedBatch()}
}

func (p *Pebble) getFloor(key []byte, itOpts IteratorOpts) (returnedKey string, value []byte, closer io.Closer, err error) {
	// There is no <= comparison in Pebble
	// We have to first check for == and then for <
	value, closer, err = p.db.Get(key)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return "", nil, nil, err
	}

	if err == nil {
		// We found record with key ==
		return p.keyEncoder.Decode(key), value, closer, nil
	}

	// Do < search
	return p.getLower(key, itOpts)
}

func (p *Pebble) getCeiling(key []byte, itOpts IteratorOpts) (returnedKey string, value []byte, closer io.Closer, err error) {
	it, err := p.db.NewIter(newIterOptions(p.keyEncoder, itOpts, key, nil))
	if err != nil {
		return "", nil, nil, err
	}
	skipper := newInternalRegionSkipper(p.keyEncoder, itOpts)

	if !it.First() || !skipper.forward(it) {
		return "", nil, nil, closeNotFound(it, pebble.ErrNotFound)
	}

	returnedKey = p.keyEncoder.Decode(it.Key())
	value, err = it.ValueAndErr()
	return returnedKey, value, it, err
}

func (p *Pebble) getLower(key []byte, itOpts IteratorOpts) (returnedKey string, value []byte, closer io.Closer, err error) {
	if len(key) == 0 {
		// Nothing sorts below the empty key. It can't be the upper bound either:
		// Pebble takes a nil bound as no bound, and the natural encoder returns
		// nil for a "" with no data pointer. Pebble also copies an empty bound
		// to nil when the buffer it copies the bounds into is not allocated yet.
		return "", nil, nil, pebble.ErrNotFound
	}

	it, err := p.db.NewIter(newIterOptions(p.keyEncoder, itOpts, nil, key))
	if err != nil {
		return "", nil, nil, err
	}
	skipper := newInternalRegionSkipper(p.keyEncoder, itOpts)

	// Backwards, the internal region is just as costly to step through: a floor
	// probe above it would walk the whole backlog in reverse.
	if !it.Last() || !skipper.backward(it) {
		return "", nil, nil, closeNotFound(it, pebble.ErrNotFound)
	}

	returnedKey = p.keyEncoder.Decode(it.Key())
	value, err = it.ValueAndErr()
	return returnedKey, value, it, err
}

func (p *Pebble) getHigher(key []byte, itOpts IteratorOpts) (returnedKey string, value []byte, closer io.Closer, err error) {
	it, err := p.db.NewIter(newIterOptions(p.keyEncoder, itOpts, key, nil))
	if err != nil {
		return "", nil, nil, err
	}
	skipper := newInternalRegionSkipper(p.keyEncoder, itOpts)

	if !it.First() || !skipper.forward(it) {
		return "", nil, nil, closeNotFound(it, pebble.ErrNotFound)
	}

	// The lower bound is inclusive, so the iterator may be positioned exactly on
	// the key. We are looking for a strict `x > y`, so step over it.
	if bytes.Equal(it.Key(), key) {
		if !it.Next() || !skipper.forward(it) {
			return "", nil, nil, closeNotFound(it, pebble.ErrNotFound)
		}
	}

	returnedKey = p.keyEncoder.Decode(it.Key())
	value, err = it.ValueAndErr()
	return returnedKey, value, it, err
}

// closeNotFound closes an iterator that found no entry. The iterator is also
// invalid when a read failed: that error is returned then, rather than
// notFound, which would pass the failed read off as a missing key.
func closeNotFound(it *pebble.Iterator, notFound error) error {
	if err := it.Close(); err != nil {
		return err
	}
	return notFound
}

func (p *Pebble) Get(key string, comparisonType ComparisonType, itOpts IteratorOpts) (returnedKey string, value []byte, closer io.Closer, err error) {
	timer := p.readLatency.Timer()
	defer timer.Done()
	p.readCount.Inc()

	k := p.keyEncoder.Encode(key)
	switch comparisonType {
	case ComparisonEqual:
		value, closer, err = p.db.Get(k)
		if err == nil {
			returnedKey = key
		}
	case ComparisonFloor:
		returnedKey, value, closer, err = p.getFloor(k, itOpts)
	case ComparisonCeiling:
		returnedKey, value, closer, err = p.getCeiling(k, itOpts)
	case ComparisonLower:
		returnedKey, value, closer, err = p.getLower(k, itOpts)
	case ComparisonHigher:
		returnedKey, value, closer, err = p.getHigher(k, itOpts)
	default:
		panic(fmt.Sprintf("Unknown comparison type: %v", comparisonType))
	}

	switch {
	case errors.Is(err, pebble.ErrNotFound):
		err = ErrKeyNotFound
	case err != nil:
		p.readErrors.Inc()
	default:
		p.readBytes.Add(len(value))
	}
	return returnedKey, value, closer, err
}

func (p *Pebble) KeyRangeScan(lowerBound, upperBound string, opts IteratorOpts) (KeyIterator, error) {
	return p.RangeScan(lowerBound, upperBound, opts)
}

func (p *Pebble) KeyPrefixIterator(prefix string) (KeyIterator, error) {
	ranges := p.keyEncoder.PrefixRanges(prefix)
	lower, upper := ranges.Bounds()
	pbit, err := p.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}

	return &PebblePrefixIterator{p: p, pi: pbit, ranges: ranges}, nil
}

func (p *Pebble) KeyRangeScanReverse(lowerBound, upperBound string, itOpts IteratorOpts) (ReverseKeyIterator, error) {
	var lb, ub []byte
	if lowerBound != "" {
		lb = p.keyEncoder.Encode(lowerBound)
	}
	if upperBound != "" {
		ub = p.keyEncoder.Encode(upperBound)
	}
	pbit, err := p.db.NewIter(newIterOptions(p.keyEncoder, itOpts, lb, ub))
	if err != nil {
		return nil, err
	}
	skipper := newInternalRegionSkipper(p.keyEncoder, itOpts)
	if pbit.Last() {
		skipper.backward(pbit)
	}
	return &PebbleReverseIterator{p, pbit, skipper}, nil
}

func (p *Pebble) RangeScan(lowerBound, upperBound string, itOpts IteratorOpts) (KeyValueIterator, error) {
	var lb, ub []byte
	if lowerBound != "" {
		lb = p.keyEncoder.Encode(lowerBound)
	}
	if upperBound != "" {
		ub = p.keyEncoder.Encode(upperBound)
	}

	pbit, err := p.db.NewIter(newIterOptions(p.keyEncoder, itOpts, lb, ub))
	if err != nil {
		return nil, err
	}

	skipper := newInternalRegionSkipper(p.keyEncoder, itOpts)
	if pbit.First() {
		skipper.forward(pbit)
	}
	return &PebbleIterator{p, pbit, skipper}, nil
}

func (p *Pebble) CompareKeys(a, b string) int {
	return bytes.Compare(p.keyEncoder.Encode(a), p.keyEncoder.Encode(b))
}

func (p *Pebble) Snapshot() (Snapshot, error) {
	return newPebbleSnapshot(p)
}

// Batch wrapper methods

type PebbleBatch struct {
	p *Pebble
	b *pebble.Batch
	// getIter serves the point reads of the batch after the first one. A Get
	// of the batch builds a whole iterator stack for every key, while seeking
	// an existing iterator costs a fraction of it. Building the iterator
	// costs more than a Get, though: the first read, which might be the only
	// one, is a Get.
	getIter *pebble.Iterator
	gotOne  bool
}

// getIterOptions must stay the same for every refresh of getIter, which then
// only extends its view of the batch.
var getIterOptions = pebble.IterOptions{}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func (b *PebbleBatch) Count() int {
	return int(b.b.Count())
}

func (b *PebbleBatch) Size() int {
	return b.b.Len()
}

func (b *PebbleBatch) DeleteRange(lowerBound, upperBound string) error {
	return b.b.DeleteRange(
		b.p.keyEncoder.Encode(lowerBound),
		b.p.keyEncoder.Encode(upperBound),
		b.p.writeOptions)
}

func (b *PebbleBatch) KeyRangeScan(lowerBound, upperBound string) (KeyIterator, error) {
	return b.RangeScan(lowerBound, upperBound)
}

// scanBounds returns the iterator bounds of RangeScan, which RangeOverlaps
// must read the same way.
func (b *PebbleBatch) scanBounds(lowerBound, upperBound string) (lb, ub []byte) {
	lb = b.p.keyEncoder.Encode(lowerBound)
	ub = b.p.keyEncoder.Encode(upperBound)
	if len(ub) == 0 {
		// Only the natural encoder encodes a key to an empty slice, for "". It
		// returns nil for the "" of a decoded request, which leaves the range
		// open: keep it open for any "", whatever its data pointer.
		ub = nil
	} else if bytes.Compare(lb, ub) > 0 {
		// An inverted range is empty. Bound it as such: the seek below would
		// clamp to the upper bound, under the lower one, which Pebble asserts
		// against in its invariants builds.
		ub = lb
	}
	return lb, ub
}

func (b *PebbleBatch) RangeScan(lowerBound, upperBound string) (KeyValueIterator, error) {
	lb, ub := b.scanBounds(lowerBound, upperBound)
	pbit, err := b.b.NewIter(&pebble.IterOptions{
		LowerBound: lb,
		UpperBound: ub,
	})
	if err != nil {
		return nil, err
	}
	pbit.SeekGE(lb)
	return &PebbleIterator{b.p, pbit, internalRegionSkipper{}}, nil
}

func (b *PebbleBatch) RangeOverlaps(lowerBound, upperBound string) (internalKeys, regularKeys bool) {
	// As for the iterator, a nil upper bound is unbounded
	lb, ub := b.scanBounds(lowerBound, upperBound)
	if ub != nil && bytes.Compare(lb, ub) >= 0 {
		// The range is empty
		return false, false
	}
	// Every internal key sorts at or after their prefix, with either encoder,
	// while the region of the hierarchical one starts lower: a range that ends
	// at the prefix covers no internal key. The regular keys sort before the
	// internal ones and, with the natural encoder, after them too.
	start := b.p.keyEncoder.Encode(constant.InternalKeyPrefix)
	_, end := b.p.keyEncoder.InternalKeyRange()
	internalKeys = (ub == nil || bytes.Compare(start, ub) < 0) && (end == nil || bytes.Compare(lb, end) < 0)
	regularKeys = bytes.Compare(lb, start) < 0 || (end != nil && (ub == nil || bytes.Compare(end, ub) < 0))
	return internalKeys, regularKeys
}

func (b *PebbleBatch) Close() error {
	return multierr.Append(b.closeGetIter(), b.b.Close())
}

func (b *PebbleBatch) closeGetIter() error {
	if b.getIter == nil {
		return nil
	}
	err := b.getIter.Close()
	b.getIter = nil
	return err
}

func (b *PebbleBatch) Put(key string, value []byte) error {
	err := b.b.Set(b.p.keyEncoder.Encode(key), value, b.p.writeOptions)
	if err != nil {
		b.p.writeErrors.Inc()
	}
	return err
}

func (b *PebbleBatch) PutMarshalable(key string, m ProtoMarshalable) error {
	encodedKey := b.p.keyEncoder.Encode(key)
	op := b.b.SetDeferred(len(encodedKey), m.SizeVT())
	copy(op.Key, encodedKey)
	n, err := m.MarshalToSizedBufferVT(op.Value)
	if err != nil {
		b.p.writeErrors.Inc()
		return err
	}
	if n != len(op.Value) {
		// The record length is fixed once reserved: a partial fill would
		// leave garbage bytes in the committed value
		b.p.writeErrors.Inc()
		return errors.Errorf("kv: marshaled size %d does not match the reserved record size %d", n, len(op.Value))
	}
	if err := op.Finish(); err != nil {
		b.p.writeErrors.Inc()
		return err
	}
	return nil
}

func (b *PebbleBatch) Delete(key string) error {
	err := b.b.Delete(b.p.keyEncoder.Encode(key), b.p.writeOptions)
	if err != nil {
		b.p.writeErrors.Inc()
	}
	return err
}

func (b *PebbleBatch) Get(key string) ([]byte, io.Closer, error) {
	encodedKey := b.p.keyEncoder.Encode(key)
	if !b.gotOne {
		b.gotOne = true
		value, closer, err := b.b.Get(encodedKey)
		if errors.Is(err, pebble.ErrNotFound) {
			err = ErrKeyNotFound
		} else if err != nil {
			b.p.readErrors.Inc()
		}
		return value, closer, err
	}
	if b.getIter == nil {
		it, err := b.b.NewIter(&getIterOptions)
		if err != nil {
			b.p.readErrors.Inc()
			return nil, nil, err
		}
		b.getIter = it
	} else {
		// The iterator sees the batch as it was when created or last
		// refreshed: refresh it, to see the mutations since then too
		b.getIter.SetOptions(&getIterOptions)
	}

	// The comparers split no suffix off: the prefix is the whole key
	if !b.getIter.SeekPrefixGE(encodedKey) || !bytes.Equal(b.getIter.Key(), encodedKey) {
		if err := b.getIter.Error(); err != nil {
			b.p.readErrors.Inc()
			return nil, nil, err
		}
		return nil, nil, ErrKeyNotFound
	}
	value, err := b.getIter.ValueAndErr()
	if err != nil {
		b.p.readErrors.Inc()
		return nil, nil, err
	}
	return value, nopCloser{}, nil
}

func (b *PebbleBatch) FindLower(key string) (lowerKey string, err error) {
	it, err := b.b.NewIter(&pebble.IterOptions{
		UpperBound: b.p.keyEncoder.Encode(key),
	})
	if err != nil {
		return "", err
	}

	if !it.Last() {
		return "", closeNotFound(it, ErrKeyNotFound)
	}

	lowerKey = b.p.keyEncoder.Decode(it.Key())
	return lowerKey, it.Close()
}

func (b *PebbleBatch) Checksum(init crc.Checksum) crc.Checksum {
	return init.Update(b.b.Repr())
}

func (b *PebbleBatch) Commit() error {
	if err := b.closeGetIter(); err != nil {
		return err
	}
	b.p.writeCount.Add(b.Count())
	b.p.writeBytes.Add(b.Size())
	b.p.batchCountHisto.Record(b.Count())
	b.p.batchSizeHisto.Record(b.Size())

	timer := b.p.batchCommitLatency.Timer()
	defer timer.Done()

	err := b.b.Commit(b.p.writeOptions)
	if err != nil {
		b.p.writeErrors.Inc()
	}
	return err
}

// Iterator wrapper methods

type PebbleIterator struct {
	p       *Pebble
	pi      *pebble.Iterator
	skipper internalRegionSkipper
}

func (p *PebbleIterator) Close() error {
	return p.pi.Close()
}

func (p *PebbleIterator) Valid() bool {
	return p.pi.Valid()
}

func (p *PebbleIterator) Key() string {
	return p.p.keyEncoder.Decode(p.pi.Key())
}

func (p *PebbleIterator) Next() bool {
	return p.pi.Next() && p.skipper.forward(p.pi)
}

func (p *PebbleIterator) Prev() bool {
	return p.pi.Prev() && p.skipper.backward(p.pi)
}

func (p *PebbleIterator) SeekGE(key string) bool {
	return p.pi.SeekGE(p.p.keyEncoder.Encode(key)) && p.skipper.forward(p.pi)
}

func (p *PebbleIterator) SeekLT(key string) bool {
	return p.pi.SeekLT(p.p.keyEncoder.Encode(key)) && p.skipper.backward(p.pi)
}

func (p *PebbleIterator) Error() error {
	return p.pi.Error()
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
	p       *Pebble
	pi      *pebble.Iterator
	skipper internalRegionSkipper
}

func (p *PebbleReverseIterator) Close() error {
	return p.pi.Close()
}

func (p *PebbleReverseIterator) Valid() bool {
	return p.pi.Valid()
}

func (p *PebbleReverseIterator) Key() string {
	return p.p.keyEncoder.Decode(p.pi.Key())
}

func (p *PebbleReverseIterator) Prev() bool {
	return p.pi.Prev() && p.skipper.backward(p.pi)
}

func (p *PebbleReverseIterator) Value() ([]byte, error) {
	res, err := p.pi.ValueAndErr()
	if err != nil {
		p.p.readErrors.Inc()
	}
	return res, err
}

// PebblePrefixIterator iterates the keys that start with a prefix. When the
// encoder does not keep them in a single range, it seeks from one range to the
// next over the keys in between.
type PebblePrefixIterator struct {
	p      *Pebble
	pi     *pebble.Iterator
	ranges compare.PrefixRanges
	valid  bool
}

func (p *PebblePrefixIterator) Close() error {
	return p.pi.Close()
}

func (p *PebblePrefixIterator) Valid() bool {
	return p.valid
}

func (p *PebblePrefixIterator) Key() string {
	return p.p.keyEncoder.Decode(p.pi.Key())
}

func (p *PebblePrefixIterator) Next() bool {
	p.pi.Next()
	return p.forward()
}

func (p *PebblePrefixIterator) Prev() bool {
	p.pi.Prev()
	return p.backward()
}

func (p *PebblePrefixIterator) SeekGE(key string) bool {
	p.pi.SeekGE(p.p.keyEncoder.Encode(key))
	return p.forward()
}

func (p *PebblePrefixIterator) SeekLT(key string) bool {
	p.pi.SeekLT(p.p.keyEncoder.Encode(key))
	return p.backward()
}

func (p *PebblePrefixIterator) Error() error {
	return p.pi.Error()
}

// forward moves the iterator from a key between two ranges to the start of
// the next one.
func (p *PebblePrefixIterator) forward() bool {
	p.valid = p.pi.Valid()
	for p.valid && !p.ranges.Contains(p.pi.Key()) {
		start := p.ranges.NextStart(p.pi.Key())
		p.valid = start != nil && p.pi.SeekGE(start)
	}
	return p.valid
}

// backward moves the iterator from a key between two ranges to the end of the
// previous one.
func (p *PebblePrefixIterator) backward() bool {
	p.valid = p.pi.Valid()
	for p.valid && !p.ranges.Contains(p.pi.Key()) {
		end := p.ranges.PrevEnd(p.pi.Key())
		p.valid = end != nil && p.pi.SeekLT(end)
	}
	return p.valid
}

// pebbleSnapshotLoader writes the snapshot files through fs, which is
// vfs.Default in production. It is a vfs.FS rather than plain os calls so
// that the tests can install a snapshot on vfs.NewCrashableMem, which
// simulates a machine crash by keeping only what was synced.
type pebbleSnapshotLoader struct {
	pf        *PebbleFactory
	namespace string
	shard     int64
	fs        vfs.FS
	dbPath    string
	complete  bool
	file      vfs.File
}

func newPebbleSnapshotLoader(pf *PebbleFactory, fs vfs.FS, namespace string, shard int64) (SnapshotLoader, error) {
	if err := validation.ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	sl := &pebbleSnapshotLoader{
		pf:        pf,
		namespace: namespace,
		shard:     shard,
		fs:        fs,
		dbPath:    pf.getKVPath(namespace, shard),
	}

	if err := fs.RemoveAll(sl.dbPath); err != nil {
		return nil, errors.Wrap(err, "failed to remove existing database")
	}

	if err := fs.MkdirAll(sl.dbPath, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create database dir")
	}

	return sl, nil
}

func (sl *pebbleSnapshotLoader) Close() error {
	if sl.complete {
		return nil
	}

	// If we failed to successfully load, remove all intermediate files
	return sl.fs.RemoveAll(sl.dbPath)
}

func (sl *pebbleSnapshotLoader) AddChunk(fileName string, chunkIndex int32, chunkCount int32, content []byte) error {
	var err error
	if chunkIndex == 0 {
		if sl.file != nil {
			return errors.Errorf("Inconsistent snapshot: previous file not finished")
		}
		// fileName arrives from the snapshot sender over the network. The sender
		// only ever emits the base names of the regular files sitting directly in
		// the checkpoint dir, so require exactly that: no traversal, no absolute
		// path, no nested path, and not the loader dir itself.
		if fileName != filepath.Base(fileName) || fileName == "." || !filepath.IsLocal(fileName) {
			return errors.Errorf("invalid snapshot chunk file name: %q", fileName)
		}
		sl.file, err = sl.fs.Create(filepath.Join(sl.dbPath, fileName), vfs.WriteCategoryUnspecified)
		if err != nil {
			return err
		}
		// Closing the file syncs it. Once the follower acks the snapshot, the
		// leader only sends the entries that follow it: the snapshot must
		// survive a machine crash
		sl.file = vfs.NewSyncingFile(sl.file, vfs.SyncingFileOptions{})
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

func (sl *pebbleSnapshotLoader) Complete() error {
	// The files are synced already. Sync the directory entries too: of the
	// files, and of the database directory, which was re-created
	if err := syncDir(sl.fs, sl.dbPath); err != nil {
		return err
	}
	if err := syncDir(sl.fs, filepath.Dir(sl.dbPath)); err != nil {
		return err
	}
	sl.complete = true
	return nil
}

func syncDir(fs vfs.FS, path string) error {
	dir, err := fs.OpenDir(path)
	if err != nil {
		return err
	}
	return multierr.Combine(dir.Sync(), dir.Close())
}

// newIterOptions builds the Pebble iterator options for a scan that may have to
// exclude the internal-key region.
//
// It deliberately does not use pebble's SkipPoint. SkipPoint is a *filter*: the
// iterator still steps through — and loads the blocks of — every internal key it
// passes, so a ceiling/higher lookup past the last user key, or a scan with no
// upper bound, walks the entire live notification backlog before returning. It
// also hides those keys from us, so we could not detect the region to seek past
// it. Instead: when the internal region runs to the end of the keyspace it is
// pruned outright with an upper bound, and otherwise internalRegionSkipper
// jumps over it with a single seek.
func newIterOptions(enc compare.Encoder, itOpts IteratorOpts, lowerBound, upperBound []byte) *pebble.IterOptions {
	if len(lowerBound) == 0 {
		// Nothing sorts below the empty key, so as a lower bound it is no bound.
		// Pass it as nil: Pebble's seek to an empty key panics in its invariants
		// builds, which the race detector enables.
		lowerBound = nil
	}
	opts := &pebble.IterOptions{LowerBound: lowerBound, UpperBound: upperBound}
	if itOpts.IncludeInternalKeys {
		return opts
	}
	if start, end := enc.InternalKeyRange(); end == nil {
		// No user key can sort after the internal keys: bound them away.
		if opts.UpperBound == nil || bytes.Compare(start, opts.UpperBound) < 0 {
			opts.UpperBound = start
		}
	}
	return opts
}

// internalRegionSkipper keeps an iterator out of the contiguous internal-key
// region, jumping over it in a single seek rather than stepping through it.
//
// It is a no-op when internal keys are wanted, and when newIterOptions already
// pruned the region with an upper bound (hierarchical). It carries its weight
// for the natural encoder, where a user key is stored raw and can sort *after*
// the internal keys, so the region cannot be bounded away.
type internalRegionSkipper struct {
	enc        compare.Encoder
	start, end []byte
}

func newInternalRegionSkipper(enc compare.Encoder, itOpts IteratorOpts) internalRegionSkipper {
	if itOpts.IncludeInternalKeys {
		return internalRegionSkipper{}
	}
	start, end := enc.InternalKeyRange()
	if end == nil {
		// Already pruned by the upper bound in newIterOptions.
		return internalRegionSkipper{}
	}
	return internalRegionSkipper{enc: enc, start: start, end: end}
}

// forward moves the iterator past the internal region when it is positioned
// inside it, and reports whether it is still valid. A single seek suffices: the
// region is contiguous, so nothing at or after end is an internal key.
func (s internalRegionSkipper) forward(it *pebble.Iterator) bool {
	if s.end == nil || !it.Valid() || !s.enc.IsInternalKey(it.Key()) {
		return it.Valid()
	}
	return it.SeekGE(s.end)
}

// backward moves the iterator before the internal region when it is positioned
// inside it, and reports whether it is still valid.
func (s internalRegionSkipper) backward(it *pebble.Iterator) bool {
	if s.end == nil || !it.Valid() || !s.enc.IsInternalKey(it.Key()) {
		return it.Valid()
	}
	return it.SeekLT(s.start)
}
