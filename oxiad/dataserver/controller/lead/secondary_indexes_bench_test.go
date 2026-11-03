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

package lead

import (
	"fmt"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

// BenchmarkSecondaryIndexesOverwrite measures the puts that overwrite a record
// with secondary indexes, keeping all its indexes or changing one of them,
// with and without FEATURE_SECONDARY_INDEX_SKIP_UNCHANGED.
func BenchmarkSecondaryIndexesOverwrite(b *testing.B) {
	// The logs go to stdout, where they'd break the result lines
	logging.LogLevel = slog.LevelWarn
	logging.ConfigureLogger()

	for _, features := range [][]proto.Feature{nil, {proto.Feature_FEATURE_SECONDARY_INDEX_SKIP_UNCHANGED}} {
		for _, indexes := range []int{1, 3} {
			// The secondary keys that the puts give the first index in turn
			for _, firstSecondaryKeys := range [][]string{{"a"}, {"a", "b"}} {
				name := fmt.Sprintf("skip-unchanged=%v/indexes=%d/changed=%v",
					len(features) > 0, indexes, len(firstSecondaryKeys) > 1)
				b.Run(name, func(b *testing.B) {
					benchmarkSecondaryIndexesOverwrite(b, features, indexes, firstSecondaryKeys)
				})
			}
		}
	}
}

// BenchmarkSecondaryIndexRangeScan measures an index range scan over all the
// records of a shard, in the flushed tables and in the memtable, per record.
func BenchmarkSecondaryIndexRangeScan(b *testing.B) {
	// The logs go to stdout, where they'd break the result lines
	logging.LogLevel = slog.LevelWarn
	logging.ConfigureLogger()

	factory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{DataDir: b.TempDir()})
	assert.NoError(b, err)
	db, err := database.NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_HIERARCHICAL, 0,
		time2.SystemClock)
	assert.NoError(b, err)
	defer db.Close()

	const records = 10_000
	const batch = 100
	timestamp := uint64(time.Now().UnixMilli())
	for i := 0; i < records; i += batch {
		write := &proto.WriteRequest{}
		for j := i; j < i+batch; j++ {
			write.Puts = append(write.Puts, &proto.PutRequest{
				Key:   fmt.Sprintf("/managed-ledgers/tenant/namespace/persistent/topic-%05d", j),
				Value: make([]byte, 100),
				// The secondary keys don't follow the order of the keys
				SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "idx", SecondaryKey: fmt.Sprintf("%05d", j*7919%records)}},
			})
		}
		if _, err := db.ProcessWrite(write, int64(i/batch), 0, timestamp, WrapperUpdateOperationCallback); err != nil {
			b.Fatal(err)
		}
		if i == records*9/10 {
			assert.NoError(b, db.Flush())
		}
	}

	idx := "idx"
	request := &proto.RangeScanRequest{SecondaryIndexName: &idx, StartInclusive: "0", EndExclusive: "9"}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ResetTimer()
	for range b.N {
		it, err := newSecondaryIndexRangeScanIterator(request, db)
		if err != nil {
			b.Fatal(err)
		}
		scanned := 0
		for ; it.Valid(); it.Next() {
			if _, err := it.Value(); err != nil {
				b.Fatal(err)
			}
			scanned++
		}
		if err := it.Close(); err != nil || scanned != records {
			b.Fatal(err, scanned)
		}
	}
	b.StopTimer()
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*records), "ns/record")
	b.ReportMetric(float64(after.Mallocs-before.Mallocs)/float64(b.N*records), "allocs/record")
}

func benchmarkSecondaryIndexesOverwrite(b *testing.B, features []proto.Feature, indexes int,
	firstSecondaryKeys []string) {
	b.Helper()
	factory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{DataDir: b.TempDir()})
	assert.NoError(b, err)
	db, err := database.NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_HIERARCHICAL, 0,
		time2.SystemClock)
	assert.NoError(b, err)
	defer db.Close()
	for _, f := range features {
		db.EnableFeature(f)
	}

	put := &proto.PutRequest{
		Key:              "/managed-ledgers/tenant/namespace/persistent/topic",
		Value:            []byte("value"),
		SecondaryIndexes: make([]*proto.SecondaryIndex, indexes),
	}
	for i := range put.SecondaryIndexes {
		put.SecondaryIndexes[i] = &proto.SecondaryIndex{
			IndexName:    fmt.Sprintf("index-%d", i),
			SecondaryKey: "/managed-ledgers/tenant/namespace/persistent",
		}
	}
	write := &proto.WriteRequest{Puts: []*proto.PutRequest{put}}
	timestamp := uint64(time.Now().UnixMilli())

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		put.SecondaryIndexes[0].SecondaryKey = firstSecondaryKeys[i%len(firstSecondaryKeys)]
		if _, err := db.ProcessWrite(write, int64(i), 0, timestamp, WrapperUpdateOperationCallback); err != nil {
			b.Fatal(err)
		}
	}
}
