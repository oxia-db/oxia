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
		if _, err := db.ProcessWrite(write, int64(i), timestamp, WrapperUpdateOperationCallback); err != nil {
			b.Fatal(err)
		}
	}
}
