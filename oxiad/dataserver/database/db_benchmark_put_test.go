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

package database

import (
	"bytes"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/logging"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

// Overwriting an existing key exercises the version-check read of the old
// entry plus the marshal of the new one — the two copy hot spots of the
// put path.
func BenchmarkPutOverwrite(b *testing.B) {
	for _, valueSize := range []int{128, 4096, 65536} {
		b.Run(fmt.Sprintf("value-%d", valueSize), func(b *testing.B) {
			factory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{DataDir: b.TempDir()})
			assert.NoError(b, err)
			db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, time2.SystemClock)
			assert.NoError(b, err)
			defer db.Close()

			value := bytes.Repeat([]byte("x"), valueSize)
			write := &proto.WriteRequest{Puts: []*proto.PutRequest{{
				Key:   "bench-key",
				Value: value,
			}}}
			timestamp := uint64(time.Now().UnixMilli())

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := db.ProcessWrite(write, int64(i), timestamp, NoOpCallback); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPutBatch measures write requests of several puts, each of which
// reads the current entry of its key before writing it. Every request either
// overwrites the same keys, which then always exist, or puts new ones.
func BenchmarkPutBatch(b *testing.B) {
	// The logs go to stdout, where they'd break the result lines, and opening
	// the db logs at the info level
	logging.LogLevel = slog.LevelWarn
	logging.ConfigureLogger()

	for _, keys := range []int{1, 10, 100} {
		for _, overwrite := range []bool{true, false} {
			b.Run(fmt.Sprintf("keys=%d/overwrite=%v", keys, overwrite), func(b *testing.B) {
				factory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{DataDir: b.TempDir()})
				assert.NoError(b, err)
				db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, time2.SystemClock)
				assert.NoError(b, err)
				defer db.Close()

				write := &proto.WriteRequest{Puts: make([]*proto.PutRequest, keys)}
				for i := range write.Puts {
					write.Puts[i] = &proto.PutRequest{Key: fmt.Sprintf("key-%03d", i), Value: []byte("value")}
				}
				timestamp := uint64(time.Now().UnixMilli())

				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if !overwrite {
						for j, put := range write.Puts {
							put.Key = fmt.Sprintf("key-%d-%03d", i, j)
						}
					}
					if _, err := db.ProcessWrite(write, int64(i), timestamp, NoOpCallback); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
