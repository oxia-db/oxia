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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

func BenchmarkRangeScan(b *testing.B) {
	const numKeys = 1000

	for _, sorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		b.Run(sorting.String(), func(b *testing.B) {
			factory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{DataDir: b.TempDir()})
			assert.NoError(b, err)
			db, err := NewDB(constant.DefaultNamespace, 1, factory, sorting, 0, time2.SystemClock)
			assert.NoError(b, err)
			defer db.Close()

			puts := make([]*proto.PutRequest, numKeys)
			for i := range numKeys {
				puts[i] = &proto.PutRequest{
					Key:   fmt.Sprintf("/app/users/%04d/profile/key-%010d", i%100, i),
					Value: []byte("0123456789abcdef0123456789abcdef"),
				}
			}
			_, err = db.ProcessWrite(&proto.WriteRequest{Puts: puts}, 0,
				uint64(time.Now().UnixMilli()), NoOpCallback)
			assert.NoError(b, err)

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				// Bounds at the same depth as the keys, so the range brackets
				// all of them under both natural and hierarchical sorting
				it, err := db.RangeScan(&proto.RangeScanRequest{
					StartInclusive: "/app/users/0000/profile/key-0000000000",
					EndExclusive:   "/app/users/zzzz/profile/key-zzzzzzzzzz",
				})
				assert.NoError(b, err)

				count := 0
				for ; it.Valid(); it.Next() {
					gr, err := it.Value()
					assert.NoError(b, err)
					if len(gr.GetKey()) == 0 {
						b.Fatal("empty key")
					}
					count++
				}
				assert.NoError(b, it.Close())
				if count != numKeys {
					b.Fatalf("scanned %d keys, expected %d", count, numKeys)
				}
			}
		})
	}
}
