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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

func BenchmarkSequencePut(b *testing.B) {
	factory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{DataDir: b.TempDir()})
	assert.NoError(b, err)
	defer factory.Close()
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, time2.SystemClock)
	assert.NoError(b, err)
	defer db.Close()

	timestamp := uint64(time.Now().UnixMilli())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{
				Key:              "seq-prefix",
				PartitionKey:     ptr.To("seq-prefix"),
				Value:            []byte("v"),
				SequenceKeyDelta: []uint64{1},
			}},
		}, int64(i), timestamp, NoOpCallback)
		if err != nil {
			b.Fatal(err)
		}
	}
}
