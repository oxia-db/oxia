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

package bench

import (
	"fmt"
	"testing"
	"time"

	db2 "github.com/oxia-db/oxia/node/db"
	"github.com/oxia-db/oxia/node/db/kv"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	"github.com/oxia-db/oxia/common/compare"

	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/proto"
)

func BenchmarkDeleteRange(b *testing.B) {
	dataDir := b.TempDir()
	factory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{DataDir: dataDir})
	assert.NoError(b, err)
	db, err := db2.NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 0, time2.SystemClock)
	assert.NoError(b, err)
	defer db.Close()
	for i := range b.N {
		_, err := db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{
				{
					Key:              "00000000000000000001",
					PartitionKey:     ptr.To("00000000000000000001"),
					Value:            []byte("00000000000000000000"),
					SequenceKeyDelta: []uint64{1},
				},
			},
			DeleteRanges: []*proto.DeleteRangeRequest{
				{
					StartInclusive: "00000000000000000001-00000000000000000000",
					EndExclusive:   fmt.Sprintf("00000000000000000001-%020d", i),
				},
			},
		}, int64(i), uint64(time.Now().UnixMilli()), db2.NoOpCallback)
		assert.NoError(b, err)
	}
}
