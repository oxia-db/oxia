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
	"k8s.io/utils/ptr"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

func newSequenceTestDB(t *testing.T) DB {
	t.Helper()
	factory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 0, time2.SystemClock)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
		assert.NoError(t, factory.Close())
	})
	return db
}

func sequencePut(t *testing.T, db DB, offset int64, prefix string, deltas ...uint64) string {
	t.Helper()
	res, err := db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:              prefix,
			PartitionKey:     ptr.To(prefix),
			Value:            []byte("v"),
			SequenceKeyDelta: deltas,
		}},
	}, offset, uint64(time.Now().UnixMilli()), NoOpCallback)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
	return res.Puts[0].GetKey()
}

func sequenceKeyOf(prefix string, values ...uint64) string {
	key := prefix
	for _, v := range values {
		key = fmt.Sprintf("%s-%020d", key, v)
	}
	return key
}

// Sequential puts across separate write batches continue the sequence: the
// second put is served from the cache and must produce the exact key the
// storage re-read would have produced.
func TestSequencePut_CrossBatchContinuity(t *testing.T) {
	db := newSequenceTestDB(t)

	assert.Equal(t, sequenceKeyOf("seq", 1), sequencePut(t, db, 0, "seq", 1))
	assert.Equal(t, sequenceKeyOf("seq", 3), sequencePut(t, db, 1, "seq", 2))
	assert.Equal(t, sequenceKeyOf("seq", 4), sequencePut(t, db, 2, "seq", 1))
}

// Two sequential puts for the same prefix inside one write batch: the second
// must see the first (previously via the indexed batch, now via the cache).
func TestSequencePut_SameBatch(t *testing.T) {
	db := newSequenceTestDB(t)

	res, err := db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "seq", PartitionKey: ptr.To("seq"), Value: []byte("a"), SequenceKeyDelta: []uint64{1}},
			{Key: "seq", PartitionKey: ptr.To("seq"), Value: []byte("b"), SequenceKeyDelta: []uint64{1}},
		},
	}, 0, uint64(time.Now().UnixMilli()), NoOpCallback)
	assert.NoError(t, err)
	assert.Equal(t, sequenceKeyOf("seq", 1), res.Puts[0].GetKey())
	assert.Equal(t, sequenceKeyOf("seq", 2), res.Puts[1].GetKey())
}

// A direct put of a sequence-shaped key moves the tail without going through
// the sequence path: the next sequential put must continue above it, not from
// the stale cached value.
func TestSequencePut_DirectPutInvalidatesCache(t *testing.T) {
	db := newSequenceTestDB(t)

	assert.Equal(t, sequenceKeyOf("seq", 1), sequencePut(t, db, 0, "seq", 1))

	// Direct put far ahead of the cached tail
	direct := sequenceKeyOf("seq", 100)
	_, err := db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{Key: direct, Value: []byte("x")}},
	}, 1, uint64(time.Now().UnixMilli()), NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, sequenceKeyOf("seq", 101), sequencePut(t, db, 2, "seq", 1))
}

// Deleting the last key of the sequence moves the tail backwards: the next
// sequential put re-reads storage, matching the pre-cache behavior.
func TestSequencePut_DeleteInvalidatesCache(t *testing.T) {
	db := newSequenceTestDB(t)

	assert.Equal(t, sequenceKeyOf("seq", 1), sequencePut(t, db, 0, "seq", 1))
	assert.Equal(t, sequenceKeyOf("seq", 2), sequencePut(t, db, 1, "seq", 1))

	_, err := db.ProcessWrite(&proto.WriteRequest{
		Deletes: []*proto.DeleteRequest{{Key: sequenceKeyOf("seq", 2)}},
	}, 2, uint64(time.Now().UnixMilli()), NoOpCallback)
	assert.NoError(t, err)

	// Tail is back to 1: the next key repeats 2, as it always has
	assert.Equal(t, sequenceKeyOf("seq", 2), sequencePut(t, db, 3, "seq", 1))
}

// A range delete can cover any sequence: the cache is dropped entirely.
func TestSequencePut_DeleteRangeInvalidatesCache(t *testing.T) {
	db := newSequenceTestDB(t)

	assert.Equal(t, sequenceKeyOf("seq", 1), sequencePut(t, db, 0, "seq", 1))
	assert.Equal(t, sequenceKeyOf("seq", 2), sequencePut(t, db, 1, "seq", 1))

	_, err := db.ProcessWrite(&proto.WriteRequest{
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: sequenceKeyOf("seq", 2),
			EndExclusive:   sequenceKeyOf("seq", 3),
		}},
	}, 2, uint64(time.Now().UnixMilli()), NoOpCallback)
	assert.NoError(t, err)

	assert.Equal(t, sequenceKeyOf("seq", 2), sequencePut(t, db, 3, "seq", 1))
}

// The cached path must apply the same validation as the storage path: a put
// with fewer deltas than the sequence already has levels is rejected.
func TestSequencePut_MissingDeltasWithCache(t *testing.T) {
	db := newSequenceTestDB(t)

	assert.Equal(t, sequenceKeyOf("seq", 1, 5), sequencePut(t, db, 0, "seq", 1, 5))

	// Cache hit with fewer deltas than cached levels
	_, err := db.ProcessWrite(&proto.WriteRequest{
		Puts: []*proto.PutRequest{{
			Key:              "seq",
			PartitionKey:     ptr.To("seq"),
			Value:            []byte("v"),
			SequenceKeyDelta: []uint64{1},
		}},
	}, 1, uint64(time.Now().UnixMilli()), NoOpCallback)
	assert.ErrorIs(t, err, ErrMissingSequenceDeltas)
}

// Multi-level sequences advance level-wise from the cache exactly as from
// storage.
func TestSequencePut_MultiLevel(t *testing.T) {
	db := newSequenceTestDB(t)

	assert.Equal(t, sequenceKeyOf("seq", 1, 10), sequencePut(t, db, 0, "seq", 1, 10))
	assert.Equal(t, sequenceKeyOf("seq", 3, 15), sequencePut(t, db, 1, "seq", 2, 5))
}
