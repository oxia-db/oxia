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
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

// fillMemTables writes into kv until its memtables have grown to full size,
// with a flushed one kept to be recycled: the most memory they reserve in the
// cache.
func fillMemTables(t *testing.T, kv KV) {
	t.Helper()
	value := make([]byte, 1024)
	for i := 0; ; i++ {
		_, _ = rand.Read(value)
		wb := kv.NewWriteBatch()
		for j := 0; j < 100; j++ {
			require.NoError(t, wb.Put(fmt.Sprintf("key-%06d-%03d", i, j), value))
		}
		require.NoError(t, wb.Commit())
		require.NoError(t, wb.Close())

		m := kv.(*Pebble).db.Metrics()
		if m.MemTable.Size+m.MemTable.ZombieSize >= memTableSlotSize {
			return
		}
		require.Less(t, i, 1000, "the memtables never reached their full size")
	}
}

func TestPebbleMemTablesLeaveTheBlockCache(t *testing.T) {
	const cacheSize = 8 * 1024 * 1024
	options := NewFactoryOptionsForTest(t)
	options.CacheSizeMB = cacheSize / 1024 / 1024
	factory, err := NewPebbleKVFactory(options)
	require.NoError(t, err)
	defer factory.Close()

	// The memtables of the two shards reserve twice the size of the cache
	var kvs []KV
	for shard := int64(0); shard < 2; shard++ {
		kv, err := factory.NewKV(constant.DefaultNamespace, shard, proto.KeySortingType_HIERARCHICAL)
		require.NoError(t, err)
		kvs = append(kvs, kv)
		fillMemTables(t, kv)
	}
	defer func() {
		for _, kv := range kvs {
			assert.NoError(t, kv.Close())
		}
	}()

	// Reading back what a shard flushed fills the cache with its blocks
	require.NoError(t, kvs[0].Flush())
	it, err := kvs[0].RangeScan("", "", ShowInternalKeys)
	require.NoError(t, err)
	for ; it.Valid(); it.Next() {
		_, err := it.Value()
		require.NoError(t, err)
	}
	require.NoError(t, it.Close())

	assert.Greater(t, factory.(*PebbleFactory).cache.Metrics().Size, int64(cacheSize/2))
}

func TestPebbleMemTableSlots(t *testing.T) {
	factory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer factory.Close()
	pf := factory.(*PebbleFactory)
	freeSlots := func() int {
		pf.memTableSlotsLock.Lock()
		defer pf.memTableSlotsLock.Unlock()
		return len(pf.memTableSlots)
	}
	assert.Equal(t, maxMemTableSlots, freeSlots())

	kv0, err := factory.NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
	require.NoError(t, err)
	kv1, err := factory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
	require.NoError(t, err)
	assert.Equal(t, maxMemTableSlots-2, freeSlots())

	require.NoError(t, kv0.Close())
	assert.Equal(t, maxMemTableSlots-1, freeSlots())
	// Closing again gives nothing back
	require.NoError(t, kv0.Close())
	assert.Equal(t, maxMemTableSlots-1, freeSlots())

	require.NoError(t, kv1.Delete())
	assert.Equal(t, maxMemTableSlots, freeSlots())

	// A shard that fails to open gives its slot back. The db of shard 0 is
	// still locked by kv: opening it again fails.
	kv, err := factory.NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
	require.NoError(t, err)
	_, err = factory.NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
	require.ErrorContains(t, err, "failed to open database")
	assert.Equal(t, maxMemTableSlots-1, freeSlots())
	require.NoError(t, kv.Close())
}
