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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
)

var testKVOptions = &FactoryOptions{
	InMemory:    true,
	CacheSizeMB: 1,
}

func TestPebbbleSimple(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("0")))
	assert.NoError(t, wb.Put("b", []byte("1")))
	assert.NoError(t, wb.Put("c", []byte("2")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	key, res, closer, err := kv.Get("a", ComparisonEqual)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonEqual)
	assert.NoError(t, err)
	assert.Equal(t, "b", key)
	assert.Equal(t, "1", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonEqual)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("non-existing", ComparisonEqual)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	wb = kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("00")))
	assert.NoError(t, wb.Put("b", []byte("11")))
	assert.NoError(t, wb.Put("d", []byte("22")))
	assert.NoError(t, wb.Delete("c"))

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	key, res, closer, err = kv.Get("a", ComparisonEqual)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "00", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonEqual)
	assert.NoError(t, err)
	assert.Equal(t, "b", key)
	assert.Equal(t, "11", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonEqual)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("d", ComparisonEqual)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "22", string(res))
	assert.NoError(t, closer.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleKeyRangeScan(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it, err := kv.KeyRangeScan("/root/a", "/root/c")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// Scan with empty result
	it, err = kv.KeyRangeScan("/xyz/a", "/xyz/c")
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleKeyRangeScanReverse(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it, err := kv.KeyRangeScanReverse("/root/a", "/root/c")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.False(t, it.Prev())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// Scan with empty result
	it, err = kv.KeyRangeScanReverse("/xyz/a", "/xyz/c")
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleRangeScan(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it, err := kv.RangeScan("/root/a", "/root/c")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	value, err := it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "a", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "b", string(value))
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// Scan with empty result
	it, err = kv.RangeScan("/xyz/a", "/xyz/c")
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleRangeScanWithSlashOrder(t *testing.T) {
	keys := []string{
		"/a/a/a/zzzzzz",
		"/a/b/a/a/a/a",
		"/a/b/a/c",
		"/a/b/a/a",
		"/a/b/a/a/a",
		"/a/b/a/b",
	}

	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()

	for _, k := range keys {
		assert.NoError(t, wb.Put(k, []byte(k)))
	}

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	it, err := kv.KeyRangeScan("/a/b/a/", "/a/b/a//")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/a/b/a/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/a/b/a/b", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/a/b/a/c", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())
}

func TestPebbbleGetWithinBatch(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("0")))
	assert.NoError(t, wb.Put("b", []byte("1")))
	assert.NoError(t, wb.Put("c", []byte("2")))

	value, closer, err := wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "0", string(value))
	assert.NoError(t, closer.Close())

	value, closer, err = wb.Get("non-existent")
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Nil(t, value)
	assert.Nil(t, closer)

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// Second batch

	wb = kv.NewWriteBatch()
	value, closer, err = wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "0", string(value))
	assert.NoError(t, closer.Close())

	assert.NoError(t, wb.Put("a", []byte("00")))

	value, closer, err = wb.Get("a")
	assert.NoError(t, err)
	assert.Equal(t, "00", string(value))
	assert.NoError(t, closer.Close())

	assert.NoError(t, wb.Delete("a"))

	value, closer, err = wb.Get("a")
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Nil(t, value)
	assert.Nil(t, closer)

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleDurability(t *testing.T) {
	options := &FactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 1,
		InMemory:    false,
	}

	// Open and write a key
	{
		factory, err := NewPebbleKVFactory(options)
		assert.NoError(t, err)
		kv, err := factory.NewKV(constant.DefaultNamespace, 1)
		assert.NoError(t, err)

		wb := kv.NewWriteBatch()
		assert.NoError(t, wb.Put("a", []byte("0")))
		assert.NoError(t, wb.Commit())
		assert.NoError(t, wb.Close())

		assert.NoError(t, kv.Close())
		assert.NoError(t, factory.Close())
	}

	// Open again and read it back
	{
		factory, err := NewPebbleKVFactory(options)
		assert.NoError(t, err)
		kv, err := factory.NewKV(constant.DefaultNamespace, 1)
		assert.NoError(t, err)

		key, res, closer, err := kv.Get("a", ComparisonEqual)
		assert.NoError(t, err)
		assert.Equal(t, "a", key)
		assert.Equal(t, "0", string(res))
		assert.NoError(t, closer.Close())

		assert.NoError(t, kv.Close())
		assert.NoError(t, factory.Close())
	}
}

func TestPebbbleRangeScanInBatch(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	wb = kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))

	it, err := wb.KeyRangeScan("/root/a", "/root/c")
	assert.NoError(t, err)
	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, wb.Put("/root/d", []byte("d")))

	assert.NoError(t, wb.Delete("/root/a"))

	it, err = wb.KeyRangeScan("/root/a", "/root/c")
	assert.NoError(t, err)
	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// Scan with empty result
	it, err = kv.KeyRangeScan("/xyz/a", "/xyz/c")
	assert.NoError(t, err)
	assert.False(t, it.Valid())
	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleDeleteRangeInBatch(t *testing.T) {
	keys := []string{
		"/a/a/a/zzzzzz",
		"/a/b/a/a/a/a",
		"/a/b/a/c",
		"/a/b/a/a",
		"/a/b/a/a/a",
		"/a/b/a/b",
	}

	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()

	for _, k := range keys {
		assert.NoError(t, wb.Put(k, []byte(k)))
	}

	err = wb.DeleteRange("/a/b/a/", "/a/b/a//")
	assert.NoError(t, err)

	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	key, res, closer, err := kv.Get("/a/b/a/a", ComparisonEqual)
	assert.Nil(t, res)
	assert.Nil(t, closer)
	assert.Equal(t, "", key)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	key, res, closer, err = kv.Get("/a/a/a/zzzzzz", ComparisonEqual)
	assert.Nil(t, err)
	assert.Equal(t, "/a/a/a/zzzzzz", key)
	assert.Equal(t, "/a/a/a/zzzzzz", string(res))
	assert.NoError(t, closer.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleDoubleOpen(t *testing.T) {
	factory, err := NewPebbleKVFactory(&FactoryOptions{
		DataDir:     t.TempDir(),
		CacheSizeMB: 1,
		InMemory:    false,
	})
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	kv2, err2 := factory.NewKV(constant.DefaultNamespace, 1)
	assert.Error(t, err2)
	assert.Nil(t, kv2)

	assert.NoError(t, kv.Close())
}

func TestPebbleSnapshot(t *testing.T) {
	originalLocation := t.TempDir()
	copiedLocation := t.TempDir()
	copiedLocationDbPath := filepath.Join(copiedLocation, constant.DefaultNamespace, "shard-1")

	{
		factory, err := NewPebbleKVFactory(&FactoryOptions{
			DataDir:     originalLocation,
			CacheSizeMB: 1,
			InMemory:    false,
		})
		assert.NoError(t, err)
		kv, err := factory.NewKV(constant.DefaultNamespace, 1)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			wb := kv.NewWriteBatch()
			for j := 0; j < 100; j++ {
				assert.NoError(t, wb.Put(fmt.Sprintf("key-%d-%d", i, j),
					[]byte(fmt.Sprintf("value-%d-%d", i, j))))
			}

			assert.NoError(t, wb.Commit())
			assert.NoError(t, wb.Close())
		}

		s, err := kv.Snapshot()
		assert.NoError(t, err)

		// Copy the snapshot to a new directory
		assert.NoError(t, os.MkdirAll(copiedLocationDbPath, 0755))

		for ; s.Valid(); s.Next() {
			f, err := s.Chunk()
			assert.NoError(t, err)
			content := f.Content()
			fileName := f.Name()
			file, err := os.OpenFile(filepath.Join(copiedLocationDbPath, fileName), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
			assert.NoError(t, err)
			for len(content) > 0 {
				n, err := file.Write(content)
				assert.NoError(t, err)
				content = content[n:]
			}
			assert.NoError(t, file.Close())
		}

		_, err = os.Stat(s.BasePath())
		assert.NoError(t, err)

		// Closing the snapshot must get rid of its directory
		assert.NoError(t, s.Close())

		_, err = os.Stat(s.BasePath())
		assert.ErrorIs(t, err, os.ErrNotExist)

		assert.NoError(t, kv.Close())
	}

	{
		// Open the database from the copied location
		factory2, err := NewPebbleKVFactory(&FactoryOptions{
			DataDir:     copiedLocation,
			CacheSizeMB: 1,
			InMemory:    false,
		})
		assert.NoError(t, err)
		kv2, err := factory2.NewKV(constant.DefaultNamespace, 1)
		assert.NoError(t, err)

		for i := 0; i < 100; i++ {
			for j := 0; j < 100; j++ {
				k := fmt.Sprintf("key-%d-%d", i, j)
				key, r, closer, err := kv2.Get(k, ComparisonEqual)
				assert.NoError(t, err)
				assert.Equal(t, k, key)
				assert.Equal(t, fmt.Sprintf("value-%d-%d", i, j), string(r))
				assert.NoError(t, closer.Close())
			}
		}

		assert.NoError(t, kv2.Close())
		assert.NoError(t, factory2.Close())
	}
}

func TestPebbleSnapshot_Loader(t *testing.T) {
	originalLocation := t.TempDir()
	newLocation := t.TempDir()

	factory, err := NewPebbleKVFactory(&FactoryOptions{
		DataDir:     originalLocation,
		CacheSizeMB: 1,
		InMemory:    false,
	})
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		wb := kv.NewWriteBatch()
		for j := 0; j < 100; j++ {
			assert.NoError(t, wb.Put(fmt.Sprintf("key-%d-%d", i, j),
				[]byte(fmt.Sprintf("value-%d-%d", i, j))))
		}

		assert.NoError(t, wb.Commit())
		assert.NoError(t, wb.Close())
	}

	snapshot, err := kv.Snapshot()
	assert.NoError(t, err)

	// Use the snapshot to load a new DB
	factory2, err := NewPebbleKVFactory(&FactoryOptions{
		DataDir:     newLocation,
		CacheSizeMB: 1,
		InMemory:    false,
	})
	assert.NoError(t, err)

	kv2, err := factory2.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	// Any existing key would be removed when we load the snapshot
	wb := kv2.NewWriteBatch()
	assert.NoError(t, wb.Put("my-key-2", []byte("my-value-2")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())
	assert.NoError(t, kv2.Close())

	loader, err := factory2.NewSnapshotLoader(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	for ; snapshot.Valid(); snapshot.Next() {
		f, err := snapshot.Chunk()
		assert.NoError(t, err)
		assert.NoError(t, loader.AddChunk(f.Name(), f.Index(), f.TotalCount(), f.Content()))
	}

	loader.Complete()
	assert.NoError(t, loader.Close())
	assert.NoError(t, snapshot.Close())

	kv2, err = factory2.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		for j := 0; j < 100; j++ {
			k := fmt.Sprintf("key-%d-%d", i, j)
			key, r, closer, err := kv2.Get(k, ComparisonEqual)
			assert.NoError(t, err)
			assert.Equal(t, k, key)
			assert.Equal(t, fmt.Sprintf("value-%d-%d", i, j), string(r))
			assert.NoError(t, closer.Close())
		}
	}

	key, r, closer, err := kv2.Get("my-key", ComparisonEqual)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, r)
	assert.Nil(t, closer)

	assert.NoError(t, kv2.Close())
	assert.NoError(t, factory2.Close())
}

func TestPebbleRangeScanNoLimits(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// No max
	it, err := kv.RangeScan("/root/b", "")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	value, err := it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "b", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "c", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "d", string(value))
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// No min
	it, err = kv.RangeScan("", "/root/c")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "a", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "b", string(value))
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// No min and max
	it, err = kv.RangeScan("", "")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "a", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "b", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "c", string(value))
	assert.True(t, it.Next())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
	value, err = it.Value()
	assert.NoError(t, err)
	assert.Equal(t, "d", string(value))
	assert.False(t, it.Next())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleReverseRangeScanNoLimits(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("/root/a", []byte("a")))
	assert.NoError(t, wb.Put("/root/b", []byte("b")))
	assert.NoError(t, wb.Put("/root/c", []byte("c")))
	assert.NoError(t, wb.Put("/root/d", []byte("d")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	// No max
	it, err := kv.KeyRangeScanReverse("/root/b", "")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.False(t, it.Prev())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// No min
	it, err = kv.KeyRangeScanReverse("", "/root/c")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.False(t, it.Prev())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	// No min and max
	it, err = kv.KeyRangeScanReverse("", "")
	assert.NoError(t, err)

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/d", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/c", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/b", it.Key())
	assert.True(t, it.Prev())

	assert.True(t, it.Valid())
	assert.Equal(t, "/root/a", it.Key())
	assert.False(t, it.Prev())

	assert.False(t, it.Valid())

	assert.NoError(t, it.Close())

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbbleFloorCeiling(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	key, res, closer, err := kv.Get("e", ComparisonFloor)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("e", ComparisonCeiling)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("e", ComparisonLower)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("e", ComparisonHigher)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	// ---------------------------------------------------------------

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("0")))
	// assert.NoError(t, wb.Put("b", []byte("1"))) // Intentionally skipped
	assert.NoError(t, wb.Put("c", []byte("2")))
	assert.NoError(t, wb.Put("d", []byte("3")))
	assert.NoError(t, wb.Put("e", []byte("4")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	key, res, closer, err = kv.Get("a", ComparisonEqual)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("a", ComparisonFloor)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("a", ComparisonCeiling)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("a", ComparisonLower)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("a", ComparisonHigher)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	// ---------------------------------------------------------------
	key, res, closer, err = kv.Get("b", ComparisonEqual)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	key, res, closer, err = kv.Get("b", ComparisonFloor)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonCeiling)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonLower)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("b", ComparisonHigher)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	// ---------------------------------------------------------------

	key, res, closer, err = kv.Get("c", ComparisonFloor)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonCeiling)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonLower)
	assert.NoError(t, err)
	assert.Equal(t, "a", key)
	assert.Equal(t, "0", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("c", ComparisonHigher)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(res))
	assert.NoError(t, closer.Close())

	// ---------------------------------------------------------------

	key, res, closer, err = kv.Get("d", ComparisonFloor)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("d", ComparisonCeiling)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("d", ComparisonLower)
	assert.NoError(t, err)
	assert.Equal(t, "c", key)
	assert.Equal(t, "2", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("d", ComparisonHigher)
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(res))
	assert.NoError(t, closer.Close())

	// ---------------------------------------------------------------

	key, res, closer, err = kv.Get("e", ComparisonFloor)
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("e", ComparisonCeiling)
	assert.NoError(t, err)
	assert.Equal(t, "e", key)
	assert.Equal(t, "4", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("e", ComparisonLower)
	assert.NoError(t, err)
	assert.Equal(t, "d", key)
	assert.Equal(t, "3", string(res))
	assert.NoError(t, closer.Close())

	key, res, closer, err = kv.Get("e", ComparisonHigher)
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)
	assert.Nil(t, res)
	assert.Nil(t, closer)

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}

func TestPebbleFindLowerInBatch(t *testing.T) {
	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	kv, err := factory.NewKV(constant.DefaultNamespace, 1)
	assert.NoError(t, err)

	wb := kv.NewWriteBatch()
	assert.NoError(t, wb.Put("a", []byte("a")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	wb = kv.NewWriteBatch()
	assert.NoError(t, wb.Put("b", []byte("b")))
	assert.NoError(t, wb.Put("c", []byte("c")))

	key, err := wb.FindLower("a")
	assert.ErrorIs(t, err, ErrKeyNotFound)
	assert.Equal(t, "", key)

	key, err = wb.FindLower("b")
	assert.NoError(t, err)
	assert.Equal(t, "a", key)

	key, err = wb.FindLower("c")
	assert.NoError(t, err)
	assert.Equal(t, "b", key)

	key, err = wb.FindLower("d")
	assert.NoError(t, err)
	assert.Equal(t, "c", key)

	key, err = wb.FindLower("z")
	assert.NoError(t, err)
	assert.Equal(t, "c", key)

	assert.NoError(t, kv.Close())
	assert.NoError(t, factory.Close())
}
