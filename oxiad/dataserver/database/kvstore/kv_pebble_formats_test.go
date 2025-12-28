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

package kvstore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestPebbleDbConversion(t *testing.T) {
	// Create DB with natural format and insert some test keys
	kvFactory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	oldKV, err := kvFactory.NewKV("default", 0, proto.KeySortingType_NATURAL)
	assert.NoError(t, err)

	keys := []string{"/key",
		"/key/a", "/key/b", "/key/c",
		"/key/a/1", "/key/a/2",
		"/key/b/1", "/key/b/2",
		"/key/c/1", "/key/c/2",
		"/key/a/1/x", "/key/a/1/y",
		"/key/b/1/x", "/key/b/1/y",
	}

	wb := oldKV.NewWriteBatch()
	for _, key := range keys {
		assert.NoError(t, wb.Put(key, []byte("value")))
	}
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	assert.NoError(t, oldKV.Close())

	kv, err := kvFactory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	// Test scan the new DB
	it, err := kv.KeyRangeScan("/", "", NoInternalKeys)
	assert.NoError(t, err)

	var scanKeys []string
	for it.Valid() {
		scanKeys = append(scanKeys, it.Key())
		it.Next()
	}

	assert.Equal(t, keys, scanKeys)
	assert.NoError(t, it.Close())

	// Test scan a range
	it, err = kv.KeyRangeScan("/key/a/", "/key/a//", NoInternalKeys)
	assert.NoError(t, err)

	scanKeys = []string{}
	for it.Valid() {
		scanKeys = append(scanKeys, it.Key())
		it.Next()
	}

	assert.Equal(t, []string{"/key/a/1", "/key/a/2"}, scanKeys)

	assert.NoError(t, it.Close())
	assert.NoError(t, kv.Close())
}

func TestPebbleDbConversionPreservesDataAfterCrash(t *testing.T) {
	// Create DB with natural format and insert some test keys
	tmpDir := t.TempDir()
	kvFactory, err := NewPebbleKVFactory(&FactoryOptions{
		DataDir:     tmpDir,
		CacheSizeMB: 1,
		UseWAL:      false,
		SyncData:    false,
	})
	assert.NoError(t, err)

	trapKvFactory, err := NewPebbleKVFactory(&FactoryOptions{
		DataDir:     tmpDir,
		CacheSizeMB: 1,
		UseWAL:      false,
		SyncData:    false,
		KvTrap: NewKvTrap(map[string]func() error{
			"convertCrashAfterMoveOldDb": func() error {
				return errors.New("you hit the trap")
			},
		}),
	})
	assert.NoError(t, err)
	oldKV, err := kvFactory.NewKV("default", 0, proto.KeySortingType_NATURAL)
	assert.NoError(t, err)

	keys := []string{"/key",
		"/key/a", "/key/b", "/key/c",
		"/key/a/1", "/key/a/2",
		"/key/b/1", "/key/b/2",
		"/key/c/1", "/key/c/2",
		"/key/a/1/x", "/key/a/1/y",
		"/key/b/1/x", "/key/b/1/y",
	}

	wb := oldKV.NewWriteBatch()
	for _, key := range keys {
		assert.NoError(t, wb.Put(key, []byte("value")))
	}
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	assert.NoError(t, oldKV.Close())

	// try to trap the kv
	_, err = trapKvFactory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	assert.Error(t, err)

	// retry it without trap
	kv, err := kvFactory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	// Test scan the new DB
	it, err := kv.KeyRangeScan("/", "", NoInternalKeys)
	assert.NoError(t, err)

	var scanKeys []string
	for it.Valid() {
		scanKeys = append(scanKeys, it.Key())
		it.Next()
	}
	assert.Equal(t, keys, scanKeys)
	assert.NoError(t, it.Close())
	// Test scan a range
	it, err = kv.KeyRangeScan("/key/a/", "/key/a//", NoInternalKeys)
	assert.NoError(t, err)
	scanKeys = []string{}
	for it.Valid() {
		scanKeys = append(scanKeys, it.Key())
		it.Next()
	}
	assert.Equal(t, []string{"/key/a/1", "/key/a/2"}, scanKeys)
	assert.NoError(t, it.Close())
	assert.NoError(t, kv.Close())
}

func TestPebbleDbCleanupExpiredBackup(t *testing.T) {
	// Create DB with natural format and insert some test keys
	tmpDir := t.TempDir()
	kvFactory, err := NewPebbleKVFactory(&FactoryOptions{
		DataDir:     tmpDir,
		CacheSizeMB: 1,
		UseWAL:      false,
		SyncData:    false,
	})
	assert.NoError(t, err)

	trapKvFactory, err := NewPebbleKVFactory(&FactoryOptions{
		DataDir:     tmpDir,
		CacheSizeMB: 1,
		UseWAL:      false,
		SyncData:    false,
		KvTrap: NewKvTrap(map[string]func() error{
			"convertCrashAfterMoveNewDb": func() error {
				return errors.New("you hit the trap")
			},
		}),
	})
	assert.NoError(t, err)
	oldKV, err := kvFactory.NewKV("default", 0, proto.KeySortingType_NATURAL)
	assert.NoError(t, err)

	keys := []string{"/key",
		"/key/a", "/key/b", "/key/c",
		"/key/a/1", "/key/a/2",
		"/key/b/1", "/key/b/2",
		"/key/c/1", "/key/c/2",
		"/key/a/1/x", "/key/a/1/y",
		"/key/b/1/x", "/key/b/1/y",
	}

	wb := oldKV.NewWriteBatch()
	for _, key := range keys {
		assert.NoError(t, wb.Put(key, []byte("value")))
	}
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())

	assert.NoError(t, oldKV.Close())

	// try to trap the kv
	_, err = trapKvFactory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	assert.Error(t, err)

	// retry it without trap
	kv, err := kvFactory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	assert.NoError(t, err)

	// Test scan the new DB
	it, err := kv.KeyRangeScan("/", "", NoInternalKeys)
	assert.NoError(t, err)

	var scanKeys []string
	for it.Valid() {
		scanKeys = append(scanKeys, it.Key())
		it.Next()
	}
	assert.Equal(t, keys, scanKeys)
	assert.NoError(t, it.Close())
	// Test scan a range
	it, err = kv.KeyRangeScan("/key/a/", "/key/a//", NoInternalKeys)
	assert.NoError(t, err)
	scanKeys = []string{}
	for it.Valid() {
		scanKeys = append(scanKeys, it.Key())
		it.Next()
	}
	assert.Equal(t, []string{"/key/a/1", "/key/a/2"}, scanKeys)
	assert.NoError(t, it.Close())

	dbPath := kv.(*Pebble).dbPath
	assert.NoError(t, kv.Close())
	// check if backup still exist
	path := makeDbBackupPath(dbPath)
	assert.False(t, pathExists(path))
}
