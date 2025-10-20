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

package kv

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

func TestPebbleDbConversion(t *testing.T) {
	// Create DB with old format and insert some test keys
	conv := pebbleDbConversion{}
	conf := conv.configForOldCompareHierarchical()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "default", "shard-0")
	oldDb, err := pebble.Open(dbPath, conf)
	assert.NoError(t, err)

	keys := []string{"/key",
		"/key/a", "/key/b", "/key/c",
		"/key/a/1", "/key/a/2",
		"/key/b/1", "/key/b/2",
		"/key/c/1", "/key/c/2",
		"/key/a/1/x", "/key/a/1/y",
		"/key/b/1/x", "/key/b/1/y",
	}
	for _, key := range keys {
		assert.NoError(t, oldDb.Set([]byte(key), []byte("value"), pebble.NoSync))
	}

	assert.NoError(t, oldDb.Flush())
	assert.NoError(t, oldDb.Close())

	kvFactory, err := NewPebbleKVFactory(&FactoryOptions{
		DataDir:     tmpDir,
		CacheSizeMB: 1,
		InMemory:    false,
	})
	assert.NoError(t, err)

	kv, err := kvFactory.NewKV("default", 0)
	assert.NoError(t, err)

	// Test scan the new DB
	it, err := kv.KeyRangeScan("/", "__oxia/")
	assert.NoError(t, err)

	var scanKeys []string
	for it.Valid() {
		scanKeys = append(scanKeys, it.Key())
		it.Next()
	}

	assert.Equal(t, keys, scanKeys)
	assert.NoError(t, it.Close())

	// Test scan a range
	it, err = kv.KeyRangeScan("/key/a/", "/key/a//")
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
