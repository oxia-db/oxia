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
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
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

func TestPebbleDbCleanupBackupAfterCrashDuringFinalCleanup(t *testing.T) {
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

// The files of the WAL segments that share the directory of the database when
// the WAL dir and the data dir are the same, with both codecs, as after an
// upgrade from the v1 codec.
var walSegmentFiles = []string{"0.txn", "0.idx", "1000.txnx", "1000.idxx"}

func writeWalSegmentFiles(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0755))
	for _, name := range walSegmentFiles {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0600))
	}
}

func assertWalSegmentFiles(t *testing.T, dir string) {
	t.Helper()
	for _, name := range walSegmentFiles {
		content, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		assert.Equal(t, name, string(content))
	}
}

// newTestKvFactory creates a factory on dataDir that fails at the given
// conversion trap, if any.
func newTestKvFactory(t *testing.T, dataDir string, trap string) Factory {
	t.Helper()
	options := &FactoryOptions{DataDir: dataDir, CacheSizeMB: 1}
	if trap != "" {
		options.KvTrap = NewKvTrap(map[string]func() error{
			trap: func() error {
				return errors.New("you hit the trap")
			},
		})
	}
	kvFactory, err := NewPebbleKVFactory(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, kvFactory.Close())
	})
	return kvFactory
}

// newDbSharingDirWithWal creates a database with the natural key encoding in
// the shard directory of the WAL, and returns the directory.
func newDbSharingDirWithWal(t *testing.T, dataDir string) string {
	t.Helper()
	dbPath := filepath.Join(dataDir, constant.DefaultNamespace, "shard-0")
	writeWalSegmentFiles(t, dbPath)
	kv, err := newTestKvFactory(t, dataDir, "").NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_NATURAL)
	require.NoError(t, err)
	wb := kv.NewWriteBatch()
	require.NoError(t, wb.Put("/key", []byte("value")))
	require.NoError(t, wb.Commit())
	require.NoError(t, wb.Close())
	require.NoError(t, kv.Close())
	return dbPath
}

func assertKey(t *testing.T, kv KV) {
	t.Helper()
	_, value, closer, err := kv.Get("/key", ComparisonEqual, NoInternalKeys)
	require.NoError(t, err)
	assert.Equal(t, "value", string(value))
	assert.NoError(t, closer.Close())
}

// The WAL creates the shard directory first when it shares the data dir: the
// database must be created in it, next to the segments.
func TestPebbleDbInWalDir(t *testing.T) {
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, constant.DefaultNamespace, "shard-0")
	writeWalSegmentFiles(t, dbPath)
	dirInfo, err := os.Stat(dbPath)
	require.NoError(t, err)

	kv, err := newTestKvFactory(t, dataDir, "").NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	// With no database to convert, the directory was not replaced
	newDirInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	assert.True(t, os.SameFile(dirInfo, newDirInfo))
	assertWalSegmentFiles(t, dbPath)
}

// Nodes from before the key encoding marker kept the WAL segments and the
// database in the same directory when the WAL dir and the data dir were the
// same: the conversion must keep the segments, which the WAL holds open.
func TestPebbleDbConversionKeepsWalSegments(t *testing.T) {
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, constant.DefaultNamespace, "shard-0")
	writeWalSegmentFiles(t, dbPath)
	oldDb, err := pebble.Open(dbPath, newPebbleDbConversion(slog.Default(), dbPath, nil).configForOldCompareHierarchical())
	require.NoError(t, err)
	require.NoError(t, oldDb.Set([]byte("/key"), []byte("value"), pebble.NoSync))
	require.NoError(t, oldDb.Flush())
	require.NoError(t, oldDb.Close())

	// The current segment, which the WAL holds open
	segment, err := os.OpenFile(filepath.Join(dbPath, "1000.txnx"), os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	defer segment.Close()

	kv, err := newTestKvFactory(t, dataDir, "").NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
	require.NoError(t, err)
	assertKey(t, kv)
	require.NoError(t, kv.Close())

	// The WAL still writes into its segment, not into a deleted file
	segmentInfo, err := segment.Stat()
	require.NoError(t, err)
	pathInfo, err := os.Stat(filepath.Join(dbPath, "1000.txnx"))
	require.NoError(t, err)
	assert.True(t, os.SameFile(segmentInfo, pathInfo))
	assertWalSegmentFiles(t, dbPath)
	assert.NoDirExists(t, makeDbBackupPath(dbPath))
}

// Recovering a conversion that crashed must keep the WAL segments.
func TestPebbleDbConversionCrashKeepsWalSegments(t *testing.T) {
	for _, trap := range []string{"convertCrashAfterMoveOldDb", "convertCrashAfterMoveNewDb"} {
		t.Run(trap, func(t *testing.T) {
			dataDir := t.TempDir()
			dbPath := newDbSharingDirWithWal(t, dataDir)
			_, err := newTestKvFactory(t, dataDir, trap).NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
			require.Error(t, err)

			// Opening the database again recovers the conversion
			kv, err := newTestKvFactory(t, dataDir, "").NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
			require.NoError(t, err)
			assertKey(t, kv)
			require.NoError(t, kv.Close())
			assertWalSegmentFiles(t, dbPath)
			assert.NoDirExists(t, makeDbBackupPath(dbPath))
		})
	}
}

// When the WAL is opened before the database, it creates the directory again
// after a crash between the two moves of the conversion. The backup then holds
// the database and the WAL segments, and must not be deleted.
func TestPebbleDbConversionCrashWithRecreatedDir(t *testing.T) {
	dataDir := t.TempDir()
	dbPath := newDbSharingDirWithWal(t, dataDir)
	_, err := newTestKvFactory(t, dataDir, "convertCrashAfterMoveOldDb").
		NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
	require.Error(t, err)

	// The WAL creates the directory again, with a new segment
	require.NoError(t, os.MkdirAll(dbPath, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dbPath, "0.txnx"), nil, 0600))

	_, err = newTestKvFactory(t, dataDir, "").NewKV(constant.DefaultNamespace, 0, proto.KeySortingType_HIERARCHICAL)
	assert.ErrorContains(t, err, "interrupted conversion")

	backupPath := makeDbBackupPath(dbPath)
	desc, err := pebble.Peek(backupPath, vfs.Default)
	require.NoError(t, err)
	assert.True(t, desc.Exists)
	assertWalSegmentFiles(t, backupPath)
}
