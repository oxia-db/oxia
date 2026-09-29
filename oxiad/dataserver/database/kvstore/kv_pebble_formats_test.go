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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/compare"
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

func TestPebbleDbConversionFailureClosesDbs(t *testing.T) {
	kvFactory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	oldKV, err := kvFactory.NewKV("default", 0, proto.KeySortingType_NATURAL)
	require.NoError(t, err)

	wb := oldKV.NewWriteBatch()
	assert.NoError(t, wb.Put("/key/a", []byte("value")))
	assert.NoError(t, wb.Commit())
	assert.NoError(t, wb.Close())
	dbPath := oldKV.(*Pebble).dbPath
	assert.NoError(t, oldKV.Close())

	// Hold the lock of the temp conversion db, so the conversion fails to open
	// it after opening the old db
	newDbPath := makeSwapTmpDbPath(dbPath, compare.EncoderHierarchical)
	require.NoError(t, os.MkdirAll(newDbPath, 0755))
	lock, err := pebble.LockDirectory(newDbPath, vfs.Default)
	require.NoError(t, err)
	_, err = kvFactory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	assert.ErrorContains(t, err, "failed to open new database")
	assert.NoError(t, lock.Close())

	// The failed conversion must have closed the old db, or its lock would
	// fail this retry
	kv, err := kvFactory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	require.NoError(t, err)

	_, value, closer, err := kv.Get("/key/a", ComparisonEqual, NoInternalKeys)
	assert.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
	assert.NoError(t, closer.Close())
	assert.NoError(t, kv.Close())
}

func TestPebbleDbConversionReadFailure(t *testing.T) {
	kvFactory, err := NewPebbleKVFactory(NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	oldKV, err := kvFactory.NewKV("default", 0, proto.KeySortingType_NATURAL)
	require.NoError(t, err)

	// Flush each key into its own sstable
	keys := []string{"/key/a", "/key/b"}
	for _, key := range keys {
		wb := oldKV.NewWriteBatch()
		assert.NoError(t, wb.Put(key, []byte("value")))
		assert.NoError(t, wb.Commit())
		assert.NoError(t, wb.Close())
		assert.NoError(t, oldKV.Flush())
	}
	dbPath := oldKV.(*Pebble).dbPath
	assert.NoError(t, oldKV.Close())

	// Make the second sstable unreadable, so the copy fails with an I/O error
	// after copying the first one. A corrupted block wouldn't do: Pebble
	// reports corruptions through the logger's Fatalf, which exits the process.
	ssts, err := filepath.Glob(filepath.Join(dbPath, "*.sst"))
	require.NoError(t, err)
	require.Len(t, ssts, 2)
	require.NoError(t, os.Chmod(ssts[1], 0))
	if f, err := os.Open(ssts[1]); err == nil {
		assert.NoError(t, f.Close())
		t.Skip("the sstable is still readable, as when running as root")
	}

	_, err = kvFactory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	assert.ErrorIs(t, err, os.ErrPermission)

	// The failed conversion left the old db in place, so once the sstable is
	// readable again the conversion copies both keys
	require.NoError(t, os.Chmod(ssts[1], 0600))
	kv, err := kvFactory.NewKV("default", 0, proto.KeySortingType_HIERARCHICAL)
	require.NoError(t, err)

	it, err := kv.KeyRangeScan("/", "", NoInternalKeys)
	require.NoError(t, err)
	var scanKeys []string
	for it.Valid() {
		scanKeys = append(scanKeys, it.Key())
		it.Next()
	}
	assert.Equal(t, keys, scanKeys)
	assert.NoError(t, it.Close())
	assert.NoError(t, kv.Close())
}

func TestCreateMarkerLeavesUpToDateMarkerAlone(t *testing.T) {
	dbPath := t.TempDir()
	markerPath := filepath.Join(dbPath, markerFileName)
	assert.NoError(t, createMarker(dbPath, compare.EncoderNatural.Name()))

	// Backdate the marker, to tell whether it gets rewritten
	modTime := time.Now().Add(-time.Hour).Truncate(time.Second)
	assert.NoError(t, os.Chtimes(markerPath, modTime, modTime))

	assert.NoError(t, createMarker(dbPath, compare.EncoderNatural.Name()))
	stat, err := os.Stat(markerPath)
	assert.NoError(t, err)
	assert.True(t, modTime.Equal(stat.ModTime()), "the marker was rewritten")

	// A different encoding is still written
	assert.NoError(t, createMarker(dbPath, compare.EncoderHierarchical.Name()))
	markerData, err := os.ReadFile(markerPath)
	assert.NoError(t, err)
	assert.Equal(t, compare.EncoderHierarchical.Name(), string(markerData))
}
