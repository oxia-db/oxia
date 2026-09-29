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

package lead

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
)

// flushIntoUnreadableSST flushes the memtable into a new sstable and makes it
// unreadable, so that the next read that needs it fails with an I/O error.
// Pebble doesn't open a table it just flushed, and the tests keep the
// notifications trimmer idle with a long retention, so that read is the first
// to open it. A corrupted block wouldn't do: Pebble reports corruptions through
// the logger's Fatalf, which exits the process.
func flushIntoUnreadableSST(t *testing.T, kv kvstore.KV, dataDir string) {
	t.Helper()
	listSSTs := func() []string {
		ssts, err := filepath.Glob(filepath.Join(dataDir, "*", "*", "*.sst"))
		require.NoError(t, err)
		return ssts
	}
	before := listSSTs()
	require.NoError(t, kv.Flush())
	flushed := slices.DeleteFunc(listSSTs(), func(sst string) bool { return slices.Contains(before, sst) })
	require.Len(t, flushed, 1)

	sst := flushed[0]
	require.NoError(t, os.Chmod(sst, 0))
	t.Cleanup(func() { _ = os.Chmod(sst, 0600) })
	if f, err := os.Open(sst); err == nil {
		assert.NoError(t, f.Close())
		t.Skip("the sstable is still readable, as when running as root")
	}
}

func putKeys(t *testing.T, kv kvstore.KV, keys ...string) {
	t.Helper()
	wb := kv.NewWriteBatch()
	for _, key := range keys {
		require.NoError(t, wb.Put(key, []byte{}))
	}
	require.NoError(t, wb.Commit())
	require.NoError(t, wb.Close())
}

func newReadErrorsTestDB(t *testing.T) (database.DB, string) {
	t.Helper()
	options := kvstore.NewFactoryOptionsForTest(t)
	kvFactory, err := kvstore.NewPebbleKVFactory(options)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kvFactory.Close()) })
	db, err := database.NewDB(constant.DefaultNamespace, 1, kvFactory, proto.KeySortingType_HIERARCHICAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	return db, options.DataDir
}

// Deleting a session deletes the ephemeral records its shadow keys list. When
// reading the shadow keys fails, the delete must fail too: committing it would
// leave the records that weren't reached behind for good.
func TestSessionDeleteShadowKeysReadFailure(t *testing.T) {
	db, dataDir := newReadErrorsTestDB(t)

	// The shadow keys go into the unreadable sstable, while the session key
	// stays readable in the memtable
	sessionId := SessionId(1)
	putKeys(t, db.RawKV(), ShadowKey(sessionId, "/a"), ShadowKey(sessionId, "/b"))
	flushIntoUnreadableSST(t, db.RawKV(), dataDir)
	putKeys(t, db.RawKV(), SessionKey(sessionId))

	_, err := db.ProcessWrite(&proto.WriteRequest{
		Deletes: []*proto.DeleteRequest{{Key: SessionKey(sessionId)}},
	}, 0, 0, WrapperUpdateOperationCallback)
	assert.ErrorIs(t, err, os.ErrPermission)
}

// A secondary index get must fail when reading the index fails. FLOOR used to
// seek back after a failed seek, which cleared the error and returned the entry
// below the unreadable one.
func TestSecondaryIndexGetReadFailure(t *testing.T) {
	db, dataDir := newReadErrorsTestDB(t)

	// Entry "0" stays readable, entry "5" goes into the unreadable sstable
	putKeys(t, db.RawKV(), secondaryIndexKey("/a", &proto.SecondaryIndex{IndexName: "idx", SecondaryKey: "0"}))
	require.NoError(t, db.RawKV().Flush())
	putKeys(t, db.RawKV(), secondaryIndexKey("/b", &proto.SecondaryIndex{IndexName: "idx", SecondaryKey: "5"}))
	flushIntoUnreadableSST(t, db.RawKV(), dataDir)

	for _, comparisonType := range []proto.KeyComparisonType{proto.KeyComparisonType_EQUAL, proto.KeyComparisonType_FLOOR} {
		primaryKey, _, err := doSecondaryGet(db, &proto.GetRequest{
			Key:                "5",
			SecondaryIndexName: pb.String("idx"),
			ComparisonType:     comparisonType,
		})
		assert.ErrorIs(t, err, os.ErrPermission, comparisonType)
		assert.Empty(t, primaryKey, comparisonType)
	}
}
