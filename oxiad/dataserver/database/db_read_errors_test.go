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
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

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
	// Once the db is reopened, Pebble retries loading the stats of an
	// unreadable sstable in a loop, for as long as the db stays open
	t.Cleanup(func() { _ = os.Chmod(sst, 0600) })
	if f, err := os.Open(sst); err == nil {
		assert.NoError(t, f.Close())
		t.Skip("the sstable is still readable, as when running as root")
	}
}

func TestDB_FeatureFlagsReadFailure(t *testing.T) {
	options := kvstore.NewFactoryOptionsForTest(t)
	factory, err := kvstore.NewPebbleKVFactory(options)
	require.NoError(t, err)
	defer factory.Close()
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)

	_, err = db.ProcessControlRequest(&proto.ControlRequest{Value: &proto.ControlRequest_FeatureEnable{
		FeatureEnable: &proto.FeatureEnableRequest{Features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}},
	}}, 0, 0, NoOpCallback)
	require.NoError(t, err)
	flushIntoUnreadableSST(t, db.RawKV(), options.DataDir)
	require.NoError(t, db.Close())

	// Opening the db reads the feature flags back. Serving the shard without
	// the ones it couldn't read would apply entries with those features off.
	db, err = NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	if err == nil {
		assert.NoError(t, db.Close())
	}
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.ErrorContains(t, err, "feature flags")
}

func TestDB_SequenceUpdatesReadFailure(t *testing.T) {
	options := kvstore.NewFactoryOptionsForTest(t)
	factory, err := kvstore.NewPebbleKVFactory(options)
	require.NoError(t, err)
	defer factory.Close()
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)
	defer db.Close()

	for offset := int64(0); offset < 2; offset++ {
		_, err = db.ProcessWrite(&proto.WriteRequest{Puts: []*proto.PutRequest{{
			Key:              "a",
			Value:            []byte("0"),
			SequenceKeyDelta: []uint64{1},
			PartitionKey:     pb.String("x"),
		}}}, offset, 0, NoOpCallback)
		require.NoError(t, err)
	}
	flushIntoUnreadableSST(t, db.RawKV(), options.DataDir)

	// A subscription starts from the last key of the sequence
	_, err = db.GetSequenceUpdates("a")
	assert.ErrorIs(t, err, os.ErrPermission)
}

func TestDB_NotificationsReadFailure(t *testing.T) {
	options := kvstore.NewFactoryOptionsForTest(t)
	factory, err := kvstore.NewPebbleKVFactory(options)
	require.NoError(t, err)
	defer factory.Close()
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, 1*time.Hour, time2.SystemClock)
	require.NoError(t, err)
	defer db.Close()

	for offset := int64(0); offset < 3; offset++ {
		_, err = db.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{Key: "a", Value: []byte("0")}},
		}, offset, now(), NoOpCallback)
		require.NoError(t, err)
	}
	flushIntoUnreadableSST(t, db.RawKV(), options.DataDir)

	// An empty result would be taken for trimmed batches: the read would skip
	// the three batches and wait for the next one
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err = db.ReadNextNotifications(ctx, 0)
	assert.ErrorIs(t, err, os.ErrPermission)
}
