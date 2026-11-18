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

package controller

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

// newLeaderWithEntries starts a shards director leading a shard with the
// given keys written, in term 1.
func newLeaderWithEntries(t *testing.T, shard int64, keys ...string) ShardsDirector {
	t.Helper()
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	sd := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	t.Cleanup(func() { assert.NoError(t, sd.Close()) })
	lc, err := sd.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: shard, Term: 1, ReplicationFactor: 1})
	require.NoError(t, err)
	for _, key := range keys {
		_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
			Shard: &shard,
			Puts:  []*proto.PutRequest{{Key: key, Value: []byte("value-" + key)}},
		})
		require.NoError(t, err)
	}
	return sd
}

func TestInstallerInstallsACheckpointAsTheDatabaseOfAShard(t *testing.T) {
	shard := int64(7)
	source := newLeaderWithEntries(t, shard, "a", "b", "c")
	checkpoint, err := source.Checkpoint(shard)
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	target := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	defer func() { assert.NoError(t, target.Close()) }()

	installer, err := target.NewInstaller(constant.DefaultNamespace, shard)
	require.NoError(t, err)
	installCheckpoint(t, installer, checkpoint)
	commitOffset, term, err := installer.Complete(&proto.HashRange{Min: 0, Max: ^uint32(0)})
	require.NoError(t, err)
	assert.EqualValues(t, 2, commitOffset)
	assert.EqualValues(t, 1, term)
	require.NoError(t, installer.Close())

	// The next term finds the restored entries
	lc, err := target.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: shard, Term: 2, ReplicationFactor: 1})
	require.NoError(t, err)
	var values []string
	require.NoError(t, lc.Read(context.Background(), &proto.ReadRequest{Gets: []*proto.GetRequest{
		{Key: "a", IncludeValue: true}, {Key: "b", IncludeValue: true}, {Key: "c", IncludeValue: true},
	}}, func(res *proto.GetResponse) error {
		values = append(values, string(res.Value))
		return nil
	}))
	assert.Equal(t, []string{"value-a", "value-b", "value-c"}, values)
}

func TestInstallerFiltersTheDatabaseToTheHashRange(t *testing.T) {
	shard := int64(7)
	keys := make([]string, 0, 64)
	for i := range 64 {
		keys = append(keys, "key-"+strings.Repeat("x", i))
	}
	source := newLeaderWithEntries(t, shard, keys...)
	checkpoint, err := source.Checkpoint(shard)
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	target := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	defer func() { assert.NoError(t, target.Close()) }()
	installer, err := target.NewInstaller(constant.DefaultNamespace, shard)
	require.NoError(t, err)
	installCheckpoint(t, installer, checkpoint)
	_, _, err = installer.Complete(&proto.HashRange{Min: 0, Max: 1<<31 - 1})
	require.NoError(t, err)

	lc, err := target.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: shard, Term: 2, ReplicationFactor: 1})
	require.NoError(t, err)
	kept, err := lc.ListBlock(context.Background(), &proto.ListRequest{Shard: &shard, StartInclusive: "key-", EndExclusive: "key."})
	require.NoError(t, err)
	assert.NotEmpty(t, kept)
	assert.Less(t, len(kept), len(keys), "the keys hashed outside the range are gone")
}

func TestInstallerRejectsAFileOfTheWrongSize(t *testing.T) {
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	target := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	defer func() { assert.NoError(t, target.Close()) }()
	installer, err := target.NewInstaller(constant.DefaultNamespace, 1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, installer.Close()) }()
	assert.ErrorContains(t, installer.AddFile("short", 10, strings.NewReader("12345")), "want 10 bytes")
	assert.ErrorContains(t, installer.AddFile("long", 2, strings.NewReader("12345")), "longer than its size")
	assert.NoError(t, installer.AddFile("empty", 0, strings.NewReader("")))
}

func TestShardsDirectorNewInstallerRefusesALedShardAndClosesAFollower(t *testing.T) {
	const shard int64 = 3
	sd := newLeaderWithEntries(t, shard, "a")
	_, err := sd.NewInstaller(constant.DefaultNamespace, shard)
	assert.ErrorIs(t, err, constant.ErrResourceConflict)

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	other := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	defer func() { assert.NoError(t, other.Close()) }()
	_, err = other.GetOrCreateFollower(constant.DefaultNamespace, shard, 1, nil)
	require.NoError(t, err)
	installer, err := other.NewInstaller(constant.DefaultNamespace, shard)
	require.NoError(t, err)
	assert.NoError(t, installer.Close())
	_, err = other.GetFollower(shard)
	assert.ErrorIs(t, err, errs.ErrShardNotMember)
}

// installCheckpoint adds every file of a checkpoint to the installer.
func installCheckpoint(t *testing.T, installer Installer, checkpoint *database.Checkpoint) {
	t.Helper()
	entries, err := os.ReadDir(checkpoint.BasePath())
	require.NoError(t, err)
	for _, entry := range entries {
		info, err := entry.Info()
		require.NoError(t, err)
		f, err := os.Open(filepath.Join(checkpoint.BasePath(), entry.Name()))
		require.NoError(t, err)
		require.NoError(t, installer.AddFile(entry.Name(), info.Size(), f))
		require.NoError(t, f.Close())
	}
}

func TestInstallerCompleteFailsWhenTheDatabaseDirectoryIsGone(t *testing.T) {
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	target := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	defer func() { assert.NoError(t, target.Close()) }()
	installer, err := target.NewInstaller(constant.DefaultNamespace, 1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, installer.Close()) }()
	require.NoError(t, os.RemoveAll(target.DatabaseDir(constant.DefaultNamespace, 1)))

	_, _, err = installer.Complete(&proto.HashRange{Min: 0, Max: ^uint32(0)})
	assert.ErrorContains(t, err, "failed to complete the snapshot")
}

func TestInstallerCompleteFailsOnADatabaseThatDoesNotOpen(t *testing.T) {
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	target := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	defer func() { assert.NoError(t, target.Close()) }()
	installer, err := target.NewInstaller(constant.DefaultNamespace, 1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, installer.Close()) }()
	require.NoError(t, installer.AddFile("CURRENT", 7, strings.NewReader("garbage")))

	_, _, err = installer.Complete(&proto.HashRange{Min: 0, Max: ^uint32(0)})
	assert.ErrorContains(t, err, "failed to open the database")
}

// failingKVFactory wraps the databases of a factory.
type failingKVFactory struct {
	kvstore.Factory
	wrap func(kv kvstore.KV) kvstore.KV
}

func (f *failingKVFactory) NewKV(namespace string, shardId int64, keySorting proto.KeySortingType) (kvstore.KV, error) {
	kv, err := f.Factory.NewKV(namespace, shardId, keySorting)
	if err != nil {
		return nil, err
	}
	return f.wrap(kv), nil
}

// failingKV fails the scans, the flushes, or the reads of the given keys.
type failingKV struct {
	kvstore.KV
	rangeScanErr error
	flushErr     error
	getErr       map[string]error
}

func (kv *failingKV) RangeScan(lowerBound, upperBound string, opts kvstore.IteratorOpts) (kvstore.KeyValueIterator, error) {
	if kv.rangeScanErr != nil {
		return nil, kv.rangeScanErr
	}
	return kv.KV.RangeScan(lowerBound, upperBound, opts)
}

func (kv *failingKV) Flush() error {
	if kv.flushErr != nil {
		return kv.flushErr
	}
	return kv.KV.Flush()
}

func (kv *failingKV) Get(key string, comparisonType kvstore.ComparisonType, opts kvstore.IteratorOpts) (string, []byte, io.Closer, error) {
	if err := kv.getErr[key]; err != nil {
		return "", nil, nil, err
	}
	return kv.KV.Get(key, comparisonType, opts)
}

func TestInstallerCompleteReportsTheFailuresOfTheFilter(t *testing.T) {
	errFailed := errors.New("disk failed")
	for name, test := range map[string]struct {
		kv      failingKV
		message string
	}{
		"scan":          {failingKV{rangeScanErr: errFailed}, "failed to filter the database"},
		"flush":         {failingKV{flushErr: errFailed}, "disk failed"},
		"commit offset": {failingKV{getErr: map[string]error{constant.InternalKeyPrefix + "commit-offset": errFailed}}, "failed to read the commit offset"},
		"term":          {failingKV{getErr: map[string]error{constant.InternalKeyPrefix + "term": errFailed}}, "failed to read the term"},
	} {
		t.Run(name, func(t *testing.T) {
			shard := int64(7)
			source := newLeaderWithEntries(t, shard, "a")
			checkpoint, err := source.Checkpoint(shard)
			require.NoError(t, err)
			defer func() { assert.NoError(t, checkpoint.Close()) }()

			pebbleFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			require.NoError(t, err)
			defer func() { assert.NoError(t, pebbleFactory.Close()) }()
			kvFactory := &failingKVFactory{Factory: pebbleFactory, wrap: func(kv kvstore.KV) kvstore.KV {
				failing := test.kv
				failing.KV = kv
				return &failing
			}}
			installer, err := newInstaller(&option.StorageOptions{}, constant.DefaultNamespace, shard, newTestWalFactory(t), kvFactory)
			require.NoError(t, err)
			defer func() { assert.NoError(t, installer.Close()) }()
			installCheckpoint(t, installer, checkpoint)

			_, _, err = installer.Complete(&proto.HashRange{Min: 0, Max: ^uint32(0)})
			assert.ErrorIs(t, err, errFailed)
			assert.ErrorContains(t, err, test.message)
		})
	}
}

// failingWalFactory fails to open the WALs, or to clear them.
type failingWalFactory struct {
	wal.Factory
	newWalErr error
	clearErr  error
}

func (f *failingWalFactory) NewWal(namespace string, shard int64, provider wal.CommitOffsetProvider) (wal.Wal, error) {
	if f.newWalErr != nil {
		return nil, f.newWalErr
	}
	w, err := f.Factory.NewWal(namespace, shard, provider)
	if err != nil {
		return nil, err
	}
	return &failingWal{Wal: w, clearErr: f.clearErr}, nil
}

type failingWal struct {
	wal.Wal
	clearErr error
}

func (w *failingWal) Clear() error {
	if w.clearErr != nil {
		return w.clearErr
	}
	return w.Wal.Clear()
}

// failingLoaderFactory fails to create the snapshot loaders.
type failingLoaderFactory struct {
	kvstore.Factory
	err error
}

func (f *failingLoaderFactory) NewSnapshotLoader(string, int64) (kvstore.SnapshotLoader, error) {
	return nil, f.err
}

func TestNewInstallerReportsTheFailuresOfThePreparation(t *testing.T) {
	errFailed := errors.New("disk failed")
	pebbleFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, pebbleFactory.Close()) }()
	for name, test := range map[string]struct {
		walFactory wal.Factory
		kvFactory  kvstore.Factory
		message    string
	}{
		"open the WAL":  {&failingWalFactory{Factory: newTestWalFactory(t), newWalErr: errFailed}, pebbleFactory, "failed to open the WAL"},
		"clear the WAL": {&failingWalFactory{Factory: newTestWalFactory(t), clearErr: errFailed}, pebbleFactory, "failed to clear the WAL"},
		"loader":        {newTestWalFactory(t), &failingLoaderFactory{Factory: pebbleFactory, err: errFailed}, "failed to create the snapshot loader"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newInstaller(&option.StorageOptions{}, constant.DefaultNamespace, 1, test.walFactory, test.kvFactory)
			assert.ErrorIs(t, err, errFailed)
			assert.ErrorContains(t, err, test.message)
		})
	}
}

func TestInstallerRejectsAFileNameOutsideTheDatabase(t *testing.T) {
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	target := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	defer func() { assert.NoError(t, target.Close()) }()
	installer, err := target.NewInstaller(constant.DefaultNamespace, 1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, installer.Close()) }()
	assert.ErrorContains(t, installer.AddFile("../outside", 1, strings.NewReader("x")), "failed to write ../outside")
}

func TestNoCommitOffsetAppliesNothing(t *testing.T) {
	assert.Equal(t, constant.I64NegativeOne, noCommitOffset{}.CommitOffset())
	assert.NoError(t, noCommitOffset{}.FlushDatabase())
}
