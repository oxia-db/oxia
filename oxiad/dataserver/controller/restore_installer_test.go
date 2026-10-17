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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
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
	assert.ErrorIs(t, err, ErrNodeIsNotFollower)
}

func TestShardsDirectorCheckpointAndCommitOffsetOfHostedShards(t *testing.T) {
	const shard int64 = 5
	sd := newLeaderWithEntries(t, shard, "a", "b")
	offset, err := sd.CommitOffset(shard)
	require.NoError(t, err)
	assert.EqualValues(t, 1, offset)
	checkpoint, err := sd.Checkpoint(shard)
	require.NoError(t, err)
	assert.EqualValues(t, 1, checkpoint.CommitOffset)
	assert.NoError(t, checkpoint.Close())

	_, err = sd.CommitOffset(shard + 1)
	assert.ErrorIs(t, err, constant.ErrNodeIsNotMember)
	_, err = sd.Checkpoint(shard + 1)
	assert.ErrorIs(t, err, constant.ErrNodeIsNotMember)
	assert.Equal(t, filepath.Join(kvFactoryDir(t, sd), constant.DefaultNamespace, "shard-5"),
		sd.DatabaseDir(constant.DefaultNamespace, shard))
}

func kvFactoryDir(t *testing.T, sd ShardsDirector) string {
	t.Helper()
	dir := sd.DatabaseDir(constant.DefaultNamespace, 0)
	return filepath.Dir(filepath.Dir(dir))
}
