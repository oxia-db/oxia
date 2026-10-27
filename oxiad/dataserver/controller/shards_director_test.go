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
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/dataserver/controller/follow"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/proto"
)

func newTestWalFactory(t *testing.T) wal.Factory {
	t.Helper()

	return wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  t.TempDir(),
		SegmentSize: 128 * 1024,
	})
}

func TestShardsDirector_DeleteShardLeader(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	sd := NewShardsDirector(&option.StorageOptions{}, walFactory, kvFactory, rpc.NewMockRpcClient())

	lc, _ := sd.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: "k1", Value: []byte("hello")}},
	})
	assert.NoError(t, err)

	_, err = lc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: constant.DefaultNamespace,
		Shard:     shard,
		Term:      1,
	})
	assert.NoError(t, err)

	// Reopen
	lc, err = sd.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}

func TestShardsDirector_GetOrCreateFollower(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	sd := NewShardsDirector(&option.StorageOptions{}, walFactory, kvFactory, rpc.NewMockRpcClient())

	lc, _ := sd.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              2,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	assert.EqualValues(t, 2, lc.Term())

	// Should fail to get closed if the term is wrong
	fc, err := sd.GetOrCreateFollower(constant.DefaultNamespace, shard, 1, nil)
	assert.ErrorIs(t, constant.ErrInvalidTerm, err)
	assert.Nil(t, fc)
	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	// Will get closed if term is correct
	fc, err = sd.GetOrCreateFollower(constant.DefaultNamespace, shard, 2, nil)
	assert.NoError(t, err)

	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	assert.NoError(t, fc.Close())
	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}

func TestShardsDirectorGetOrCreateFollowerAfterClose(t *testing.T) {
	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)
	sd := NewShardsDirector(&option.StorageOptions{}, walFactory, kvFactory, rpc.NewMockRpcClient())
	assert.NoError(t, sd.Close())

	fc, err := sd.GetOrCreateFollower(constant.DefaultNamespace, 1, 1, nil)
	assert.ErrorIs(t, err, constant.ErrResourceUnavailable)
	assert.Nil(t, fc)
	assert.NoError(t, walFactory.Close())
}

func TestShardsDirectorRoutesCheckpointAndCommitOffsetToTheHostingController(t *testing.T) {
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	sd := NewShardsDirector(&option.StorageOptions{}, newTestWalFactory(t), kvFactory, rpc.NewMockRpcClient())
	t.Cleanup(func() { assert.NoError(t, sd.Close()) })

	// A shard led here, with two entries
	var led int64 = 1
	lc, err := sd.GetOrCreateLeader(constant.DefaultNamespace, led, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: led, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: led, Term: 1, ReplicationFactor: 1})
	require.NoError(t, err)
	for _, key := range []string{"a", "b"} {
		_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
			Shard: &led, Puts: []*proto.PutRequest{{Key: key, Value: []byte("v")}}})
		require.NoError(t, err)
	}
	offset, err := sd.CommitOffset(led)
	require.NoError(t, err)
	assert.EqualValues(t, 1, offset)
	checkpoint, err := sd.Checkpoint(led)
	require.NoError(t, err)
	commitOffset, _, err := database.ReadCheckpoint(checkpoint.BasePath())
	require.NoError(t, err)
	assert.EqualValues(t, 1, commitOffset)
	assert.NoError(t, checkpoint.Close())
	require.NoError(t, sd.WithDatabase(led, checkpoint.Generation, func(dir string) error {
		assert.Equal(t, sd.DatabaseDir(constant.DefaultNamespace, led), dir)
		return nil
	}))

	// A shard followed here, with no entry yet
	var followed int64 = 2
	fc, err := sd.GetOrCreateFollower(constant.DefaultNamespace, followed, 1, nil)
	require.NoError(t, err)
	_, err = fc.NewTerm(&proto.NewTermRequest{Shard: followed, Term: 1})
	require.NoError(t, err)
	offset, err = sd.CommitOffset(followed)
	require.NoError(t, err)
	assert.EqualValues(t, wal.InvalidOffset, offset)
	checkpoint, err = sd.Checkpoint(followed)
	require.NoError(t, err)
	assert.NoError(t, checkpoint.Close())
	require.NoError(t, sd.WithDatabase(followed, checkpoint.Generation, func(string) error { return nil }))

	// A shard not hosted here
	var absent int64 = 3
	_, err = sd.CommitOffset(absent)
	assert.ErrorIs(t, err, constant.ErrNodeIsNotMember)
	_, err = sd.Checkpoint(absent)
	assert.ErrorIs(t, err, constant.ErrNodeIsNotMember)
	assert.ErrorIs(t, sd.WithDatabase(absent, 0, func(string) error { return nil }), constant.ErrNodeIsNotMember)
}

func TestShardsDirectorGetOrCreateLeaderFollowerCloseError(t *testing.T) {
	sd := newTestShardsDirector(t)
	closeErr := errors.New("close failure")
	follower := &fakeFollowerController{close: func() error { return closeErr }}
	sd.followers[1] = follower

	lc, err := sd.GetOrCreateLeader(constant.DefaultNamespace, 1, nil)
	assert.ErrorIs(t, err, closeErr)
	assert.Nil(t, lc)

	// The follower stays in place
	fc, err := sd.GetFollower(1)
	assert.NoError(t, err)
	assert.Same(t, follower, fc)
	_, err = sd.GetLeader(1)
	assert.ErrorIs(t, err, constant.ErrNodeIsNotLeader)
}

func TestShardsDirectorGetOrCreateLeaderFailsWhenClosed(t *testing.T) {
	sd := newTestShardsDirector(t)
	require.NoError(t, sd.Close())

	lc, err := sd.GetOrCreateLeader(constant.DefaultNamespace, 1, nil)
	assert.ErrorIs(t, err, constant.ErrResourceUnavailable)
	assert.Nil(t, lc)
	assert.Empty(t, sd.leaders)
}

func TestShardsDirectorGetOrCreateFollowerLeaderCloseError(t *testing.T) {
	sd := newTestShardsDirector(t)
	closeErr := errors.New("close failure")
	leader := &fakeLeaderController{term: 2, close: func() error { return closeErr }}
	sd.leaders[1] = leader

	fc, err := sd.GetOrCreateFollower(constant.DefaultNamespace, 1, 2, nil)
	assert.ErrorIs(t, err, closeErr)
	assert.Nil(t, fc)

	// The leader stays in place
	lc, err := sd.GetLeader(1)
	assert.NoError(t, err)
	assert.Same(t, leader, lc)
}

func TestShardsDirectorGetOrCreateFollowerInvalidNamespace(t *testing.T) {
	sd := newTestShardsDirector(t)

	fc, err := sd.GetOrCreateFollower(invalidNamespace, 1, 1, nil)
	assert.Error(t, err)
	assert.Nil(t, fc)

	_, err = sd.GetFollower(1)
	assert.ErrorIs(t, err, errs.ErrNotFollower)
}

func TestShardsDirectorDeleteShardLeaderError(t *testing.T) {
	sd := newTestShardsDirector(t)
	deleteErr := errors.New("delete failure")
	leader := &fakeLeaderController{deleteShard: func(*proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
		return nil, deleteErr
	}}
	sd.leaders[1] = leader

	_, err := sd.DeleteShard(&proto.DeleteShardRequest{Namespace: constant.DefaultNamespace, Shard: 1, Term: 1})
	assert.ErrorIs(t, err, deleteErr)

	// The leader stays in place
	lc, err := sd.GetLeader(1)
	assert.NoError(t, err)
	assert.Same(t, leader, lc)
}

func TestShardsDirectorDeleteShardFollowerError(t *testing.T) {
	sd := newTestShardsDirector(t)
	deleteErr := errors.New("delete failure")
	follower := &fakeFollowerController{delete: func(*proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
		return nil, deleteErr
	}}
	sd.followers[1] = follower

	_, err := sd.DeleteShard(&proto.DeleteShardRequest{Namespace: constant.DefaultNamespace, Shard: 1, Term: 1})
	assert.ErrorIs(t, err, deleteErr)

	// The follower stays in place
	fc, err := sd.GetFollower(1)
	assert.NoError(t, err)
	assert.Same(t, follower, fc)
}

// A shard that the node doesn't host is opened from its storage, to delete it.
func TestShardsDirectorDeleteShardNotHosted(t *testing.T) {
	var shard int64 = 1
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kvFactory.Close()) })
	walFactory := newTestWalFactory(t)
	t.Cleanup(func() { assert.NoError(t, walFactory.Close()) })

	// A previous instance of the node wrote an entry in the shard
	sd := NewShardsDirector(&option.StorageOptions{}, walFactory, kvFactory, rpc.NewMockRpcClient())
	lc, err := sd.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: shard, Term: 1, ReplicationFactor: 1})
	require.NoError(t, err)
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: "k1", Value: []byte("hello")}},
	})
	require.NoError(t, err)
	require.NoError(t, sd.Close())

	sd = NewShardsDirector(&option.StorageOptions{}, walFactory, kvFactory, rpc.NewMockRpcClient())
	_, err = sd.DeleteShard(&proto.DeleteShardRequest{Namespace: constant.DefaultNamespace, Shard: shard, Term: 1})
	require.NoError(t, err)

	_, err = sd.GetFollower(shard)
	assert.ErrorIs(t, err, errs.ErrNotFollower)

	// The shard starts over empty
	lc, err = sd.GetOrCreateLeader(constant.DefaultNamespace, shard, nil)
	require.NoError(t, err)
	res, err := lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	require.NoError(t, err)
	assert.EqualValues(t, wal.InvalidOffset, res.HeadEntryId.Offset)
	assert.NoError(t, sd.Close())
}

func TestShardsDirectorDeleteShardNotHostedInvalidNamespace(t *testing.T) {
	sd := newTestShardsDirector(t)

	_, err := sd.DeleteShard(&proto.DeleteShardRequest{Namespace: invalidNamespace, Shard: 1, Term: 1})
	assert.Error(t, err)
}

func TestShardsDirectorCheckpointFailsWhenClosed(t *testing.T) {
	sd := newTestShardsDirector(t)
	require.NoError(t, sd.Close())

	checkpoint, err := sd.Checkpoint(1)
	assert.ErrorIs(t, err, constant.ErrResourceUnavailable)
	assert.Nil(t, checkpoint)
}

func TestShardsDirectorCommitOffsetFailsWhenClosed(t *testing.T) {
	sd := newTestShardsDirector(t)
	require.NoError(t, sd.Close())

	_, err := sd.CommitOffset(1)
	assert.ErrorIs(t, err, constant.ErrResourceUnavailable)
}

func TestShardsDirectorWithDatabaseFailsWhenClosed(t *testing.T) {
	sd := newTestShardsDirector(t)
	require.NoError(t, sd.Close())

	err := sd.WithDatabase(1, 0, func(string) error {
		assert.Fail(t, "the database of a closed director was used")
		return nil
	})
	assert.ErrorIs(t, err, constant.ErrResourceUnavailable)
}

// A namespace with a path traversal, that the controllers reject.
const invalidNamespace = "../invalid"

func newTestShardsDirector(t *testing.T) *shardsDirector {
	t.Helper()
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, kvFactory.Close()) })
	walFactory := newTestWalFactory(t)
	t.Cleanup(func() { assert.NoError(t, walFactory.Close()) })
	return NewShardsDirector(&option.StorageOptions{}, walFactory, kvFactory, rpc.NewMockRpcClient()).(*shardsDirector)
}

type fakeLeaderController struct {
	lead.LeaderController
	term        int64
	close       func() error
	deleteShard func(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)
}

func (f *fakeLeaderController) Term() int64 {
	return f.term
}

func (f *fakeLeaderController) Close() error {
	return f.close()
}

func (f *fakeLeaderController) DeleteShard(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	return f.deleteShard(request)
}

type fakeFollowerController struct {
	follow.FollowerController
	close  func() error
	delete func(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)
}

func (f *fakeFollowerController) Close() error {
	return f.close()
}

func (f *fakeFollowerController) Delete(request *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	return f.delete(request)
}
