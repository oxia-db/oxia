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
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
)

// writeError writes through the asynchronous Write, and returns its error.
func writeError(t *testing.T, lc LeaderController, shard int64) error {
	t.Helper()

	result := make(chan error, 1)
	lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: "k", Value: []byte("v")}},
	}, concurrent.NewOnce(func(*proto.WriteResponse) { result <- nil }, func(err error) { result <- err }))
	return <-result
}

// A leader that closes lets the write it appended and its followers did not
// acknowledge yet complete, instead of abandoning it: an abandoned write may
// still commit on the next leader, and its client could not tell.
func TestLeaderController_CloseDrainsTheWritesInFlight(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)
	t.Cleanup(func() {
		assert.NoError(t, kvFactory.Close())
		assert.NoError(t, walFactory.Close())
	})
	follower := rpc.NewMockRpcClient()
	lc, err := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, follower, walFactory, kvFactory, nil)
	require.NoError(t, err)

	// The follower acknowledges every entry, except the one of the write
	// under test, which it acknowledges once released
	var holdFrom atomic.Int64
	holdFrom.Store(math.MaxInt64)
	release := make(chan struct{})
	go func() {
		for req := range follower.AppendReqs {
			if req.Entry != nil && req.Entry.Offset >= holdFrom.Load() {
				<-release
			}
			follower.AckResps <- &proto.Ack{Offset: req.GetEntry().GetOffset()}
		}
	}()

	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard: shard, Term: 1, ReplicationFactor: 2,
		FollowerMaps: map[string]*proto.EntryId{"f1": {Term: -1, Offset: -1}},
	})
	require.NoError(t, err)
	require.NoError(t, writeError(t, lc, shard))

	status, err := lc.GetStatus(&proto.GetStatusRequest{Shard: shard})
	require.NoError(t, err)
	holdFrom.Store(status.HeadOffset + 1)
	result := make(chan error, 1)
	lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: "in-flight", Value: []byte("v")}},
	}, concurrent.NewOnce(func(*proto.WriteResponse) { result <- nil }, func(err error) { result <- err }))

	closed := make(chan error, 1)
	go func() { closed <- lc.Close() }()
	select {
	case err := <-result:
		require.Fail(t, "the write in flight ended before its followers acknowledged it", "err: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-result:
		require.NoError(t, err, "the write in flight completes while the leader closes")
	case <-time.After(10 * time.Second):
		require.Fail(t, "the write in flight did not complete")
	}
	require.NoError(t, <-closed)
}

// A write that the leader rejects before appending it to its log, because the
// shard is frozen or the controller is no longer the leader, fails with an
// error that says so: it was not applied.
func TestLeaderController_RejectedWriteIsNotAppended(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)
	lc, err := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, lc.Close())
		assert.NoError(t, kvFactory.Close())
		assert.NoError(t, walFactory.Close())
	})

	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{Shard: shard, Term: 1, ReplicationFactor: 1})
	require.NoError(t, err)
	require.NoError(t, writeError(t, lc, shard))

	_, err = lc.Freeze(&proto.FreezeShardRequest{Shard: shard, Term: 1, Frozen: true})
	require.NoError(t, err)
	err = writeError(t, lc, shard)
	require.ErrorIs(t, err, constant.ErrNodeIsNotLeader)
	assert.True(t, IsNotAppended(err), "a frozen leader rejects the write before appending it")

	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	require.NoError(t, err)
	err = writeError(t, lc, shard)
	require.ErrorIs(t, err, constant.ErrNodeIsNotLeader)
	assert.True(t, IsNotAppended(err), "a fenced controller rejects the write before appending it")
}
