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
	"testing"

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
