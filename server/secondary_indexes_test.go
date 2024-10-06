// Copyright 2023 StreamNative, Inc.
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

package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
)

func TestSecondaryIndices_List(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: map[string]string{"my-idx": "0"}},
			{Key: "/b", Value: []byte("1"), SecondaryIndexes: map[string]string{"my-idx": "1"}},
			{Key: "/c", Value: []byte("2"), SecondaryIndexes: map[string]string{"my-idx": "2"}},
			{Key: "/d", Value: []byte("3"), SecondaryIndexes: map[string]string{"my-idx": "3"}},
			{Key: "/e", Value: []byte("4"), SecondaryIndexes: map[string]string{"my-idx": "4"}},
		},
	})
	assert.NoError(t, err)

	strCh, err := lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "1",
		EndExclusive:       "3",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)

	k := <-strCh
	assert.Equal(t, "/b", k)
	k = <-strCh
	assert.Equal(t, "/c", k)
	assert.Empty(t, strCh)

	// Wrong index
	strCh, err = lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "/a",
		EndExclusive:       "/d",
		SecondaryIndexName: pb.String("wrong-idx"),
	})
	assert.NoError(t, err)
	assert.Empty(t, strCh)

	// Individual delete
	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard:   &shard,
		Deletes: []*proto.DeleteRequest{{Key: "/b"}},
	})
	assert.NoError(t, err)

	strCh, err = lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)
	k = <-strCh
	assert.Equal(t, "/a", k)
	k = <-strCh
	assert.Equal(t, "/c", k)
	k = <-strCh
	assert.Equal(t, "/d", k)
	k = <-strCh
	assert.Equal(t, "/e", k)

	assert.Empty(t, strCh)

	// Range delete
	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "/a",
			EndExclusive:   "/d",
		}},
	})
	assert.NoError(t, err)

	strCh, err = lc.List(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)

	k = <-strCh
	assert.Equal(t, "/d", k)
	k = <-strCh
	assert.Equal(t, "/e", k)
	assert.Empty(t, strCh)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestSecondaryIndices_RangeScan(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(testKVOptions)
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, common.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: map[string]string{"my-idx": "0"}},
			{Key: "/b", Value: []byte("1"), SecondaryIndexes: map[string]string{"my-idx": "1"}},
			{Key: "/c", Value: []byte("2"), SecondaryIndexes: map[string]string{"my-idx": "2"}},
			{Key: "/d", Value: []byte("3"), SecondaryIndexes: map[string]string{"my-idx": "3"}},
			{Key: "/e", Value: []byte("4"), SecondaryIndexes: map[string]string{"my-idx": "4"}},
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	ch, errCh, err := lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "1",
		EndExclusive:       "3",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)

	gr := <-ch
	assert.Equal(t, "/b", *gr.Key)
	assert.Equal(t, "1", string(gr.Value))
	gr = <-ch
	assert.Equal(t, "/c", *gr.Key)
	assert.Equal(t, "2", string(gr.Value))
	assert.Empty(t, ch)

	assert.NoError(t, <-errCh)

	// Wrong index
	ch, errCh, err = lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "/a",
		EndExclusive:       "/d",
		SecondaryIndexName: pb.String("wrong-idx"),
	})
	assert.NoError(t, err)
	assert.Empty(t, ch)
	assert.NoError(t, <-errCh)

	// Individual delete
	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard:   &shard,
		Deletes: []*proto.DeleteRequest{{Key: "/b"}},
	})
	assert.NoError(t, err)

	ch, errCh, err = lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)

	gr = <-ch
	assert.Equal(t, "/a", *gr.Key)
	gr = <-ch
	assert.Equal(t, "/c", *gr.Key)
	gr = <-ch
	assert.Equal(t, "/d", *gr.Key)
	gr = <-ch
	assert.Equal(t, "/e", *gr.Key)
	assert.Empty(t, ch)
	assert.NoError(t, <-errCh)

	assert.NoError(t, <-errCh)

	// Range delete
	_, err = lc.Write(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "/a",
			EndExclusive:   "/d",
		}},
	})
	assert.NoError(t, err)

	ch, errCh, err = lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)

	gr = <-ch
	assert.Equal(t, "/d", *gr.Key)
	gr = <-ch
	assert.Equal(t, "/e", *gr.Key)
	assert.Empty(t, ch)
	assert.NoError(t, <-errCh)

	cancel()
	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}
