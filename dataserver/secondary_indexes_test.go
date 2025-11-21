// Copyright 2023-2025 The Oxia Authors
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

package dataserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/entity"

	"github.com/oxia-db/oxia/dataserver/kv"
	"github.com/oxia-db/oxia/proto"
)

func TestSecondaryIndices_List(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "0"}}},
			{Key: "/b", Value: []byte("1"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "1"}}},
			{Key: "/c", Value: []byte("2"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "2"}}},
			{Key: "/d", Value: []byte("3"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "3"}}},
			{Key: "/e", Value: []byte("4"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "4"}}},
		},
	})
	assert.NoError(t, err)

	keys, err := lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "1",
		EndExclusive:       "3",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)

	assert.Equal(t, 2, len(keys))
	assert.Contains(t, keys, "/b")
	assert.Contains(t, keys, "/c")

	// Wrong index
	keys, err = lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "/a",
		EndExclusive:       "/d",
		SecondaryIndexName: pb.String("wrong-idx"),
	})
	assert.NoError(t, err)
	assert.Empty(t, keys)

	// Individual delete
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard:   &shard,
		Deletes: []*proto.DeleteRequest{{Key: "/b"}},
	})
	assert.NoError(t, err)

	keys, err = lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, len(keys))
	assert.Contains(t, keys, "/a")
	assert.Contains(t, keys, "/c")
	assert.Contains(t, keys, "/d")
	assert.Contains(t, keys, "/e")

	// Range delete
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "/a",
			EndExclusive:   "/d",
		}},
	})
	assert.NoError(t, err)

	keys, err = lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)

	assert.Equal(t, 2, len(keys))
	assert.Contains(t, keys, "/d")
	assert.Contains(t, keys, "/e")

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestSecondaryIndices_RangeScan(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "0"}}},
			{Key: "/b", Value: []byte("1"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "1"}}},
			{Key: "/c", Value: []byte("2"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "2"}}},
			{Key: "/d", Value: []byte("3"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "3"}}},
			{Key: "/e", Value: []byte("4"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "4"}}},
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan *entity.TWithError[*proto.GetResponse], 100)
	lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "1",
		EndExclusive:       "3",
		SecondaryIndexName: pb.String("my-idx"),
	}, concurrent.ReadFromStreamCallback(ch))
	assert.NoError(t, err)

	gr := <-ch
	assert.Equal(t, "/b", *gr.T.Key)
	assert.Equal(t, "1", string(gr.T.Value))
	gr = <-ch
	assert.Equal(t, "/c", *gr.T.Key)
	assert.Equal(t, "2", string(gr.T.Value))
	assert.Empty(t, ch)

	ch = make(chan *entity.TWithError[*proto.GetResponse], 100)
	// Wrong index
	lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "/a",
		EndExclusive:       "/d",
		SecondaryIndexName: pb.String("wrong-idx"),
	}, concurrent.ReadFromStreamCallback(ch))
	assert.Empty(t, ch)

	// Individual delete
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard:   &shard,
		Deletes: []*proto.DeleteRequest{{Key: "/b"}},
	})
	assert.NoError(t, err)

	ch = make(chan *entity.TWithError[*proto.GetResponse], 100)
	lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	}, concurrent.ReadFromStreamCallback(ch))

	gr = <-ch
	assert.Equal(t, "/a", *gr.T.Key)
	gr = <-ch
	assert.Equal(t, "/c", *gr.T.Key)
	gr = <-ch
	assert.Equal(t, "/d", *gr.T.Key)
	gr = <-ch
	assert.Equal(t, "/e", *gr.T.Key)
	assert.Empty(t, ch)

	// Range delete
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "/a",
			EndExclusive:   "/d",
		}},
	})
	assert.NoError(t, err)

	ch = make(chan *entity.TWithError[*proto.GetResponse], 100)
	lc.RangeScan(ctx, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	}, concurrent.ReadFromStreamCallback(ch))

	gr = <-ch
	assert.Equal(t, "/d", *gr.T.Key)
	gr = <-ch
	assert.Equal(t, "/e", *gr.T.Key)
	assert.Empty(t, ch)

	cancel()
	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestSecondaryIndices_MultipleKeysForSameIdx(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "a"},
				{IndexName: "idx", SecondaryKey: "A"},
			}},
			{Key: "/b", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "b"},
				{IndexName: "idx", SecondaryKey: "B"},
			}},
			{Key: "/c", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "c"},
				{IndexName: "idx", SecondaryKey: "C"},
			}},
			{Key: "/d", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "d"},
				{IndexName: "idx", SecondaryKey: "D"},
			}},
			{Key: "/e", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "idx", SecondaryKey: "e"},
				{IndexName: "idx", SecondaryKey: "E"},
			}},
		},
	})
	assert.NoError(t, err)

	keys, err := lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "b",
		EndExclusive:       "d",
		SecondaryIndexName: pb.String("idx"),
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))
	assert.Contains(t, keys, "/b")
	assert.Contains(t, keys, "/c")

	// using alternate values on same index
	keys, err = lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "B",
		EndExclusive:       "D",
		SecondaryIndexName: pb.String("idx"),
	})
	assert.NoError(t, err)

	assert.Equal(t, 2, len(keys))
	assert.Contains(t, keys, "/b")
	assert.Contains(t, keys, "/c")

	// Repeated primary keys when multiple indexes
	keys, err = lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "A",
		EndExclusive:       "z",
		SecondaryIndexName: pb.String("idx"),
	})
	assert.NoError(t, err)

	assert.Equal(t, 10, len(keys))
	assert.Contains(t, keys, "/a")
	assert.Contains(t, keys, "/b")
	assert.Contains(t, keys, "/c")
	assert.Contains(t, keys, "/d")
	assert.Contains(t, keys, "/e")
	assert.Contains(t, keys, "/a")
	assert.Contains(t, keys, "/b")
	assert.Contains(t, keys, "/c")
	assert.Contains(t, keys, "/d")
	assert.Contains(t, keys, "/e")

	// Delete
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard:   &shard,
		Deletes: []*proto.DeleteRequest{{Key: "/b"}},
	})
	assert.NoError(t, err)

	keys, err = lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "a",
		EndExclusive:       "z",
		SecondaryIndexName: pb.String("idx"),
	})
	assert.NoError(t, err)

	assert.Equal(t, 4, len(keys))
	assert.Contains(t, keys, "/a")
	assert.Contains(t, keys, "/c")
	assert.Contains(t, keys, "/d")
	assert.Contains(t, keys, "/e")

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}
