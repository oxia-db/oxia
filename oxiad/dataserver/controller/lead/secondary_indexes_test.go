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
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/proto"
)

func TestSecondaryIndices_List(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory, nil)
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

	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory, nil)
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

	results, err := scanAll(ctx, lc, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "1",
		EndExclusive:       "3",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, "/b", *results[0].Key)
	assert.Equal(t, "1", string(results[0].Value))
	assert.Equal(t, "/c", *results[1].Key)
	assert.Equal(t, "2", string(results[1].Value))

	// Wrong index
	results, err = scanAll(ctx, lc, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "/a",
		EndExclusive:       "/d",
		SecondaryIndexName: pb.String("wrong-idx"),
	})
	assert.NoError(t, err)
	assert.Empty(t, results)

	// Individual delete
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard:   &shard,
		Deletes: []*proto.DeleteRequest{{Key: "/b"}},
	})
	assert.NoError(t, err)

	results, err = scanAll(ctx, lc, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, len(results))
	assert.Equal(t, "/a", *results[0].Key)
	assert.Equal(t, "/c", *results[1].Key)
	assert.Equal(t, "/d", *results[2].Key)
	assert.Equal(t, "/e", *results[3].Key)

	// Range delete
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		DeleteRanges: []*proto.DeleteRangeRequest{{
			StartInclusive: "/a",
			EndExclusive:   "/d",
		}},
	})
	assert.NoError(t, err)

	results, err = scanAll(ctx, lc, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "99999",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, "/d", *results[0].Key)
	assert.Equal(t, "/e", *results[1].Key)

	cancel()
	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestSecondaryIndices_MultipleKeysForSameIdx(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory, nil)
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

func TestSecondaryIndices_GetBoundedToRequestedIndex(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory, nil)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	// Three indexes whose regions are adjacent in the key space:
	// "id" < "md5" < "schemaId". The "md5" index only contains "m", so gets
	// around its edges must not return entries of the neighboring indexes.
	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/id-aa", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "id", SecondaryKey: "aa"}}},
			{Key: "/md5-m", Value: []byte("1"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "md5", SecondaryKey: "m"}}},
			{Key: "/schema-zz", Value: []byte("2"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "schemaId", SecondaryKey: "zz"}}},
		},
	})
	assert.NoError(t, err)

	tests := []struct {
		name           string
		comparison     proto.KeyComparisonType
		index          string
		key            string
		expectedKey    string // "" means KEY_NOT_FOUND expected
		expectedSecKey string
	}{
		// Same-index matches around the "md5" entry.
		{"equal-match", proto.KeyComparisonType_EQUAL, "md5", "m", "/md5-m", "m"},
		{"ceiling-from-below", proto.KeyComparisonType_CEILING, "md5", "b", "/md5-m", "m"},
		{"higher-from-below", proto.KeyComparisonType_HIGHER, "md5", "b", "/md5-m", "m"},
		{"floor-from-above", proto.KeyComparisonType_FLOOR, "md5", "x", "/md5-m", "m"},
		{"lower-from-above", proto.KeyComparisonType_LOWER, "md5", "x", "/md5-m", "m"},

		// Below the lowest "md5" entry: must not return entries of the
		// preceding "id" index.
		{"floor-below-all", proto.KeyComparisonType_FLOOR, "md5", "b", "", ""},
		{"lower-below-all", proto.KeyComparisonType_LOWER, "md5", "b", "", ""},
		{"equal-of-preceding-index", proto.KeyComparisonType_EQUAL, "md5", "aa", "", ""},

		// Above the highest "md5" entry: must not return entries of the
		// following "schemaId" index.
		{"ceiling-above-all", proto.KeyComparisonType_CEILING, "md5", "x", "", ""},
		{"higher-above-all", proto.KeyComparisonType_HIGHER, "md5", "x", "", ""},
		{"higher-at-max", proto.KeyComparisonType_HIGHER, "md5", "m", "", ""},
		{"equal-of-following-index", proto.KeyComparisonType_EQUAL, "md5", "zz", "", ""},

		// Index with no entries at all: the neighboring indexes must stay
		// invisible.
		{"floor-missing-index", proto.KeyComparisonType_FLOOR, "missing", "x", "", ""},
		{"ceiling-missing-index", proto.KeyComparisonType_CEILING, "missing", "b", "", ""},
		{"lower-missing-index", proto.KeyComparisonType_LOWER, "missing", "x", "", ""},
		{"higher-missing-index", proto.KeyComparisonType_HIGHER, "missing", "b", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resps, err := readAll(context.Background(), lc, &proto.ReadRequest{
				Shard: &shard,
				Gets: []*proto.GetRequest{{
					Key:                tc.key,
					ComparisonType:     tc.comparison,
					SecondaryIndexName: pb.String(tc.index),
				}},
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(resps))

			res := resps[0]
			if tc.expectedKey == "" {
				assert.Equal(t, proto.Status_KEY_NOT_FOUND, res.Status)
				assert.Nil(t, res.Key)
			} else {
				assert.Equal(t, proto.Status_OK, res.Status)
				assert.Equal(t, tc.expectedKey, *res.Key)
				assert.Equal(t, tc.expectedSecKey, *res.SecondaryIndexKey)
			}
		})
	}

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestDoSecondaryGet_UnsupportedComparisonType(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory, nil)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	// Write data with a secondary index so the iterator has entries to iterate
	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "0"}}},
		},
	})
	assert.NoError(t, err)

	// Call doSecondaryGet with an unsupported ComparisonType to hit the default branch
	db := lc.(*leaderController).db
	_, _, err = doSecondaryGet(db, &proto.GetRequest{
		Key:                "0",
		SecondaryIndexName: pb.String("my-idx"),
		ComparisonType:     proto.KeyComparisonType(999),
	})
	assert.ErrorContains(t, err, "unsupported comparison type")

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}
