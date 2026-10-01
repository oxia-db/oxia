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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/proto"
)

func TestSecondaryIndexNameValidation(t *testing.T) {
	tests := []struct {
		name              string
		validationEnabled bool
		secondaryIndexes  []*proto.SecondaryIndex
		expected          proto.Status
	}{
		{
			name: "validation disabled",
			secondaryIndexes: []*proto.SecondaryIndex{{
				IndexName: "tenant/users",
			}},
			expected: proto.Status_OK,
		},
		{name: "no indexes", validationEnabled: true, expected: proto.Status_OK},
		{
			name:              "valid index name",
			validationEnabled: true,
			secondaryIndexes: []*proto.SecondaryIndex{{
				IndexName:    "tenant",
				SecondaryKey: "users/email",
			}},
			expected: proto.Status_OK,
		},
		{
			name:              "invalid index name",
			validationEnabled: true,
			secondaryIndexes: []*proto.SecondaryIndex{{
				IndexName:    "tenant/users",
				SecondaryKey: "email",
			}},
			expected: proto.Status_INVALID_ARGUMENT,
		},
		{
			name:              "invalid later index name",
			validationEnabled: true,
			secondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "tenant", SecondaryKey: "email"},
				{IndexName: "tenant/users", SecondaryKey: "email"},
			},
			expected: proto.Status_INVALID_ARGUMENT,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &proto.PutRequest{SecondaryIndexes: tt.secondaryIndexes}
			features := testFeatureChecker{secondaryIndexNameValidation: tt.validationEnabled}
			assert.Equal(t, tt.expected, WrapperUpdateOperationCallback.ValidatePut(request, features))
		})
	}
}

type testFeatureChecker struct {
	secondaryIndexNameValidation   bool
	ephemeralCleanupNaturalSorting bool
}

func (f testFeatureChecker) IsFeatureEnabled(candidate proto.Feature) bool {
	switch candidate {
	case proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION:
		return f.secondaryIndexNameValidation
	case proto.Feature_FEATURE_EPHEMERAL_CLEANUP_NATURAL_SORTING:
		return f.ephemeralCleanupNaturalSorting
	default:
		return false
	}
}

var _ feature.Checker = testFeatureChecker{}

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

func TestSecondaryIndices_GetFollowsKeySorting(t *testing.T) {
	var shard int64 = 1

	// Every record is indexed by its own key, so a get through the index has to
	// return the same record as the same get on the primary keys, in either key
	// sorting. The keys mix '/' with the bytes right below and above it, and "/"
	// right after the '/' that ends the index prefix makes a "//".
	//
	// The hierarchical sorting groups the entries of an index by level, and
	// other internal keys sort between the groups: here, the entries of a second
	// index, and the session shadow key of the ephemeral record.
	keys := []string{"a.c", "a0", "b/x", "a/y/z", "/"}
	ephemeralKey := "a0"
	queries := []string{"/", "a", "a.c", "a/", "a/y/y", "a/y/z", "a/z", "a0", "b", "b/x", "b/y", "c/d/e/f"}
	comparisons := []proto.KeyComparisonType{
		proto.KeyComparisonType_FLOOR,
		proto.KeyComparisonType_LOWER,
		proto.KeyComparisonType_CEILING,
		proto.KeyComparisonType_HIGHER,
	}

	for _, keySorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(keySorting.String(), func(t *testing.T) {
			kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			walFactory := newTestWalFactory(t)

			lc, _ := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory,
				&proto.NewTermOptions{KeySorting: keySorting})
			_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
			_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
				Shard:             shard,
				Term:              1,
				ReplicationFactor: 1,
				FollowerMaps:      nil,
			})

			session, err := lc.CreateSession(&proto.CreateSessionRequest{Shard: shard, SessionTimeoutMs: 60_000})
			assert.NoError(t, err)

			var puts []*proto.PutRequest
			for _, key := range keys {
				put := &proto.PutRequest{
					Key:   key,
					Value: []byte(key),
					SecondaryIndexes: []*proto.SecondaryIndex{
						{IndexName: "idx", SecondaryKey: key},
						{IndexName: "other", SecondaryKey: key},
					},
				}
				if key == ephemeralKey {
					put.SessionId = &session.SessionId
				}
				puts = append(puts, put)
			}
			_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{Shard: &shard, Puts: puts})
			assert.NoError(t, err)

			for _, key := range keys {
				resps, err := readAll(context.Background(), lc, &proto.ReadRequest{
					Shard: &shard,
					Gets: []*proto.GetRequest{{
						Key:                key,
						ComparisonType:     proto.KeyComparisonType_EQUAL,
						SecondaryIndexName: pb.String("idx"),
					}},
				})
				assert.NoError(t, err)
				assert.Equal(t, 1, len(resps))
				assert.Equal(t, proto.Status_OK, resps[0].Status, "EQUAL %q", key)
				assert.Equal(t, key, resps[0].GetKey(), "EQUAL %q", key)
			}

			for _, comparison := range comparisons {
				for _, query := range queries {
					resps, err := readAll(context.Background(), lc, &proto.ReadRequest{
						Shard: &shard,
						Gets: []*proto.GetRequest{
							{Key: query, ComparisonType: comparison},
							{Key: query, ComparisonType: comparison, SecondaryIndexName: pb.String("idx")},
						},
					})
					assert.NoError(t, err)
					assert.Equal(t, 2, len(resps))

					primary, index := resps[0], resps[1]
					assert.Equal(t, primary.Status, index.Status, "%v %q", comparison, query)
					assert.Equal(t, primary.GetKey(), index.GetKey(), "%v %q", comparison, query)
					assert.Equal(t, primary.GetKey(), index.GetSecondaryIndexKey(), "%v %q", comparison, query)
				}
			}

			assert.NoError(t, lc.Close())
			assert.NoError(t, kvFactory.Close())
			assert.NoError(t, walFactory.Close())
		})
	}
}

func TestSecondaryIndices_ListFollowsKeySorting(t *testing.T) {
	var shard int64 = 1

	// Every record is indexed by its own key, so a list or a range scan through
	// the index has to return the same records as on the primary keys, in either
	// key sorting.
	//
	// The hierarchical sorting groups the entries of an index by level, and other
	// internal keys sort between the groups: here, the entries of a second index,
	// and the session shadow key of the ephemeral record. Most ranges go from a
	// level to another, and some start or end on "/", or end on a key ending in
	// "//", like the ranges of the children of a key.
	keys := []string{"a", "b", "a/b", "b/c", "/", "/a", "/b", "/a/b", "a/b/c", "/a/b/c"}
	ephemeralKey := "b"
	ranges := []struct{ start, end string }{
		{"a", "b/d"},
		{"", "/a/b"},
		{"/c", "a/b/d"},
		{"c", "/"},
		{"/", "//"},
		{"/a/", "/a//"},
		{"a/", "c"},
		{"", "z"},
		{"b", "a"},
	}

	for _, keySorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(keySorting.String(), func(t *testing.T) {
			kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			walFactory := newTestWalFactory(t)

			lc, _ := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(),
				walFactory, kvFactory, &proto.NewTermOptions{KeySorting: keySorting})
			_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
			_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
				Shard:             shard,
				Term:              1,
				ReplicationFactor: 1,
				FollowerMaps:      nil,
			})

			session, err := lc.CreateSession(&proto.CreateSessionRequest{Shard: shard, SessionTimeoutMs: 60_000})
			assert.NoError(t, err)

			var puts []*proto.PutRequest
			for _, key := range keys {
				put := &proto.PutRequest{
					Key:   key,
					Value: []byte(key),
					SecondaryIndexes: []*proto.SecondaryIndex{
						{IndexName: "idx", SecondaryKey: key},
						{IndexName: "other", SecondaryKey: key},
					},
				}
				if key == ephemeralKey {
					put.SessionId = &session.SessionId
				}
				puts = append(puts, put)
			}
			_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{Shard: &shard, Puts: puts})
			assert.NoError(t, err)

			list := func(start, end string, index *string) []string {
				listed, err := lc.ListBlock(context.Background(), &proto.ListRequest{
					Shard:              &shard,
					StartInclusive:     start,
					EndExclusive:       end,
					SecondaryIndexName: index,
				})
				assert.NoError(t, err)
				return listed
			}
			scan := func(start, end string, index *string) []string {
				results, err := scanAll(context.Background(), lc, &proto.RangeScanRequest{
					Shard:              &shard,
					StartInclusive:     start,
					EndExclusive:       end,
					SecondaryIndexName: index,
				})
				assert.NoError(t, err)
				var scanned []string
				for _, result := range results {
					// Each record has its key as value
					assert.Equal(t, result.GetKey(), string(result.Value))
					scanned = append(scanned, result.GetKey())
				}
				return scanned
			}

			for _, r := range ranges {
				assert.Equal(t, list(r.start, r.end, nil), list(r.start, r.end, pb.String("idx")),
					"List [%q, %q)", r.start, r.end)
				assert.Equal(t, scan(r.start, r.end, nil), scan(r.start, r.end, pb.String("idx")),
					"RangeScan [%q, %q)", r.start, r.end)
			}

			// As before, a range through an index ends before its first entry
			// when the end key is empty, while on the primary keys an empty end
			// key means no upper bound
			for _, start := range []string{"", "a", "/"} {
				assert.Empty(t, list(start, "", pb.String("idx")), "List [%q, \"\")", start)
				assert.Empty(t, scan(start, "", pb.String("idx")), "RangeScan [%q, \"\")", start)
			}

			assert.NoError(t, lc.Close())
			assert.NoError(t, kvFactory.Close())
			assert.NoError(t, walFactory.Close())
		})
	}
}

func TestSecondaryIndices_SecondaryKeyWithSeparator(t *testing.T) {
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

	// The secondary key is stored as the client supplied it, so it can be empty
	// or carry the byte that separates it from the primary key. Neither may
	// change which record an index entry points back to.
	sneaky := "k1" + secondaryIdxSeparator + url.PathEscape("/not-a-real-key")
	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "my-idx", SecondaryKey: sneaky}}},
			{Key: "/b", Value: []byte("1"), SecondaryIndexes: []*proto.SecondaryIndex{
				{IndexName: "my-idx", SecondaryKey: ""}}},
		},
	})
	assert.NoError(t, err)

	keys, err := lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "",
		EndExclusive:       "\xff",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"/a", "/b"}, keys)

	// The same keys have to come back through a get on the index.
	for _, tc := range []struct{ secondaryKey, expectedKey string }{
		{sneaky, "/a"},
		{"", "/b"},
	} {
		resps, err := readAll(context.Background(), lc, &proto.ReadRequest{
			Shard: &shard,
			Gets: []*proto.GetRequest{{
				Key:                tc.secondaryKey,
				ComparisonType:     proto.KeyComparisonType_EQUAL,
				SecondaryIndexName: pb.String("my-idx"),
			}},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resps))
		assert.Equal(t, proto.Status_OK, resps[0].Status)
		assert.Equal(t, tc.expectedKey, *resps[0].Key)
		assert.Equal(t, tc.secondaryKey, *resps[0].SecondaryIndexKey)
	}

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// The ephemeral records deleted when their session ends must take their
// secondary index entries with them. Until the feature is enabled, the session
// end must be applied as by the servers that don't support it, which leave the
// entries behind: otherwise the replicas of a mixed ensemble would diverge.
func TestSecondaryIndices_SessionEnd(t *testing.T) {
	for _, tc := range []struct {
		name             string
		features         []proto.Feature
		expectedMyIdx    []string
		expectedOtherIdx []string
	}{{
		name:          "feature enabled",
		features:      []proto.Feature{proto.Feature_FEATURE_EPHEMERAL_SECONDARY_INDEX_CLEANUP},
		expectedMyIdx: []string{"/persistent"},
	}, {
		name:             "feature disabled",
		expectedMyIdx:    []string{"/ephemeral", "/persistent"},
		expectedOtherIdx: []string{"/ephemeral"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			var shard int64 = 1

			kvFactory, _ := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
			walFactory := newTestWalFactory(t)

			lc, _ := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory, nil)
			_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
			_, err := lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
				Shard:             shard,
				Term:              1,
				ReplicationFactor: 1,
				FollowerMaps:      nil,
				FeaturesSupported: tc.features,
			})
			assert.NoError(t, err)
			sm := lc.(*leaderController).sessionManager

			createResp, err := sm.CreateSession(&proto.CreateSessionRequest{Shard: shard, SessionTimeoutMs: 5000})
			assert.NoError(t, err)
			sessionId := createResp.SessionId

			_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
				Shard: &shard,
				Puts: []*proto.PutRequest{
					{Key: "/ephemeral", Value: []byte("0"), SessionId: &sessionId, SecondaryIndexes: []*proto.SecondaryIndex{
						{IndexName: "my-idx", SecondaryKey: "0"},
						{IndexName: "other-idx", SecondaryKey: "0"},
					}},
					{Key: "/persistent", Value: []byte("1"), SecondaryIndexes: []*proto.SecondaryIndex{
						{IndexName: "my-idx", SecondaryKey: "1"},
					}},
				},
			})
			assert.NoError(t, err)

			_, err = sm.CloseSession(&proto.CloseSessionRequest{Shard: shard, SessionId: sessionId})
			assert.NoError(t, err)
			assert.Empty(t, getData(t, lc.(*leaderController), "/ephemeral"))

			listIndex := func(indexName string) []string {
				keys, err := lc.ListBlock(context.Background(), &proto.ListRequest{
					Shard:              &shard,
					StartInclusive:     "",
					EndExclusive:       "\xff",
					SecondaryIndexName: pb.String(indexName),
				})
				assert.NoError(t, err)
				return keys
			}
			assert.Equal(t, tc.expectedMyIdx, listIndex("my-idx"))
			assert.Equal(t, tc.expectedOtherIdx, listIndex("other-idx"))

			assert.NoError(t, lc.Close())
			assert.NoError(t, kvFactory.Close())
			assert.NoError(t, walFactory.Close())
		})
	}
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
