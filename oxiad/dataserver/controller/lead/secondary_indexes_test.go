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
	"bytes"
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/compare"
	"github.com/oxia-db/oxia/common/rpc"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
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
	secondaryIndexSkipUnchanged    bool
}

func (f testFeatureChecker) IsFeatureEnabled(candidate proto.Feature) bool {
	switch candidate {
	case proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION:
		return f.secondaryIndexNameValidation
	case proto.Feature_FEATURE_EPHEMERAL_CLEANUP_NATURAL_SORTING:
		return f.ephemeralCleanupNaturalSorting
	case proto.Feature_FEATURE_SECONDARY_INDEX_SKIP_UNCHANGED:
		return f.secondaryIndexSkipUnchanged
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
	assert.Equal(t, pb.String("1"), results[0].SecondaryIndexKey)
	assert.Equal(t, "/c", *results[1].Key)
	assert.Equal(t, "2", string(results[1].Value))
	assert.Equal(t, pb.String("2"), results[1].SecondaryIndexKey)

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

// An index range scan reads the records as they were when it started, like the
// entries of the index: a record deleted since, or moved to another secondary
// key, comes as it was, with its entry.
func TestSecondaryIndexRangeScanReturnsRecordsAsOfStart(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(),
		walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte("0"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "0"}}},
			{Key: "/b", Value: []byte("1"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "1"}}},
			{Key: "/c", Value: []byte("2"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "2"}}},
			{Key: "/d", Value: []byte("3"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "3"}}},
			{Key: "/e", Value: []byte("4"), SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "4"}}},
		},
	})
	require.NoError(t, err)

	var keys []string
	err = lc.RangeScan(context.Background(), &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "9",
		SecondaryIndexName: pb.String("my-idx"),
	}, func(r *proto.GetResponse) error {
		if len(keys) == 0 {
			// Once the scan started
			_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
				Shard: &shard,
				Puts: []*proto.PutRequest{{Key: "/c", Value: []byte("8"),
					SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "8"}}}},
				Deletes: []*proto.DeleteRequest{{Key: "/b"}, {Key: "/e"}},
			})
			require.NoError(t, err)
		}
		assert.Equal(t, proto.Status_OK, r.Status, r.GetKey())
		assert.Equal(t, r.GetSecondaryIndexKey(), string(r.Value), r.GetKey())
		keys = append(keys, r.GetKey())
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/b", "/c", "/d", "/e"}, keys)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// Without FEATURE_EPHEMERAL_SECONDARY_INDEX_CLEANUP, a session end deletes its
// ephemeral records but leaves their index entries behind, and no write deletes
// them later: an index range scan skips them too.
func TestSecondaryIndexRangeScanSkipsLeakedEntries(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(),
		walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)
	sm := lc.(*leaderController).sessionManager

	createResp, err := sm.CreateSession(&proto.CreateSessionRequest{Shard: shard, SessionTimeoutMs: 5000})
	require.NoError(t, err)
	sessionId := createResp.SessionId
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/ephemeral", Value: []byte("0"), SessionId: &sessionId,
				SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "0"}}},
			{Key: "/persistent", Value: []byte("1"),
				SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: "1"}}},
		},
	})
	require.NoError(t, err)
	_, err = sm.CloseSession(&proto.CloseSessionRequest{Shard: shard, SessionId: sessionId})
	require.NoError(t, err)

	// The index still lists the record of the ended session
	keys, err := lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "9",
		SecondaryIndexName: pb.String("my-idx"),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"/ephemeral", "/persistent"}, keys)

	results, err := scanAll(context.Background(), lc, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "0",
		EndExclusive:       "9",
		SecondaryIndexName: pb.String("my-idx"),
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, proto.Status_OK, results[0].Status)
	assert.Equal(t, "/persistent", results[0].GetKey())
	assert.Equal(t, "1", string(results[0].Value))

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

// An index get reads the record of the entry it finds as it was when it found
// the entry: a record moved to another secondary key since comes as it was.
func TestSecondaryIndexGetReturnsRecordAsOfEntry(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(&option.StorageOptions{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(),
		walFactory, kvFactory, nil)
	require.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	require.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	put := func(secondaryKey string) {
		_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
			Shard: &shard,
			Puts: []*proto.PutRequest{{Key: "/a", Value: []byte(secondaryKey),
				SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "my-idx", SecondaryKey: secondaryKey}}}},
		})
		require.NoError(t, err)
	}
	put("0")

	db := &hookedPrefixDB{DB: lc.(*leaderController).db, afterPrefixIterator: func() { put("1") }}
	res, err := secondaryIndexGet(&proto.GetRequest{
		Key:                "0",
		IncludeValue:       true,
		SecondaryIndexName: pb.String("my-idx"),
	}, db)
	require.NoError(t, err)
	assert.Equal(t, proto.Status_OK, res.Status)
	assert.Equal(t, "/a", res.GetKey())
	assert.Equal(t, "0", res.GetSecondaryIndexKey())
	assert.Equal(t, "0", string(res.Value))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
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

// A range through an index with an inclusive end returns the records of the
// range without its end, followed by the records whose secondary key is the
// end, unless the start is after the end. The secondary keys aren't unique, and
// many extend others: with a byte below '~', like "k-12" for "k-1", with the
// separator of the index entries, or with a byte below it. None of them may
// come with the records of the end, in either key sorting, nor may the entries
// of another index or the session shadow key of an ephemeral record, which the
// hierarchical sorting puts between the levels of the entries. On the primary
// keys, a range with an inclusive end returns the record of the end too.
func TestSecondaryIndexListAndRangeScanEndInclusive(t *testing.T) {
	var shard int64 = 1

	secondaryKeys := []string{
		"k-1", "k-12", "k-1:a", "k-1_b", "k-1~", "k-1\x00", "k-1\x01", "k-1\x01x", "k-0", "k-2",
		"", "/", "//", "a/b", "a/b/c", "a//", "/a", "/a//", "z",
	}
	bounds := append(slices.Clone(secondaryKeys), "k", "k-1~~", "a/", "~")

	// Two records for each secondary key. An index entry has the escaped
	// primary key, so the record with a '/' in its key comes first.
	type record struct{ key, secondaryKey string }
	var records []record
	for i, secondaryKey := range secondaryKeys {
		records = append(records,
			record{fmt.Sprintf("p-%02d", i), secondaryKey},
			record{fmt.Sprintf("p/%02d", i), secondaryKey})
	}
	ephemeralKey := records[0].key
	// Records without an index, with keys that end in "//" or extend others
	unindexedKeys := []string{"q/", "q//", "q-1", "q-1\x00"}
	primaryBounds := append(slices.Clone(unindexedKeys), "p-00", "p-05", "p/00", "p/05~", "p", "q", "r")

	for _, keySorting := range []proto.KeySortingType{proto.KeySortingType_NATURAL, proto.KeySortingType_HIERARCHICAL} {
		t.Run(keySorting.String(), func(t *testing.T) {
			encoder := compare.EncoderNatural
			if keySorting == proto.KeySortingType_HIERARCHICAL {
				encoder = compare.EncoderHierarchical
			}
			compareKeys := func(a, b string) int {
				return bytes.Compare(encoder.Encode(a), encoder.Encode(b))
			}

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
			require.NoError(t, err)

			var puts []*proto.PutRequest
			for _, r := range records {
				put := &proto.PutRequest{
					Key:   r.key,
					Value: []byte(r.key),
					SecondaryIndexes: []*proto.SecondaryIndex{
						{IndexName: "idx", SecondaryKey: r.secondaryKey},
						{IndexName: "other", SecondaryKey: r.secondaryKey},
					},
				}
				if r.key == ephemeralKey {
					put.SessionId = &session.SessionId
				}
				puts = append(puts, put)
			}
			for _, key := range unindexedKeys {
				puts = append(puts, &proto.PutRequest{Key: key, Value: []byte(key)})
			}
			_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{Shard: &shard, Puts: puts})
			require.NoError(t, err)

			list := func(start, end string, index *string, endInclusive bool) []string {
				listed, err := lc.ListBlock(context.Background(), &proto.ListRequest{
					Shard:              &shard,
					StartInclusive:     start,
					EndExclusive:       end,
					SecondaryIndexName: index,
					EndInclusive:       endInclusive,
				})
				require.NoError(t, err)
				return listed
			}
			scan := func(start, end string, index *string, endInclusive bool) []record {
				results, err := scanAll(context.Background(), lc, &proto.RangeScanRequest{
					Shard:              &shard,
					StartInclusive:     start,
					EndExclusive:       end,
					SecondaryIndexName: index,
					EndInclusive:       endInclusive,
				})
				require.NoError(t, err)
				var scanned []record
				for _, result := range results {
					// Each record has its key as value
					assert.Equal(t, result.GetKey(), string(result.Value))
					scanned = append(scanned, record{result.GetKey(), result.GetSecondaryIndexKey()})
				}
				return scanned
			}

			// The records in the order of their entries in the index: for the
			// keys that no other key extends with a byte up to the separator, and
			// that don't end in "//", which the hierarchical sorting puts one
			// level after the key, it's the order of the secondary keys
			ordered := slices.Clone(records)
			slices.SortFunc(ordered, func(a, b record) int {
				if c := compareKeys(a.secondaryKey, b.secondaryKey); c != 0 {
					return c
				}
				return strings.Compare(url.PathEscape(a.key), url.PathEscape(b.key))
			})
			plain := func(key string) bool {
				return !strings.ContainsAny(key, "\x00\x01") && !strings.HasSuffix(key, "//")
			}

			idx := pb.String("idx")
			// The point lookup of the motivation: none of the keys that extend
			// "k-1" comes with it
			assert.Equal(t, []string{"p/00", "p-00"}, list("k-1", "k-1", idx, true))
			assert.Empty(t, list("k-1", "k-1", idx, false))

			for _, start := range bounds {
				for _, end := range bounds {
					expected := list(start, end, idx, false)
					expectedScan := scan(start, end, idx, false)
					if compareKeys(start, end) <= 0 {
						for _, r := range ordered {
							if r.secondaryKey == end {
								expected = append(expected, r.key)
								expectedScan = append(expectedScan, r)
							}
						}
					}
					listed := list(start, end, idx, true)
					assert.Equal(t, expected, listed, "List [%q, %q]", start, end)
					assert.Equal(t, expectedScan, scan(start, end, idx, true), "RangeScan [%q, %q]", start, end)

					if !plain(start) || !plain(end) {
						continue
					}
					// Through the plain keys, the range is the one of the key sorting
					var inRange []string
					for _, r := range ordered {
						if plain(r.secondaryKey) &&
							compareKeys(start, r.secondaryKey) <= 0 && compareKeys(r.secondaryKey, end) <= 0 {
							inRange = append(inRange, r.key)
						}
					}
					var listedPlain []string
					for _, key := range listed {
						i := slices.IndexFunc(records, func(r record) bool { return r.key == key })
						if plain(records[i].secondaryKey) {
							listedPlain = append(listedPlain, key)
						}
					}
					assert.Equal(t, inRange, listedPlain, "List [%q, %q] of the plain keys", start, end)
				}
			}

			// On the primary keys, the end is a key like any other
			stored := func(key string) bool {
				return slices.Contains(unindexedKeys, key) ||
					slices.ContainsFunc(records, func(r record) bool { return r.key == key })
			}
			for _, start := range primaryBounds {
				for _, end := range primaryBounds {
					expected := list(start, end, nil, false)
					expectedScan := scan(start, end, nil, false)
					if compareKeys(start, end) <= 0 && stored(end) {
						expected = append(expected, end)
						expectedScan = append(expectedScan, record{key: end})
					}
					assert.Equal(t, expected, list(start, end, nil, true), "List [%q, %q]", start, end)
					assert.Equal(t, expectedScan, scan(start, end, nil, true), "RangeScan [%q, %q]", start, end)
				}
			}
			// With an inclusive end, an empty end is the empty key, not an open end
			assert.Empty(t, list("", "", nil, true))
			assert.Empty(t, scan("", "", nil, true))

			assert.NoError(t, lc.Close())
			assert.NoError(t, kvFactory.Close())
			assert.NoError(t, walFactory.Close())
		})
	}
}

// With the hierarchical sorting, the entries of a key ending in "//" are one
// level after the end bound of the key. A range with an inclusive end seeks to
// them, rather than step through the rest of the level and the start of the
// next one, and stops after them, before the entries of the keys that extend
// the end key with the byte after the separator.
func TestSecondaryIndexListEndInclusiveSeeksToEndKey(t *testing.T) {
	kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
	require.NoError(t, err)
	defer func() { assert.NoError(t, kvFactory.Close()) }()
	db, err := database.NewDB(constant.DefaultNamespace, 1, kvFactory, proto.KeySortingType_HIERARCHICAL,
		time.Hour, time2.SystemClock)
	require.NoError(t, err)
	defer func() { assert.NoError(t, db.Close()) }()

	write := &proto.WriteRequest{}
	add := func(key, secondaryKey string) {
		write.Puts = append(write.Puts, &proto.PutRequest{Key: key, Value: []byte(key),
			SecondaryIndexes: []*proto.SecondaryIndex{{IndexName: "idx", SecondaryKey: secondaryKey}}})
	}
	add("p-a", "a//")
	for i := range 100 {
		// After the end bound of "a//", in its level
		add(fmt.Sprintf("p-b%03d", i), fmt.Sprintf("b/%03d", i))
		// In the next level, before the entries of "a//"
		add(fmt.Sprintf("p-c%03d", i), fmt.Sprintf("0/0/%03d", i))
		// After the entries of "a//"
		add(fmt.Sprintf("p-d%03d", i), fmt.Sprintf("a//\x02%03d", i))
	}
	_, err = db.ProcessWrite(write, 0, 0, WrapperUpdateOperationCallback)
	require.NoError(t, err)

	hooked := &hookedPrefixDB{DB: db}
	it, err := newSecondaryIndexListIterator(&proto.ListRequest{
		SecondaryIndexName: pb.String("idx"), StartInclusive: "a//", EndExclusive: "a//", EndInclusive: true,
	}, hooked)
	require.NoError(t, err)
	var listed []string
	for ; it.Valid(); it.Next() {
		listed = append(listed, it.Key())
	}
	require.NoError(t, it.Close())
	assert.Equal(t, []string{"p-a"}, listed)
	// The step from the entry of "a//" to the end of the range
	assert.Equal(t, 1, hooked.nextCalls)
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

	// A range scan returns the records with their secondary keys, the empty one
	// included.
	results, err := scanAll(context.Background(), lc, &proto.RangeScanRequest{
		Shard:              &shard,
		StartInclusive:     "",
		EndExclusive:       "\xff",
		SecondaryIndexName: pb.String("my-idx"),
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, "/b", *results[0].Key)
	assert.Equal(t, pb.String(""), results[0].SecondaryIndexKey)
	assert.Equal(t, "/a", *results[1].Key)
	assert.Equal(t, pb.String(sneaky), results[1].SecondaryIndexKey)

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

// With FEATURE_SECONDARY_INDEX_SKIP_UNCHANGED, a put that overwrites a record
// deletes only the index entries of the indexes that it drops, and writes only
// the ones of the indexes that it adds. Without the feature, it deletes and
// writes again all the entries, as the servers that don't support the feature
// do. Either way, the record ends up with the entries of its new indexes.
func TestSecondaryIndices_OverwriteSkipsUnchangedEntries(t *testing.T) {
	a := &proto.SecondaryIndex{IndexName: "idx", SecondaryKey: "a"}
	b := &proto.SecondaryIndex{IndexName: "idx", SecondaryKey: "b"}
	c := &proto.SecondaryIndex{IndexName: "other-idx", SecondaryKey: "a"}
	// Index names could have a '/' before
	// FEATURE_SECONDARY_INDEX_NAME_VALIDATION: these two indexes give a record
	// the same entry
	slashInName := &proto.SecondaryIndex{IndexName: "x/y", SecondaryKey: "z"}
	slashInKey := &proto.SecondaryIndex{IndexName: "x", SecondaryKey: "y/z"}

	for _, tc := range []struct {
		name     string
		existing []*proto.SecondaryIndex
		updated  []*proto.SecondaryIndex
		// The write batch operations of the overwrite with the feature
		operations int
	}{
		{name: "same indexes", existing: []*proto.SecondaryIndex{a, c}, updated: []*proto.SecondaryIndex{a, c}},
		{name: "reordered", existing: []*proto.SecondaryIndex{a, c}, updated: []*proto.SecondaryIndex{c, a}},
		{name: "changed", existing: []*proto.SecondaryIndex{a, c}, updated: []*proto.SecondaryIndex{b, c}, operations: 2},
		{name: "dropped", existing: []*proto.SecondaryIndex{a, c}, updated: []*proto.SecondaryIndex{c}, operations: 1},
		{name: "added", existing: []*proto.SecondaryIndex{c}, updated: []*proto.SecondaryIndex{a, c}, operations: 1},
		{name: "all dropped", existing: []*proto.SecondaryIndex{a, c}, operations: 2},
		{name: "duplicate", existing: []*proto.SecondaryIndex{a, a}, updated: []*proto.SecondaryIndex{a}},
		{name: "same entry", existing: []*proto.SecondaryIndex{slashInName}, updated: []*proto.SecondaryIndex{slashInKey}},
		{
			name:     "same entry kept",
			existing: []*proto.SecondaryIndex{slashInName, slashInKey},
			updated:  []*proto.SecondaryIndex{slashInKey},
		},
	} {
		for _, skipUnchanged := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/feature=%v", tc.name, skipUnchanged), func(t *testing.T) {
				kvFactory, err := kvstore.NewPebbleKVFactory(kvstore.NewFactoryOptionsForTest(t))
				require.NoError(t, err)
				defer kvFactory.Close()
				kv, err := kvFactory.NewKV(constant.DefaultNamespace, 1, proto.KeySortingType_HIERARCHICAL)
				require.NoError(t, err)
				defer kv.Close()

				features := testFeatureChecker{secondaryIndexSkipUnchanged: skipUnchanged}
				put := func(existingEntry *proto.StorageEntry, indexes []*proto.SecondaryIndex) int {
					batch := kv.NewWriteBatch()
					defer batch.Close()
					request := &proto.PutRequest{Key: "/record", SecondaryIndexes: indexes}
					status, err := secondaryIndexesUpdateCallback.OnPut(batch, nil, request, existingEntry, features)
					require.NoError(t, err)
					require.Equal(t, proto.Status_OK, status)
					operations := batch.Count()
					require.NoError(t, batch.Commit())
					return operations
				}

				put(nil, tc.existing)
				operations := put(&proto.StorageEntry{SecondaryIndexes: tc.existing}, tc.updated)
				if skipUnchanged {
					assert.Equal(t, tc.operations, operations)
				} else {
					assert.Equal(t, len(tc.existing)+len(tc.updated), operations)
				}

				var expected []string
				for _, si := range tc.updated {
					expected = append(expected, "__oxia/idx/"+si.IndexName+"/"+si.SecondaryKey+"\x01%2Frecord")
				}
				it, err := kv.KeyPrefixIterator(secondaryIdxKeyPrefix+"/", "")
				require.NoError(t, err)
				var entries []string
				for it.SeekGE(secondaryIdxKeyPrefix + "/"); it.Valid(); it.Next() {
					entries = append(entries, it.Key())
				}
				require.NoError(t, it.Close())
				assert.ElementsMatch(t, expected, entries)
			})
		}
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

	// Get with an unsupported ComparisonType to hit the default branch of doSecondaryGet
	db := lc.(*leaderController).db
	_, err = secondaryIndexGet(&proto.GetRequest{
		Key:                "0",
		SecondaryIndexName: pb.String("my-idx"),
		ComparisonType:     proto.KeyComparisonType(999),
	}, db)
	assert.ErrorContains(t, err, "unsupported comparison type")

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}
