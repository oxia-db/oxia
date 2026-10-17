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

package backup

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLineageIsRandomHex(t *testing.T) {
	a, err := NewLineage()
	require.NoError(t, err)
	b, err := NewLineage()
	require.NoError(t, err)
	assert.Len(t, a, 16)
	assert.Regexp(t, "^[0-9a-f]{16}$", a)
	assert.NotEqual(t, a, b)
}

func TestIDRoundTrips(t *testing.T) {
	at := time.Date(2026, 10, 3, 14, 25, 7, 999, time.FixedZone("x", 3600))
	id := NewID(at, "5f3a9c1e2b7d4e60")
	assert.Equal(t, "2026-10-03T13-25-07Z-5f3a9c1e2b7d4e60", id.String())
	parsed, err := ParseID(id.String())
	require.NoError(t, err)
	assert.Equal(t, id, parsed)
	assert.True(t, parsed.Time.Equal(at.Truncate(time.Second)))
}

func TestParseIDRejectsMalformedIDs(t *testing.T) {
	for _, s := range []string{"", "nodash", "2026-10-03T13-25-07Z-", "-abc", "2026-10-03-abc", "20261003T132507Z-abc"} {
		_, err := ParseID(s)
		assert.ErrorIs(t, err, ErrInvalidID, s)
	}
}

func TestIDCompareOrdersByTimeThenLineage(t *testing.T) {
	t1 := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	assert.Negative(t, NewID(t1, "b").Compare(NewID(t2, "a")))
	assert.Negative(t, NewID(t1, "a").Compare(NewID(t1, "b")))
	assert.Zero(t, NewID(t1, "a").Compare(NewID(t1, "a")))
	assert.Positive(t, NewID(t2, "a").Compare(NewID(t1, "z")))
}

func TestRangeCoversAndOverlaps(t *testing.T) {
	assert.True(t, FullRange.Covers(Range{Min: 10, Max: 20}))
	assert.True(t, Range{Min: 10, Max: 20}.Covers(Range{Min: 10, Max: 20}))
	assert.False(t, Range{Min: 10, Max: 20}.Covers(Range{Min: 10, Max: 21}))
	assert.True(t, Range{Min: 10, Max: 20}.Overlaps(Range{Min: 20, Max: 30}))
	assert.False(t, Range{Min: 10, Max: 20}.Overlaps(Range{Min: 21, Max: 30}))
	assert.Equal(t, uint32(math.MaxUint32), FullRange.Max)
}

func TestShardPathKeys(t *testing.T) {
	p := ShardPath{Namespace: "orders", Shard: 12}
	id := NewID(time.Date(2026, 10, 3, 14, 25, 7, 0, time.UTC), "abcd")
	assert.Equal(t, "orders/", NamespacePrefix("orders"))
	assert.Equal(t, "orders/shard-12/", p.Prefix())
	assert.Equal(t, "orders/shard-12/sst/", p.SSTPrefix())
	assert.Equal(t, "orders/shard-12/sst/abcd-000123.sst", p.SSTKey("abcd", "000123.sst", ""))
	assert.Equal(t, "orders/shard-12/sst/abcd-000123.sst.zst", p.SSTKey("abcd", "000123.sst", CompressionZstd))
	assert.Equal(t, "orders/shard-12/backups/", p.ManifestsPrefix())
	assert.Equal(t, "orders/shard-12/backups/2026-10-03T14-25-07Z-abcd.json", p.ManifestKey(id))
	assert.Equal(t, "orders/shard-12/backups/2026-10-03T14-25-07Z-abcd/", p.FilesPrefix(id))
	assert.Equal(t, "orders/shard-12/backups/2026-10-03T14-25-07Z-abcd/MANIFEST-000001", p.FileKey(id, "MANIFEST-000001"))
}

func TestParseShardDir(t *testing.T) {
	p, ok := ParseShardDir("orders/shard-12/")
	require.True(t, ok)
	assert.Equal(t, ShardPath{Namespace: "orders", Shard: 12}, p)
	for _, dir := range []string{"orders/", "orders/shard-/", "orders/shard-x/", "orders/shard--1/", "shard-1/", "orders/other-1/"} {
		_, ok := ParseShardDir(dir)
		assert.False(t, ok, dir)
	}
}

func TestParseManifestKey(t *testing.T) {
	p, id, ok := ParseManifestKey("orders/shard-12/backups/2026-10-03T14-25-07Z-abcd.json")
	require.True(t, ok)
	assert.Equal(t, ShardPath{Namespace: "orders", Shard: 12}, p)
	assert.Equal(t, "2026-10-03T14-25-07Z-abcd", id.String())
	for _, key := range []string{
		"orders/shard-12/backups/2026-10-03T14-25-07Z-abcd/MANIFEST-000001",
		"orders/shard-12/sst/abcd-000001.sst",
		"orders/shard-12/backups/garbage.json",
		"orders/backups/2026-10-03T14-25-07Z-abcd.json",
	} {
		_, _, ok := ParseManifestKey(key)
		assert.False(t, ok, key)
	}
}

func TestIsCompressedKey(t *testing.T) {
	assert.True(t, IsCompressedKey("orders/shard-12/sst/abcd-000123.sst.zst"))
	assert.False(t, IsCompressedKey("orders/shard-12/sst/abcd-000123.sst"))
}

func TestIsSSTName(t *testing.T) {
	assert.True(t, IsSSTName("000123.sst"))
	assert.False(t, IsSSTName("MANIFEST-000001"))
	assert.False(t, IsSSTName("000002.log"))
}
