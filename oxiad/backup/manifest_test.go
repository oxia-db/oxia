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
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

func TestManifestValidate(t *testing.T) {
	valid := newManifest("orders", 12, "abcd", time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC))
	require.NoError(t, valid.Validate())
	for name, modify := range map[string]func(m *Manifest){
		"format version": func(m *Manifest) { m.FormatVersion = 2 },
		"namespace":      func(m *Manifest) { m.Namespace = "" },
		"shard":          func(m *Manifest) { m.Shard = -1 },
		"lineage":        func(m *Manifest) { m.Lineage = "" },
		"range":          func(m *Manifest) { m.Range = Range{Min: 2, Max: 1} },
		"compression":    func(m *Manifest) { m.Compression = "gzip" },
		"id":             func(m *Manifest) { m.ID = "garbage" },
		"id lineage":     func(m *Manifest) { m.ID = "2026-10-03T14-00-00Z-other" },
		"file key":       func(m *Manifest) { m.SSTs[0].Key = "" },
		"file size":      func(m *Manifest) { m.Files[0].Size = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			m := newManifest("orders", 12, "abcd", time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC))
			modify(m)
			assert.ErrorIs(t, m.Validate(), ErrInvalidManifest)
		})
	}
}

func TestManifestSummary(t *testing.T) {
	m := newManifest("orders", 12, "abcd", time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC))
	s := m.Summary()
	assert.Equal(t, "orders", s.Namespace)
	assert.Equal(t, int64(12), s.Shard)
	assert.Equal(t, m.ID, s.ID.String())
	assert.Equal(t, m.CreatedAt, s.CreatedAt)
	assert.Equal(t, m.CreatedAt, s.StartedAt())
	assert.Equal(t, s.ID.Time, Summary{ID: s.ID}.StartedAt())
	assert.Equal(t, m.Range, s.Range)
	assert.Equal(t, int64(7), s.Term)
	assert.Equal(t, int64(1234), s.CommitOffset)
	assert.Equal(t, int64(1000+2000+300), s.SizeBytes)
	assert.Equal(t, 2, s.SSTCount)
	assert.Equal(t, m.NamespaceConfig, s.NamespaceConfig)
	assert.Equal(t, "orders/shard-12/backups/2026-10-03T14-00-00Z-abcd.json", s.ManifestKey())
}

func TestWriteManifestThenReadManifestRoundTrips(t *testing.T) {
	store := blob.NewMemory()
	m := newManifest("orders", 12, "abcd", time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC))
	require.NoError(t, WriteManifest(t.Context(), store, m))
	assert.Equal(t, []string{"orders/shard-12/backups/2026-10-03T14-00-00Z-abcd.json"}, store.Keys())
	read, err := ReadManifest(t.Context(), store, m.Summary().ManifestKey())
	require.NoError(t, err)
	assert.Equal(t, m, read)
}

func TestWriteManifestRejectsInvalidManifest(t *testing.T) {
	store := blob.NewMemory()
	m := newManifest("orders", 12, "abcd", time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC))
	m.Lineage = ""
	assert.ErrorIs(t, WriteManifest(t.Context(), store, m), ErrInvalidManifest)
	assert.Empty(t, store.Keys())
}

func TestReadManifestReportsMissingAndCorruptManifests(t *testing.T) {
	store := blob.NewMemory()
	_, err := ReadManifest(t.Context(), store, "orders/shard-1/backups/x.json")
	assert.ErrorIs(t, err, blob.ErrNotFound)
	require.NoError(t, store.Put(t.Context(), "corrupt.json", strings.NewReader("{not json")))
	_, err = ReadManifest(t.Context(), store, "corrupt.json")
	assert.ErrorIs(t, err, ErrInvalidManifest)
	require.NoError(t, store.Put(t.Context(), "wrong.json", strings.NewReader(`{"format_version": 99}`)))
	_, err = ReadManifest(t.Context(), store, "wrong.json")
	assert.ErrorIs(t, err, ErrInvalidManifest)
}

func TestListManifestsReturnsBackupsOfShardOldestFirst(t *testing.T) {
	store := blob.NewMemory()
	base := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	for _, m := range []*Manifest{
		newManifest("orders", 12, "abcd", base.Add(2*time.Hour)),
		newManifest("orders", 12, "abcd", base),
		newManifest("orders", 12, "ef01", base.Add(time.Hour)),
		newManifest("orders", 13, "abcd", base),
		newManifest("users", 12, "abcd", base),
	} {
		require.NoError(t, WriteManifest(t.Context(), store, m))
	}
	// The per-backup files are listed under the same prefix, and skipped
	require.NoError(t, store.Put(t.Context(), "orders/shard-12/backups/2026-10-03T14-00-00Z-abcd/MANIFEST-000001",
		strings.NewReader("")))

	summaries, err := ListManifests(t.Context(), store, ShardPath{Namespace: "orders", Shard: 12})
	require.NoError(t, err)
	ids := make([]string, 0, len(summaries))
	for _, s := range summaries {
		ids = append(ids, s.ID.String())
		assert.Equal(t, int64(12), s.Shard)
	}
	assert.Equal(t, []string{
		"2026-10-03T14-00-00Z-abcd",
		"2026-10-03T15-00-00Z-ef01",
		"2026-10-03T16-00-00Z-abcd",
	}, ids)
}

func TestListShardsReturnsShardsWithBackups(t *testing.T) {
	store := blob.NewMemory()
	base := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	for _, m := range []*Manifest{
		newManifest("orders", 12, "abcd", base),
		newManifest("orders", 3, "abcd", base),
		newManifest("users", 1, "abcd", base),
	} {
		require.NoError(t, WriteManifest(t.Context(), store, m))
	}
	require.NoError(t, store.Put(t.Context(), "orders/stray", strings.NewReader("")))
	shards, err := ListShards(t.Context(), store, "orders")
	require.NoError(t, err)
	assert.Equal(t, []ShardPath{{Namespace: "orders", Shard: 12}, {Namespace: "orders", Shard: 3}}, shards)
}

func newManifest(namespace string, shard int64, lineage string, at time.Time) *Manifest {
	id := NewID(at, lineage)
	p := ShardPath{Namespace: namespace, Shard: shard}
	return &Manifest{
		FormatVersion:   ManifestFormatVersion,
		ID:              id.String(),
		Lineage:         lineage,
		Namespace:       namespace,
		Shard:           shard,
		Range:           FullRange,
		NamespaceConfig: json.RawMessage(`{"name":"orders","replicationFactor":3}`),
		Term:            7,
		CommitOffset:    1234,
		CreatedAt:       at,
		SSTs: []File{
			{Name: "000005.sst", Key: p.SSTKey(lineage, "000005.sst", ""), Size: 1000, StoredSize: 1000, CRC32C: 1},
			{Name: "000007.sst", Key: p.SSTKey(lineage, "000007.sst", ""), Size: 2000, StoredSize: 2000, CRC32C: 2},
		},
		Files: []File{
			{Name: "MANIFEST-000001", Key: p.FileKey(id, "MANIFEST-000001"), Size: 300, StoredSize: 300, CRC32C: 3},
		},
	}
}
