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
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

func TestCleanupJobRunExpiresBackupsAndTheirFiles(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, map[string][]byte{"MANIFEST-000001": []byte("m")})
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	backups := backupSeries(t, store, host, 1, start, 24*time.Hour, 5)
	now := start.Add(6 * 24 * time.Hour)
	// Day 0 and day 1 are older than the 4-day retention; day 1 covers day 0
	j := &cleanupJob{store: store, namespace: "ns", retention: 4 * 24 * time.Hour, grace: DefaultGracePeriod, now: now,
		log: slog.Default()}
	result, err := j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.backupsExpired)
	keys := manifestKeys(store)
	assert.NotContains(t, keys, "ns/shard-1/backups/"+backups[0].ID+".json")
	assert.Contains(t, keys, "ns/shard-1/backups/"+backups[1].ID+".json", "the newest backup before the window stays")
	assert.Len(t, keys, 4)
	for _, key := range store.Keys() {
		assert.NotContains(t, key, backups[0].ID, "the per-backup files of the expired backup are gone")
	}
	// Every SST is still referenced by the later backups of the lineage
	assert.Equal(t, 1, result.filesDeleted, "the MANIFEST per-backup file of the expired backup")
	assert.Contains(t, store.Keys(), backups[0].SSTs[0].Key)

	again, err := j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, cleanupResult{}, again, "a second run has nothing to do")
}

func TestCleanupJobRunDeletesUnreferencedSSTsAfterTheGracePeriod(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, map[string][]byte{"MANIFEST-000001": []byte("m")})
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	backupSeries(t, store, host, 1, start, time.Hour, 2)
	now := start.Add(3 * time.Hour)
	store.SetClock(func() time.Time { return now })
	// An SST of a backup that never wrote its manifest, just uploaded, and
	// one uploaded two days ago
	require.NoError(t, store.Put(t.Context(), "ns/shard-1/sst/other-000009.sst", strings.NewReader("orphan")))
	require.NoError(t, store.Put(t.Context(), "ns/shard-1/sst/other-000008.sst", strings.NewReader("old orphan")))
	store.Touch("ns/shard-1/sst/other-000008.sst", now.Add(-2*DefaultGracePeriod))
	// The files of a backup that never wrote its manifest, from two days ago
	oldID := backup.NewID(now.Add(-2*DefaultGracePeriod), "other")
	require.NoError(t, store.Put(t.Context(), "ns/shard-1/backups/"+oldID.String()+"/MANIFEST-000001",
		strings.NewReader("stale")))
	newID := backup.NewID(now, "other")
	require.NoError(t, store.Put(t.Context(), "ns/shard-1/backups/"+newID.String()+"/MANIFEST-000001",
		strings.NewReader("in progress")))

	j := &cleanupJob{store: store, namespace: "ns", retention: 7 * 24 * time.Hour, grace: DefaultGracePeriod, now: now,
		log: slog.Default()}
	result, err := j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, result.backupsExpired)
	assert.Equal(t, 2, result.filesDeleted)
	assert.EqualValues(t, len("old orphan")+len("stale"), result.bytesDeleted)
	keys := store.Keys()
	assert.NotContains(t, keys, "ns/shard-1/sst/other-000008.sst")
	assert.Contains(t, keys, "ns/shard-1/sst/other-000009.sst", "younger than the grace period")
	assert.NotContains(t, keys, "ns/shard-1/backups/"+oldID.String()+"/MANIFEST-000001")
	assert.Contains(t, keys, "ns/shard-1/backups/"+newID.String()+"/MANIFEST-000001")
}

func TestCleanupJobRunExpiresAParentCoveredByItsChildren(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	write := func(shard int64, r backup.Range, lineage string, at time.Time) *LastBackup {
		host.addShard(shard, map[string][]byte{"000001.sst": []byte("data"), "MANIFEST-000001": []byte("m")})
		store.SetClock(func() time.Time { return at })
		checkpoint, err := host.Checkpoint(shard)
		require.NoError(t, err)
		defer func() { assert.NoError(t, checkpoint.Close()) }()
		j := &backupJob{store: store, path: backup.ShardPath{Namespace: "ns", Shard: shard}, checkpoint: checkpoint,
			record: &Record{Lineage: lineage}, policy: testPolicy(), hashRange: r, now: at, clock: time.Now,
			reuseWindow: time.Hour, uploadWorkers: 1}
		last, err := j.run(context.Background())
		require.NoError(t, err)
		return last
	}
	left := backup.Range{Min: 0, Max: 1<<31 - 1}
	right := backup.Range{Min: 1 << 31, Max: backup.FullRange.Max}
	parent := write(1, backup.FullRange, "aaaaaaaaaaaaaaaa", start)
	write(2, left, "bbbbbbbbbbbbbbbb", start.Add(time.Hour))
	now := start.Add(10 * 24 * time.Hour)
	j := &cleanupJob{store: store, namespace: "ns", retention: 7 * 24 * time.Hour, grace: DefaultGracePeriod, now: now,
		log: slog.Default()}
	result, err := j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, result.backupsExpired, "the right half is only covered by the parent")

	write(3, right, "cccccccccccccccc", start.Add(2*time.Hour))
	result, err = j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.backupsExpired)
	assert.NotContains(t, manifestKeys(store), "ns/shard-1/backups/"+parent.ID+".json")
	assert.Equal(t, 1+1, result.filesDeleted, "the per-backup file and the SST of the parent")
	assert.NotContains(t, store.Keys(), parent.SSTs[0].Key)
}
