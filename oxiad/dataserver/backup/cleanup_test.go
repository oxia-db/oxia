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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commontime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

func TestCleanupJobRunExpiresBackupsAndTheirFiles(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	backups := backupSeries(t, store, host, 1, start, 24*time.Hour, 5)
	expired, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/"+backups[0].ID+".json")
	require.NoError(t, err)
	now := start.Add(6 * 24 * time.Hour)
	// Day 0 and day 1 are older than the 4-day retention; day 1 covers day 0
	j := &cleanupJob{store: store, namespace: "ns", retention: 4 * 24 * time.Hour, grace: gracePeriod, now: now,
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
	assert.Len(t, expired.Files, result.filesDeleted, "the per-backup files of the expired backup")
	assert.Contains(t, store.Keys(), backups[0].SSTs[0].Key)

	again, err := j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, cleanupResult{}, again, "a second run has nothing to do")
}

func TestCleanupJobRunDeletesUnreferencedSSTsAfterTheGracePeriod(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	backupSeries(t, store, host, 1, start, time.Hour, 2)
	now := start.Add(3 * time.Hour)
	store.SetClock(func() time.Time { return now })
	// An SST of a backup that never wrote its manifest, just uploaded, and
	// one uploaded two days ago
	require.NoError(t, store.Put(t.Context(), "ns/shard-1/sst/other-000009.sst", strings.NewReader("orphan")))
	require.NoError(t, store.Put(t.Context(), "ns/shard-1/sst/other-000008.sst", strings.NewReader("old orphan")))
	store.Touch("ns/shard-1/sst/other-000008.sst", now.Add(-2*gracePeriod))
	// The files of a backup that never wrote its manifest, from two days ago
	oldID := backup.NewID(now.Add(-2*gracePeriod), "other")
	require.NoError(t, store.Put(t.Context(), "ns/shard-1/backups/"+oldID.String()+"/MANIFEST-000001",
		strings.NewReader("stale")))
	newID := backup.NewID(now, "other")
	require.NoError(t, store.Put(t.Context(), "ns/shard-1/backups/"+newID.String()+"/MANIFEST-000001",
		strings.NewReader("in progress")))

	j := &cleanupJob{store: store, namespace: "ns", retention: 7 * 24 * time.Hour, grace: gracePeriod, now: now,
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
		host.addShard(shard, 1)
		store.SetClock(func() time.Time { return at })
		checkpoint, err := host.Checkpoint(shard)
		require.NoError(t, err)
		defer func() { assert.NoError(t, checkpoint.Close()) }()
		j := &backupJob{store: store, path: backup.ShardPath{Namespace: "ns", Shard: shard}, checkpoint: checkpoint,
			record: &Record{Lineage: lineage}, policy: testPolicy(), hashRange: r, now: at, clock: commontime.SystemClock,
			reuseWindow: time.Hour, uploadWorkers: 1}
		last, err := j.run(context.Background())
		require.NoError(t, err)
		return last
	}
	left := backup.Range{Min: 0, Max: 1<<31 - 1}
	right := backup.Range{Min: 1 << 31, Max: backup.FullRange.Max}
	parent := write(1, backup.FullRange, "aaaaaaaaaaaaaaaa", start)
	parentManifest, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/"+parent.ID+".json")
	require.NoError(t, err)
	write(2, left, "bbbbbbbbbbbbbbbb", start.Add(time.Hour))
	now := start.Add(10 * 24 * time.Hour)
	j := &cleanupJob{store: store, namespace: "ns", retention: 7 * 24 * time.Hour, grace: gracePeriod, now: now,
		log: slog.Default()}
	result, err := j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, result.backupsExpired, "the right half is only covered by the parent")

	write(3, right, "cccccccccccccccc", start.Add(2*time.Hour))
	result, err = j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.backupsExpired)
	assert.NotContains(t, manifestKeys(store), "ns/shard-1/backups/"+parent.ID+".json")
	assert.Equal(t, len(parentManifest.Files)+len(parent.SSTs), result.filesDeleted,
		"the per-backup files and the SST of the parent")
	assert.NotContains(t, store.Keys(), parent.SSTs[0].Key)
}

// failingStore is a store that fails the operations its hooks reject, and
// counts its deletions.
type failingStore struct {
	blob.Store
	failList    func(prefix string) error
	failDirs    func(prefix string) error
	failGet     func(key string) error
	failDelete  func(keys []string) error
	deleteCalls int
}

func (s *failingStore) List(ctx context.Context, prefix string, visit func(blob.Object) error) error {
	if s.failList != nil {
		if err := s.failList(prefix); err != nil {
			return err
		}
	}
	return s.Store.List(ctx, prefix, visit)
}

func (s *failingStore) ListDirs(ctx context.Context, prefix string) ([]string, error) {
	if s.failDirs != nil {
		if err := s.failDirs(prefix); err != nil {
			return nil, err
		}
	}
	return s.Store.ListDirs(ctx, prefix)
}

func (s *failingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if s.failGet != nil {
		if err := s.failGet(key); err != nil {
			return nil, err
		}
	}
	return s.Store.Get(ctx, key)
}

func (s *failingStore) Delete(ctx context.Context, keys []string) error {
	s.deleteCalls++
	if s.failDelete != nil {
		if err := s.failDelete(keys); err != nil {
			return err
		}
	}
	return s.Store.Delete(ctx, keys)
}

// orphanedShard puts a shard with an expired backup and a batch and one of
// unreferenced SSTs older than the grace period in the store, and returns
// the job that cleans it with the given store, and how many per-backup
// files the expired backup holds.
func orphanedShard(t *testing.T, memory *blob.Memory, store blob.Store) (*cleanupJob, int) {
	t.Helper()
	host := newFakeHost(t)
	host.addShard(1, 1)
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	backups := backupSeries(t, memory, host, 1, start, 24*time.Hour, 3)
	expired, err := backup.ReadManifest(t.Context(), memory, "ns/shard-1/backups/"+backups[0].ID+".json")
	require.NoError(t, err)
	now := start.Add(6 * 24 * time.Hour)
	for i := range sweepBatchSize + 1 {
		key := fmt.Sprintf("ns/shard-1/sst/orphan-%06d.sst", i)
		require.NoError(t, memory.Put(t.Context(), key, strings.NewReader("x")))
		memory.Touch(key, now.Add(-2*gracePeriod))
	}
	return &cleanupJob{store: store, namespace: "ns", retention: 4 * 24 * time.Hour, grace: gracePeriod,
		now: now, log: slog.Default()}, len(expired.Files)
}

func TestCleanupJobRunDeletesTheUnreferencedFilesABatchAtATime(t *testing.T) {
	memory := blob.NewMemory()
	store := &failingStore{Store: memory}
	j, expiredFiles := orphanedShard(t, memory, store)
	result, err := j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, result.backupsExpired)
	// The orphans and the per-backup files of the expired backup, a batch
	// at a time, after the deletion of the manifest: a full batch of
	// orphans, then the orphan left with the per-backup files
	assert.Equal(t, sweepBatchSize+1+expiredFiles, result.filesDeleted)
	assert.Equal(t, 1+2, store.deleteCalls)
	for _, key := range memory.Keys() {
		assert.NotContains(t, key, "orphan")
	}
}

func TestCleanupJobRunReportsTheErrorsOfTheStore(t *testing.T) {
	fail := errors.New("store unavailable")
	for name, test := range map[string]struct {
		arrange func(s *failingStore)
		want    string
	}{
		"listing the shards": {func(s *failingStore) {
			s.failDirs = func(prefix string) error { return fail }
		}, "list shards"},
		"listing the manifests": {func(s *failingStore) {
			s.failList = func(prefix string) error {
				if strings.HasSuffix(prefix, "/backups/") {
					return fail
				}
				return nil
			}
		}, "list manifests"},
		"expiring the manifests": {func(s *failingStore) {
			s.failDelete = func(keys []string) error {
				if strings.HasSuffix(keys[0], ".json") {
					return fail
				}
				return nil
			}
		}, "delete the expired manifests"},
		"reading a live manifest": {func(s *failingStore) {
			// The manifests are read once to list them, and again by the sweep
			gets := 0
			s.failGet = func(key string) error {
				gets++
				if gets > 3 {
					return fail
				}
				return nil
			}
		}, "read manifest"},
		"listing the SSTs": {func(s *failingStore) {
			s.failList = func(prefix string) error {
				if strings.HasSuffix(prefix, "/sst/") {
					return fail
				}
				return nil
			}
		}, "list the SSTs"},
		"listing the backup directories": {func(s *failingStore) {
			s.failDirs = func(prefix string) error {
				if strings.HasSuffix(prefix, "/backups/") {
					return fail
				}
				return nil
			}
		}, "list the backups of"},
		"listing the files of a backup": {func(s *failingStore) {
			s.failList = func(prefix string) error {
				if strings.Contains(prefix, "/backups/2026") {
					return fail
				}
				return nil
			}
		}, "list the files of"},
		"deleting the unreferenced files": {func(s *failingStore) {
			s.failDelete = func(keys []string) error {
				if strings.Contains(keys[0], "orphan") {
					return fail
				}
				return nil
			}
		}, "delete the unreferenced files"},
	} {
		t.Run(name, func(t *testing.T) {
			memory := blob.NewMemory()
			store := &failingStore{Store: memory}
			j, _ := orphanedShard(t, memory, store)
			test.arrange(store)
			_, err := j.run(context.Background())
			require.ErrorIs(t, err, fail)
			assert.ErrorContains(t, err, test.want)
		})
	}
}
