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
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/dataserver/database"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

// runBackup takes a backup of the shard of the host at the clock's time, on
// top of the record, and returns the record of the backup.
func runBackup(t *testing.T, store blob.Store, host *fakeHost, shard int64, record *Record, p policy,
	now time.Time) (*LastBackup, error) {
	t.Helper()
	checkpoint, err := host.Checkpoint(shard)
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()
	commitOffset, term, err := host.readCheckpoint(checkpoint.BasePath())
	require.NoError(t, err)
	j := &backupJob{
		store:           store,
		path:            backup.ShardPath{Namespace: "ns", Shard: shard},
		checkpoint:      checkpoint,
		commitOffset:    commitOffset,
		term:            term,
		record:          record,
		policy:          p,
		hashRange:       backup.FullRange,
		namespaceConfig: json.RawMessage(`{"name":"ns"}`),
		now:             now,
		clock:           time.Now,
		reuseWindow:     p.reuseWindow(DefaultGracePeriod),
		uploadWorkers:   2,
	}
	return j.run(context.Background())
}

func TestBackupJobRunUploadsEveryFileOfAFirstBackup(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, sstFiles(2))
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)

	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), now)
	require.NoError(t, err)
	assert.Equal(t, "2026-10-03T14-00-00Z-abcd", last.ID)
	assert.Equal(t, now, last.Time)
	assert.EqualValues(t, 5, last.CommitOffset)
	assert.EqualValues(t, 1, last.Term)
	assert.EqualValues(t, 1000+2000+8+7+7, last.SizeBytes)
	assert.Equal(t, last.SizeBytes, last.UploadedBytes, "nothing is compressed")
	assert.Equal(t, []string{
		"ns/shard-1/backups/2026-10-03T14-00-00Z-abcd.json",
		"ns/shard-1/backups/2026-10-03T14-00-00Z-abcd/MANIFEST-000001",
		"ns/shard-1/backups/2026-10-03T14-00-00Z-abcd/OPTIONS-000003",
		"ns/shard-1/backups/2026-10-03T14-00-00Z-abcd/marker.manifest.000001",
		"ns/shard-1/backups/2026-10-03T14-00-00Z-abcd/oxia-key-encoding-format",
		"ns/shard-1/sst/abcd-000001.sst",
		"ns/shard-1/sst/abcd-000002.sst",
	}, store.Keys())

	m, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/2026-10-03T14-00-00Z-abcd.json")
	require.NoError(t, err)
	assert.Equal(t, "abcd", m.Lineage)
	assert.Equal(t, backup.FullRange, m.Range)
	assert.JSONEq(t, `{"name":"ns"}`, string(m.NamespaceConfig))
	assert.EqualValues(t, 5, m.CommitOffset)
	require.Len(t, m.SSTs, 2)
	assert.Equal(t, backup.File{Name: "000001.sst", Key: "ns/shard-1/sst/abcd-000001.sst", Size: 1000,
		CRC32C: crc32.Checksum(host.shards[1].files["000001.sst"], crc32cTable)}, m.SSTs[0])
	assert.Len(t, m.Files, 4)
	assert.Equal(t, m.SSTs, last.SSTs)
	assert.Equal(t, host.shards[1].files["000002.sst"], readObject(t.Context(), t, store, "ns/shard-1/sst/abcd-000002.sst"))
}

func TestBackupJobRunUploadsOnlyTheNewSSTs(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, sstFiles(2))
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	first, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), now)
	require.NoError(t, err)
	uploadsBefore := len(store.Keys())

	host.write(1, "000003.sst", []byte("new sst content"))
	second, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: first}, testPolicy(),
		now.Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, "2026-10-03T15-00-00Z-abcd", second.ID)
	assert.EqualValues(t, 15+8+7+7, second.UploadedBytes, "the new SST and the per-backup files")
	assert.Len(t, second.SSTs, 3)
	assert.Len(t, store.Keys(), uploadsBefore+1+1+4, "one manifest, one SST, four per-backup files more")

	m, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/2026-10-03T15-00-00Z-abcd.json")
	require.NoError(t, err)
	names := make([]string, 0, 3)
	for _, f := range m.SSTs {
		names = append(names, f.Name)
	}
	assert.Equal(t, []string{"000001.sst", "000002.sst", "000003.sst"}, names)
	assert.Equal(t, first.SSTs[0], m.SSTs[0], "the shared SST keeps its entry")
}

func TestBackupJobRunUploadsEverythingWhenTheLastBackupIsTooOldToReuse(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, sstFiles(2))
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	p := testPolicy()
	first, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, p, now)
	require.NoError(t, err)

	tooOld := now.Add(p.reuseWindow(DefaultGracePeriod))
	second, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: first}, p, tooOld)
	require.NoError(t, err)
	assert.Equal(t, second.SizeBytes, second.UploadedBytes, "every SST is uploaded again")
	secondID, err := backup.ParseID(second.ID)
	require.NoError(t, err)
	assert.NotEqual(t, "abcd", secondID.Lineage, "under a new lineage: no SST key is written twice")

	justInTime := now.Add(p.reuseWindow(DefaultGracePeriod) - time.Second)
	third, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: first}, p, justInTime)
	require.NoError(t, err)
	assert.Less(t, third.UploadedBytes, third.SizeBytes, "the SSTs are shared")
	thirdID, err := backup.ParseID(third.ID)
	require.NoError(t, err)
	assert.Equal(t, "abcd", thirdID.Lineage)
}

func TestBackupJobRunReusesAnSSTOnlyWithTheSameSize(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, sstFiles(2))
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	first, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), now)
	require.NoError(t, err)

	host.write(1, "000001.sst", []byte("rewritten"))
	second, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: first}, testPolicy(),
		now.Add(time.Hour))
	require.NoError(t, err)
	assert.EqualValues(t, 9+8+7+7, second.UploadedBytes)
	assert.Equal(t, []byte("rewritten"), readObject(t.Context(), t, store, "ns/shard-1/sst/abcd-000001.sst"))
}

func TestBackupJobRunTakesTheNextSecondAfterTheLastBackup(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	first, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), now)
	require.NoError(t, err)
	second, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: first}, testPolicy(), now)
	require.NoError(t, err)
	assert.Equal(t, "2026-10-03T14-00-01Z-abcd", second.ID)
	assert.Equal(t, now.Add(time.Second), second.Time)
}

func TestBackupJobRunWritesNoManifestWhenAnUploadFails(t *testing.T) {
	store := blob.NewMemory()
	store.FailPut(func(key string) error {
		if strings.HasSuffix(key, "000002.sst") {
			return assert.AnError
		}
		return nil
	})
	host := newFakeHost(t)
	host.addShard(1, sstFiles(3))
	_, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	assert.ErrorIs(t, err, assert.AnError)
	for _, key := range store.Keys() {
		assert.NotContains(t, key, ".json")
	}
}

func TestBackupJobRunWritesNoManifestWhenItsWriteFails(t *testing.T) {
	store := blob.NewMemory()
	store.FailPut(func(key string) error {
		if strings.HasSuffix(key, ".json") {
			return assert.AnError
		}
		return nil
	})
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	_, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	assert.ErrorIs(t, err, assert.AnError)
	assert.Len(t, store.Keys(), 5, "the SST and the per-backup files are orphans")
}

func TestBackupNeeded(t *testing.T) {
	p := testPolicy()
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	last := func(age time.Duration, commitOffset int64) *LastBackup {
		return &LastBackup{Time: now.Add(-age), CommitOffset: commitOffset}
	}
	assert.True(t, backupNeeded(nil, 5, p, now), "no backup yet")
	assert.False(t, backupNeeded(last(30*time.Minute, 4), 5, p, now), "younger than the interval")
	assert.True(t, backupNeeded(last(time.Hour, 4), 5, p, now), "changed since")
	assert.False(t, backupNeeded(last(time.Hour, 5), 5, p, now), "unchanged")
	assert.False(t, backupNeeded(last(p.retention-p.interval-time.Second, 5), 5, p, now), "unchanged, inside the window")
	assert.True(t, backupNeeded(last(p.retention-p.interval, 5), 5, p, now), "unchanged, about to leave the window")
}

func TestPolicyReuseWindow(t *testing.T) {
	p := testPolicy()
	assert.Equal(t, 6*24*time.Hour, p.reuseWindow(24*time.Hour))
	p.retention = 4 * time.Hour
	assert.Equal(t, 3*time.Hour, p.reuseWindow(24*time.Hour), "the margin is at most a quarter of the retention")
}

func TestBackupJobRunFailsWhenCanceled(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, sstFiles(3))
	checkpoint, err := host.Checkpoint(1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	j := &backupJob{store: store, path: backup.ShardPath{Namespace: "ns", Shard: 1}, checkpoint: checkpoint,
		record: &Record{Lineage: "abcd"}, policy: testPolicy(), hashRange: backup.FullRange, now: time.Now(),
		clock: time.Now, reuseWindow: time.Hour, uploadWorkers: 2}
	_, err = j.run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, manifestKeys(store))
}

func TestCompareNamesOrdersPebbleFileNamesNumerically(t *testing.T) {
	assert.Equal(t, 0, compareNames("000010.sst", "000010.sst"))
	assert.Negative(t, compareNames("000009.sst", "000010.sst"))
	assert.Positive(t, compareNames("000010.sst", "000009.sst"))
}

// checkpointJob builds a job over a directory standing in for a checkpoint.
func checkpointJob(t *testing.T, store blob.Store, dir string) *backupJob {
	t.Helper()
	return &backupJob{
		store:           store,
		path:            backup.ShardPath{Namespace: "ns", Shard: 1},
		checkpoint:      &database.Checkpoint{Snapshot: &fakeSnapshot{dir: dir}},
		record:          &Record{Lineage: "abcd"},
		policy:          testPolicy(),
		hashRange:       backup.FullRange,
		namespaceConfig: json.RawMessage(`{"name":"ns"}`),
		now:             time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC),
		clock:           time.Now,
		reuseWindow:     time.Hour,
		uploadWorkers:   2,
	}
}

func TestBackupJobRunReportsACheckpointItCannotList(t *testing.T) {
	store := blob.NewMemory()
	_, err := checkpointJob(t, store, filepath.Join(t.TempDir(), "gone")).run(context.Background())
	assert.ErrorContains(t, err, "list the checkpoint")
	assert.Empty(t, store.Keys())
}

func TestBackupJobRunSkipsTheDirectoriesOfACheckpoint(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.sst"), []byte("sst"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MANIFEST-000001"), []byte("m"), 0600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "lost+found"), 0700))
	store := blob.NewMemory()
	last, err := checkpointJob(t, store, dir).run(context.Background())
	require.NoError(t, err)
	assert.Len(t, last.SSTs, 1)
	for _, key := range store.Keys() {
		assert.NotContains(t, key, "lost+found")
	}
}

func TestBackupJobRunReportsAFileItCannotOpen(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "000001.sst"), []byte("sst"), 0000))
	store := blob.NewMemory()
	_, err := checkpointJob(t, store, dir).run(context.Background())
	assert.ErrorContains(t, err, "permission denied")
	assert.Empty(t, store.Keys())
}
