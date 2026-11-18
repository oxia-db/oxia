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
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commontime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
)

// runBackup takes a backup of the shard of the host at now, on top of the
// record, and returns the record of the backup.
func runBackup(t *testing.T, store blob.Store, host *fakeHost, shard int64, record *Record, p policy,
	now time.Time) (*LastBackup, error) {
	t.Helper()
	checkpoint, err := host.Checkpoint(shard)
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()
	return checkpointJob(store, shard, checkpoint, record, now).run(context.Background())
}

// checkpointJob builds a job over a checkpoint of a shard of namespace ns.
func checkpointJob(store blob.Store, shard int64, checkpoint *database.Checkpoint, record *Record,
	now time.Time) *backupJob {
	p := testPolicy()
	return &backupJob{
		store:           store,
		path:            backup.ShardPath{Namespace: testNamespace, Shard: shard},
		checkpoint:      checkpoint,
		record:          record,
		policy:          p,
		hashRange:       backup.FullRange,
		namespaceConfig: json.RawMessage(`{"name":"ns"}`),
		now:             now,
		clock:           commontime.SystemClock,
		reuseWindow:     p.reuseWindow(),
		uploadWorkers:   2,
	}
}

// manifestNames returns the names of the files of a manifest, SSTs first.
func manifestNames(m *backup.Manifest) []string {
	var names []string
	for _, f := range slices.Concat(m.SSTs, m.Files) {
		names = append(names, f.Name)
	}
	return names
}

func TestBackupJobRunUploadsEveryFileOfAFirstBackup(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 2)
	files := host.files(1)
	ssts := sstNames(files)
	require.Len(t, ssts, 1)
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)

	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), now)
	require.NoError(t, err)
	assert.Equal(t, "2026-10-03T14-00-00Z-abcd", last.ID)
	assert.Equal(t, now, last.Time)
	assert.EqualValues(t, 1, last.CommitOffset)
	assert.EqualValues(t, 1, last.Term)
	assert.Equal(t, last.SizeBytes, last.UploadedBytes, "nothing is compressed")
	expected := []string{"ns/shard-1/backups/2026-10-03T14-00-00Z-abcd.json"}
	for name := range files {
		if backup.IsSSTName(name) {
			expected = append(expected, "ns/shard-1/sst/abcd-"+name)
		} else {
			expected = append(expected, "ns/shard-1/backups/2026-10-03T14-00-00Z-abcd/"+name)
		}
	}
	slices.Sort(expected)
	assert.Equal(t, expected, store.Keys())

	m, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/2026-10-03T14-00-00Z-abcd.json")
	require.NoError(t, err)
	assert.Equal(t, "abcd", m.Lineage)
	assert.Equal(t, backup.FullRange, m.Range)
	assert.JSONEq(t, `{"name":"ns"}`, string(m.NamespaceConfig))
	assert.EqualValues(t, 1, m.CommitOffset)
	require.Len(t, m.SSTs, 1)
	assert.Equal(t, backup.File{Name: ssts[0], Key: "ns/shard-1/sst/abcd-" + ssts[0], Size: int64(len(files[ssts[0]])),
		CRC32C: crc32.Checksum(files[ssts[0]], crc32cTable)}, m.SSTs[0])
	assert.Len(t, m.Files, len(files)-1)
	assert.Equal(t, m.SSTs, last.SSTs)
	assert.Equal(t, files[ssts[0]], readObject(t.Context(), t, store, "ns/shard-1/sst/abcd-"+ssts[0]))
	var size int64
	for _, f := range slices.Concat(m.SSTs, m.Files) {
		size += f.Size
		assert.Len(t, readObject(t.Context(), t, store, f.Key), int(f.Size), f.Name)
	}
	assert.Equal(t, size, last.SizeBytes)
}
func TestBackupJobRunHoldsTheFilesOfTheCheckpointAlone(t *testing.T) {
	// Reading the commit offset of the checkpoint opens it with Pebble,
	// which adds its LOCK file to the directory: the backup lists the
	// checkpoint before that, and holds what the checkpoint held
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	files := host.files(1)
	require.NotContains(t, files, "LOCK")

	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	require.NoError(t, err)
	m, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/"+last.ID+".json")
	require.NoError(t, err)
	assert.ElementsMatch(t, slices.Collect(maps.Keys(files)), manifestNames(m))
}

func TestBackupJobRunSharesTheSSTsOfTheLastBackup(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 2)
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	first, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), now)
	require.NoError(t, err)
	require.Len(t, first.SSTs, 1)
	uploadsBefore := len(store.Keys())

	// Unchanged: the SST of the last backup is listed, not uploaded
	second, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: first}, testPolicy(),
		now.Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, "2026-10-03T15-00-00Z-abcd", second.ID)
	assert.Equal(t, first.SSTs, second.SSTs, "the shared SST keeps its entry")
	assert.Equal(t, second.SizeBytes-first.SSTs[0].Size, second.UploadedBytes, "the per-backup files alone")
	m, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/2026-10-03T15-00-00Z-abcd.json")
	require.NoError(t, err)
	assert.Len(t, store.Keys(), uploadsBefore+1+len(m.Files), "one manifest and the per-backup files more")

	// Changed: the SSTs the checkpoint holds no more are left out, the new
	// ones are uploaded
	host.write(1, []byte("more"))
	third, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: second}, testPolicy(),
		now.Add(2*time.Hour))
	require.NoError(t, err)
	assert.NotEqual(t, second.SSTs, third.SSTs)
	var shared int64
	for _, f := range third.SSTs {
		if slices.Contains(second.SSTs, f) {
			shared += f.Size
		}
	}
	assert.Equal(t, third.SizeBytes-shared, third.UploadedBytes, "the SSTs not shared and the per-backup files")
}
func TestBackupJobRunUploadsEverythingWhenTheLastBackupIsTooOldToReuse(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	p := testPolicy()
	first, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, p, now)
	require.NoError(t, err)

	tooOld := now.Add(p.reuseWindow())
	second, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: first}, p, tooOld)
	require.NoError(t, err)
	assert.Equal(t, second.SizeBytes, second.UploadedBytes, "every SST is uploaded again")
	secondID, err := backup.ParseID(second.ID)
	require.NoError(t, err)
	assert.NotEqual(t, "abcd", secondID.Lineage, "under a new lineage: no SST key is written twice")

	justInTime := now.Add(p.reuseWindow() - time.Second)
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
	host.addShard(1, 1)
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	first, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), now)
	require.NoError(t, err)
	second, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: first}, testPolicy(),
		now.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, first.SSTs, second.SSTs)

	// The record lists the SST of the checkpoint with another size: it is
	// not the file the checkpoint holds, which is uploaded
	last := *second
	last.SSTs = slices.Clone(second.SSTs)
	last.SSTs[0].Size++
	third, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: &last}, testPolicy(),
		now.Add(2*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, third.SizeBytes, third.UploadedBytes, "the SST and the per-backup files")
	assert.Greater(t, third.UploadedBytes, second.UploadedBytes)
	assert.Equal(t, second.SSTs[0], third.SSTs[0], "with its real size")
}
func TestBackupJobRunTakesTheNextSecondAfterTheLastBackup(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	first, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), now)
	require.NoError(t, err)
	second, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd", LastBackup: first}, testPolicy(), now)
	require.NoError(t, err)
	assert.Equal(t, "2026-10-03T14-00-01Z-abcd", second.ID)
	assert.Equal(t, now, second.Time, "the ID moves to the next second, the backup is still at its start")
}

func TestBackupJobRunWritesNoManifestWhenAnUploadFails(t *testing.T) {
	store := blob.NewMemory()
	store.FailPut(func(key string) error {
		if strings.HasSuffix(key, ".sst") {
			return assert.AnError
		}
		return nil
	})
	host := newFakeHost(t)
	host.addShard(1, 3)
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
	host.addShard(1, 1)
	_, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	assert.ErrorIs(t, err, assert.AnError)
	assert.Len(t, store.Keys(), len(host.files(1)), "the SST and the per-backup files are orphans")
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
	assert.Equal(t, 6*24*time.Hour, p.reuseWindow())
	p.retention = 4 * time.Hour
	assert.Equal(t, 3*time.Hour, p.reuseWindow(), "the margin is at most a quarter of the retention")
}

func TestBackupJobRunFailsWhenCanceled(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 3)
	checkpoint, err := host.Checkpoint(1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = checkpointJob(store, 1, checkpoint, &Record{Lineage: "abcd"}, time.Now()).run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, manifestKeys(store))
}

func TestCompareNamesOrdersPebbleFileNamesNumerically(t *testing.T) {
	assert.Equal(t, 0, compareNames("000010.sst", "000010.sst"))
	assert.Negative(t, compareNames("000009.sst", "000010.sst"))
	assert.Positive(t, compareNames("000010.sst", "000009.sst"))
}

func TestBackupJobRunReportsACheckpointItCannotList(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	checkpoint, err := host.Checkpoint(1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()
	require.NoError(t, os.RemoveAll(checkpoint.BasePath()))
	_, err = checkpointJob(store, 1, checkpoint, &Record{Lineage: "abcd"}, time.Now()).run(context.Background())
	assert.ErrorContains(t, err, "list the checkpoint")
	assert.Empty(t, store.Keys())
}

func TestBackupJobRunSkipsTheDirectoriesOfACheckpoint(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	checkpoint, err := host.Checkpoint(1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()
	require.NoError(t, os.Mkdir(filepath.Join(checkpoint.BasePath(), "lost+found"), 0700))
	last, err := checkpointJob(store, 1, checkpoint, &Record{Lineage: "abcd"}, time.Now()).run(context.Background())
	require.NoError(t, err)
	assert.Len(t, last.SSTs, 1)
	for _, key := range store.Keys() {
		assert.NotContains(t, key, "lost+found")
	}
}

func TestBackupJobRunReportsAFileItCannotOpen(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	checkpoint, err := host.Checkpoint(1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, checkpoint.Close()) }()
	// A marker file of Pebble, which reading the checkpoint never opens
	entries, err := os.ReadDir(checkpoint.BasePath())
	require.NoError(t, err)
	markers := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "marker.") {
			require.NoError(t, os.Chmod(filepath.Join(checkpoint.BasePath(), entry.Name()), 0000))
			markers++
		}
	}
	require.Positive(t, markers)
	_, err = checkpointJob(store, 1, checkpoint, &Record{Lineage: "abcd"}, time.Now()).run(context.Background())
	assert.ErrorContains(t, err, "upload marker.")
	assert.ErrorContains(t, err, "permission denied")
	assert.Empty(t, manifestKeys(store))
}
