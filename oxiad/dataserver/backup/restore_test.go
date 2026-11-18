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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
	"github.com/oxia-db/oxia/oxiad/common/errs"
)

func newRestoreJob(t *testing.T, store blob.Store, last *LastBackup, hashRange *proto.HashRange) (*restoreJob, *fakeInstaller) {
	t.Helper()
	id, err := backup.ParseID(last.ID)
	require.NoError(t, err)
	installer := &fakeInstaller{files: map[string][]byte{}, commitOffset: 42, term: 3}
	return &restoreJob{
		store:     store,
		source:    backup.ShardPath{Namespace: "ns", Shard: 1},
		backupID:  id,
		hashRange: hashRange,
		installer: installer,
	}, installer
}

func TestRestoreJobRunInstallsTheFilesOfTheBackup(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 3)
	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	require.NoError(t, err)

	j, installer := newRestoreJob(t, store, last, &proto.HashRange{Min: 0, Max: 10})
	result, err := j.run(context.Background())
	require.NoError(t, err)
	assert.Equal(t, restoreResult{commitOffset: 42, term: 3}, result)
	m, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/"+last.ID+".json")
	require.NoError(t, err)
	assert.Equal(t, backupFiles(t, store, m), installer.files)
	assert.True(t, installer.completed)
	assert.Equal(t, &proto.HashRange{Min: 0, Max: 10}, installer.hashRange)
}

func TestRestoreJobRunRejectsACorruptedFile(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), last.SSTs[0].Key, strings.NewReader(strings.Repeat("y", int(last.SSTs[0].Size)))))

	j, installer := newRestoreJob(t, store, last, fullRange())
	_, err = j.run(context.Background())
	assert.ErrorContains(t, err, "checksum")
	assert.False(t, installer.completed)
}

func TestRestoreJobRunFailsOnAMissingBackup(t *testing.T) {
	store := blob.NewMemory()
	j, installer := newRestoreJob(t, store, &LastBackup{ID: "2026-10-03T14-00-00Z-abcd"}, fullRange())
	_, err := j.run(context.Background())
	assert.ErrorIs(t, err, errs.ErrNotFound)
	assert.False(t, installer.completed)
}

func TestRestoreJobRunRejectsAManifestOfAnotherShard(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	require.NoError(t, err)
	// The manifest of shard 1 copied under shard 2's prefix
	data := readObject(t.Context(), t, store, "ns/shard-1/backups/"+last.ID+".json")
	require.NoError(t, store.Put(t.Context(), "ns/shard-2/backups/"+last.ID+".json", strings.NewReader(string(data))))

	j, _ := newRestoreJob(t, store, last, fullRange())
	j.source.Shard = 2
	_, err = j.run(context.Background())
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
}

func TestRestoreJobRunFailsOnAMissingFile(t *testing.T) {
	for name, pick := range map[string]func(m *backup.Manifest) backup.File{
		"sst":             func(m *backup.Manifest) backup.File { return m.SSTs[0] },
		"per-backup file": func(m *backup.Manifest) backup.File { return m.Files[0] },
	} {
		t.Run(name, func(t *testing.T) {
			store := blob.NewMemory()
			host := newFakeHost(t)
			host.addShard(1, 1)
			last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
			require.NoError(t, err)
			manifest, err := backup.ReadManifest(t.Context(), store, "ns/shard-1/backups/"+last.ID+".json")
			require.NoError(t, err)
			require.NoError(t, store.Delete(t.Context(), []string{pick(manifest).Key}))

			j, installer := newRestoreJob(t, store, last, fullRange())
			_, err = j.run(context.Background())
			assert.ErrorIs(t, err, errs.ErrNotFound)
			assert.False(t, installer.completed)
		})
	}
}

func TestRestoreJobRunReportsAFileTheInstallerRefuses(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	require.NoError(t, err)

	j, installer := newRestoreJob(t, store, last, fullRange())
	errDiskFull := errors.New("disk full")
	installer.addFileErr = errDiskFull
	_, err = j.run(context.Background())
	assert.ErrorIs(t, err, errDiskFull)
	assert.False(t, installer.completed)
}

func TestRestoreJobRunReportsAnInstallationThatFails(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, 1)
	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	require.NoError(t, err)

	j, installer := newRestoreJob(t, store, last, fullRange())
	errCorrupted := errors.New("database corrupted")
	installer.completeErr = errCorrupted
	_, err = j.run(context.Background())
	assert.ErrorIs(t, err, errCorrupted)
	assert.ErrorContains(t, err, "install the database")
}
