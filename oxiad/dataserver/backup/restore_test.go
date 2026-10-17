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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
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
	for _, compression := range []string{"", backup.CompressionZstd} {
		t.Run("compression="+compression, func(t *testing.T) {
			store := blob.NewMemory()
			host := newFakeHost(t)
			host.addShard(1, sstFiles(3))
			p := testPolicy()
			p.compression = compression
			last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, p, time.Now())
			require.NoError(t, err)

			j, installer := newRestoreJob(t, store, last, &proto.HashRange{Min: 0, Max: 10})
			result, err := j.run(context.Background())
			require.NoError(t, err)
			assert.Equal(t, restoreResult{commitOffset: 42, term: 3}, result)
			assert.Equal(t, host.shards[1].files, installer.files)
			assert.True(t, installer.completed)
			assert.Equal(t, &proto.HashRange{Min: 0, Max: 10}, installer.hashRange)
		})
	}
}

func TestRestoreJobRunRejectsACorruptedFile(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), last.SSTs[0].Key, strings.NewReader(strings.Repeat("y", 1000))))

	j, installer := newRestoreJob(t, store, last, fullRange())
	_, err = j.run(context.Background())
	assert.ErrorContains(t, err, "checksum")
	assert.False(t, installer.completed)
}

func TestRestoreJobRunFailsOnAMissingBackup(t *testing.T) {
	store := blob.NewMemory()
	j, installer := newRestoreJob(t, store, &LastBackup{ID: "2026-10-03T14-00-00Z-abcd"}, fullRange())
	_, err := j.run(context.Background())
	assert.ErrorIs(t, err, blob.ErrNotFound)
	assert.False(t, installer.completed)
}

func TestRestoreJobRunRejectsAManifestOfAnotherShard(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, sstFiles(1))
	last, err := runBackup(t, store, host, 1, &Record{Lineage: "abcd"}, testPolicy(), time.Now())
	require.NoError(t, err)
	// The manifest of shard 1 copied under shard 2's prefix
	data := readObject(t.Context(), t, store, "ns/shard-1/backups/"+last.ID+".json")
	require.NoError(t, store.Put(t.Context(), "ns/shard-2/backups/"+last.ID+".json", strings.NewReader(string(data))))

	j, _ := newRestoreJob(t, store, last, fullRange())
	j.source.Shard = 2
	_, err = j.run(context.Background())
	assert.ErrorIs(t, err, backup.ErrInvalidManifest)
}
