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

	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

// backupSeries takes a backup of the shard every interval from start, count
// times, with the store stamping the objects at the backup's time.
func backupSeries(t *testing.T, store *blob.Memory, host *fakeHost, shard int64, start time.Time,
	interval time.Duration, count int) []*LastBackup {
	t.Helper()
	record := &Record{Lineage: "l" + strings.Repeat("0", 14) + string(rune('a'+shard))}
	var backups []*LastBackup
	for i := range count {
		now := start.Add(time.Duration(i) * interval)
		store.SetClock(func() time.Time { return now })
		host.write(shard, "00000"+string(rune('1'+i))+".sst", []byte(strings.Repeat("x", 100*(i+1))))
		last, err := runBackup(t, store, host, shard, record, testPolicy(), now)
		require.NoError(t, err)
		record.LastBackup = last
		backups = append(backups, last)
	}
	return backups
}

func manifestKeys(store *blob.Memory) []string {
	var keys []string
	for _, key := range store.Keys() {
		if strings.HasSuffix(key, ".json") {
			keys = append(keys, key)
		}
	}
	return keys
}

func TestListBackupsReturnsTheBackupsOfANamespaceOrShard(t *testing.T) {
	store := blob.NewMemory()
	host := newFakeHost(t)
	host.addShard(1, map[string][]byte{"MANIFEST-000001": []byte("m")})
	host.addShard(2, map[string][]byte{"MANIFEST-000001": []byte("m")})
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	backupSeries(t, store, host, 1, start, time.Hour, 2)
	backupSeries(t, store, host, 2, start, time.Hour, 1)

	all, err := listBackups(context.Background(), store, "ns", nil)
	require.NoError(t, err)
	assert.Len(t, all, 3)
	shard := int64(2)
	one, err := listBackups(context.Background(), store, "ns", &shard)
	require.NoError(t, err)
	require.Len(t, one, 1)
	assert.EqualValues(t, 2, one[0].Shard)
	none, err := listBackups(context.Background(), store, "other", nil)
	require.NoError(t, err)
	assert.Empty(t, none)
}
