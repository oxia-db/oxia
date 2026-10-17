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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadRecordReportsAMissingRecord(t *testing.T) {
	_, found, err := readRecord(t.TempDir())
	require.NoError(t, err)
	assert.False(t, found)
	_, found, err = readRecord(filepath.Join(t.TempDir(), "missing-dir"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestLoadRecordStartsALineageWithoutPersistingIt(t *testing.T) {
	dir := t.TempDir()
	record, err := loadRecord(dir, "mem://a")
	require.NoError(t, err)
	assert.Len(t, record.Lineage, 16)
	assert.Nil(t, record.LastBackup)
	assert.Equal(t, "mem://a", record.Destination)
	_, found, err := readRecord(dir)
	require.NoError(t, err)
	assert.False(t, found, "a lineage without a backup is not persisted")

	again, err := loadRecord(dir, "mem://a")
	require.NoError(t, err)
	assert.NotEqual(t, record.Lineage, again.Lineage)
}

func TestWriteRecordThenLoadRecordRoundTrips(t *testing.T) {
	dir := t.TempDir()
	record := &Record{Lineage: "abcd", Destination: "mem://a", LastBackup: &LastBackup{
		ID:           "2026-10-03T14-00-00Z-abcd",
		Time:         time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC),
		CommitOffset: 12,
		Term:         3,
		SizeBytes:    100,
	}}
	require.NoError(t, writeRecord(dir, record))
	loaded, err := loadRecord(dir, "mem://a")
	require.NoError(t, err)
	assert.Equal(t, record, loaded)
	_, found, err := readRecord(dir)
	require.NoError(t, err)
	assert.True(t, found)
}

func TestLoadRecordDropsTheLastBackupOfAnotherDestination(t *testing.T) {
	dir := t.TempDir()
	record := &Record{Lineage: "abcd", Destination: "mem://a", LastBackup: &LastBackup{
		ID:   "2026-10-03T14-00-00Z-abcd",
		Time: time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC),
	}}
	require.NoError(t, writeRecord(dir, record))
	loaded, err := loadRecord(dir, "mem://b")
	require.NoError(t, err)
	assert.Equal(t, "abcd", loaded.Lineage)
	assert.Equal(t, "mem://b", loaded.Destination)
	assert.Nil(t, loaded.LastBackup, "the SSTs of the last backup are not at the new destination")
}

func TestReadRecordTreatsAnEmptyOrCorruptFileAsNoneOrAnError(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, recordFileName), []byte("{}"), 0600))
	_, found, err := readRecord(dir)
	require.NoError(t, err)
	assert.False(t, found, "a record without a lineage is no record")

	require.NoError(t, os.WriteFile(filepath.Join(dir, recordFileName), []byte("{not json"), 0600))
	_, _, err = readRecord(dir)
	assert.Error(t, err)
	_, err = loadRecord(dir, "mem://a")
	assert.Error(t, err)
}
