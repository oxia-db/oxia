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

// Package backup is the part of the shard backups that runs on the data
// servers: it takes the backups of the shards the server hosts, expires the
// old ones, lists them, and installs the backups of a restore, on the
// coordinator's requests through the OxiaBackup service. See
// docs/backups.md.
package backup

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/oxia-db/oxia/common/commonio"
	"github.com/oxia-db/oxia/oxiad/backup"
)

// recordFileName is the file, next to the database of a shard, that holds
// the lineage of the database and the record of its last backup. It goes
// with the database: a snapshot install, a restore or a deletion of the
// shard wipes it, and the next backup starts a new lineage.
const recordFileName = "oxia-backup.json"

// Record is the backup state a data server keeps for a shard's database.
type Record struct {
	Lineage string `json:"lineage"`
	// Destination is where the last backup went. The record of another
	// destination is the record of no backup: the SSTs it lists are not
	// there.
	Destination string      `json:"destination,omitempty"`
	LastBackup  *LastBackup `json:"last_backup,omitempty"`
}

// LastBackup is the record of the last backup of a database, which the next
// backup shares its SSTs with, and which answers the coordinator's requests
// without reading the object store.
type LastBackup struct {
	ID           string    `json:"id"`
	Time         time.Time `json:"time"`
	CommitOffset int64     `json:"commit_offset"`
	Term         int64     `json:"term"`
	// SizeBytes is the size of the files of the backup.
	SizeBytes int64 `json:"size_bytes"`
	// UploadedBytes is what the backup uploaded: the SSTs it didn't share
	// with the previous backup, and the per-backup files.
	UploadedBytes int64 `json:"uploaded_bytes"`
	// DurationMillis is how long the backup took.
	DurationMillis int64 `json:"duration_millis"`
	// SSTs are the SST files of the backup, which the next backup reuses
	// when it still holds them.
	SSTs []backup.File `json:"ssts"`
}

// readRecord returns the record of the database in dir, and false when
// there is none.
func readRecord(dir string) (*Record, bool, error) {
	record := &Record{}
	found, err := commonio.ReadJSONFromFile(filepath.Join(dir, recordFileName), record)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !found || record.Lineage == "" {
		return nil, false, nil
	}
	return record, true, nil
}

// loadRecord returns the record of the database in dir, for the backups to
// destination: a new lineage when there is none, and the record of another
// destination without its last backup. The new lineage, or the new
// destination, is only persisted with the next backup.
func loadRecord(dir string, destination string) (*Record, error) {
	record, found, err := readRecord(dir)
	if err != nil {
		return nil, err
	}
	if !found {
		lineage := backup.NewLineage()
		record = &Record{Lineage: lineage}
	}
	if record.Destination != destination {
		record.LastBackup = nil
		record.Destination = destination
	}
	return record, nil
}

// writeRecord stores the record next to the database in dir, atomically.
func writeRecord(dir string, record *Record) error {
	return commonio.WriteJSONToFile(filepath.Join(dir, recordFileName), record)
}
