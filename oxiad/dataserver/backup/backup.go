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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
)

// backupJob is one backup of a shard: a checkpoint uploaded to the store,
// sharing the SSTs of the last backup of the same lineage.
type backupJob struct {
	store      blob.Store
	path       backup.ShardPath
	checkpoint *database.Checkpoint
	// commitOffset and term are those the files of the checkpoint hold,
	// see database.ReadCheckpoint.
	commitOffset int64
	term         int64
	record       *Record
	policy       policy
	hashRange    backup.Range
	// namespaceConfig is recorded in the manifest, see backup.Manifest.
	namespaceConfig json.RawMessage
	now             time.Time
	clock           func() time.Time
	// reuseWindow is how old the last backup can be for the job to share its
	// SSTs, see policy.reuseWindow.
	reuseWindow   time.Duration
	uploadWorkers int
}

// upload is a file of the checkpoint to upload.
type upload struct {
	name string
	key  string
}

// run takes the backup and returns its record. On an error, the manifest
// is not written: the backup doesn't exist, and the files it uploaded are
// orphans that the cleanup deletes.
func (j *backupJob) run(ctx context.Context) (*LastBackup, error) {
	started := j.now
	reused := j.reusableSSTs()
	if reused == nil {
		// The last backup is too old to share: the backup uploads every SST,
		// under a new lineage rather than over the keys of the old one. An
		// SST key is thus written once, and the cleanup, which deletes the
		// unreferenced SSTs it listed a moment ago, can't delete one that a
		// backup wrote again in the meantime.
		lineage := backup.NewLineage()
		j.record.Lineage = lineage
	}
	id := j.newID()
	ssts, files, err := j.listCheckpoint()
	if err != nil {
		return nil, err
	}
	var uploads []upload
	for _, name := range ssts {
		if _, ok := reused[name]; !ok {
			uploads = append(uploads, upload{name: name, key: j.path.SSTKey(j.record.Lineage, name)})
		}
	}
	for _, name := range files {
		uploads = append(uploads, upload{name: name, key: j.path.FileKey(id, name)})
	}
	uploaded, err := j.uploadAll(ctx, uploads)
	if err != nil {
		return nil, err
	}

	manifest := &backup.Manifest{
		FormatVersion:   backup.ManifestFormatVersion,
		ID:              id.String(),
		Lineage:         j.record.Lineage,
		Namespace:       j.path.Namespace,
		Shard:           j.path.Shard,
		Range:           j.hashRange,
		NamespaceConfig: j.namespaceConfig,
		Term:            j.term,
		CommitOffset:    j.commitOffset,
		CreatedAt:       started,
	}
	var uploadedBytes int64
	for _, f := range uploaded {
		uploadedBytes += f.Size
		if backup.IsSSTName(f.Name) {
			manifest.SSTs = append(manifest.SSTs, f)
		} else {
			manifest.Files = append(manifest.Files, f)
		}
	}
	for _, name := range ssts {
		if f, ok := reused[name]; ok {
			manifest.SSTs = append(manifest.SSTs, f)
		}
	}
	slices.SortFunc(manifest.SSTs, func(a, b backup.File) int { return compareNames(a.Name, b.Name) })
	slices.SortFunc(manifest.Files, func(a, b backup.File) int { return compareNames(a.Name, b.Name) })
	if err := backup.WriteManifest(ctx, j.store, manifest); err != nil {
		return nil, err
	}
	return &LastBackup{
		ID:             manifest.ID,
		Time:           id.Time,
		CommitOffset:   manifest.CommitOffset,
		Term:           manifest.Term,
		SizeBytes:      manifest.Summary().SizeBytes,
		UploadedBytes:  uploadedBytes,
		DurationMillis: j.clock().Sub(started).Milliseconds(),
		SSTs:           manifest.SSTs,
	}, nil
}

func compareNames(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// newID returns the ID of the backup: the next second when the last backup
// of the lineage started in the same one.
func (j *backupJob) newID() backup.ID {
	id := backup.NewID(j.now, j.record.Lineage)
	if last := j.record.LastBackup; last != nil {
		if lastID, err := backup.ParseID(last.ID); err == nil && !id.Time.After(lastID.Time) {
			id.Time = lastID.Time.Add(time.Second)
		}
	}
	return id
}

// listCheckpoint returns the names of the SSTs and of the other files of
// the checkpoint.
func (j *backupJob) listCheckpoint() (ssts []string, files []string, err error) {
	entries, err := os.ReadDir(j.checkpoint.BasePath())
	if err != nil {
		return nil, nil, fmt.Errorf("list the checkpoint: %w", err)
	}
	for _, entry := range entries {
		switch {
		case entry.IsDir():
			continue
		case backup.IsSSTName(entry.Name()):
			ssts = append(ssts, entry.Name())
		default:
			files = append(files, entry.Name())
		}
	}
	return ssts, files, nil
}

// reusableSSTs returns the SSTs of the last backup that this one can list
// without uploading them again, by name: those of a backup younger than the
// reuse window, which the cleanup can't have expired. An SST is reused only
// with the size the checkpoint has for it. It returns nil when the last
// backup is too old to share: the backup then starts a new lineage. Without
// a last backup, the lineage is new already.
func (j *backupJob) reusableSSTs() map[string]backup.File {
	reusable := map[string]backup.File{}
	last := j.record.LastBackup
	if last == nil {
		return reusable
	}
	if j.now.Sub(last.Time) >= j.reuseWindow {
		return nil
	}
	for _, f := range last.SSTs {
		info, err := os.Stat(filepath.Join(j.checkpoint.BasePath(), f.Name))
		if err == nil && info.Size() == f.Size {
			reusable[f.Name] = f
		}
	}
	return reusable
}

// uploadAll uploads the files with uploadWorkers at a time, and returns
// them in the order of uploads.
func (j *backupJob) uploadAll(ctx context.Context, uploads []upload) ([]backup.File, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]backup.File, len(uploads))
	errs := make([]error, len(uploads))
	next := make(chan int)
	var wg sync.WaitGroup
	for range max(1, j.uploadWorkers) {
		wg.Go(func() {
			for i := range next {
				results[i], errs[i] = j.uploadFile(ctx, uploads[i])
				if errs[i] != nil {
					cancel()
				}
			}
		})
	}
	for i := range uploads {
		select {
		case next <- i:
		case <-ctx.Done():
		}
	}
	close(next)
	wg.Wait()
	// The first failure cancels the others: report it rather than them
	var failed error
	for i, err := range errs {
		if err != nil && (failed == nil || errors.Is(failed, context.Canceled)) {
			failed = fmt.Errorf("upload %s: %w", uploads[i].name, err)
		}
	}
	if failed != nil {
		return nil, failed
	}
	// A cancellation from outside skips the uploads left without an error
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// uploadFile uploads one file of the checkpoint, and returns its entry of
// the manifest.
func (j *backupJob) uploadFile(ctx context.Context, u upload) (backup.File, error) {
	f, err := os.Open(filepath.Join(j.checkpoint.BasePath(), u.name))
	if err != nil {
		return backup.File{}, err
	}
	defer func() { _ = f.Close() }()
	// The store reads the file as it sends it, and rewinds it to send it
	// again: the size and the checksum are those of the last pass
	body := newHashingReader(f)
	if err := j.store.Put(ctx, u.key, body); err != nil {
		return backup.File{}, err
	}
	return backup.File{
		Name:   u.name,
		Key:    u.key,
		Size:   body.size,
		CRC32C: body.crc.Sum32(),
	}, nil
}
