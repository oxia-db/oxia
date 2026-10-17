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
	"fmt"
	"io"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
)

// restoreResult is the state of the installed database.
type restoreResult struct {
	commitOffset int64
	term         int64
}

// restoreJob downloads a backup and installs it as the database of a shard.
type restoreJob struct {
	store     blob.Store
	source    backup.ShardPath
	backupID  backup.ID
	hashRange *proto.HashRange
	installer controller.Installer
}

func (j *restoreJob) run(ctx context.Context) (restoreResult, error) {
	manifest, err := backup.ReadManifest(ctx, j.store, j.source.ManifestKey(j.backupID))
	if err != nil {
		return restoreResult{}, err
	}
	if manifest.Namespace != j.source.Namespace || manifest.Shard != j.source.Shard {
		return restoreResult{}, fmt.Errorf("%w: the manifest is of shard %d of %s", backup.ErrInvalidManifest,
			manifest.Shard, manifest.Namespace)
	}
	for _, f := range manifest.SSTs {
		if err := j.download(ctx, f); err != nil {
			return restoreResult{}, err
		}
	}
	for _, f := range manifest.Files {
		if err := j.download(ctx, f); err != nil {
			return restoreResult{}, err
		}
	}
	commitOffset, term, err := j.installer.Complete(j.hashRange)
	if err != nil {
		return restoreResult{}, fmt.Errorf("install the database: %w", err)
	}
	return restoreResult{commitOffset: commitOffset, term: term}, nil
}

// download writes a file of the backup to the installer, checking its
// checksum.
func (j *restoreJob) download(ctx context.Context, f backup.File) error {
	object, err := j.store.Get(ctx, f.Key)
	if err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	defer func() { _ = object.Close() }()
	var body io.Reader = object
	if backup.IsCompressedKey(f.Key) {
		decompressed, err := decompress(object)
		if err != nil {
			return fmt.Errorf("download %s: %w", f.Name, err)
		}
		defer decompressed.Close()
		body = decompressed
	}
	crc := newCRC32C()
	if err := j.installer.AddFile(f.Name, f.Size, io.TeeReader(body, crc)); err != nil {
		return fmt.Errorf("download %s: %w", f.Name, err)
	}
	if crc.Sum32() != f.CRC32C {
		return fmt.Errorf("download %s: checksum %08x, want %08x", f.Name, crc.Sum32(), f.CRC32C)
	}
	return nil
}
