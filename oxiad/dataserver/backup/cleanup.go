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
	"log/slog"
	"strings"
	"time"

	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

// cleanupResult counts what a cleanup deleted.
type cleanupResult struct {
	backupsExpired int
	filesDeleted   int
	bytesDeleted   int64
}

// cleanupJob expires the backups of a namespace under the retention, and
// deletes the files that no remaining backup references. It reads every
// manifest of the namespace.
type cleanupJob struct {
	store     blob.Store
	namespace string
	retention time.Duration
	// grace is how old an unreferenced file must be to be deleted: the files
	// of a backup that hasn't written its manifest yet are younger.
	grace time.Duration
	now   time.Time
	log   *slog.Logger
	// batchSize is how many unreferenced files the sweep deletes at a time:
	// sweepBatchSize unless set.
	batchSize int
}

// sweepBatchSize is how many unreferenced files a sweep deletes at a time:
// it holds one batch, not every unreferenced file of a shard.
const sweepBatchSize = 1000

func (j *cleanupJob) run(ctx context.Context) (cleanupResult, error) {
	var result cleanupResult
	shards, err := backup.ListShards(ctx, j.store, j.namespace)
	if err != nil {
		return result, err
	}
	var all []backup.Summary
	for _, shard := range shards {
		summaries, err := backup.ListManifests(ctx, j.store, shard)
		if err != nil {
			return result, err
		}
		all = append(all, summaries...)
	}

	expired := map[string]struct{}{}
	for _, s := range backup.Expired(all, j.retention, j.now) {
		expired[s.ManifestKey()] = struct{}{}
	}
	// The manifests go first: a backup ceases to exist before its files do
	if err := j.expireManifests(ctx, expired, &result); err != nil {
		return result, err
	}
	for _, shard := range shards {
		live := make([]backup.Summary, 0, len(all))
		for _, s := range all {
			if _, gone := expired[s.ManifestKey()]; s.Shard == shard.Shard && !gone {
				live = append(live, s)
			}
		}
		if err := j.sweepShard(ctx, shard, live, &result); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (j *cleanupJob) expireManifests(ctx context.Context, expired map[string]struct{}, result *cleanupResult) error {
	if len(expired) == 0 {
		return nil
	}
	keys := make([]string, 0, len(expired))
	for key := range expired {
		keys = append(keys, key)
		j.log.Info("Expiring backup", slog.String("manifest", key))
	}
	if err := j.store.Delete(ctx, keys); err != nil {
		return fmt.Errorf("delete the expired manifests: %w", err)
	}
	result.backupsExpired += len(keys)
	return nil
}

// sweepShard deletes the files of a shard that the live backups don't
// reference and that are older than the grace period: the SSTs, and the
// per-backup files of the backups without a manifest.
func (j *cleanupJob) sweepShard(ctx context.Context, shard backup.ShardPath, live []backup.Summary,
	result *cleanupResult) error {
	referenced := map[string]struct{}{}
	liveIDs := map[string]struct{}{}
	for _, s := range live {
		m, err := backup.ReadManifest(ctx, j.store, s.ManifestKey())
		if err != nil {
			return err
		}
		for _, f := range m.SSTs {
			referenced[f.Key] = struct{}{}
		}
		liveIDs[s.ID.String()] = struct{}{}
	}
	cutoff := j.now.Add(-j.grace)
	// The unreferenced files are deleted as the listings find them, a batch
	// at a time
	batch := &deleteBatch{job: j, result: result}
	err := j.store.List(ctx, shard.SSTPrefix(), func(object blob.Object) error {
		if _, ok := referenced[object.Key]; !ok && object.LastModified.Before(cutoff) {
			return batch.add(ctx, object)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("list the SSTs of %s: %w", shard.Prefix(), err)
	}
	// The per-backup files of a backup whose manifest is gone, or was never
	// written, from a backup old enough to be no longer in progress
	dirs, err := j.store.ListDirs(ctx, shard.ManifestsPrefix())
	if err != nil {
		return fmt.Errorf("list the backups of %s: %w", shard.Prefix(), err)
	}
	for _, dir := range dirs {
		idString := strings.TrimSuffix(strings.TrimPrefix(dir, shard.ManifestsPrefix()), "/")
		id, err := backup.ParseID(idString)
		if _, isLive := liveIDs[idString]; err != nil || isLive || !id.Time.Before(cutoff) {
			continue
		}
		err = j.store.List(ctx, dir, func(object blob.Object) error { return batch.add(ctx, object) })
		if err != nil {
			return fmt.Errorf("list the files of %s: %w", dir, err)
		}
	}
	return batch.flush(ctx)
}

// deleteBatch gathers the unreferenced files a sweep finds and deletes them
// batchSize at a time.
type deleteBatch struct {
	job     *cleanupJob
	result  *cleanupResult
	objects []blob.Object
}

func (b *deleteBatch) add(ctx context.Context, object blob.Object) error {
	b.objects = append(b.objects, object)
	size := b.job.batchSize
	if size <= 0 {
		size = sweepBatchSize
	}
	if len(b.objects) < size {
		return nil
	}
	return b.flush(ctx)
}

func (b *deleteBatch) flush(ctx context.Context) error {
	if len(b.objects) == 0 {
		return nil
	}
	err := b.job.deleteObjects(ctx, b.objects, b.result)
	b.objects = b.objects[:0]
	return err
}

func (j *cleanupJob) deleteObjects(ctx context.Context, objects []blob.Object, result *cleanupResult) error {
	keys := make([]string, 0, len(objects))
	var bytes int64
	for _, object := range objects {
		keys = append(keys, object.Key)
		bytes += object.Size
	}
	if err := j.store.Delete(ctx, keys); err != nil {
		return fmt.Errorf("delete the unreferenced files: %w", err)
	}
	result.filesDeleted += len(keys)
	result.bytesDeleted += bytes
	return nil
}
