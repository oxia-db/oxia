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
}

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
	var stale []blob.Object
	err := j.store.List(ctx, shard.SSTPrefix(), func(object blob.Object) error {
		if _, ok := referenced[object.Key]; !ok && object.LastModified.Before(cutoff) {
			stale = append(stale, object)
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
		err = j.store.List(ctx, dir, func(object blob.Object) error {
			stale = append(stale, object)
			return nil
		})
		if err != nil {
			return fmt.Errorf("list the files of %s: %w", dir, err)
		}
	}
	return j.deleteObjects(ctx, stale, result)
}

func (j *cleanupJob) deleteObjects(ctx context.Context, objects []blob.Object, result *cleanupResult) error {
	if len(objects) == 0 {
		return nil
	}
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
