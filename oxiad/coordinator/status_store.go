// Copyright 2023-2025 The Oxia Authors
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

package coordinator

import (
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/util"
)

type ensembleSupplier func(namespaceConfig *model.NamespaceConfig, status *model.ClusterStatus) ([]model.Server, error)

func (c *coordinator) ensureStatusLoadedLocked() {
	if c.currentStatus != nil {
		return
	}

	_ = backoff.RetryNotify(func() error {
		clusterStatus, version, err := c.metadata.Get()
		if err != nil {
			return err
		}
		c.currentStatus = clusterStatus
		c.currentVersionID = version
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		c.Warn(
			"failed to load status, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if c.currentStatus == nil {
		c.currentStatus = &model.ClusterStatus{}
	}
}

func (c *coordinator) loadStatus() *model.ClusterStatus {
	c.statusLock.RLock()
	if c.currentStatus != nil {
		defer c.statusLock.RUnlock()
		return c.currentStatus
	}
	c.statusLock.RUnlock()

	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	c.ensureStatusLoadedLocked()
	return c.currentStatus
}

func (c *coordinator) storeStatusLocked(newStatus *model.ClusterStatus, logMessage string) {
	_ = backoff.RetryNotify(func() error {
		versionID, err := c.metadata.Store(newStatus, c.currentVersionID)
		if err != nil {
			return err
		}
		c.currentStatus = newStatus
		c.currentVersionID = versionID
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		c.Warn(
			logMessage,
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

func (c *coordinator) applyClusterChanges(
	config *model.ClusterConfig,
	supplier ensembleSupplier,
) (*model.ClusterStatus, map[int64]string, []int64) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	c.ensureStatusLoadedLocked()
	newStatus := c.currentStatus.Clone()
	shardsToAdd, shardsToDelete := util.ApplyClusterChanges(config, newStatus, supplier)
	if len(shardsToAdd) == 0 && len(shardsToDelete) == 0 {
		return newStatus, shardsToAdd, shardsToDelete
	}

	c.storeStatusLocked(newStatus, "failed to apply cluster changes, retrying later")
	return newStatus, shardsToAdd, shardsToDelete
}

func (c *coordinator) updateStatus(newStatus *model.ClusterStatus) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	c.ensureStatusLoadedLocked()
	c.storeStatusLocked(newStatus, "failed to update status, retrying later")
}

func (c *coordinator) updateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	c.ensureStatusLoadedLocked()
	clonedStatus := c.currentStatus.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	ns.Shards[shard] = shardMetadata.Clone()
	c.storeStatusLocked(clonedStatus, "failed to update shard metadata, retrying later")
}

func (c *coordinator) deleteShardMetadata(namespace string, shard int64) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	c.ensureStatusLoadedLocked()
	clonedStatus := c.currentStatus.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	delete(ns.Shards, shard)
	if len(ns.Shards) == 0 {
		delete(clonedStatus.Namespaces, namespace)
	}
	c.storeStatusLocked(clonedStatus, "failed to delete shard metadata, retrying later")
}

func (c *coordinator) statusCallbacks() controller.StatusCallbacks {
	return controller.StatusCallbacks{
		Load:                c.loadStatus,
		Update:              c.updateStatus,
		UpdateShardMetadata: c.updateShardMetadata,
		DeleteShardMetadata: c.deleteShardMetadata,
	}
}
