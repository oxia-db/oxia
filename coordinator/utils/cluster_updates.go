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

//revive:disable-next-line:var-naming
package utils

import (
	"log/slog"

	"github.com/oxia-db/oxia/common/sharding"
	"github.com/oxia-db/oxia/coordinator/model"
)

func findNamespaceConfig(config *model.ClusterConfig, ns string) *model.NamespaceConfig {
	for _, cns := range config.Namespaces {
		if cns.Name == ns {
			return &cns
		}
	}
	return nil
}

func ApplyClusterChanges(config *model.ClusterConfig, currentStatus *model.ClusterStatus, ensembleSupplier func(namespaceConfig *model.NamespaceConfig, status *model.ClusterStatus) ([]model.Server, error)) (
	newStatus *model.ClusterStatus,
	shardsToAdd map[int64]string,
	shardsToDelete []int64) {
	shardsToAdd = map[int64]string{}
	shardsToDelete = []int64{}
	var err error

	newStatus = &model.ClusterStatus{
		Namespaces:       map[string]model.NamespaceStatus{},
		ShardIdGenerator: currentStatus.ShardIdGenerator,
		ServerIdx:        currentStatus.ServerIdx,
	}
	for k, v := range currentStatus.Namespaces {
		newStatus.Namespaces[k] = v.Clone()
	}

	// Check for new namespaces
	for _, nc := range config.Namespaces {
		nss, existing := currentStatus.Namespaces[nc.Name]
		if existing {
			continue
		}

		// This is a new namespace
		nss = model.NamespaceStatus{
			Shards:            map[int64]model.ShardMetadata{},
			ReplicationFactor: nc.ReplicationFactor,
		}

		for _, shard := range sharding.GenerateShards(newStatus.ShardIdGenerator, nc.InitialShardCount) {
			var esm []model.Server
			if esm, err = ensembleSupplier(&nc, newStatus); err != nil {
				slog.Error("failed to select new ensembles.", slog.Any("shard", shard), slog.Any("error", err))
				continue
			}
			shardMetadata := model.ShardMetadata{
				Status:   model.ShardStatusUnknown,
				Term:     -1,
				Leader:   nil,
				Ensemble: esm,
				Int32HashRange: model.Int32HashRange{
					Min: shard.Min,
					Max: shard.Max,
				},
			}

			nss.Shards[shard.Id] = shardMetadata
			newStatus.ServerIdx = (newStatus.ServerIdx + nc.ReplicationFactor) % uint32(len(config.Servers))
			shardsToAdd[shard.Id] = nc.Name
		}
		newStatus.Namespaces[nc.Name] = nss

		newStatus.ShardIdGenerator += int64(nc.InitialShardCount)
	}

	// Check for any namespace that was removed
	for name, ns := range currentStatus.Namespaces {
		namespaceConfig := findNamespaceConfig(config, name)
		if namespaceConfig != nil {
			continue
		}

		// Keep the shards in the status and mark them as being deleted
		nss := ns.Clone()
		for shardId, shard := range nss.Shards {
			shard.Status = model.ShardStatusDeleting
			nss.Shards[shardId] = shard
			shardsToDelete = append(shardsToDelete, shardId)
		}

		newStatus.Namespaces[name] = nss
	}

	return newStatus, shardsToAdd, shardsToDelete
}
