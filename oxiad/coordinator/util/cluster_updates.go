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
package util

import (
	"log/slog"

	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/sharding"
)

func findNamespaceConfig(config *commonproto.ClusterConfiguration, ns string) *commonproto.Namespace {
	for _, cns := range config.GetNamespaces() {
		if cns.GetName() == ns {
			return cns
		}
	}
	return nil
}

func ApplyClusterChanges(config *commonproto.ClusterConfiguration, status *commonproto.ClusterStatus, ensembleSupplier func(namespaceConfig *commonproto.Namespace, status *commonproto.ClusterStatus) ([]*commonproto.DataServerIdentity, error)) (
	shardsToAdd map[int64]string,
	shardsToDelete []int64) {
	shardsToAdd = map[int64]string{}
	shardsToDelete = []int64{}
	var err error

	if status.Namespaces == nil {
		status.Namespaces = map[string]*commonproto.NamespaceStatus{}
	}

	// Check for new namespaces
	for _, nc := range config.GetNamespaces() {
		_, existing := status.Namespaces[nc.GetName()]
		if existing {
			continue
		}

		// This is a new namespace
		nss := &commonproto.NamespaceStatus{
			Shards:            map[int64]*commonproto.ShardMetadata{},
			ReplicationFactor: nc.GetReplicationFactor(),
		}
		// Publish the namespace into status *before* placing shards so that
		// each per-shard ensembleSupplier call sees the placements made earlier
		// in this same init cycle.
		status.Namespaces[nc.GetName()] = nss

		for _, shard := range sharding.GenerateShards(status.ShardIdGenerator, nc.GetInitialShardCount()) {
			var esm []*commonproto.DataServerIdentity
			if esm, err = ensembleSupplier(nc, status); err != nil {
				slog.Error("failed to select new ensembles.", slog.Any("shard", shard), slog.Any("error", err))
				continue
			}
			shardMetadata := &commonproto.ShardMetadata{
				Status:   commonproto.ShardStatusUnknown,
				Term:     -1,
				Leader:   nil,
				Ensemble: esm,
				Int32HashRange: &commonproto.HashRange{
					Min: shard.Min,
					Max: shard.Max,
				},
			}

			status.Namespaces[nc.GetName()].Shards[shard.Id] = shardMetadata
			status.ServerIdx = (status.ServerIdx + nc.GetReplicationFactor()) % uint32(len(config.GetServers()))
			shardsToAdd[shard.Id] = nc.GetName()
		}
		status.ShardIdGenerator += int64(nc.GetInitialShardCount())
	}

	// Check for any namespace that was removed
	for name, ns := range status.Namespaces {
		if namespaceConfig := findNamespaceConfig(config, name); namespaceConfig != nil {
			continue
		}

		// Keep the shards in the status and mark them as being deleted
		for shardId, shard := range ns.Shards {
			shard.Status = commonproto.ShardStatusDeleting
			ns.Shards[shardId] = shard
			shardsToDelete = append(shardsToDelete, shardId)
		}
	}

	return shardsToAdd, shardsToDelete
}
