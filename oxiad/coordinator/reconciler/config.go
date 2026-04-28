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

package reconciler

import (
	"context"
	"log/slog"

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
)

type Coordinator interface {
	HasDataServer(id string) bool
	DataServerIDs() []string
	CreateDataServer(dataServer *proto.DataServerIdentity)
	DeleteDataServer(id string)
	SyncShardControllerServerAddresses()
	ApplyStatusChanges(config *proto.ClusterConfiguration) (*proto.ClusterStatus, map[int64]string, []int64)
	CreateShardController(namespace string, shard int64, shardMetadata *proto.ShardMetadata)
	DeleteShardController(shard int64)
	RecomputeAssignments()
	TriggerLoadBalancer()
}

type ConfigReconciler struct {
	log         *slog.Logger
	watch       *commonwatch.Watch[*proto.ClusterConfiguration]
	coordinator Coordinator
}

func NewConfigReconciler(
	log *slog.Logger,
	watch *commonwatch.Watch[*proto.ClusterConfiguration],
	coordinator Coordinator,
) *ConfigReconciler {
	return &ConfigReconciler{
		log:         log,
		watch:       watch,
		coordinator: coordinator,
	}
}

func (r *ConfigReconciler) Run(ctx context.Context) {
	receiver, err := r.watch.Subscribe()
	if err != nil {
		r.log.Warn("failed to subscribe to cluster config watch", slog.Any("error", err))
		return
	}
	defer func() {
		_ = receiver.Close()
	}()

	r.reconcileCurrent(receiver)
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-receiver.Changed():
			if !ok {
				return
			}
		}

		r.reconcileCurrent(receiver)
	}
}

func (r *ConfigReconciler) Reconcile(config *proto.ClusterConfiguration) {
	r.log.Info("Reconciling cluster config", slog.Any("newClusterConfig", config))

	r.reconcileDataServers(config)
	r.coordinator.SyncShardControllerServerAddresses()

	clusterStatus, shardsToAdd, shardsToDelete := r.coordinator.ApplyStatusChanges(config)
	for shard, namespace := range shardsToAdd {
		shardMetadata := clusterStatus.Namespaces[namespace].Shards[shard]
		r.coordinator.CreateShardController(namespace, shard, shardMetadata)
	}
	for _, shard := range shardsToDelete {
		r.coordinator.DeleteShardController(shard)
	}

	r.coordinator.RecomputeAssignments()
	r.coordinator.TriggerLoadBalancer()
}

func (r *ConfigReconciler) reconcileCurrent(receiver *commonwatch.Receiver[*proto.ClusterConfiguration]) {
	config, ok := receiver.Load()
	if !ok || config == nil {
		return
	}
	r.Reconcile(config)
}

func (r *ConfigReconciler) reconcileDataServers(config *proto.ClusterConfiguration) {
	for _, dataServer := range config.GetServers() {
		id := dataServer.GetNameOrDefault()
		if r.coordinator.HasDataServer(id) {
			continue
		}
		r.log.Info("Detected new node", slog.Any("server", dataServer))
		r.coordinator.CreateDataServer(dataServer)
	}

	desired := make(map[string]struct{}, len(config.GetServers()))
	for _, dataServer := range config.GetServers() {
		desired[dataServer.GetNameOrDefault()] = struct{}{}
	}
	for _, currentID := range r.coordinator.DataServerIDs() {
		if _, ok := desired[currentID]; ok {
			continue
		}
		r.log.Info("Detected a removed node", slog.Any("server", currentID))
		r.coordinator.DeleteDataServer(currentID)
	}
}
