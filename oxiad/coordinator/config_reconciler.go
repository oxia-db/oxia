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
	"context"
	"errors"
	"log/slog"

	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
)

type ConfigReconciler struct {
	log         *slog.Logger
	coordinator *coordinator
}

func NewConfigReconciler(log *slog.Logger, coordinator *coordinator) *ConfigReconciler {
	return &ConfigReconciler{
		log:         log,
		coordinator: coordinator,
	}
}

func NewConfigReconcilerForCoordinator(log *slog.Logger, coord Coordinator) (*ConfigReconciler, error) {
	concrete, ok := coord.(*coordinator)
	if !ok {
		return nil, errors.New("unexpected coordinator implementation")
	}
	return NewConfigReconciler(log, concrete), nil
}

func (r *ConfigReconciler) Run(ctx context.Context) {
	receiver, err := r.coordinator.metadata.ConfigWatch().Subscribe()
	if err != nil {
		r.log.Warn("failed to subscribe to cluster config watch", slog.Any("error", err))
		return
	}
	defer func() {
		_ = receiver.Close()
	}()

	r.reconcileCurrent(receiver) //nolint:contextcheck // Reconciliation creates coordinator-owned controllers that use the coordinator lifecycle context.
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-receiver.Changed():
			if !ok {
				return
			}
		}

		r.reconcileCurrent(receiver) //nolint:contextcheck // Reconciliation creates coordinator-owned controllers that use the coordinator lifecycle context.
	}
}

func (r *ConfigReconciler) Reconcile(config *proto.ClusterConfiguration) {
	r.log.Info("Reconciling cluster config", slog.Any("newClusterConfig", config))

	r.reconcileDataServers(config)
	r.syncShardControllerServerAddresses()

	clusterStatus, shardsToAdd, shardsToDelete := r.coordinator.metadata.ApplyStatusChanges(config, r.coordinator.selectNewEnsemble)
	for shard, namespace := range shardsToAdd {
		shardMetadata := clusterStatus.Namespaces[namespace].Shards[shard]
		r.createShardController(namespace, shard, shardMetadata)
	}
	for _, shard := range shardsToDelete {
		r.deleteShardController(shard)
	}

	r.recomputeAssignments()
	r.coordinator.loadBalancer.Trigger()
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
		if r.hasDataServer(id) {
			continue
		}
		r.log.Info("Detected new node", slog.Any("server", dataServer))
		r.createDataServer(dataServer)
	}

	desired := make(map[string]struct{}, len(config.GetServers()))
	for _, dataServer := range config.GetServers() {
		desired[dataServer.GetNameOrDefault()] = struct{}{}
	}
	for _, currentID := range r.dataServerIDs() {
		if _, ok := desired[currentID]; ok {
			continue
		}
		r.log.Info("Detected a removed node", slog.Any("server", currentID))
		r.deleteDataServer(currentID)
	}
}

func (r *ConfigReconciler) hasDataServer(id string) bool {
	r.coordinator.RLock()
	defer r.coordinator.RUnlock()
	_, ok := r.coordinator.nodeControllers[id]
	return ok
}

func (r *ConfigReconciler) dataServerIDs() []string {
	r.coordinator.RLock()
	defer r.coordinator.RUnlock()

	ids := make([]string, 0, len(r.coordinator.nodeControllers))
	for id := range r.coordinator.nodeControllers {
		ids = append(ids, id)
	}
	return ids
}

func (r *ConfigReconciler) createDataServer(dataServer *proto.DataServerIdentity) {
	r.coordinator.Lock()
	defer r.coordinator.Unlock()

	id := dataServer.GetNameOrDefault()
	if _, ok := r.coordinator.nodeControllers[id]; ok {
		return
	}
	if nc, ok := r.coordinator.drainingNodes[id]; ok {
		_ = nc.Close()
		delete(r.coordinator.drainingNodes, id)
	}
	r.coordinator.nodeControllers[id] = controller.NewDataServerController(
		r.coordinator.ctx,
		dataServer,
		r.coordinator,
		r.coordinator,
		r.coordinator.rpc,
		r.coordinator.insID,
	)
}

func (r *ConfigReconciler) deleteDataServer(id string) {
	r.coordinator.Lock()
	defer r.coordinator.Unlock()

	nc, ok := r.coordinator.nodeControllers[id]
	if !ok {
		return
	}
	delete(r.coordinator.nodeControllers, id)
	nc.SetStatus(controller.Draining)
	r.coordinator.drainingNodes[id] = nc
}

func (r *ConfigReconciler) syncShardControllerServerAddresses() {
	r.coordinator.RLock()
	defer r.coordinator.RUnlock()
	for _, sc := range r.coordinator.shardControllers {
		sc.SyncServerAddress()
	}
}

func (r *ConfigReconciler) createShardController(namespace string, shard int64, shardMetadata *proto.ShardMetadata) {
	r.coordinator.Lock()
	defer r.coordinator.Unlock()

	if _, ok := r.coordinator.shardControllers[shard]; ok {
		return
	}
	namespaceConfig, exist := r.coordinator.metadata.Namespace(namespace)
	if !exist {
		return
	}
	r.coordinator.shardControllers[shard] = controller.NewShardController(
		namespace,
		shard,
		namespaceConfig,
		pb.Clone(shardMetadata).(*proto.ShardMetadata),
		r.coordinator.metadata,
		r.coordinator.findDataServerFeatures,
		r.coordinator,
		r.coordinator.rpc,
		controller.DefaultPeriodicTasksInterval,
	)
}

func (r *ConfigReconciler) deleteShardController(shard int64) {
	r.coordinator.RLock()
	sc, ok := r.coordinator.shardControllers[shard]
	r.coordinator.RUnlock()
	if !ok {
		return
	}
	sc.DeleteShard()
}

func (r *ConfigReconciler) recomputeAssignments() {
	r.coordinator.Lock()
	defer r.coordinator.Unlock()
	r.coordinator.computeNewAssignments()
}
