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
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	"github.com/oxia-db/oxia/oxiad/coordinator/reconciler"
)

var _ reconciler.Coordinator = (*configReconcilerAdapter)(nil)

type configReconcilerAdapter struct {
	coordinator *coordinator
}

func (a *configReconcilerAdapter) HasDataServer(id string) bool {
	a.coordinator.RLock()
	defer a.coordinator.RUnlock()
	_, ok := a.coordinator.nodeControllers[id]
	return ok
}

func (a *configReconcilerAdapter) DataServerIDs() []string {
	a.coordinator.RLock()
	defer a.coordinator.RUnlock()

	ids := make([]string, 0, len(a.coordinator.nodeControllers))
	for id := range a.coordinator.nodeControllers {
		ids = append(ids, id)
	}
	return ids
}

func (a *configReconcilerAdapter) CreateDataServer(dataServer *proto.DataServerIdentity) {
	a.coordinator.Lock()
	defer a.coordinator.Unlock()

	id := dataServer.GetNameOrDefault()
	if _, ok := a.coordinator.nodeControllers[id]; ok {
		return
	}
	if nc, ok := a.coordinator.drainingNodes[id]; ok {
		_ = nc.Close()
		delete(a.coordinator.drainingNodes, id)
	}
	a.coordinator.nodeControllers[id] = controller.NewDataServerController(
		a.coordinator.ctx,
		dataServer,
		a.coordinator,
		a.coordinator,
		a.coordinator.rpc,
		a.coordinator.insID,
	)
}

func (a *configReconcilerAdapter) DeleteDataServer(id string) {
	a.coordinator.Lock()
	defer a.coordinator.Unlock()

	nc, ok := a.coordinator.nodeControllers[id]
	if !ok {
		return
	}
	delete(a.coordinator.nodeControllers, id)
	nc.SetStatus(controller.Draining)
	a.coordinator.drainingNodes[id] = nc
}

func (a *configReconcilerAdapter) SyncShardControllerServerAddresses() {
	a.coordinator.RLock()
	defer a.coordinator.RUnlock()
	for _, sc := range a.coordinator.shardControllers {
		sc.SyncServerAddress()
	}
}

func (a *configReconcilerAdapter) ApplyStatusChanges(config *proto.ClusterConfiguration) (*proto.ClusterStatus, map[int64]string, []int64) {
	return a.coordinator.metadata.ApplyStatusChanges(config, a.coordinator.selectNewEnsemble)
}

func (a *configReconcilerAdapter) CreateShardController(namespace string, shard int64, shardMetadata *proto.ShardMetadata) {
	a.coordinator.Lock()
	defer a.coordinator.Unlock()

	if _, ok := a.coordinator.shardControllers[shard]; ok {
		return
	}
	namespaceConfig, exist := a.coordinator.metadata.Namespace(namespace)
	if !exist {
		return
	}
	a.coordinator.shardControllers[shard] = controller.NewShardController(
		namespace,
		shard,
		namespaceConfig,
		pb.Clone(shardMetadata).(*proto.ShardMetadata),
		a.coordinator.metadata,
		a.coordinator.findDataServerFeatures,
		a.coordinator,
		a.coordinator.rpc,
		controller.DefaultPeriodicTasksInterval,
	)
}

func (a *configReconcilerAdapter) DeleteShardController(shard int64) {
	a.coordinator.RLock()
	sc, ok := a.coordinator.shardControllers[shard]
	a.coordinator.RUnlock()
	if !ok {
		return
	}
	sc.DeleteShard()
}

func (a *configReconcilerAdapter) RecomputeAssignments() {
	a.coordinator.Lock()
	defer a.coordinator.Unlock()
	a.coordinator.computeNewAssignments()
}

func (a *configReconcilerAdapter) TriggerLoadBalancer() {
	a.coordinator.loadBalancer.Trigger()
}
