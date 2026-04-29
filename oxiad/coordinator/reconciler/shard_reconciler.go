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

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

var _ Reconciler = (*shardReconciler)(nil)

type shardReconciler struct {
	runtime runtime.Runtime
}

func (*shardReconciler) Close() error { return nil }

func (r *shardReconciler) Reconcile(_ context.Context, snapshot *proto.ClusterConfiguration) error {
	metadata := r.runtime.Metadata()
	clusterStatus, shardsToAdd, shardsToDelete := metadata.ApplyStatusChanges(snapshot, r.runtime.SelectNewEnsemble)

	for shard, namespace := range shardsToAdd {
		shardMetadata := clusterStatus.Namespaces[namespace].Shards[shard]
		r.runtime.PutShard(namespace, shard, shardMetadata)
	}
	for _, shard := range shardsToDelete {
		r.runtime.DeleteShard(shard)
	}

	return nil
}
