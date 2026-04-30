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

	namespaceConfigs := make(map[string]*proto.Namespace, len(snapshot.GetNamespaces()))
	for _, namespaceConfig := range snapshot.GetNamespaces() {
		namespaceConfigs[namespaceConfig.GetName()] = namespaceConfig
	}

	for _, namespaceConfig := range snapshot.GetNamespaces() {
		namespace := namespaceConfig.GetName()
		if _, exists := metadata.GetNamespaceStatus(namespace); exists {
			continue
		}

		for shard, shardMetadata := range metadata.CreateNamespaceStatus(namespaceConfig, r.runtime.SelectNewEnsemble) {
			r.runtime.PutShardIfAbsent(namespace, shard, shardMetadata)
		}
	}

	for _, namespace := range metadata.ListNamespaces().Values() {
		if _, exists := namespaceConfigs[namespace]; exists {
			continue
		}
		for _, shard := range metadata.DeleteNamespace(namespace) {
			r.runtime.DeleteShard(shard)
		}
	}

	return nil
}
