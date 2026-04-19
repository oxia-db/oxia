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

package controller

import "github.com/oxia-db/oxia/oxiad/coordinator/model"

type testStatusResource struct {
	status *model.ClusterStatus
}

func newTestStatusResource(initial *model.ClusterStatus) *testStatusResource {
	if initial == nil {
		initial = &model.ClusterStatus{}
	}
	return &testStatusResource{status: initial}
}

func (r *testStatusResource) callbacks() StatusCallbacks {
	return StatusCallbacks{
		Load:                r.Load,
		Update:              r.Update,
		UpdateShardMetadata: r.UpdateShardMetadata,
		DeleteShardMetadata: r.DeleteShardMetadata,
	}
}

func (r *testStatusResource) Load() *model.ClusterStatus {
	return r.status
}

func (r *testStatusResource) Update(newStatus *model.ClusterStatus) {
	r.status = newStatus
}

func (r *testStatusResource) UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata) {
	cloned := r.status.Clone()
	ns, ok := cloned.Namespaces[namespace]
	if !ok {
		return
	}
	ns.Shards[shard] = shardMetadata.Clone()
	cloned.Namespaces[namespace] = ns
	r.status = cloned
}

func (r *testStatusResource) DeleteShardMetadata(namespace string, shard int64) {
	cloned := r.status.Clone()
	ns, ok := cloned.Namespaces[namespace]
	if !ok {
		return
	}
	delete(ns.Shards, shard)
	if len(ns.Shards) == 0 {
		delete(cloned.Namespaces, namespace)
	} else {
		cloned.Namespaces[namespace] = ns
	}
	r.status = cloned
}

type testClusterConfigResource struct {
	config model.ClusterConfig
}

func newTestClusterConfigResource(config model.ClusterConfig) *testClusterConfigResource {
	return &testClusterConfigResource{config: config}
}

func (r *testClusterConfigResource) callbacks() ClusterConfigCallbacks {
	return ClusterConfigCallbacks{
		NamespaceConfig: r.NamespaceConfig,
		Node:            r.Node,
	}
}

func (r *testClusterConfigResource) NamespaceConfig(namespace string) (*model.NamespaceConfig, bool) {
	for idx := range r.config.Namespaces {
		if r.config.Namespaces[idx].Name == namespace {
			return &r.config.Namespaces[idx], true
		}
	}
	return nil, false
}

func (r *testClusterConfigResource) Node(id string) (*model.Server, bool) {
	for idx := range r.config.Servers {
		if r.config.Servers[idx].GetIdentifier() == id {
			return &r.config.Servers[idx], true
		}
	}
	return nil, false
}
