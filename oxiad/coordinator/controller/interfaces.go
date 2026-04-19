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

type StatusCallbacks struct {
	Load                func() *model.ClusterStatus
	Update              func(newStatus *model.ClusterStatus)
	UpdateShardMetadata func(namespace string, shard int64, shardMetadata model.ShardMetadata)
	DeleteShardMetadata func(namespace string, shard int64)
}

type ClusterConfigCallbacks struct {
	NamespaceConfig func(namespace string) (*model.NamespaceConfig, bool)
	Node            func(id string) (*model.Server, bool)
}
