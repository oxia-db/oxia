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

package runtime

import (
	"io"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"
)

type Runtime interface {
	io.Closer
	controller.ShardSplitter
	controller.ShardEventListener
	controller.ShardAssignmentsProvider
	controller.DataServerEventListener

	PutDataServerIfAbsent(server *proto.DataServer)
	DeleteDataServer(dataServerID string)
	SyncShardControllerServerAddresses()
	PutShard(namespace string, shard int64, shardMetadata *proto.ShardMetadata)
	DeleteShard(shard int64)
	RecomputeAssignments()
	SelectNewEnsemble(namespaceConfig *proto.Namespace, editingStatus *proto.ClusterStatus) ([]*proto.DataServerIdentity, error)

	NodeControllers() map[string]controller.DataServerController

	LoadBalancer() balancer.LoadBalancer

	Metadata() coordmetadata.Metadata
}
