// Copyright 2023-2026 The Oxia Authors
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
	"context"
	"io"

	commonobject "github.com/oxia-db/oxia/common/object"
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

	CreateDataServer(name string, dataServer *proto.DataServer) bool
	DeleteDataServer(name string)

	ListDataServer() map[string]commonobject.Borrowed[*proto.DataServer]
	ListDataServerStatus() map[string]*proto.DataServerStatus
	GetDataServerStatus(name string) (*proto.DataServerStatus, bool)

	// CreateNamespace creates the status if absent and starts any missing shard
	// controllers from the existing status when the namespace is already saved.
	// Returns ErrConflict if the status disappears before controller initialization.
	CreateNamespace(name string, namespaceConfig *proto.Namespace) error
	DeleteNamespace(namespace string)

	LoadBalancer() balancer.LoadBalancer
	Metadata() coordmetadata.Metadata

	RecomputeAssignments()
	RefreshShardControllers()

	// RequestBackup makes the backups of the shards of a namespace, or of one
	// of its shards, or of every namespace when it is empty, due now, and
	// returns how many. It returns ErrBackupsUnsupported when the RPC
	// provider has no OxiaBackup service.
	RequestBackup(namespace string, shard *int64) (int, error)
	// ListBackups lists the backups of a namespace, or of one of its shards,
	// at the destination of the policy or the given one, through a running
	// data server.
	ListBackups(ctx context.Context, namespace string, shard *int64, destination string) ([]*proto.BackupInfo, error)
	// RestoreNamespace restores a namespace as of a point in time into a new
	// namespace, see docs/backups.md. It returns the shards of the new
	// namespace, which install their backups and elect their leaders in the
	// background.
	RestoreNamespace(ctx context.Context, req *proto.RestoreNamespaceRequest) (*proto.RestoreNamespaceResponse, error)
}
