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
	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
)

type Runtime interface {
	io.Closer
	controller.ShardSplitter
	controller.ShardEventListener
	controller.ShardAssignmentsProvider
	controller.DataServerEventListener

	CreateDataServer(name string, dataServer *proto.DataServer) bool
	DeleteDataServer(name string)
	// DrainedDataServer deletes the stored status of a data server that
	// left the configuration and is DRAINED, and drops it from the pending
	// deletes of the shards: its name is then free. It returns
	// ErrFailedPrecondition for a data server of the configuration, or one
	// not DRAINED, and ErrNotFound for one without a stored status.
	DrainedDataServer(name string) error

	// ListLiveDataServer returns the data servers that have a controller and
	// take shards: a draining or drained one is left out. The configured data
	// servers are those of Metadata().ListDataServer.
	ListLiveDataServer() map[string]commonobject.Borrowed[*proto.DataServer]
	// ListLiveDataServerStatus and GetLiveDataServerStatus return the status
	// that the controllers hold in memory, of every data server that has one.
	// The stored status is that of Metadata().ListDataServerStatus.
	ListLiveDataServerStatus() map[string]*proto.DataServerStatus
	GetLiveDataServerStatus(name string) (*proto.DataServerStatus, bool)

	// CreateNamespace creates the status if absent and starts any missing shard
	// controllers from the existing status when the namespace is already saved.
	// Returns errs.ErrConflict if the status disappears before controller initialization.
	CreateNamespace(name string, namespaceConfig *proto.Namespace) error
	DeleteNamespace(namespace string)

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
	// data server: a message per shard, given to visit as they arrive.
	ListBackups(ctx context.Context, namespace string, shard *int64, destination string,
		visit func(*proto.ListBackupsResponse) error) error
}
