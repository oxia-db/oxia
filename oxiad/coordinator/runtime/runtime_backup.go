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

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	backupscheduler "github.com/oxia-db/oxia/oxiad/coordinator/runtime/backup"
	dataservercontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/dataserver"
)

// This file owns the backup operations of the runtime: the scheduler.

// startBackupScheduler starts the scheduler, when the RPC provider serves
// the backup calls.
func (c *runtime) startBackupScheduler() {
	backupRPC, ok := c.rpc.(rpc.BackupProvider)
	if !ok {
		c.logger.Info("The RPC provider has no backup service: backups are disabled")
		return
	}
	c.backupScheduler = backupscheduler.New(backupscheduler.Options{
		Metadata: c.metadata,
		RPC: struct {
			statusGetter
			rpc.BackupProvider
		}{c.rpc, backupRPC},
		Available: c.isDataServerRunning,
	})
	c.backupScheduler.Start()
}

type statusGetter interface {
	GetStatus(ctx context.Context, node *proto.DataServerIdentity,
		req *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
}

// isDataServerRunning reports whether a data server passes its health
// checks, by name.
func (c *runtime) isDataServerRunning(name string) bool {
	c.RLock()
	defer c.RUnlock()
	nc, ok := c.dataServerControllers[name]
	return ok && nc.GetStatus().GetState() == dataservercontroller.Running
}
