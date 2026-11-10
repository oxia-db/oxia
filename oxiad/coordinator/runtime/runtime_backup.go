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
	"fmt"
	"slices"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	backupscheduler "github.com/oxia-db/oxia/oxiad/coordinator/runtime/backup"
)

// This file owns the backup operations of the runtime: the scheduler, and
// the admin operations on the backups.

var (
	// ErrBackupsUnsupported is returned when the RPC provider of the runtime
	// has no OxiaBackup service.
	ErrBackupsUnsupported = fmt.Errorf("%w: backups are not supported by this coordinator", errs.ErrNotImplemented)
	// ErrNoBackupDestination is returned by a backup operation without a
	// destination when the backup policy has none either.
	ErrNoBackupDestination = fmt.Errorf("%w: no backup destination is configured", errs.ErrFailedPrecondition)
)

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

// isDataServerRunning reports whether a data server, by name, answers its
// health checks and runs: not draining, nor drained.
func (c *runtime) isDataServerRunning(name string) bool {
	c.RLock()
	defer c.RUnlock()
	nc, ok := c.dataServerControllers[name]
	if !ok {
		return false
	}
	status := nc.GetStatus()
	return status.IsAvailable() && status.GetState() == proto.DataServerState_DATA_SERVER_STATE_RUNNING
}

func (c *runtime) RequestBackup(namespace string, shard *int64) (int, error) {
	if c.backupScheduler == nil {
		return 0, ErrBackupsUnsupported
	}
	return c.backupScheduler.RequestBackup(namespace, shard), nil
}

func (c *runtime) ListBackups(ctx context.Context, req *proto.ListBackupsRequest,
	visit func(*proto.ListBackupsResponse) error) error {
	backupRPC, node, destination, err := c.backupCaller(req.GetDestination())
	if err != nil {
		return err
	}
	req = &proto.ListBackupsRequest{
		Destination:            destination,
		Namespace:              req.GetNamespace(),
		Shard:                  req.Shard,
		NewestAtOrBeforeMillis: req.NewestAtOrBeforeMillis,
	}
	if err := backupRPC.ListBackups(ctx, node, req, visit); err != nil {
		return fmt.Errorf("list the backups through %s: %w", node.GetNameOrDefault(), err)
	}
	return nil
}

// backupCaller returns the backup service, a running data server to call
// it on, and the destination: the one given, or the one of the policy.
func (c *runtime) backupCaller(destination string) (rpc.BackupProvider, *proto.DataServerIdentity, string, error) {
	backupRPC, ok := c.rpc.(rpc.BackupProvider)
	if !ok {
		return nil, nil, "", ErrBackupsUnsupported
	}
	if destination == "" {
		config, err := c.metadata.GetConfig()
		if err != nil {
			return nil, nil, "", err
		}
		destination = config.UnsafeBorrow().GetBackupWithDefaults().GetDestination()
		if destination == "" {
			return nil, nil, "", ErrNoBackupDestination
		}
	}
	dataServers, err := c.metadata.ListDataServer()
	if err != nil {
		return nil, nil, "", err
	}
	names := make([]string, 0, len(dataServers))
	for name := range dataServers {
		if c.isDataServerRunning(name) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil, nil, "", fmt.Errorf("%w: no data server is running", constant.ErrResourceUnavailable)
	}
	slices.Sort(names)
	return backupRPC, dataServers[names[0]].UnsafeBorrow().GetIdentity(), destination, nil
}
