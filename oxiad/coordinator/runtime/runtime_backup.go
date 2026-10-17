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
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	backupscheduler "github.com/oxia-db/oxia/oxiad/coordinator/runtime/backup"
	dataservercontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/dataserver"
)

// This file owns the backup operations of the runtime: the scheduler, and
// the admin operations on the backups. The shard controllers run the
// restores (see shard_controller_restore.go).

var (
	// ErrBackupsUnsupported is returned when the RPC provider of the runtime
	// has no OxiaBackup service.
	ErrBackupsUnsupported = errors.New("backups are not supported by this coordinator")
	// ErrNoBackupDestination is returned by a backup operation without a
	// destination when the backup policy has none either.
	ErrNoBackupDestination = errors.New("no backup destination is configured")
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

// isDataServerRunning reports whether a data server passes its health
// checks, by name.
func (c *runtime) isDataServerRunning(name string) bool {
	c.RLock()
	defer c.RUnlock()
	nc, ok := c.dataServerControllers[name]
	return ok && nc.GetStatus().GetState() == dataservercontroller.Running
}

func (c *runtime) RequestBackup(namespace string, shard *int64) (int, error) {
	if c.backupScheduler == nil {
		return 0, ErrBackupsUnsupported
	}
	return c.backupScheduler.RequestBackup(namespace, shard), nil
}

func (c *runtime) ListBackups(ctx context.Context, namespace string, shard *int64,
	destination string) ([]*proto.BackupInfo, error) {
	backupRPC, node, destination, err := c.backupCaller(destination)
	if err != nil {
		return nil, err
	}
	res, err := backupRPC.ListBackups(ctx, node, &proto.ListBackupsRequest{
		Destination: destination,
		Namespace:   namespace,
		Shard:       shard,
	})
	if err != nil {
		return nil, fmt.Errorf("list the backups through %s: %w", node.GetNameOrDefault(), err)
	}
	return res.GetBackups(), nil
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

func (c *runtime) RestoreNamespace(ctx context.Context, req *proto.RestoreNamespaceRequest) (*proto.RestoreNamespaceResponse, error) {
	backupRPC, node, destination, err := c.backupCaller(req.GetDestination())
	if err != nil {
		return nil, err
	}
	if _, exists, err := c.metadata.GetNamespace(req.GetTargetNamespace()); err != nil {
		return nil, err
	} else if exists {
		return nil, fmt.Errorf("%w: namespace %s", metadatacommon.ErrAlreadyExists, req.GetTargetNamespace())
	}

	res, err := backupRPC.ListBackups(ctx, node, &proto.ListBackupsRequest{
		Destination: destination,
		Namespace:   req.GetSourceNamespace(),
	})
	if err != nil {
		return nil, fmt.Errorf("list the backups through %s: %w", node.GetNameOrDefault(), err)
	}
	plan, config, err := planRestore(res.GetBackups(), req)
	if err != nil {
		return nil, err
	}
	shards, err := c.createRestoredStatus(req.GetTargetNamespace(), destination, req.GetSourceNamespace(), plan, config)
	if err != nil {
		return nil, err
	}
	// The configuration goes last: the reconciler creates the shard
	// controllers from the status, which then restore the shards before
	// their first election. Restoring again after a crash in between
	// completes this step.
	if err := c.metadata.CreateNamespace(config); err != nil {
		return nil, err
	}
	c.logger.Info("Restoring namespace", slog.String("source", req.GetSourceNamespace()),
		slog.String("target", req.GetTargetNamespace()), slog.Int("shards", len(shards)),
		slog.Time("as-of", time.UnixMilli(req.GetAsOfMillis()).UTC()))
	return &proto.RestoreNamespaceResponse{Shards: shards}, nil
}

// planRestore picks the backups to restore, and the configuration of the
// target namespace: the one recorded by the newest backup, with the name of
// the target and as many initial shards as the plan has.
func planRestore(infos []*proto.BackupInfo, req *proto.RestoreNamespaceRequest) ([]backup.Part, *proto.Namespace, error) {
	summaries := make([]backup.Summary, 0, len(infos))
	configs := map[string]*proto.Namespace{}
	for _, info := range infos {
		id, err := backup.ParseID(info.GetBackupId())
		if err != nil {
			return nil, nil, err
		}
		summaries = append(summaries, backup.Summary{
			Namespace:    req.GetSourceNamespace(),
			Shard:        info.GetShard(),
			ID:           id,
			CreatedAt:    time.UnixMilli(info.GetTimestampMillis()),
			Range:        backup.Range{Min: info.GetHashRange().GetMin(), Max: info.GetHashRange().GetMax()},
			Term:         info.GetTerm(),
			CommitOffset: info.GetCommitOffset(),
		})
		configs[info.GetBackupId()] = info.GetNamespaceConfig()
	}
	plan, err := backup.Plan(summaries, time.UnixMilli(req.GetAsOfMillis()))
	if err != nil {
		return nil, nil, err
	}
	newest := plan[0].Source
	for _, part := range plan[1:] {
		if part.Source.Newer(newest) {
			newest = part.Source
		}
	}
	source := configs[newest.ID.String()]
	config := &proto.Namespace{
		Name:                 req.GetTargetNamespace(),
		InitialShardCount:    uint32(len(plan)),
		ReplicationFactor:    source.GetReplicationFactor(),
		NotificationsEnabled: source.NotificationsEnabled,
		KeySorting:           source.GetKeySorting(),
		AntiAffinities:       source.GetAntiAffinities(),
	}
	if config.ReplicationFactor == 0 {
		return nil, nil, fmt.Errorf("%w: the backup %s records no replication factor", backup.ErrInvalidManifest,
			newest.ID)
	}
	return plan, config, nil
}

// createRestoredStatus creates the status of the target namespace, with a
// shard per part of the plan marked with the backup to restore, unless a
// previous restore created it already.
func (c *runtime) createRestoredStatus(target string, destination string, source string, plan []backup.Part,
	config *proto.Namespace) ([]*proto.RestoredShard, error) {
	if borrowed, exists, err := c.metadata.GetNamespaceStatus(target); err != nil {
		return nil, err
	} else if exists {
		return restoredShards(borrowed.UnsafeBorrow())
	}
	baseShardID, err := c.metadata.AllocateShardIDs(uint32(len(plan)))
	if err != nil {
		return nil, err
	}
	status, err := c.metadata.ListNamespaceStatus()
	if err != nil {
		return nil, err
	}
	namespaceStatus := &proto.NamespaceStatus{
		Shards:            map[int64]*proto.ShardMetadata{},
		ReplicationFactor: config.GetReplicationFactor(),
	}
	status[target] = commonobject.Borrow(namespaceStatus)
	for i, part := range plan {
		shard := baseShardID + int64(i)
		ensemble, err := c.selectNewEnsemble(target, shard, config, status, nil)
		if err != nil {
			return nil, fmt.Errorf("select the ensemble of shard %d: %w", shard, err)
		}
		namespaceStatus.Shards[shard] = &proto.ShardMetadata{
			Status:         proto.ShardStatusUnknown,
			Term:           -1,
			Ensemble:       ensemble,
			Int32HashRange: &proto.HashRange{Min: part.Range.Min, Max: part.Range.Max},
			Restore: &proto.RestoreMetadata{
				Destination:     destination,
				SourceNamespace: source,
				SourceShardId:   part.Source.Shard,
				BackupId:        part.Source.ID.String(),
			},
		}
	}
	if err := c.metadata.CreateNamespaceStatus(target, namespaceStatus); err != nil {
		return nil, err
	}
	return restoredShards(namespaceStatus)
}

// restoredShards describes the shards of a restored namespace, from its
// status.
func restoredShards(status *proto.NamespaceStatus) ([]*proto.RestoredShard, error) {
	shards := make([]*proto.RestoredShard, 0, len(status.GetShards()))
	for id, meta := range status.GetShards() {
		restore := meta.GetRestore()
		if restore == nil {
			return nil, fmt.Errorf("%w: the namespace exists and is not being restored", metadatacommon.ErrAlreadyExists)
		}
		shards = append(shards, &proto.RestoredShard{
			Shard:       id,
			HashRange:   meta.GetInt32HashRange(),
			SourceShard: restore.GetSourceShardId(),
			BackupId:    restore.GetBackupId(),
		})
	}
	slices.SortFunc(shards, func(a, b *proto.RestoredShard) int {
		return int(a.GetHashRange().GetMin()>>1) - int(b.GetHashRange().GetMin()>>1)
	})
	return shards, nil
}
