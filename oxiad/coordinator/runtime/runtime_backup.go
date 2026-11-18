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
	"log/slog"
	"slices"
	"time"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	backupscheduler "github.com/oxia-db/oxia/oxiad/coordinator/runtime/backup"
)

// This file owns the backup operations of the runtime: the scheduler, and
// the admin operations on the backups. The shard controllers run the
// restores (see shard_controller_restore.go).

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

func (c *runtime) RestoreNamespace(ctx context.Context, req *proto.RestoreNamespaceRequest) (*proto.RestoreNamespaceResponse, error) {
	backupRPC, node, destination, err := c.backupCaller(req.GetDestination())
	if err != nil {
		return nil, err
	}
	if _, exists, err := c.metadata.GetNamespace(req.GetTargetNamespace()); err != nil {
		return nil, err
	} else if exists {
		return nil, fmt.Errorf("%w: namespace %s", errs.ErrAlreadyExists, req.GetTargetNamespace())
	}

	// The newest backup of each shard at or before the time is all the plan
	// needs: the listing is bounded by the shard count
	var infos []*proto.BackupInfo
	err = backupRPC.ListBackups(ctx, node, &proto.ListBackupsRequest{
		Destination:            destination,
		Namespace:              req.GetSourceNamespace(),
		NewestAtOrBeforeMillis: &req.AsOfMillis,
	}, func(res *proto.ListBackupsResponse) error {
		infos = append(infos, res.GetBackups()...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list the backups through %s: %w", node.GetNameOrDefault(), err)
	}
	plan, config, err := planRestore(infos, req)
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
		if part.Source.Compare(newest) > 0 {
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
		return nil, nil, fmt.Errorf("%w: invalid backup manifest: the backup %s records no replication factor", errs.ErrInvalidArgument,
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
		if err := sameRestore(target, borrowed.UnsafeBorrow(), destination, source, plan); err != nil {
			return nil, err
		}
		return restoredShards(borrowed.UnsafeBorrow()), nil
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
		c.RLock()
		ensemble, err := c.placeShard(target, config, status, nil)
		c.RUnlock()
		if err != nil {
			return nil, fmt.Errorf("select the ensemble of shard %d: %w", shard, err)
		}
		namespaceStatus.Shards[shard] = &proto.ShardMetadata{
			Status: proto.ShardStatusUnknown,
			// The installed database holds the term of the backup: the first
			// election fences above it
			Term:           part.Source.Term,
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
	return restoredShards(namespaceStatus), nil
}

// sameRestore checks that the status of the target namespace, which an
// earlier request created, restores the backups of plan from source at
// destination, to the same ranges: a request for other backups fails
// rather than take the shards of the earlier one for its own.
func sameRestore(target string, status *proto.NamespaceStatus, destination string, source string,
	plan []backup.Part) error {
	parts := make(map[backup.Range]backup.Part, len(plan))
	for _, part := range plan {
		parts[part.Range] = part
	}
	if len(status.GetShards()) != len(parts) {
		return fmt.Errorf("%w: namespace %s is being restored from other backups", errs.ErrAlreadyExists, target)
	}
	for _, meta := range status.GetShards() {
		restore := meta.GetRestore()
		if restore == nil {
			return fmt.Errorf("%w: namespace %s exists and is not being restored", errs.ErrAlreadyExists, target)
		}
		hashRange := backup.Range{Min: meta.GetInt32HashRange().GetMin(), Max: meta.GetInt32HashRange().GetMax()}
		part, ok := parts[hashRange]
		if !ok || restore.GetDestination() != destination || restore.GetSourceNamespace() != source ||
			restore.GetSourceShardId() != part.Source.Shard || restore.GetBackupId() != part.Source.ID.String() {
			return fmt.Errorf("%w: namespace %s is being restored from other backups", errs.ErrAlreadyExists, target)
		}
	}
	return nil
}

// restoredShards describes the shards of a namespace being restored, from
// its status.
func restoredShards(status *proto.NamespaceStatus) []*proto.RestoredShard {
	shards := make([]*proto.RestoredShard, 0, len(status.GetShards()))
	for id, meta := range status.GetShards() {
		restore := meta.GetRestore()
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
	return shards
}
