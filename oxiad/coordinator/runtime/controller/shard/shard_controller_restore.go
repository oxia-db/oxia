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

package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
)

// This file owns the restore of a shard from a backup, which runs before
// the first election of a shard created by a restore (see
// runtime.RestoreNamespace).

// restorePollInterval is how often the controller asks a data server about
// the restore it runs.
const restorePollInterval = time.Second

// ErrRestoreUnsupported is returned when the RPC provider has no OxiaBackup
// service to restore a shard with.
var ErrRestoreUnsupported = errors.New("the RPC provider has no backup service: the shard cannot be restored")

// restoreShard has every member of the ensemble install the backup the
// shard metadata names, then clears the mark. A failure is returned for the
// caller to retry the whole restore: a data server starts a new
// installation on the next request after a failed one.
func (s *controller) restoreShard(shardMeta *proto.ShardMetadata) error {
	backupRPC, ok := s.rpc.(rpc.BackupProvider)
	if !ok {
		return ErrRestoreUnsupported
	}
	restore := shardMeta.GetRestore()
	s.logger.Info("Restoring the shard from a backup", slog.String("backup-id", restore.GetBackupId()),
		slog.String("source-namespace", restore.GetSourceNamespace()),
		slog.Int64("source-shard", restore.GetSourceShardId()))
	req := &proto.RestoreShardRequest{
		Namespace:       s.namespace,
		Shard:           s.shard,
		HashRange:       shardMeta.GetInt32HashRange(),
		Destination:     restore.GetDestination(),
		SourceNamespace: restore.GetSourceNamespace(),
		SourceShard:     restore.GetSourceShardId(),
		BackupId:        restore.GetBackupId(),
	}
	errs := make([]error, len(shardMeta.GetEnsemble()))
	var wg sync.WaitGroup
	for i, member := range shardMeta.GetEnsemble() {
		wg.Go(func() {
			errs[i] = s.restoreMember(backupRPC, member, req)
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	err := s.metadataStore.PatchNamespaceStatus(s.namespace, func(status *proto.NamespaceStatus) bool {
		current, ok := status.GetShards()[s.shard]
		if !ok || current.GetRestore() == nil {
			return false
		}
		current.Restore = nil
		return true
	})
	if err != nil {
		return fmt.Errorf("clear the restore of the shard: %w", err)
	}
	s.logger.Info("Restored the shard from a backup", slog.String("backup-id", restore.GetBackupId()))
	return nil
}

// restoreMember asks a member to install the backup, and waits for the end
// of the installation.
func (s *controller) restoreMember(backupRPC rpc.BackupProvider, member *proto.DataServerIdentity,
	req *proto.RestoreShardRequest) error {
	timer := time.NewTimer(restorePollInterval)
	defer timer.Stop()
	for {
		res, err := backupRPC.RestoreShard(s.ctx, member, req)
		if err != nil {
			return fmt.Errorf("restore the shard on %s: %w", member.GetNameOrDefault(), err)
		}
		switch res.GetState() {
		case proto.BackupJobState_BACKUP_JOB_STATE_DONE:
			s.logger.Info("A member installed the backup", slog.String("member", member.GetNameOrDefault()),
				slog.Int64("commit-offset", res.GetCommitOffset()), slog.Int64("term", res.GetTerm()))
			return nil
		case proto.BackupJobState_BACKUP_JOB_STATE_FAILED:
			return fmt.Errorf("restore the shard on %s: %s", member.GetNameOrDefault(), res.GetError())
		case proto.BackupJobState_BACKUP_JOB_STATE_STARTED, proto.BackupJobState_BACKUP_JOB_STATE_RUNNING:
			timer.Reset(restorePollInterval)
			select {
			case <-timer.C:
			case <-s.ctx.Done():
				return context.Cause(s.ctx)
			}
		default:
			return fmt.Errorf("restore the shard on %s: unexpected state %s", member.GetNameOrDefault(), res.GetState())
		}
	}
}
