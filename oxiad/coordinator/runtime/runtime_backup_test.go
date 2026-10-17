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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
)

func backupInfo(shard int64, at time.Time, minHash, maxHash uint32, config *proto.Namespace) *proto.BackupInfo {
	return &proto.BackupInfo{
		Shard:           shard,
		BackupId:        backup.NewID(at, "lineage").String(),
		TimestampMillis: at.UnixMilli(),
		HashRange:       &proto.HashRange{Min: minHash, Max: maxHash},
		NamespaceConfig: config,
	}
}

func TestPlanRestoreTakesTheConfigurationOfTheNewestBackup(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	old := &proto.Namespace{Name: "orders", ReplicationFactor: 3, KeySorting: "natural"}
	newer := &proto.Namespace{Name: "orders", ReplicationFactor: 2, KeySorting: "natural"}
	infos := []*proto.BackupInfo{
		backupInfo(1, now.Add(-2*time.Hour), 0, ^uint32(0), old),
		backupInfo(2, now.Add(-time.Hour), 0, 1<<31-1, newer),
	}
	req := &proto.RestoreNamespaceRequest{SourceNamespace: "orders", TargetNamespace: "orders-restored",
		AsOfMillis: now.UnixMilli()}
	plan, config, err := planRestore(infos, req)
	require.NoError(t, err)
	require.Len(t, plan, 2)
	assert.EqualValues(t, 2, plan[0].Source.Shard)
	assert.EqualValues(t, 1, plan[1].Source.Shard)
	assert.Equal(t, "orders-restored", config.GetName())
	assert.EqualValues(t, 2, config.GetInitialShardCount())
	assert.EqualValues(t, 2, config.GetReplicationFactor(), "the configuration of the newest backup")
	assert.Equal(t, "natural", config.GetKeySorting())
}

func TestPlanRestoreFailsWithoutABackupAtTheTime(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	infos := []*proto.BackupInfo{
		backupInfo(1, now, 0, ^uint32(0), &proto.Namespace{ReplicationFactor: 3}),
	}
	req := &proto.RestoreNamespaceRequest{SourceNamespace: "orders", TargetNamespace: "orders-restored",
		AsOfMillis: now.Add(-time.Hour).UnixMilli()}
	_, _, err := planRestore(infos, req)
	assert.ErrorIs(t, err, backup.ErrNoBackup)
}

func TestPlanRestoreRejectsABackupWithoutReplicationFactor(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	infos := []*proto.BackupInfo{backupInfo(1, now, 0, ^uint32(0), &proto.Namespace{})}
	req := &proto.RestoreNamespaceRequest{SourceNamespace: "orders", TargetNamespace: "orders-restored",
		AsOfMillis: now.UnixMilli()}
	_, _, err := planRestore(infos, req)
	assert.ErrorIs(t, err, backup.ErrInvalidManifest)
}
