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

package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/tests/mock"
)

// TestCoordinatorBackupAndRestore drives the backups end to end: a cluster
// with the backup policy enabled backs the shard up on its own, through a
// follower, and a restore as of a time between two backups brings back the
// records deleted after it, into a new namespace. It runs once with the
// SST files compressed on upload and once with them stored as they are.
func TestCoordinatorBackupAndRestore(t *testing.T) {
	t.Run("compressed", func(t *testing.T) { testCoordinatorBackupAndRestore(t, true) })
	t.Run("plain", func(t *testing.T) { testCoordinatorBackupAndRestore(t, false) })
}

func testCoordinatorBackupAndRestore(t *testing.T, compression bool) {
	t.Helper()
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	// The servers close after the coordinator: their databases must outlive
	// the backups and the restores it drives, and close before the test's
	// directories go
	defer func() {
		for _, s := range []*dataserver.Server{s1, s2, s3} {
			assert.NoError(t, s.Close())
		}
	}()

	metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              constant.DefaultNamespace,
		ReplicationFactor: 3,
		InitialShardCount: 1,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})
	destination := "mem://" + t.Name()
	clusterConfig.Backup = &proto.BackupConfig{
		Enabled:          true,
		Destination:      destination,
		Interval:         "1s",
		Retention:        "1h",
		ScheduleInterval: "100ms",
		Compression:      compression,
	}
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   clusterConfig,
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)

	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))
	defer func() { assert.NoError(t, coordinatorInstance.Close()) }()
	metadata := coordinatorInstance.Metadata()
	require.Eventually(t, func() bool {
		shard := mock.StatusSnapshot(t, metadata).Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.GetStatusOrDefault() == proto.ShardStatusSteadyState
	}, 30*time.Second, 100*time.Millisecond)

	ctx := context.Background()
	client, err := oxia.NewSyncClient(sa1.Public)
	require.NoError(t, err)
	defer func() { assert.NoError(t, client.Close()) }()
	const keys = 100
	for i := range keys {
		_, _, err := client.Put(ctx, fmt.Sprintf("key-%03d", i), []byte(fmt.Sprintf("value-%d", i)))
		require.NoError(t, err)
	}

	// The first backup holds every key
	var first *proto.BackupInfo
	require.Eventually(t, func() bool {
		backups, err := coordinatorInstance.ListBackups(ctx, constant.DefaultNamespace, nil, "")
		if err != nil {
			t.Logf("ListBackups: %v (%T)", err, err)
			return false
		}
		for _, b := range backups {
			if b.GetCommitOffset() >= keys-1 {
				first = b
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)
	shard := mock.StatusSnapshot(t, metadata).Namespaces[constant.DefaultNamespace].Shards[0]
	require.NotNil(t, shard.GetBackupReplica())
	assert.NotEqual(t, shard.GetLeader().GetNameOrDefault(), shard.GetBackupReplica().GetNameOrDefault(),
		"the backups come from a follower")
	assert.Equal(t, constant.DefaultNamespace, first.GetNamespaceConfig().GetName())
	assert.EqualValues(t, 3, first.GetNamespaceConfig().GetReplicationFactor())
	asOf := time.Now()

	// Half the keys are deleted, and backed up gone
	for i := 0; i < keys; i += 2 {
		require.NoError(t, client.Delete(ctx, fmt.Sprintf("key-%03d", i)))
	}
	require.Eventually(t, func() bool {
		backups, err := coordinatorInstance.ListBackups(ctx, constant.DefaultNamespace, nil, "")
		require.NoError(t, err)
		for _, b := range backups {
			if b.GetCommitOffset() >= keys+keys/2-1 {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	// The restore as of the time between the backups brings every key back
	res, err := coordinatorInstance.RestoreNamespace(ctx, &proto.RestoreNamespaceRequest{
		SourceNamespace: constant.DefaultNamespace,
		TargetNamespace: "restored",
		AsOfMillis:      asOf.UnixMilli(),
	})
	require.NoError(t, err)
	require.Len(t, res.GetShards(), 1)
	assert.Equal(t, first.GetBackupId(), res.GetShards()[0].GetBackupId())
	assert.EqualValues(t, 0, res.GetShards()[0].GetSourceShard())

	var restoredShard int64
	require.Eventually(t, func() bool {
		ns, ok := mock.StatusSnapshot(t, metadata).Namespaces["restored"]
		if !ok {
			return false
		}
		for id, meta := range ns.Shards {
			if meta.GetStatusOrDefault() != proto.ShardStatusSteadyState || meta.GetLeader() == nil ||
				meta.GetRestore() != nil {
				return false
			}
			restoredShard = id
		}
		return len(ns.Shards) == 1
	}, 60*time.Second, 100*time.Millisecond)
	assert.Equal(t, res.GetShards()[0].GetShard(), restoredShard)

	restored, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace("restored"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, restored.Close()) }()
	restoredKeys, err := restored.List(ctx, "key-", "key.")
	require.NoError(t, err)
	assert.Len(t, restoredKeys, keys)
	_, value, _, err := restored.Get(ctx, "key-000")
	require.NoError(t, err)
	assert.Equal(t, "value-0", string(value))

	sourceKeys, err := client.List(ctx, "key-", "key.")
	require.NoError(t, err)
	assert.Len(t, sourceKeys, keys/2, "the source namespace is left as it is")

	// The restored namespace is backed up too
	require.Eventually(t, func() bool {
		backups, err := coordinatorInstance.ListBackups(ctx, "restored", nil, "")
		require.NoError(t, err)
		return len(backups) > 0
	}, 30*time.Second, 100*time.Millisecond)
}
