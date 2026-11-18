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
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
)

// backupRPCProvider is the test provider with the backup service. It
// answers a listing with backups, and records the request.
type backupRPCProvider struct {
	*rpctest.RpcProvider
	mu       sync.Mutex
	listReq  *proto.ListBackupsRequest
	listNode string
	listErr  error
	backups  []*proto.BackupInfo
}

// ListBackups sends each backup in its own message, as the shards of a
// namespace come.
func (p *backupRPCProvider) ListBackups(_ context.Context, node *proto.DataServerIdentity,
	req *proto.ListBackupsRequest, visit func(*proto.ListBackupsResponse) error) error {
	p.mu.Lock()
	p.listReq, p.listNode = req, node.GetNameOrDefault()
	listErr, backups := p.listErr, p.backups
	p.mu.Unlock()
	if listErr != nil {
		return listErr
	}
	for _, b := range backups {
		if err := visit(&proto.ListBackupsResponse{Backups: []*proto.BackupInfo{b}}); err != nil {
			return err
		}
	}
	return nil
}

// collectBackups lists the backups of "ns" through the runtime and gathers
// every backup sent.
func collectBackups(r Runtime, destination string) ([]*proto.BackupInfo, error) {
	var all []*proto.BackupInfo
	req := &proto.ListBackupsRequest{Namespace: "ns", Destination: destination}
	err := r.ListBackups(context.Background(), req, func(res *proto.ListBackupsResponse) error {
		all = append(all, res.GetBackups()...)
		return nil
	})
	return all, err
}

func (*backupRPCProvider) BackupShard(context.Context, *proto.DataServerIdentity, *proto.BackupShardRequest) (*proto.BackupShardResponse, error) {
	return &proto.BackupShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED}, nil
}

func (*backupRPCProvider) ExpireBackups(context.Context, *proto.DataServerIdentity, *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
	return &proto.ExpireBackupsResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_DONE}, nil
}

func (*backupRPCProvider) RestoreShard(context.Context, *proto.DataServerIdentity, *proto.RestoreShardRequest) (*proto.RestoreShardResponse, error) {
	return &proto.RestoreShardResponse{State: proto.BackupJobState_BACKUP_JOB_STATE_DONE}, nil
}

func TestRuntimeNewStartsTheBackupSchedulerWhenTheProviderServesBackups(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers: []*proto.DataServerIdentity{{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}},
	})
	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider {
		return &backupRPCProvider{RpcProvider: rpctest.NewRpcProvider()}
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	rt := r.(*runtime)
	assert.NotNil(t, rt.backupScheduler, "the scheduler runs when the provider serves the backup calls")
	assert.False(t, rt.isDataServerRunning("unknown"))
	// A data server runs for the backups once it answers its health checks,
	// until it drains
	running := newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_RUNNING)
	running.status.SetAvailable(true)
	draining := newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_DRAINING)
	draining.status.SetAvailable(true)
	rt.Lock()
	rt.dataServerControllers["running"] = running
	rt.dataServerControllers["draining"] = draining
	rt.dataServerControllers["unreachable"] = newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_RUNNING)
	rt.Unlock()
	assert.True(t, rt.isDataServerRunning("running"))
	assert.False(t, rt.isDataServerRunning("draining"))
	assert.False(t, rt.isDataServerRunning("unreachable"))

	plain, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, plain.Close()) })
	assert.Nil(t, plain.(*runtime).backupScheduler)
}

func TestRuntimeRequestBackupNeedsTheScheduler(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	plain, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, plain.Close()) })
	_, err = plain.RequestBackup("ns", nil)
	assert.ErrorIs(t, err, ErrBackupsUnsupported)
	_, err = collectBackups(plain, "")
	assert.ErrorIs(t, err, ErrBackupsUnsupported)

	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider {
		return &backupRPCProvider{RpcProvider: rpctest.NewRpcProvider()}
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	count, err := r.RequestBackup("ns", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "no shard to make due")
}

func TestRuntimeListBackupsGoesThroughARunningDataServer(t *testing.T) {
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: constant.DefaultNamespace, InitialShardCount: 1, ReplicationFactor: 1}},
		Servers:    []*proto.DataServerIdentity{server},
		Backup:     &proto.BackupConfig{Enabled: true, Destination: "s3://bucket/prefix"},
	})
	// A data server is tracked once a shard is assigned to it
	_, err := metadata.AllocateShardIDs(1)
	require.NoError(t, err)
	require.NoError(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{0: {
			Status:         proto.ShardStatusSteadyState,
			Term:           1,
			Leader:         server,
			Ensemble:       []*proto.DataServerIdentity{server},
			Int32HashRange: &proto.HashRange{Min: 0, Max: math.MaxUint32},
		}},
	}))
	var failConfig, failServers atomic.Bool
	failing := &fakeMetadata{
		Metadata: metadata,
		getConfig: func() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
			if failConfig.Load() {
				return commonobject.Borrowed[*proto.ClusterConfiguration]{}, errors.New("metadata unavailable")
			}
			return metadata.GetConfig()
		},
		listDataServer: func() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
			if failServers.Load() {
				return nil, errors.New("metadata unavailable")
			}
			return metadata.ListDataServer()
		},
	}
	provider := &backupRPCProvider{RpcProvider: rpctest.NewRpcProvider(), backups: []*proto.BackupInfo{{Shard: 1}}}
	r, err := New(t.Context(), failing, func(string) rpc.ControllerRPCProvider { return provider })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	require.Eventually(t, func() bool { return r.(*runtime).isDataServerRunning("s1") }, 10*time.Second, 10*time.Millisecond)

	backups, err := collectBackups(r, "")
	require.NoError(t, err)
	assert.Len(t, backups, 1)
	provider.mu.Lock()
	assert.Equal(t, "s3://bucket/prefix", provider.listReq.GetDestination(), "the destination of the policy")
	assert.Equal(t, "s1", provider.listNode)
	provider.mu.Unlock()
	_, err = collectBackups(r, "s3://other")
	require.NoError(t, err)
	provider.mu.Lock()
	assert.Equal(t, "s3://other", provider.listReq.GetDestination(), "the destination given")
	provider.mu.Unlock()
	cutoff := int64(1791037507000)
	require.NoError(t, r.ListBackups(context.Background(), &proto.ListBackupsRequest{Namespace: "ns",
		NewestAtOrBeforeMillis: &cutoff}, func(*proto.ListBackupsResponse) error { return nil }))
	provider.mu.Lock()
	assert.Equal(t, cutoff, provider.listReq.GetNewestAtOrBeforeMillis(), "the request reaches the data server as asked")
	provider.listErr = errors.New("bucket unavailable")
	provider.mu.Unlock()
	_, err = collectBackups(r, "")
	assert.ErrorContains(t, err, "list the backups through s1")

	failServers.Store(true)
	_, err = collectBackups(r, "")
	assert.ErrorContains(t, err, "metadata unavailable")
	failServers.Store(false)
	failConfig.Store(true)
	_, err = collectBackups(r, "")
	assert.ErrorContains(t, err, "metadata unavailable")
	failConfig.Store(false)
}

func TestRuntimeListBackupsNeedsADestination(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{Backup: &proto.BackupConfig{Enabled: false}})
	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider {
		return &backupRPCProvider{RpcProvider: rpctest.NewRpcProvider()}
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	_, err = collectBackups(r, "")
	assert.ErrorIs(t, err, ErrNoBackupDestination)
}

func TestRuntimeListBackupsNeedsARunningDataServer(t *testing.T) {
	// A server without a shard has no controller, so it is not running
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers: []*proto.DataServerIdentity{{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}},
		Backup:  &proto.BackupConfig{Enabled: true, Destination: "s3://bucket/prefix"},
	})
	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider {
		return &backupRPCProvider{RpcProvider: rpctest.NewRpcProvider()}
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	_, err = collectBackups(r, "")
	assert.ErrorIs(t, err, constant.ErrResourceUnavailable)
}

func backupInfo(shard int64, at time.Time, minHash, maxHash uint32, config *proto.Namespace) *proto.BackupInfo {
	return &proto.BackupInfo{
		Shard:           shard,
		BackupId:        backup.NewID(at, "lineage").String(),
		TimestampMillis: at.UnixMilli(),
		Term:            40 + shard,
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
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
}

func TestPlanRestoreRejectsAnInvalidBackupID(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	infos := []*proto.BackupInfo{{Shard: 1, BackupId: "not-an-id", TimestampMillis: now.UnixMilli()}}
	req := &proto.RestoreNamespaceRequest{SourceNamespace: "orders", TargetNamespace: "orders-restored",
		AsOfMillis: now.UnixMilli()}
	_, _, err := planRestore(infos, req)
	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
}

// restoreTestCluster is a runtime over one running data server, with a
// backup policy, whose provider lists the backups of the source namespace.
// The named metadata call fails while failing holds its name.
type restoreTestCluster struct {
	runtime  Runtime
	metadata coordmetadata.Metadata
	provider *backupRPCProvider
	failing  atomic.Pointer[string]
}

var errMetadataUnavailable = errors.New("metadata unavailable")

func (c *restoreTestCluster) fails(call string) bool {
	return c.failing.Load() != nil && *c.failing.Load() == call
}

func (c *restoreTestCluster) fail(call string) {
	c.failing.Store(&call)
}

func newRestoreTestCluster(t *testing.T, backups []*proto.BackupInfo) *restoreTestCluster {
	t.Helper()
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: constant.DefaultNamespace, InitialShardCount: 1, ReplicationFactor: 1}},
		Servers:    []*proto.DataServerIdentity{server},
		Backup:     &proto.BackupConfig{Enabled: true, Destination: "s3://bucket/prefix"},
	})
	// A data server is tracked once a shard is assigned to it
	_, err := metadata.AllocateShardIDs(1)
	require.NoError(t, err)
	require.NoError(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{0: {
			Status:         proto.ShardStatusSteadyState,
			Term:           1,
			Leader:         server,
			Ensemble:       []*proto.DataServerIdentity{server},
			Int32HashRange: &proto.HashRange{Min: 0, Max: math.MaxUint32},
		}},
	}))
	c := &restoreTestCluster{metadata: metadata}
	failing := &fakeMetadata{
		Metadata: metadata,
		getNamespace: func(name string) (commonobject.Borrowed[*proto.Namespace], bool, error) {
			if c.fails("GetNamespace") {
				return commonobject.Borrowed[*proto.Namespace]{}, false, errMetadataUnavailable
			}
			return metadata.GetNamespace(name)
		},
		getNamespaceStatus: func(namespace string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
			if c.fails("GetNamespaceStatus") {
				return commonobject.Borrowed[*proto.NamespaceStatus]{}, false, errMetadataUnavailable
			}
			return metadata.GetNamespaceStatus(namespace)
		},
		allocateShardIDs: func(count uint32) (int64, error) {
			if c.fails("AllocateShardIDs") {
				return 0, errMetadataUnavailable
			}
			return metadata.AllocateShardIDs(count)
		},
		listNamespaceStatus: func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
			if c.fails("ListNamespaceStatus") {
				return nil, errMetadataUnavailable
			}
			return metadata.ListNamespaceStatus()
		},
		createNamespaceStatus: func(namespace string, status *proto.NamespaceStatus) error {
			if c.fails("CreateNamespaceStatus") {
				return errMetadataUnavailable
			}
			return metadata.CreateNamespaceStatus(namespace, status)
		},
		createNamespace: func(namespace *proto.Namespace) error {
			if c.fails("CreateNamespace") {
				return errMetadataUnavailable
			}
			return metadata.CreateNamespace(namespace)
		},
	}
	c.provider = &backupRPCProvider{RpcProvider: rpctest.NewRpcProvider(), backups: backups}
	r, err := New(t.Context(), failing, func(string) rpc.ControllerRPCProvider { return c.provider })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	require.Eventually(t, func() bool { return r.(*runtime).isDataServerRunning("s1") }, 10*time.Second, 10*time.Millisecond)
	c.runtime = r
	return c
}

func restoreRequest(now time.Time) *proto.RestoreNamespaceRequest {
	return &proto.RestoreNamespaceRequest{SourceNamespace: "orders", TargetNamespace: "orders-restored",
		AsOfMillis: now.UnixMilli()}
}

func TestRuntimeRestoreNamespaceCreatesTheShardsFromTheBackups(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	config := &proto.Namespace{Name: "orders", ReplicationFactor: 1}
	// The newer backup of shard 2 covers the right half; the left half comes
	// from the older backup of shard 1
	older, newer := backupInfo(1, now.Add(-2*time.Hour), 0, math.MaxUint32, config), backupInfo(2, now.Add(-time.Hour), 1<<31, math.MaxUint32, config)
	c := newRestoreTestCluster(t, []*proto.BackupInfo{older, newer})

	res, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	require.NoError(t, err)
	require.Len(t, res.GetShards(), 2)
	assert.EqualValues(t, 1, res.GetShards()[0].GetSourceShard())
	assert.Equal(t, older.GetBackupId(), res.GetShards()[0].GetBackupId())
	assert.EqualValues(t, 1<<31-1, res.GetShards()[0].GetHashRange().GetMax())
	assert.EqualValues(t, 2, res.GetShards()[1].GetSourceShard())
	assert.EqualValues(t, 1<<31, res.GetShards()[1].GetHashRange().GetMin())
	assert.NotEqual(t, res.GetShards()[0].GetShard(), res.GetShards()[1].GetShard())

	borrowed, exists, err := c.metadata.GetNamespace("orders-restored")
	require.NoError(t, err)
	require.True(t, exists, "the namespace is created last")
	assert.EqualValues(t, 2, borrowed.UnsafeBorrow().GetInitialShardCount())
	status, exists, err := c.metadata.GetNamespaceStatus("orders-restored")
	require.NoError(t, err)
	require.True(t, exists)
	for _, shard := range res.GetShards() {
		meta := status.UnsafeBorrow().GetShards()[shard.GetShard()]
		require.NotNil(t, meta)
		assert.Equal(t, "s3://bucket/prefix", meta.GetRestore().GetDestination(), "the destination of the policy")
		assert.Equal(t, "orders", meta.GetRestore().GetSourceNamespace())
		assert.Equal(t, shard.GetBackupId(), meta.GetRestore().GetBackupId())
		assert.EqualValues(t, 40+shard.GetSourceShard(), meta.GetTerm(), "the first election fences above the term of the backup")
	}
	c.provider.mu.Lock()
	assert.Equal(t, "orders", c.provider.listReq.GetNamespace())
	assert.Nil(t, c.provider.listReq.Shard, "every shard of the source is listed")
	assert.Equal(t, now.UnixMilli(), c.provider.listReq.GetNewestAtOrBeforeMillis(), "one backup per shard, as of the time")
	c.provider.mu.Unlock()

	// The namespace exists now
	_, err = c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorIs(t, err, errs.ErrAlreadyExists)
}

func TestRuntimeRestoreNamespaceResumesAnInterruptedRestore(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	info := backupInfo(1, now.Add(-time.Hour), 0, math.MaxUint32, &proto.Namespace{Name: "orders", ReplicationFactor: 1})
	c := newRestoreTestCluster(t, []*proto.BackupInfo{info})
	// A restore that crashed after creating the status, before the namespace
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	require.NoError(t, c.metadata.CreateNamespaceStatus("orders-restored", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{7: {
			Status:         proto.ShardStatusUnknown,
			Term:           -1,
			Ensemble:       []*proto.DataServerIdentity{server},
			Int32HashRange: &proto.HashRange{Min: 0, Max: math.MaxUint32},
			Restore: &proto.RestoreMetadata{Destination: "s3://bucket/prefix", SourceNamespace: "orders",
				SourceShardId: 1, BackupId: info.GetBackupId()},
		}},
	}))

	res, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	require.NoError(t, err)
	require.Len(t, res.GetShards(), 1)
	assert.EqualValues(t, 7, res.GetShards()[0].GetShard(), "the shard of the interrupted restore")
	_, exists, err := c.metadata.GetNamespace("orders-restored")
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestRuntimeRestoreNamespaceRefusesAStatusOfOtherBackups(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	info := backupInfo(1, now.Add(-time.Hour), 0, math.MaxUint32, &proto.Namespace{Name: "orders", ReplicationFactor: 1})
	c := newRestoreTestCluster(t, []*proto.BackupInfo{info})
	// The status of an earlier request, for another backup
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	require.NoError(t, c.metadata.CreateNamespaceStatus("orders-restored", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{7: {
			Status:         proto.ShardStatusUnknown,
			Term:           41,
			Ensemble:       []*proto.DataServerIdentity{server},
			Int32HashRange: &proto.HashRange{Min: 0, Max: math.MaxUint32},
			Restore: &proto.RestoreMetadata{Destination: "s3://bucket/prefix", SourceNamespace: "orders",
				SourceShardId: 1, BackupId: "2026-10-03T12-00-00Z-other"},
		}},
	}))

	_, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorIs(t, err, errs.ErrAlreadyExists)
	assert.ErrorContains(t, err, "other backups")

	// As many shards as the plan has, at least
	require.NoError(t, c.metadata.PatchNamespaceStatus("orders-restored", func(status *proto.NamespaceStatus) (bool, error) {
		status.Shards[8] = status.Shards[7]
		return true, nil
	}))
	_, err = c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorIs(t, err, errs.ErrAlreadyExists)
}

func TestRuntimeRestoreNamespaceRefusesAStatusThatIsNotARestore(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	info := backupInfo(1, now.Add(-time.Hour), 0, math.MaxUint32, &proto.Namespace{Name: "orders", ReplicationFactor: 1})
	c := newRestoreTestCluster(t, []*proto.BackupInfo{info})
	require.NoError(t, c.metadata.CreateNamespaceStatus("orders-restored", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards:            map[int64]*proto.ShardMetadata{7: {Status: proto.ShardStatusSteadyState, Term: 1}},
	}))

	_, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorIs(t, err, errs.ErrAlreadyExists)
}

func TestRuntimeRestoreNamespaceNeedsTheBackupService(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Backup: &proto.BackupConfig{Enabled: true, Destination: "s3://bucket/prefix"},
	})
	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })
	_, err = r.RestoreNamespace(context.Background(), restoreRequest(time.Now()))
	assert.ErrorIs(t, err, ErrBackupsUnsupported)
}

func TestRuntimeRestoreNamespaceReportsTheFailures(t *testing.T) {
	now := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC)
	info := backupInfo(1, now.Add(-time.Hour), 0, math.MaxUint32, &proto.Namespace{Name: "orders", ReplicationFactor: 1})
	c := newRestoreTestCluster(t, []*proto.BackupInfo{info})

	// The listing fails
	c.provider.mu.Lock()
	c.provider.listErr = errors.New("bucket unavailable")
	c.provider.mu.Unlock()
	_, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorContains(t, err, "list the backups through s1")
	// No backup is old enough
	c.provider.mu.Lock()
	c.provider.listErr = nil
	c.provider.mu.Unlock()
	_, err = c.runtime.RestoreNamespace(context.Background(), restoreRequest(now.Add(-2*time.Hour)))
	assert.ErrorIs(t, err, backup.ErrNoBackup)
	// The ensemble of a restored shard can't be selected
	c.provider.mu.Lock()
	c.provider.backups = []*proto.BackupInfo{backupInfo(1, now.Add(-time.Hour), 0, math.MaxUint32,
		&proto.Namespace{Name: "orders", ReplicationFactor: 3})}
	c.provider.mu.Unlock()
	_, err = c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
	assert.ErrorIs(t, err, errs.ErrNotFound)
	c.provider.mu.Lock()
	c.provider.backups = []*proto.BackupInfo{info}
	c.provider.mu.Unlock()

	// The metadata fails
	for _, call := range []string{"GetNamespace", "GetNamespaceStatus", "AllocateShardIDs", "ListNamespaceStatus",
		"CreateNamespaceStatus", "CreateNamespace"} {
		t.Run(call, func(t *testing.T) {
			c.fail(call)
			_, err := c.runtime.RestoreNamespace(context.Background(), restoreRequest(now))
			assert.ErrorIs(t, err, errMetadataUnavailable)
		})
	}
}
