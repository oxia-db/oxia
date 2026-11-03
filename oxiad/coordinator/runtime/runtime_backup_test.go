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

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
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
	require.Eventually(t, func() bool {
		status, ok := r.GetLiveDataServerStatus("s1")
		return ok && status.GetState() == proto.DataServerState_DATA_SERVER_STATE_RUNNING
	}, 10*time.Second, 10*time.Millisecond)

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
