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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
)

// backupRPCProvider is the test provider with the backup service.
type backupRPCProvider struct {
	*rpctest.RpcProvider
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
	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return &backupRPCProvider{rpctest.NewRpcProvider()} })
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
