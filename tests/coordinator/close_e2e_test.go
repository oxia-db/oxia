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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordserver "github.com/oxia-db/oxia/oxiad/coordinator"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
)

// Closing the coordinator must not hang while an election is retrying a shard
// status write that keeps failing, as it happens after the coordinator lost
// its leadership or when the metadata store is unreachable.
func TestCoordinator_CloseWhileStatusWritesFail(t *testing.T) {
	// The status writes are made to fail through the file permissions
	readOnly := filepath.Join(t.TempDir(), "read-only")
	require.NoError(t, os.WriteFile(readOnly, nil, 0o400))
	if f, err := os.OpenFile(readOnly, os.O_WRONLY, 0); err == nil {
		require.NoError(t, f.Close())
		t.Skip("file permissions are not enforced for this user")
	}

	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	dataServers := []*proto.DataServerIdentity{sa1, sa2, sa3}

	metadataDir := t.TempDir()
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              constant.DefaultNamespace,
		ReplicationFactor: 3,
		InitialShardCount: 1,
	}}, dataServers)
	configData, err := metadatacodec.ClusterConfigCodec.MarshalYAML(clusterConfig)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(metadataDir, coordoption.DefaultFileConfigName), configData, 0o600))

	adminAddr := freeAddress(t)
	options := coordoption.NewDefaultOptions()
	options.Server.Internal.BindAddress = "127.0.0.1:0"
	options.Server.Public.BindAddress = adminAddr
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.ProviderName = metadatacommon.NameFile
	options.Metadata.File.Dir = metadataDir

	coordinatorServer, err := coordserver.NewGrpcServer(t.Context(), commonwatch.New(options))
	require.NoError(t, err)

	adminClient, err := oxia.NewAdminClient(adminAddr, nil, nil)
	require.NoError(t, err)

	requireDataServersState := func(state proto.DataServerState) {
		t.Helper()
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			for _, ds := range dataServers {
				view, err := adminClient.GetDataServer(t.Context(), ds.GetNameOrDefault())
				if assert.NoError(c, err) {
					assert.Equal(c, state, view.GetDataServerStatus().GetState())
				}
			}
		}, 30*time.Second, 50*time.Millisecond)
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ns, err := adminClient.GetNamespace(t.Context(), constant.DefaultNamespace)
		if assert.NoError(c, err) {
			shard := ns.GetNamespaceStatus().GetShards()[0]
			assert.Equal(c, proto.ShardStatusSteadyState, shard.GetStatusOrDefault())
			assert.NotNil(c, shard.GetLeader())
		}
	}, 30*time.Second, 50*time.Millisecond)
	requireDataServersState(proto.DataServerState_DATA_SERVER_STATE_RUNNING)

	// From now on, every shard status write fails
	require.NoError(t, os.Chmod(filepath.Join(metadataDir, coordoption.DefaultFileStatusName), 0o400))

	// Losing the shard leader starts an election, which keeps retrying to
	// persist the new term
	require.NoError(t, s1.Close())
	require.NoError(t, s2.Close())
	require.NoError(t, s3.Close())
	requireDataServersState(proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE)
	require.NoError(t, adminClient.Close())

	closed := make(chan error, 1)
	go func() {
		closed <- coordinatorServer.Close()
	}()
	select {
	case err := <-closed:
		assert.NoError(t, err)
	case <-time.After(60 * time.Second):
		require.FailNow(t, "the coordinator did not close")
	}
}
