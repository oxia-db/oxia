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
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

// Recreating the coordinator metadata must not require one election retry for
// every term still persisted by the data servers. Server data stays intact.
func TestCoordinator_RecoversTermAfterMetadataLoss(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := []*dataserver.Server{s1, s2, s3}
	defer func() {
		for _, s := range servers {
			assert.NoError(t, s.Close())
		}
	}()
	identities := []*proto.DataServerIdentity{sa1, sa2, sa3}

	metadataDir := t.TempDir()
	config := newClusterConfig([]*proto.Namespace{{Name: constant.DefaultNamespace, ReplicationFactor: 3, InitialShardCount: 1}}, identities)
	configData, err := metadatacodec.ClusterConfigCodec.MarshalYAML(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(metadataDir, coordoption.DefaultFileConfigName), configData, 0o600))

	start := func() (*coordserver.GrpcServer, oxia.AdminClient) {
		options := coordoption.NewDefaultOptions()
		options.Server.Internal.BindAddress = "127.0.0.1:0"
		options.Server.Public.BindAddress = freeAddress(t)
		options.Observability.Metric.Enabled = &constant.FlagFalse
		options.Metadata.ProviderName = metadatacommon.NameFile
		options.Metadata.File.Dir = metadataDir
		server, err := coordserver.NewGrpcServer(t.Context(), commonwatch.New(options))
		require.NoError(t, err)
		admin, err := oxia.NewAdminClient(options.Server.Public.BindAddress, nil, nil)
		require.NoError(t, err)
		return server, admin
	}
	elected := func(admin oxia.AdminClient, minTerm int64) *proto.ShardMetadata {
		var shard *proto.ShardMetadata
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			namespace, err := admin.GetNamespace(t.Context(), constant.DefaultNamespace)
			if !assert.NoError(c, err) {
				return
			}
			shard = namespace.GetNamespaceStatus().GetShards()[0]
			if !assert.NotNil(c, shard) {
				return
			}
			assert.Equal(c, proto.ShardStatusSteadyState, shard.GetStatusOrDefault())
			assert.GreaterOrEqual(c, shard.GetTerm(), minTerm)
		}, 10*time.Second, 20*time.Millisecond)
		return shard
	}

	coordinator, admin := start()
	shard := elected(admin, 0)
	client, err := oxia.NewSyncClient(shard.GetLeader().GetPublic())
	require.NoError(t, err)
	_, _, err = client.Put(t.Context(), "retained-key", []byte("retained-value"))
	require.NoError(t, err)
	require.NoError(t, client.Close())
	require.NoError(t, admin.Close())
	require.NoError(t, coordinator.Close())

	// Simulate an old cluster with a large term while retaining its databases.
	// The recreated coordinator loses its shard metadata but retains cluster identity.
	const retainedTerm int64 = 1000
	for _, server := range servers {
		leader, err := server.GetShardDirector().GetOrCreateLeader(constant.DefaultNamespace, 0, nil)
		require.NoError(t, err)
		_, err = leader.NewTerm(&proto.NewTermRequest{Namespace: constant.DefaultNamespace, Shard: 0, Term: retainedTerm})
		require.NoError(t, err)
	}
	statusPath := filepath.Join(metadataDir, coordoption.DefaultFileStatusName)
	statusData, err := os.ReadFile(statusPath)
	require.NoError(t, err)
	status, err := metadatacodec.ClusterStatusCodec.UnmarshalYAML(statusData)
	require.NoError(t, err)
	// Preserve the instance ID: current versions deliberately reject servers
	// from a different cluster. Recovery must not bypass that protection.
	statusData, err = metadatacodec.ClusterStatusCodec.MarshalYAML(&proto.ClusterStatus{InstanceId: status.GetInstanceId()})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(statusPath, statusData, 0o600))

	coordinator, admin = start()
	defer func() { assert.NoError(t, admin.Close()); assert.NoError(t, coordinator.Close()) }()
	shard = elected(admin, retainedTerm+1)
	client, err = oxia.NewSyncClient(shard.GetLeader().GetPublic())
	require.NoError(t, err)
	defer client.Close()
	_, value, _, err := client.Get(t.Context(), "retained-key")
	require.NoError(t, err)
	require.Equal(t, []byte("retained-value"), value)
	_, _, err = client.Put(t.Context(), "new-key", []byte("new-value"))
	require.NoError(t, err)
}
