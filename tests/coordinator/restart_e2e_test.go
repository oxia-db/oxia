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

// A restarted coordinator only knows the features of a data server after a
// handshake with it. A data server that is down across the restart must not
// keep the shards it belongs to, which already have features enabled, from
// electing a leader with the rest of their ensemble.
func TestCoordinator_RestartWithDataServerDown(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}
	defer func() {
		for _, s := range servers {
			assert.NoError(t, s.Close())
		}
	}()

	metadataDir := t.TempDir()
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              constant.DefaultNamespace,
		ReplicationFactor: 3,
		InitialShardCount: 1,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})
	configData, err := metadatacodec.ClusterConfigCodec.MarshalYAML(clusterConfig)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(metadataDir, coordoption.DefaultFileConfigName), configData, 0o600))

	startCoordinator := func() (*coordserver.GrpcServer, oxia.AdminClient) {
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
		return coordinatorServer, adminClient
	}
	// The shard leader enables the features. If the first election did not
	// know the features of every data server yet, a new election enables them,
	// and it can move the leader.
	requireLeaderElected := func(adminClient oxia.AdminClient, minTerm int64) *proto.ShardMetadata {
		t.Helper()
		var shard *proto.ShardMetadata
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			ns, err := adminClient.GetNamespace(t.Context(), constant.DefaultNamespace)
			if !assert.NoError(c, err) {
				return
			}
			shard = ns.GetNamespaceStatus().GetShards()[0]
			assert.Equal(c, proto.ShardStatusSteadyState, shard.GetStatusOrDefault())
			assert.GreaterOrEqual(c, shard.GetTerm(), minTerm)
			if assert.NotNil(c, shard.GetLeader()) {
				lead, err := servers[shard.GetLeader().GetNameOrDefault()].GetShardDirector().GetLeader(0)
				if assert.NoError(c, err) {
					assert.True(c, lead.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM))
				}
			}
		}, 30*time.Second, 50*time.Millisecond)
		return shard
	}

	coordinatorServer, adminClient := startCoordinator()
	shard := requireLeaderElected(adminClient, 0)
	leader := shard.GetLeader().GetNameOrDefault()

	require.NoError(t, adminClient.Close())
	require.NoError(t, coordinatorServer.Close())

	// Stop a follower: the restarted coordinator can't verify the ensemble
	// and starts a new election, which the follower can't take part in
	var follower string
	for _, dataServer := range shard.GetEnsemble() {
		if dataServer.GetNameOrDefault() != leader {
			follower = dataServer.GetNameOrDefault()
			break
		}
	}
	require.NoError(t, servers[follower].Close())
	delete(servers, follower)

	coordinatorServer, adminClient = startCoordinator()
	defer func() {
		assert.NoError(t, adminClient.Close())
		assert.NoError(t, coordinatorServer.Close())
	}()
	shard = requireLeaderElected(adminClient, shard.GetTerm()+1)

	client, err := oxia.NewSyncClient(shard.GetLeader().GetPublic(), oxia.WithNamespace(constant.DefaultNamespace))
	require.NoError(t, err)
	defer client.Close()
	_, _, err = client.Put(t.Context(), "/key", []byte("value"))
	assert.NoError(t, err)
}
