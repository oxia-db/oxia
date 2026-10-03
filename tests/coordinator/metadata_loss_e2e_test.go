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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	commonrpc "github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxia"
	coord "github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

// TestRetainedServerTermAfterMetadataLoss keeps server data while replacing coordinator metadata.
func TestRetainedServerTermAfterMetadataLoss(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := []*dataserver.Server{s1, s2, s3}
	defer func() {
		for _, s := range servers {
			assert.NoError(t, s.Close())
		}
	}()
	config := model.ClusterConfig{Namespaces: []model.NamespaceConfig{{Name: constant.DefaultNamespace, ReplicationFactor: 3, InitialShardCount: 1}}, Servers: []model.Server{sa1, sa2, sa3}}
	start := func() (coord.Coordinator, commonrpc.ClientPool) {
		pool := commonrpc.NewClientPool(nil, nil)
		c, err := coord.NewCoordinator(metadata.NewMetadataProviderMemory(), func() (model.ClusterConfig, error) { return config, nil }, nil, coordrpc.NewRpcProvider(pool))
		require.NoError(t, err)
		return c, pool
	}
	c, pool := start()
	require.Eventually(t, func() bool {
		return c.StatusResource().Load().Namespaces[constant.DefaultNamespace].Shards[0].Status == model.ShardStatusSteadyState
	}, 10*time.Second, 20*time.Millisecond)
	shard := c.StatusResource().Load().Namespaces[constant.DefaultNamespace].Shards[0]
	client, err := oxia.NewSyncClient(shard.Leader.Public)
	require.NoError(t, err)
	_, _, err = client.Put(t.Context(), "key", []byte("value"))
	require.NoError(t, err)
	require.NoError(t, client.Close())
	require.NoError(t, c.Close())
	require.NoError(t, pool.Close())
	for _, server := range servers {
		leader, err := server.GetShardDirector().GetOrCreateLeader(constant.DefaultNamespace, 0, nil)
		require.NoError(t, err)
		_, err = leader.NewTerm(&proto.NewTermRequest{Namespace: constant.DefaultNamespace, Shard: 0, Term: 1000})
		require.NoError(t, err)
	}
	c, pool = start()
	defer func() { assert.NoError(t, c.Close()); assert.NoError(t, pool.Close()) }()
	require.Eventually(t, func() bool {
		shard := c.StatusResource().Load().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState && shard.Term > 1000
	}, 10*time.Second, 20*time.Millisecond, "fresh coordinator metadata must recover retained term 1000")
	shard = c.StatusResource().Load().Namespaces[constant.DefaultNamespace].Shards[0]
	client, err = oxia.NewSyncClient(shard.Leader.Public)
	require.NoError(t, err)
	defer func() { assert.NoError(t, client.Close()) }()
	_, value, _, err := client.Get(t.Context(), "key")
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
	_, _, err = client.Put(t.Context(), "new-key", []byte("new-value"))
	require.NoError(t, err)
}
