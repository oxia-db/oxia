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

package resolver

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	coordinatorrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/tests/mock"
)

type testCluster struct {
	name        string
	authority   string
	coordinator coordinator.Coordinator
	servers     map[string]*dataserver.Server
	addresses   []model.Server
}

const integrationRequestTimeout = 30 * time.Second
const rejectionRequestTimeout = 5 * time.Second

func dataServer(server model.Server) *proto.DataServer {
	return &proto.DataServer{
		Name:            server.Name,
		PublicAddress:   server.Public,
		InternalAddress: server.Internal,
	}
}

func newCoordinatorCluster(t *testing.T, prefix string, serverCount int) *testCluster {
	t.Helper()

	cluster := &testCluster{
		name:      prefix,
		authority: fmt.Sprintf("%s-bootstrap:6648", prefix),
		servers:   make(map[string]*dataserver.Server, serverCount),
		addresses: make([]model.Server, 0, serverCount),
	}

	for i := 0; i < serverCount; i++ {
		name := fmt.Sprintf("%s-s%d", prefix, i+1)
		server, addr := mock.NewServer(t, name)
		cluster.servers[addr.GetIdentifier()] = server
		cluster.addresses = append(cluster.addresses, addr)
	}

	metadataProvider := memory.NewProvider()
	cluster.coordinator = newCoordinatorInstance(
		t,
		metadataProvider,
		func() (*proto.ClusterConfiguration, error) {
			return &proto.ClusterConfiguration{
				Namespaces: []*proto.Namespace{{
					Name:              constant.DefaultNamespace,
					ReplicationFactor: 1,
					InitialShardCount: 3,
				}},
				Servers: func() []*proto.DataServer {
					dataServers := make([]*proto.DataServer, 0, len(cluster.addresses))
					for _, addr := range cluster.addresses {
						dataServers = append(dataServers, dataServer(addr))
					}
					return dataServers
				}(),
				AllowExtraAuthorities: []string{cluster.authority},
			}, nil
		},
		nil,
		coordinatorrpc.NewRpcProviderFactory(nil),
	)

	require.Eventually(t, func() bool {
		shard := cluster.coordinator.Metadata().LoadStatus().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState && shard.Leader != nil
	}, 20*time.Second, 100*time.Millisecond)

	return cluster
}

func (c *testCluster) close(t *testing.T) {
	t.Helper()
	if c.coordinator != nil {
		assert.NoError(t, c.coordinator.Close())
	}
	for _, server := range c.servers {
		assert.NoError(t, server.Close())
	}
}

func (c *testCluster) stopAllServers(t *testing.T) {
	t.Helper()
	for id, server := range c.servers {
		assert.NoError(t, server.Close())
		delete(c.servers, id)
	}
}

func (c *testCluster) leader() model.Server {
	return *c.coordinator.Metadata().LoadStatus().Namespaces[constant.DefaultNamespace].Shards[0].Leader
}

func (c *testCluster) bootstrapTargetExcluding(excluded model.Server) model.Server {
	for _, addr := range c.addresses {
		if addr.GetIdentifier() != excluded.GetIdentifier() {
			return addr
		}
	}
	panic("no bootstrap target available")
}

func getValue(t *testing.T, client oxia.SyncClient, key string) ([]byte, error) {
	t.Helper()
	_, value, _, err := client.Get(context.Background(), key)
	return value, err
}

func newClusterClient(t *testing.T, serviceAddress string, opts ...oxia.ClientOption) oxia.SyncClient {
	t.Helper()

	client, err := oxia.NewSyncClient(serviceAddress, append(opts, oxia.WithRequestTimeout(integrationRequestTimeout))...)
	require.NoError(t, err)
	return client
}

func waitForClusterReady(t *testing.T, cluster *testCluster, name string) {
	t.Helper()

	bootstrapTarget := cluster.bootstrapTargetExcluding(cluster.leader()).Public
	assert.Eventually(t, func() bool {
		client, err := oxia.NewSyncClient(bootstrapTarget, oxia.WithRequestTimeout(2*time.Second))
		if err != nil {
			return false
		}
		defer client.Close()

		key := fmt.Sprintf("/resolver/%s/ready/%d", name, time.Now().UnixNano())
		_, _, err = client.Put(context.Background(), key, []byte(name))
		if err != nil {
			return false
		}

		value, err := getValue(t, client, key)
		return err == nil && string(value) == name
	}, 30*time.Second, 200*time.Millisecond)
}

func TestTargetIPFlipBeforeBootstrapRejectsWrongCluster(t *testing.T) {
	cluster1 := newCoordinatorCluster(t, "cluster1", 2)
	defer cluster1.close(t)

	cluster2 := newCoordinatorCluster(t, "cluster2", 2)
	defer cluster2.close(t)

	waitForClusterReady(t, cluster1, "cluster1")
	waitForClusterReady(t, cluster2, "cluster2")

	resolver := &dynamicResolver{
		scheme:  fmt.Sprintf("oxia-bootstrap-%d-before", os.Getpid()),
		targets: []string{cluster1.bootstrapTargetExcluding(cluster1.leader()).Public},
	}

	_, err := oxia.NewSyncClient(
		fmt.Sprintf("%s:///%s", resolver.scheme, cluster2.authority),
		oxia.WithDialResolver(resolver),
		oxia.WithRequestTimeout(rejectionRequestTimeout),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to retrieve the initial list of shard assignments")
}

func TestTargetIPFlipAfterBootstrapRejectsWrongCluster(t *testing.T) {
	cluster1 := newCoordinatorCluster(t, "cluster1", 2)
	defer cluster1.close(t)

	cluster2 := newCoordinatorCluster(t, "cluster2", 2)
	defer cluster2.close(t)

	waitForClusterReady(t, cluster1, "cluster1")
	waitForClusterReady(t, cluster2, "cluster2")

	cluster2Leader := cluster2.leader()
	cluster2Bootstrap := cluster2.bootstrapTargetExcluding(cluster2Leader)

	resolver := &dynamicResolver{
		scheme:  fmt.Sprintf("oxia-bootstrap-%d-after", os.Getpid()),
		targets: []string{cluster2Bootstrap.Public},
	}

	client := newClusterClient(
		t,
		fmt.Sprintf("%s:///%s", resolver.scheme, cluster2.authority),
		oxia.WithDialResolver(resolver),
	)
	defer client.Close()

	cluster1Client := newClusterClient(t, cluster1.addresses[0].Public)
	defer cluster1Client.Close()

	cluster2Client := newClusterClient(t, cluster2.addresses[0].Public)
	defer cluster2Client.Close()

	initialKey := "/resolver/before-flip"
	_, _, err := client.Put(context.Background(), initialKey, []byte("cluster-2"))
	require.NoError(t, err)

	value, err := getValue(t, cluster2Client, initialKey)
	require.NoError(t, err)
	assert.Equal(t, []byte("cluster-2"), value)

	_, err = getValue(t, cluster1Client, initialKey)
	assert.ErrorIs(t, err, oxia.ErrKeyNotFound)

	resolver.UpdateTargets([]string{cluster1.bootstrapTargetExcluding(cluster1.leader()).Public})
	cluster2.stopAllServers(t)

	assert.Eventually(t, func() bool {
		rejectedKey := fmt.Sprintf("/resolver/rejected-after-flip-%d", time.Now().UnixNano())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, _, err = client.Put(ctx, rejectedKey, []byte(cluster1.name))
		if err == nil {
			value, getErr := getValue(t, cluster1Client, rejectedKey)
			if getErr == nil && string(value) == cluster1.name {
				return false
			}
		}
		return err != nil
	}, 20*time.Second, 100*time.Millisecond)
}
