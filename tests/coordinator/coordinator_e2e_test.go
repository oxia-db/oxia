// Copyright 2023-2025 The Oxia Authors
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
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"

	"github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxia"
)

func newServer(t *testing.T) (s *dataserver.Server, addr model.Server) {
	t.Helper()

	var err error
	s, err = dataserver.New(&option.Options{
		Server: option.ServerOptions{
			Public: option.PublicServerOptions{
				BindAddress: "localhost:0",
			},
			Internal: option.InternalServerOptions{
				BindAddress: "localhost:0",
			},
		},
		Observability: commonoption.ObservabilityOptions{
			Metric: commonoption.MetricOptions{
				Enabled: &constant.FlagFalse,
			},
		},
		Storage: option.StorageOptions{
			Database: option.DatabaseOptions{
				Dir: t.TempDir(),
			},
			WAL: option.WALOptions{
				Dir: t.TempDir(),
			},
			Notification: option.NotificationOptions{
				Retention: 1 * time.Minute,
			},
		},
	})

	assert.NoError(t, err)

	addr = model.Server{
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}

func TestCoordinatorE2E(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	statusResource := coordinatorInstance.StatusResource()
	status := statusResource.Load()

	assert.EqualValues(t, 1, len(status.Namespaces))
	nsStatus := status.Namespaces[constant.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	assert.Eventually(t, func() bool {
		shard := statusResource.Load().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, coordinatorInstance.Close())
	assert.NoError(t, clientPool.Close())

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
}

func TestCoordinatorE2E_ShardsRanges(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 4,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	statusResource := coordinatorInstance.StatusResource()
	status := statusResource.Load()
	nsStatus := status.Namespaces[constant.DefaultNamespace]
	assert.EqualValues(t, 4, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	// Check that the entire hash range is covered
	assert.EqualValues(t, 0, nsStatus.Shards[0].Int32HashRange.Min)

	for i := int64(1); i < 4; i++ {
		slog.Info(
			"Checking shard",
			slog.Any("range", nsStatus.Shards[i].Int32HashRange),
			slog.Int64("shard", i),
		)

		// The hash ranges should be exclusive & consecutive
		assert.Equal(t, nsStatus.Shards[i-1].Int32HashRange.Max+1, nsStatus.Shards[i].Int32HashRange.Min)
	}

	assert.EqualValues(t, math.MaxUint32, nsStatus.Shards[3].Int32HashRange.Max)

	assert.NoError(t, coordinatorInstance.Close())
	assert.NoError(t, clientPool.Close())

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
}

func TestCoordinator_LeaderFailover(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.Server]*dataserver.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
	}

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	statusResource := coordinatorInstance.StatusResource()
	status := statusResource.Load()

	nsStatus := status.Namespaces[constant.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	assert.Eventually(t, func() bool {
		shard := statusResource.Load().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	nsStatus = statusResource.Load().Namespaces[constant.DefaultNamespace]

	leader := *nsStatus.Shards[0].Leader
	var follower model.Server
	for serverObj := range servers {
		if serverObj != leader {
			follower = serverObj
			break
		}
	}
	slog.Info(
		"Cluster is ready",
		slog.Any("leader", leader),
		slog.Any("follower", follower),
	)

	client, err := oxia.NewSyncClient(follower.Public)
	assert.NoError(t, err)

	ctx := context.Background()

	_, version1, err := client.Put(ctx, "my-key", []byte("my-value"))
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version1.VersionId)

	key, res, version2, err := client.Get(ctx, "my-key")
	assert.NoError(t, err)
	assert.Equal(t, "my-key", key)
	assert.Equal(t, []byte("my-value"), res)
	assert.Equal(t, version1, version2)
	assert.NoError(t, client.Close())

	// Stop the leader to cause a leader election
	assert.NoError(t, servers[leader].Close())
	delete(servers, leader)

	assert.Eventually(t, func() bool {
		shard := statusResource.Load().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	// Wait for the client to receive the updated assignment list
	assert.Eventually(t, func() bool {
		client, _ = oxia.NewSyncClient(follower.Public)
		_, _, _, err := client.Get(ctx, "my-key")
		return err == nil
	}, 10*time.Second, 10*time.Millisecond)

	_, res, version3, err := client.Get(ctx, "my-key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("my-value"), res)
	assert.Equal(t, version1, version3)
	assert.NoError(t, client.Close())

	assert.NoError(t, coordinatorInstance.Close())
	assert.NoError(t, clientPool.Close())

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_MultipleNamespaces(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.Server]*dataserver.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
	}

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}, {
			Name:              "my-ns-1",
			ReplicationFactor: 1,
			InitialShardCount: 2,
		}, {
			Name:              "my-ns-2",
			ReplicationFactor: 2,
			InitialShardCount: 3,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	statusResource := coordinatorInstance.StatusResource()
	status := statusResource.Load()
	nsDefaultStatus := status.Namespaces[constant.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsDefaultStatus.Shards))
	assert.EqualValues(t, 3, nsDefaultStatus.ReplicationFactor)

	ns1Status := status.Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	ns2Status := status.Namespaces["my-ns-2"]
	assert.EqualValues(t, 3, len(ns2Status.Shards))
	assert.EqualValues(t, 2, ns2Status.ReplicationFactor)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	slog.Info("Cluster is ready")

	clientDefault, err := oxia.NewSyncClient(sa1.Public)
	assert.NoError(t, err)
	defer clientDefault.Close()

	clientNs1, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace("my-ns-1"))
	assert.NoError(t, err)
	defer clientNs1.Close()

	clientNs3, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace("my-ns-does-not-exist"))
	assert.ErrorIs(t, err, constant.ErrNamespaceNotFound)
	assert.Nil(t, clientNs3)

	ctx := context.Background()

	// Write in default ns
	_, version1, err := clientDefault.Put(ctx, "my-key", []byte("my-value"))
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version1.ModificationsCount)

	// Key will not be visible in other namespace
	_, res, _, err := clientNs1.Get(ctx, "my-key")
	assert.ErrorIs(t, err, oxia.ErrKeyNotFound)
	assert.Nil(t, res)

	_, version2, err := clientNs1.Put(ctx, "my-key", []byte("my-value-2"))
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version2.ModificationsCount)

	_, res, version3, err := clientDefault.Get(ctx, "my-key")
	assert.NoError(t, err)
	assert.EqualValues(t, []byte("my-value"), res)
	assert.EqualValues(t, 0, version3.ModificationsCount)

	assert.NoError(t, coordinatorInstance.Close())
	assert.NoError(t, clientPool.Close())

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_DeleteNamespace(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.Server]*dataserver.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
	}

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "my-ns-1",
			ReplicationFactor: 1,
			InitialShardCount: 2,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	statusResource := coordinatorInstance.StatusResource()
	status := statusResource.Load()
	ns1Status := status.Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	// Trigger new leader election in order to have a new term
	ns1Status = statusResource.Load().Namespaces["my-ns-1"]
	coordinatorInstance.BecameUnavailable(*ns1Status.Shards[0].Leader)

	// Wait (again) for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	slog.Info("Cluster is ready")

	// Restart the coordinator and remove the namespace
	assert.NoError(t, coordinatorInstance.Close())

	newClusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{},
		Servers:    []model.Server{sa1, sa2, sa3},
	}

	slog.Info("Restarting coordinator")
	coordinatorInstance, err = coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return newClusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	statusResource = coordinatorInstance.StatusResource()
	// Wait for all shards to be deleted
	assert.Eventually(t, func() bool {
		load := statusResource.Load()
		slog.Info("load", slog.Any("load", load))
		return len(load.Namespaces) == 0
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, coordinatorInstance.Close())
	assert.NoError(t, clientPool.Close())

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_DynamicallAddNamespace(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.Server]*dataserver.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
	}

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "my-ns-1",
			ReplicationFactor: 1,
			InitialShardCount: 2,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	configChangesCh := make(chan any)
	configProvider := func() (model.ClusterConfig, error) {
		return clusterConfig, nil
	}

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, configProvider, configChangesCh, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	statusResource := coordinatorInstance.StatusResource()
	status := statusResource.Load()
	ns1Status := status.Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	slog.Info("Cluster is ready")

	clusterConfig.Namespaces = append(clusterConfig.Namespaces, model.NamespaceConfig{
		Name:              "my-ns-2",
		InitialShardCount: 2,
		ReplicationFactor: 1,
	})
	configChangesCh <- nil

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		foundNS2 := false
		for name, ns := range statusResource.Load().Namespaces {
			if name == "my-ns-2" {
				foundNS2 = true
			}
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return foundNS2
	}, 10*time.Second, 10*time.Millisecond)

	ns1Status = statusResource.Load().Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	ns2Status := statusResource.Load().Namespaces["my-ns-2"]
	assert.EqualValues(t, 2, len(ns2Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	assert.NoError(t, coordinatorInstance.Close())
	assert.NoError(t, clientPool.Close())

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_AddRemoveNodes(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	s4, sa4 := newServer(t)
	s5, sa5 := newServer(t)
	servers := map[model.Server]*dataserver.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
		sa4: s4,
		sa5: s5,
	}

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "my-ns-1",
			ReplicationFactor: 1,
			InitialShardCount: 2,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	configProvider := func() (model.ClusterConfig, error) {
		return clusterConfig, nil
	}

	configChangesCh := make(chan any)
	c, err := coordinator.NewCoordinator(metadataProvider, configProvider, configChangesCh, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	assert.Equal(t, 3, len(c.NodeControllers()))

	// Add s4, s5
	clusterConfig.Servers = append(clusterConfig.Servers, sa4, sa5)
	// Remove s1
	clusterConfig.Servers = clusterConfig.Servers[1:]

	configChangesCh <- nil

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		return len(c.NodeControllers()) == 4
	}, 10*time.Second, 10*time.Millisecond)

	_, ok := c.NodeControllers()[sa1.Internal]
	assert.False(t, ok)

	_, ok = c.NodeControllers()[sa4.Internal]
	assert.True(t, ok)

	_, ok = c.NodeControllers()[sa5.Internal]
	assert.True(t, ok)

	assert.NoError(t, c.Close())
	assert.NoError(t, clientPool.Close())

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_ShrinkCluster(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	s4, sa4 := newServer(t)
	servers := map[model.Server]*dataserver.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
		sa4: s4,
	}

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "my-ns-1",
			ReplicationFactor: 1,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3, sa4},
	}
	clientPool := rpc.NewClientPool(nil, nil)

	configProvider := func() (model.ClusterConfig, error) {
		return clusterConfig, nil
	}

	configChangesCh := make(chan any)
	c, err := coordinator.NewCoordinator(metadataProvider, configProvider, configChangesCh, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)

	statusResource := c.StatusResource()

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	assert.Equal(t, 4, len(c.NodeControllers()))

	// Remove leader dataserver
	leaderID := statusResource.Load().Namespaces["my-ns-1"].Shards[0].Leader.GetIdentifier()
	d := make([]model.Server, 0)
	for _, sv := range clusterConfig.Servers {
		if sv.GetIdentifier() != leaderID {
			d = append(d, sv)
		}
	}
	clusterConfig.Servers = d

	configChangesCh <- nil
	assert.Eventually(t, func() bool {
		return len(c.NodeControllers()) == 3
	}, 10*time.Second, 10*time.Millisecond)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				return shard.Term > 0 && shard.Status == model.ShardStatusSteadyState
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithNamespace("my-ns-1"))
	assert.NoError(t, err)

	_, _, err = client.Put(context.Background(), "test", []byte("value"))
	assert.NoError(t, err)

	assert.NoError(t, client.Close())
	assert.NoError(t, c.Close())
	assert.NoError(t, clientPool.Close())

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_RefreshServerInfo(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "my-ns-1",
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	configChangesCh := make(chan any)
	c, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) {
		return clusterConfig, nil
	}, configChangesCh,
		rpc2.NewRpcProvider(rpc.NewClientPool(nil, nil)))
	assert.NoError(t, err)

	statusResource := c.StatusResource()
	// wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	// change the localhost to 127.0.0.1
	clusterServer := make([]model.Server, 0)
	for _, sv := range clusterConfig.Servers {
		clusterServer = append(clusterServer, model.Server{
			Public:   strings.ReplaceAll(sv.Public, "localhost", "127.0.0.1"),
			Internal: sv.Internal,
		})
	}

	clusterConfig.Servers = clusterServer
	configChangesCh <- nil

	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
				for _, sv := range shard.Ensemble {
					if !strings.HasPrefix(sv.Public, "127.0.0.1") {
						return false
					}
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	err = s1.Close()
	assert.NoError(t, err)
	err = s2.Close()
	assert.NoError(t, err)
	err = s3.Close()
	assert.NoError(t, err)
	err = c.Close()
	assert.NoError(t, err)
}

func TestCoordinator_KeySorting(t *testing.T) {
	for _, test := range []struct {
		sorting string
	}{
		{"hierarchical"},
		{"natural"},
	} {
		t.Run(test.sorting, func(t *testing.T) {

			s1, err := dataserver.New(&option.Options{
				Server: option.ServerOptions{
					Public: option.PublicServerOptions{
						BindAddress: "localhost:0",
					},
					Internal: option.InternalServerOptions{
						BindAddress: "localhost:0",
					},
				},
				Observability: commonoption.ObservabilityOptions{
					Metric: commonoption.MetricOptions{
						Enabled: &constant.FlagFalse,
					},
				},
				Storage: option.StorageOptions{
					Database: option.DatabaseOptions{
						Dir: t.TempDir(),
					},
					WAL: option.WALOptions{
						Dir: t.TempDir(),
					},
					Notification: option.NotificationOptions{
						Retention: 1 * time.Minute,
					},
				},
			})
			assert.NoError(t, err)

			sa1 := model.Server{
				Public:   fmt.Sprintf("localhost:%d", s1.PublicPort()),
				Internal: fmt.Sprintf("localhost:%d", s1.InternalPort()),
			}

			metadataProvider := metadata.NewMetadataProviderMemory()
			clusterConfig := model.ClusterConfig{
				Namespaces: []model.NamespaceConfig{{
					Name:              constant.DefaultNamespace,
					ReplicationFactor: 1,
					InitialShardCount: 1,
					KeySorting:        model.KeySorting(test.sorting),
				}},
				Servers: []model.Server{sa1},
			}
			clientPool := rpc.NewClientPool(nil, nil)

			coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
			assert.NoError(t, err)

			statusResource := coordinatorInstance.StatusResource()
			status := statusResource.Load()

			assert.EqualValues(t, 1, len(status.Namespaces))
			nsStatus := status.Namespaces[constant.DefaultNamespace]
			assert.EqualValues(t, 1, len(nsStatus.Shards))
			assert.EqualValues(t, 1, nsStatus.ReplicationFactor)

			assert.Eventually(t, func() bool {
				shard := statusResource.Load().Namespaces[constant.DefaultNamespace].Shards[0]
				return shard.Status == model.ShardStatusSteadyState
			}, 10*time.Second, 10*time.Millisecond)

			client, err := oxia.NewSyncClient(sa1.Public)
			assert.NoError(t, err)

			_, _, _ = client.Put(context.Background(), "/a", []byte("a"))
			_, _, _ = client.Put(context.Background(), "/b", []byte("b"))
			_, _, _ = client.Put(context.Background(), "/a/b", []byte("a/b"))

			list, err := client.List(context.Background(), "", "")
			assert.NoError(t, err)

			ks := model.KeySorting(test.sorting)
			if ks.ToProto() == proto.KeySortingType_HIERARCHICAL {
				assert.Equal(t, []string{"/a", "/b", "/a/b"}, list)
			} else {
				assert.Equal(t, []string{"/a", "/a/b", "/b"}, list)
			}

			assert.NoError(t, client.Close())
			assert.NoError(t, coordinatorInstance.Close())
			assert.NoError(t, clientPool.Close())

			assert.NoError(t, s1.Close())
		})
	}
}
