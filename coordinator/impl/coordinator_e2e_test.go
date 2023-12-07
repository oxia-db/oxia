// Copyright 2023 StreamNative, Inc.
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

package impl

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/server"
)

func newServer(t *testing.T) (s *server.Server, addr model.ServerAddress) {
	t.Helper()

	var err error
	s, err = server.New(server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
	})

	assert.NoError(t, err)

	addr = model.ServerAddress{
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}

func TestCoordinatorE2E(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, NewRpcProvider(clientPool))

	assert.NoError(t, err)

	assert.EqualValues(t, 1, len(coordinator.ClusterStatus().Namespaces))
	nsStatus := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
}

func TestCoordinatorE2E_ShardsRanges(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 4,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	cs := coordinator.ClusterStatus()
	nsStatus := cs.Namespaces[common.DefaultNamespace]
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

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
}

func TestCoordinator_LeaderFailover(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.ServerAddress]*server.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
	}

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	nsStatus := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	cs := coordinator.ClusterStatus()
	nsStatus = cs.Namespaces[common.DefaultNamespace]

	leader := *nsStatus.Shards[0].Leader
	var follower model.ServerAddress
	for server := range servers {
		if server != leader {
			follower = server
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

	version1, err := client.Put(ctx, "my-key", []byte("my-value"))
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version1.VersionId)

	res, version2, err := client.Get(ctx, "my-key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("my-value"), res)
	assert.Equal(t, version1, version2)
	assert.NoError(t, client.Close())

	// Stop the leader to cause a leader election
	assert.NoError(t, servers[leader].Close())
	delete(servers, leader)

	assert.Eventually(t, func() bool {
		shard := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	// Wait for the client to receive the updated assignment list
	assert.Eventually(t, func() bool {
		client, _ = oxia.NewSyncClient(follower.Public)
		_, _, err := client.Get(ctx, "my-key")
		return err == nil
	}, 10*time.Second, 10*time.Millisecond)

	res, version3, err := client.Get(ctx, "my-key")
	assert.NoError(t, err)
	assert.Equal(t, []byte("my-value"), res)
	assert.Equal(t, version1, version3)
	assert.NoError(t, client.Close())

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	for _, server := range servers {
		assert.NoError(t, server.Close())
	}
}

func TestCoordinator_MultipleNamespaces(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.ServerAddress]*server.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
	}

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
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
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	nsDefaultStatus := coordinator.ClusterStatus().Namespaces[common.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsDefaultStatus.Shards))
	assert.EqualValues(t, 3, nsDefaultStatus.ReplicationFactor)

	ns1Status := coordinator.ClusterStatus().Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	ns2Status := coordinator.ClusterStatus().Namespaces["my-ns-2"]
	assert.EqualValues(t, 3, len(ns2Status.Shards))
	assert.EqualValues(t, 2, ns2Status.ReplicationFactor)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range coordinator.ClusterStatus().Namespaces {
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
	assert.ErrorIs(t, err, common.ErrorNamespaceNotFound)
	assert.Nil(t, clientNs3)

	ctx := context.Background()

	// Write in default ns
	version1, err := clientDefault.Put(ctx, "my-key", []byte("my-value"))
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version1.ModificationsCount)

	// Key will not be visible in other namespace
	res, _, err := clientNs1.Get(ctx, "my-key")
	assert.ErrorIs(t, err, oxia.ErrKeyNotFound)
	assert.Nil(t, res)

	version2, err := clientNs1.Put(ctx, "my-key", []byte("my-value-2"))
	assert.NoError(t, err)
	assert.EqualValues(t, 0, version2.ModificationsCount)

	res, version3, err := clientDefault.Get(ctx, "my-key")
	assert.NoError(t, err)
	assert.EqualValues(t, []byte("my-value"), res)
	assert.EqualValues(t, 0, version3.ModificationsCount)

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	for _, server := range servers {
		assert.NoError(t, server.Close())
	}
}

func TestCoordinator_DeleteNamespace(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.ServerAddress]*server.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
	}

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "my-ns-1",
			ReplicationFactor: 1,
			InitialShardCount: 2,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	coordinator, err := NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, 0, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	ns1Status := coordinator.ClusterStatus().Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range coordinator.ClusterStatus().Namespaces {
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
	assert.NoError(t, coordinator.Close())

	newClusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{},
		Servers:    []model.ServerAddress{sa1, sa2, sa3},
	}

	coordinator, err = NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return newClusterConfig, nil }, 0, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	// Wait for all shards to be deleted
	assert.Eventually(t, func() bool {
		return len(coordinator.ClusterStatus().Namespaces) == 0
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	for _, server := range servers {
		assert.NoError(t, server.Close())
	}
}

func TestCoordinator_DynamicallAddNamespace(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.ServerAddress]*server.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
	}

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "my-ns-1",
			ReplicationFactor: 1,
			InitialShardCount: 2,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()

	configProvider := func() (model.ClusterConfig, error) {
		return clusterConfig, nil
	}

	coordinator, err := NewCoordinator(metadataProvider, configProvider, 1*time.Second, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	ns1Status := coordinator.ClusterStatus().Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range coordinator.ClusterStatus().Namespaces {
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

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		foundNS2 := false
		for name, ns := range coordinator.ClusterStatus().Namespaces {
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

	ns1Status = coordinator.ClusterStatus().Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	ns2Status := coordinator.ClusterStatus().Namespaces["my-ns-2"]
	assert.EqualValues(t, 2, len(ns2Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	for _, server := range servers {
		assert.NoError(t, server.Close())
	}
}

func TestCoordinator_RebalanceCluster(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	s4, sa4 := newServer(t)
	servers := map[model.ServerAddress]*server.Server{
		sa1: s1,
		sa2: s2,
		sa3: s3,
		sa4: s4,
	}

	metadataProvider := NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "my-ns-1",
			ReplicationFactor: 3,
			InitialShardCount: 2,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool()
	mutex := &sync.Mutex{}

	configProvider := func() (model.ClusterConfig, error) {
		mutex.Lock()
		defer mutex.Unlock()
		return clusterConfig, nil
	}

	coordinator, err := NewCoordinator(metadataProvider, configProvider, 1*time.Second, NewRpcProvider(clientPool))
	assert.NoError(t, err)

	ns1Status := coordinator.ClusterStatus().Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 3, ns1Status.ReplicationFactor)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range coordinator.ClusterStatus().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	slog.Info(
		"Cluster is ready",
		slog.Any("cluster-status", coordinator.ClusterStatus()),
	)

	ns1Status = coordinator.ClusterStatus().Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 3, ns1Status.ReplicationFactor)
	checkServerLists(t, []model.ServerAddress{sa1, sa2, sa3}, ns1Status.Shards[0].Ensemble)
	checkServerLists(t, []model.ServerAddress{sa1, sa2, sa3}, ns1Status.Shards[1].Ensemble)

	// Add `s4` and remove `s1` from the cluster config
	mutex.Lock()
	clusterConfig.Servers = []model.ServerAddress{sa2, sa3, sa4}
	mutex.Unlock()

	time.Sleep(2 * time.Second)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range coordinator.ClusterStatus().Namespaces {
			for _, shard := range ns.Shards {
				// Use Term to detect for EnsembleChange have done, we cann't use status
				// because it always be Steady when EnsembleChange.
				if shard.Term != 1 {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	ns1Status = coordinator.ClusterStatus().Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 3, ns1Status.ReplicationFactor)
	checkServerLists(t, []model.ServerAddress{sa2, sa3, sa4}, ns1Status.Shards[0].Ensemble)
	checkServerLists(t, []model.ServerAddress{sa2, sa3, sa4}, ns1Status.Shards[1].Ensemble)

	assert.NoError(t, coordinator.Close())
	assert.NoError(t, clientPool.Close())

	for _, server := range servers {
		assert.NoError(t, server.Close())
	}
}

func checkServerLists(t *testing.T, expected, actual []model.ServerAddress) {
	t.Helper()

	assert.Equal(t, len(expected), len(actual))
	mExpected := map[string]bool{}
	for _, x := range expected {
		mExpected[x.Public] = true
	}

	for _, x := range actual {
		_, ok := mExpected[x.Public]
		if !ok {
			slog.Warn(
				"Got unexpected server",
				slog.Any("expected-servers", expected),
				slog.Any("found-server", x),
			)
		}
		assert.True(t, ok)
	}
}
