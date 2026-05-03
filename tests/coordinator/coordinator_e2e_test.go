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

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"

	"github.com/stretchr/testify/assert"

	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"

	"github.com/oxia-db/oxia/common/proto"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxia"
)

func newServer(t *testing.T) (s *dataserver.Server, addr *proto.DataServerIdentity) {
	t.Helper()

	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()

	var err error
	s, err = dataserver.New(t.Context(), commonwatch.New(options))

	assert.NoError(t, err)

	addr = &proto.DataServerIdentity{
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}

func TestCoordinatorE2E(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              constant.DefaultNamespace,
		ReplicationFactor: 3,
		InitialShardCount: 1,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

	metadata := coordinatorInstance.Metadata()
	status := metadata.GetStatus().UnsafeBorrow()

	assert.EqualValues(t, 1, len(status.Namespaces))
	nsStatus := status.Namespaces[constant.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	assert.Eventually(t, func() bool {
		shard := metadata.GetStatus().UnsafeBorrow().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.GetStatusOrDefault() == proto.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, coordinatorInstance.Close())

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
}

func TestCoordinatorE2E_ShardsRanges(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              constant.DefaultNamespace,
		ReplicationFactor: 3,
		InitialShardCount: 4,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

	metadata := coordinatorInstance.Metadata()
	status := metadata.GetStatus().UnsafeBorrow()
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

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
}

func TestCoordinator_LeaderFailover(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}

	metadataProvider := memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              constant.DefaultNamespace,
		ReplicationFactor: 3,
		InitialShardCount: 1,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

	metadata := coordinatorInstance.Metadata()
	status := metadata.GetStatus().UnsafeBorrow()

	nsStatus := status.Namespaces[constant.DefaultNamespace]
	assert.EqualValues(t, 1, len(nsStatus.Shards))
	assert.EqualValues(t, 3, nsStatus.ReplicationFactor)

	assert.Eventually(t, func() bool {
		shard := metadata.GetStatus().UnsafeBorrow().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.GetStatusOrDefault() == proto.ShardStatusSteadyState
	}, 10*time.Second, 10*time.Millisecond)

	nsStatus = metadata.GetStatus().UnsafeBorrow().Namespaces[constant.DefaultNamespace]

	leader := nsStatus.Shards[0].Leader
	var follower *proto.DataServerIdentity
	for _, addr := range []*proto.DataServerIdentity{sa1, sa2, sa3} {
		if addr.GetNameOrDefault() != leader.GetNameOrDefault() {
			follower = addr
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
	assert.NoError(t, servers[leader.GetNameOrDefault()].Close())
	delete(servers, leader.GetNameOrDefault())

	assert.Eventually(t, func() bool {
		shard := metadata.GetStatus().UnsafeBorrow().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.GetStatusOrDefault() == proto.ShardStatusSteadyState
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

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_MultipleNamespaces(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}

	metadataProvider := memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
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
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

	metadata := coordinatorInstance.Metadata()
	status := metadata.GetStatus().UnsafeBorrow()
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
		for _, ns := range metadata.GetStatus().UnsafeBorrow().Namespaces {
			for _, shard := range ns.Shards {
				if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
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

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_DeleteNamespace(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}

	metadataProvider := memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              "my-ns-1",
		ReplicationFactor: 1,
		InitialShardCount: 2,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

	metadata := coordinatorInstance.Metadata()
	status := metadata.GetStatus().UnsafeBorrow()
	ns1Status := status.Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range metadata.GetStatus().UnsafeBorrow().Namespaces {
			for _, shard := range ns.Shards {
				if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	// Trigger new leader election in order to have a new term
	ns1Status = metadata.GetStatus().UnsafeBorrow().Namespaces["my-ns-1"]
	coordinatorInstance.BecameUnavailable(ns1Status.Shards[0].Leader)

	// Wait (again) for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range metadata.GetStatus().UnsafeBorrow().Namespaces {
			for _, shard := range ns.Shards {
				if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	slog.Info("Cluster is ready")

	// Restart the coordinator and remove the namespace
	assert.NoError(t, coordinatorInstance.Close())

	newClusterConfig := newClusterConfig([]*proto.Namespace{}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	slog.Info("Restarting coordinator")
	newConfigProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err = newConfigProvider.Store(newClusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	restartedCoordinator := newCoordinatorInstance(t, metadataProvider, newConfigProvider, rpc2.NewRpcProviderFactory(nil))
	coordinatorInstance = restartedCoordinator

	metadata = coordinatorInstance.Metadata()
	// Wait for all shards to be deleted
	assert.Eventually(t, func() bool {
		load := metadata.GetStatus().UnsafeBorrow()
		slog.Info("load", slog.Any("load", load))
		return len(load.Namespaces) == 0
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, coordinatorInstance.Close())

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_DynamicallAddNamespace(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}

	metadataProvider := memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              "my-ns-1",
		ReplicationFactor: 1,
		InitialShardCount: 2,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

	metadata := coordinatorInstance.Metadata()
	status := metadata.GetStatus().UnsafeBorrow()
	ns1Status := status.Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range metadata.GetStatus().UnsafeBorrow().Namespaces {
			for _, shard := range ns.Shards {
				if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	slog.Info("Cluster is ready")

	clusterConfig.Namespaces = append(clusterConfig.Namespaces, &proto.Namespace{
		Name:              "my-ns-2",
		InitialShardCount: 2,
		ReplicationFactor: 1,
	})
	version := configProvider.Watch().Load().Version
	_, err = configProvider.Store(clusterConfig, version)
	assert.NoError(t, err)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		foundNS2 := false
		for name, ns := range metadata.GetStatus().UnsafeBorrow().Namespaces {
			if name == "my-ns-2" {
				foundNS2 = true
			}
			for _, shard := range ns.Shards {
				if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
					return false
				}
			}
		}
		return foundNS2
	}, 10*time.Second, 10*time.Millisecond)

	ns1Status = metadata.GetStatus().UnsafeBorrow().Namespaces["my-ns-1"]
	assert.EqualValues(t, 2, len(ns1Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	ns2Status := metadata.GetStatus().UnsafeBorrow().Namespaces["my-ns-2"]
	assert.EqualValues(t, 2, len(ns2Status.Shards))
	assert.EqualValues(t, 1, ns1Status.ReplicationFactor)

	assert.NoError(t, coordinatorInstance.Close())

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
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
		sa4.GetNameOrDefault(): s4,
		sa5.GetNameOrDefault(): s5,
	}

	metadataProvider := memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              "my-ns-1",
		ReplicationFactor: 1,
		InitialShardCount: 2,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	c := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

	assert.Equal(t, 3, len(c.ListDataServer()))

	// Add s4, s5
	clusterConfig.Servers = append(clusterConfig.Servers,
		&proto.DataServerIdentity{Name: sa4.Name, Public: sa4.Public, Internal: sa4.Internal},
		&proto.DataServerIdentity{Name: sa5.Name, Public: sa5.Public, Internal: sa5.Internal},
	)
	// Remove s1
	clusterConfig.Servers = clusterConfig.Servers[1:]

	version := configProvider.Watch().Load().Version
	_, err = configProvider.Store(clusterConfig, version)
	assert.NoError(t, err)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		return len(c.ListDataServer()) == 4
	}, 10*time.Second, 10*time.Millisecond)

	_, ok := c.ListDataServer()[sa1.Internal]
	assert.False(t, ok)

	_, ok = c.ListDataServer()[sa4.Internal]
	assert.True(t, ok)

	_, ok = c.ListDataServer()[sa5.Internal]
	assert.True(t, ok)

	assert.NoError(t, c.Close())

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_ShrinkCluster(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	s4, sa4 := newServer(t)
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
		sa4.GetNameOrDefault(): s4,
	}

	metadataProvider := memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              "my-ns-1",
		ReplicationFactor: 1,
		InitialShardCount: 1,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3, sa4})

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	c := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

	metadata := c.Metadata()

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range metadata.GetStatus().UnsafeBorrow().Namespaces {
			for _, shard := range ns.Shards {
				if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	assert.Equal(t, 4, len(c.ListDataServer()))

	// Remove leader dataserver
	leaderID := metadata.GetStatus().UnsafeBorrow().Namespaces["my-ns-1"].Shards[0].Leader.GetNameOrDefault()
	d := make([]*proto.DataServerIdentity, 0)
	for _, sv := range clusterConfig.Servers {
		if sv.GetNameOrDefault() != leaderID {
			d = append(d, sv)
		}
	}
	clusterConfig.Servers = d

	version := configProvider.Watch().Load().Version
	_, err = configProvider.Store(clusterConfig, version)
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		return len(c.ListDataServer()) == 3
	}, 10*time.Second, 10*time.Millisecond)

	// Wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range metadata.GetStatus().UnsafeBorrow().Namespaces {
			for _, shard := range ns.Shards {
				return shard.Term > 0 && shard.GetStatusOrDefault() == proto.ShardStatusSteadyState
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

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

func TestCoordinator_RefreshServerInfo(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)

	metadataProvider := memory.NewProvider(metadatacommon.ClusterStatusCodec, metadatacommon.WatchDisabled)
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              "my-ns-1",
		ReplicationFactor: 3,
		InitialShardCount: 1,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})
	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(clusterConfig, metadatacommon.NotExists)
	assert.NoError(t, err)
	c := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

	metadata := c.Metadata()
	// wait for all shards to be ready
	assert.Eventually(t, func() bool {
		for _, ns := range metadata.GetStatus().UnsafeBorrow().Namespaces {
			for _, shard := range ns.Shards {
				if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)

	// change the localhost to 127.0.0.1
	clusterServer := make([]*proto.DataServerIdentity, 0, len(clusterConfig.Servers))
	for _, sv := range clusterConfig.Servers {
		clusterServer = append(clusterServer, &proto.DataServerIdentity{
			Name:     sv.Name,
			Public:   strings.ReplaceAll(sv.GetPublic(), "localhost", "127.0.0.1"),
			Internal: sv.GetInternal(),
		})
	}

	clusterConfig.Servers = clusterServer
	version := configProvider.Watch().Load().Version
	_, err = configProvider.Store(clusterConfig, version)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		for _, ns := range metadata.GetStatus().UnsafeBorrow().Namespaces {
			for _, shard := range ns.Shards {
				if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
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

	assert.NoError(t, s1.Close())
	assert.NoError(t, s2.Close())
	assert.NoError(t, s3.Close())
	assert.NoError(t, c.Close())
}
