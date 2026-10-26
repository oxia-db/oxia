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
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/ensemble"
	dataservercontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/dataserver"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/shard"
)

// The data server controllers that New starts may discover a node's features
// while New is still starting the shard controllers: the discovery must not
// observe the shard controllers half-built.
func TestRuntimeNewDiscoversFeaturesWhileStartingShardControllers(t *testing.T) {
	servers := []*proto.DataServerIdentity{
		{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"},
		{Name: new("s2"), Public: "s2:9091", Internal: "s2:8191"},
		{Name: new("s3"), Public: "s3:9091", Internal: "s3:8191"},
	}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              constant.DefaultNamespace,
			InitialShardCount: 64,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	})

	const shardCount = 64
	_, err := metadata.AllocateShardIDs(shardCount)
	require.NoError(t, err)
	shards := make(map[int64]*proto.ShardMetadata, shardCount)
	width := uint32(math.MaxUint32 / shardCount)
	for shard := range int64(shardCount) {
		hashRange := &proto.HashRange{Min: uint32(shard) * width, Max: uint32(shard+1)*width - 1}
		if shard == shardCount-1 {
			hashRange.Max = math.MaxUint32
		}
		shards[shard] = &proto.ShardMetadata{
			Status:         proto.ShardStatusSteadyState,
			Term:           1,
			Leader:         servers[shard%3],
			Ensemble:       servers,
			Int32HashRange: hashRange,
		}
	}
	require.NoError(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards:            shards,
	}))

	// Each node supports features, so its first handshake is a discovery
	rpcProvider := rpctest.NewRpcProvider()
	for _, server := range servers {
		rpcProvider.GetNode(server).SetNodeFeatures([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})
	}

	r, err := New(metadata, func(string) rpc.Provider { return rpcProvider })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	require.Eventually(t, func() bool {
		for _, server := range servers {
			status, ok := r.GetDataServerStatus(server.GetNameOrDefault())
			if !ok || status.GetState() != proto.DataServerState_DATA_SERVER_STATE_RUNNING {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond, "data servers did not become available")
}

// The runtime does not start without an instance id.
func TestRuntimeNewFailsWithoutInstanceID(t *testing.T) {
	metadata := &fakeMetadata{
		Metadata: newTestMetadata(t, &proto.ClusterConfiguration{}),
		getInstanceID: func() (string, error) {
			return "", errors.New("status unavailable")
		},
	}
	_, err := New(metadata, func(string) rpc.Provider { return rpctest.NewRpcProvider() })
	require.ErrorContains(t, err, "status unavailable")
}

// The runtime does not start without the status, the configuration and the
// load balancer configuration.
func TestRuntimeNewFailsWithoutMetadata(t *testing.T) {
	readErr := errors.New("metadata unavailable")
	for _, tc := range []struct {
		name  string
		setup func(m *fakeMetadata)
	}{
		{"status", func(m *fakeMetadata) {
			m.listNamespaceStatus = func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
				return nil, readErr
			}
		}},
		{"config", func(m *fakeMetadata) {
			m.getConfig = func() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
				return commonobject.Borrowed[*proto.ClusterConfiguration]{}, readErr
			}
		}},
		{"load balancer config", func(m *fakeMetadata) {
			m.getLoadBalancerConfig = func() (commonobject.Borrowed[*proto.LoadBalancer], error) {
				return commonobject.Borrowed[*proto.LoadBalancer]{}, readErr
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &fakeMetadata{Metadata: newTestMetadata(t, &proto.ClusterConfiguration{})}
			tc.setup(metadata)
			_, err := New(metadata, func(string) rpc.Provider { return rpctest.NewRpcProvider() })
			require.ErrorIs(t, err, readErr)
		})
	}
}

// The shards of a namespace that is no longer in the configuration still get
// a shard controller, and the data servers of their status a data server
// controller.
func TestRuntimeNewStartsControllersOfNamespaceMissingFromConfig(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{Servers: []*proto.DataServerIdentity{server}})
	require.NoError(t, metadata.CreateNamespaceStatus("removed", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         server,
				Ensemble:       []*proto.DataServerIdentity{server},
				Int32HashRange: &proto.HashRange{Min: 0, Max: math.MaxUint32},
			},
		},
	}))

	r, err := New(metadata, func(string) rpc.Provider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	c := r.(*runtime)
	c.RLock()
	defer c.RUnlock()
	assert.Contains(t, c.shardControllers, int64(0))
	assert.Contains(t, c.dataServerControllers, server.GetNameOrDefault())
}

// A shard deletion reported after Close started is left to Close, which
// closes the shard controllers itself.
func TestRuntimeShardDeletedIgnoredAfterClose(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	sc := &fakeShardController{}
	c.shardControllers[0] = sc
	require.NoError(t, c.Close())
	require.EqualValues(t, 1, sc.closes.Load())

	c.ShardDeleted(0)

	assert.Contains(t, c.shardControllers, int64(0))
	assert.EqualValues(t, 1, sc.closes.Load())
}

// A shard controller that fails to close is still removed.
func TestRuntimeShardDeletedRemovesControllerFailingToClose(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	sc := &fakeShardController{closeErr: errors.New("close failed")}
	c.shardControllers[0] = sc

	c.ShardDeleted(0)

	assert.NotContains(t, c.shardControllers, int64(0))
	assert.EqualValues(t, 1, sc.closes.Load())
}

// The status of a data server removed from the configuration is still
// reported while it drains.
func TestRuntimeGetDataServerStatusFallsBackToDrainingNodes(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	c.drainingNodes["s1"] = &fakeDataServerController{
		status:   dataservercontroller.NewStatus(dataservercontroller.Draining),
		features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
	}

	status, found := c.GetDataServerStatus("s1")
	require.True(t, found)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINING, status.GetState())
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, status.GetSupportedFeatures())

	_, found = c.GetDataServerStatus("unknown")
	assert.False(t, found)
}

func TestRuntimeCreateDataServerRejectsMissingIdentity(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))

	assert.False(t, c.CreateDataServer("s1", &proto.DataServer{}))
	assert.Empty(t, c.dataServerControllers)
}

// A data server added back while it drains gets a new controller, and its
// draining one is closed.
func TestRuntimeCreateDataServerReplacesDrainingController(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	draining := &fakeDataServerController{}
	c.drainingNodes["s1"] = draining
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}

	require.True(t, c.CreateDataServer("s1", &proto.DataServer{Identity: server}))

	assert.EqualValues(t, 1, draining.closes.Load())
	assert.NotContains(t, c.drainingNodes, "s1")
	require.Contains(t, c.dataServerControllers, "s1")
	assert.NotSame(t, draining, c.dataServerControllers["s1"])
	assert.False(t, c.CreateDataServer("s1", &proto.DataServer{Identity: server}))
}

func TestRuntimeDeleteDataServerIgnoresUnknownDataServer(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))

	c.DeleteDataServer("unknown")

	assert.Empty(t, c.drainingNodes)
}

func TestRuntimeCreateNamespaceReturnsStatusReadError(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
	namespace := &proto.Namespace{Name: "default", InitialShardCount: 2, ReplicationFactor: 1}
	readErr := errors.New("status unavailable")
	base := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers:    []*proto.DataServerIdentity{server},
		Namespaces: []*proto.Namespace{namespace},
	})
	allocations := 0
	c := newNamespaceTestRuntime(t, &fakeMetadata{
		Metadata: base,
		getNamespaceStatus: func(string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
			return commonobject.Borrowed[*proto.NamespaceStatus]{}, false, readErr
		},
		allocateShardIDs: func(count uint32) (int64, error) {
			allocations++
			return base.AllocateShardIDs(count)
		},
	})

	require.ErrorIs(t, c.CreateNamespace(namespace.Name, namespace), readErr)
	require.Zero(t, allocations)
	require.Empty(t, c.shardControllers)
}

func TestRuntimeCreateNamespaceRepairsCommittedWrite(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
	namespace := &proto.Namespace{Name: "default", InitialShardCount: 2, ReplicationFactor: 1}
	storeErr := errors.New("metadata write timed out after committing")
	base := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers:    []*proto.DataServerIdentity{server},
		Namespaces: []*proto.Namespace{namespace},
	})
	allocations := 0
	metadata := &fakeMetadata{
		Metadata: base,
		allocateShardIDs: func(count uint32) (int64, error) {
			allocations++
			return base.AllocateShardIDs(count)
		},
		createNamespaceStatus: func(name string, status *proto.NamespaceStatus) error {
			if err := base.CreateNamespaceStatus(name, status); err != nil {
				return err
			}
			return storeErr
		},
	}
	c := newNamespaceTestRuntime(t, metadata)

	require.ErrorIs(t, c.CreateNamespace(namespace.Name, namespace), storeErr)
	require.Empty(t, c.shardControllers)
	saved, exists, err := metadata.GetNamespaceStatus(namespace.Name)
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, saved.UnsafeBorrow().Shards, 2)
	require.Equal(t, 1, allocations)

	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	c.RLock()
	controllers := maps.Clone(c.shardControllers)
	c.RUnlock()
	require.Len(t, controllers, 2)
	for shard := range saved.UnsafeBorrow().Shards {
		require.Contains(t, controllers, shard)
	}

	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	c.RLock()
	repeated := maps.Clone(c.shardControllers)
	c.RUnlock()
	for shard, controller := range controllers {
		require.Same(t, controller, repeated[shard])
	}

	// Repair a partially initialized namespace without replacing its other controller.
	c.Lock()
	delete(c.shardControllers, 0)
	c.Unlock()
	require.NoError(t, controllers[0].Close())
	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	c.RLock()
	defer c.RUnlock()
	require.Len(t, c.shardControllers, 2)
	require.NotSame(t, controllers[0], c.shardControllers[0])
	require.Same(t, controllers[1], c.shardControllers[1])
	require.Equal(t, 1, allocations)
}

func TestRuntimeCreateNamespaceRepairsAlreadyExistsOnRetry(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
	namespace := &proto.Namespace{Name: "default", InitialShardCount: 1, ReplicationFactor: 1}
	createErr := fmt.Errorf("competing creation: %w", metadatacommon.ErrAlreadyExists)
	competing := &proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
		42: {Term: 7, Ensemble: []*proto.DataServerIdentity{server}},
	}}
	base := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers:    []*proto.DataServerIdentity{server},
		Namespaces: []*proto.Namespace{namespace},
	})
	allocations := 0
	c := newNamespaceTestRuntime(t, &fakeMetadata{
		Metadata: base,
		allocateShardIDs: func(count uint32) (int64, error) {
			allocations++
			return base.AllocateShardIDs(count)
		},
		createNamespaceStatus: func(name string, _ *proto.NamespaceStatus) error {
			if err := base.CreateNamespaceStatus(name, competing); err != nil {
				return err
			}
			return createErr
		},
	})

	require.Same(t, createErr, c.CreateNamespace(namespace.Name, namespace))
	require.Empty(t, c.shardControllers)
	require.Equal(t, 1, allocations)

	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	c.RLock()
	defer c.RUnlock()
	require.Len(t, c.shardControllers, 1)
	require.Contains(t, c.shardControllers, int64(42))
	require.NotContains(t, c.shardControllers, int64(0))
	require.Equal(t, 1, allocations)
}

func TestRuntimeCreateNamespaceDoesNotReviveDeletedShards(t *testing.T) {
	for _, count := range []uint32{1, 2} {
		t.Run(fmt.Sprintf("%d shards", count), func(t *testing.T) {
			server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
			namespace := &proto.Namespace{Name: "default", InitialShardCount: count, ReplicationFactor: 1}
			base := newTestMetadata(t, &proto.ClusterConfiguration{
				Servers:    []*proto.DataServerIdentity{server},
				Namespaces: []*proto.Namespace{namespace},
			})
			var afterRead func()
			c := newNamespaceTestRuntime(t, &fakeMetadata{
				Metadata: base,
				getNamespaceStatus: func(name string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
					status, exists, err := base.GetNamespaceStatus(name)
					if hook := afterRead; hook != nil {
						afterRead = nil
						hook()
					}
					return status, exists, err
				},
			})
			require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
			// Wait for the original controllers to start before deleting their status.
			requests := c.rpc.(*rpctest.RpcProvider).GetNode(server).NewTermRequests
			for range count {
				select {
				case <-requests:
				case <-time.After(5 * time.Second):
					t.Fatal("initial shard controller did not start")
				}
			}

			// Remove the shard and its controller after the initial status read,
			// while returning the old snapshot to CreateNamespace.
			afterRead = func() {
				require.NoError(t, base.DeleteShardStatus(namespace.Name, 0))
				c.ShardDeleted(0)
			}
			err := c.CreateNamespace(namespace.Name, namespace)
			if count == 1 {
				require.ErrorIs(t, err, metadatacommon.ErrConflict)
				require.Contains(t, err.Error(), namespace.Name)
			} else {
				require.NoError(t, err)
			}
			c.RLock()
			defer c.RUnlock()
			require.NotContains(t, c.shardControllers, int64(0))
			require.Len(t, c.shardControllers, int(count)-1)
		})
	}
}

func TestRuntimeCreateNamespaceSkipsDeletingShards(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
	namespace := &proto.Namespace{Name: "default", InitialShardCount: 2, ReplicationFactor: 1}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers:    []*proto.DataServerIdentity{server},
		Namespaces: []*proto.Namespace{namespace},
	})
	require.NoError(t, metadata.CreateNamespaceStatus(namespace.Name, &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			0: {Status: proto.ShardStatusDeleting, Ensemble: []*proto.DataServerIdentity{server}},
			1: {Status: proto.ShardStatusUnknown, Ensemble: []*proto.DataServerIdentity{server}},
		},
	}))
	c := newNamespaceTestRuntime(t, metadata)

	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	c.RLock()
	defer c.RUnlock()
	require.NotContains(t, c.shardControllers, int64(0))
	require.Contains(t, c.shardControllers, int64(1))
}

// A namespace whose ensemble cannot be selected for one of its shards is not
// created, rather than created without that shard.
func TestRuntimeCreateNamespaceFailsWhenEnsembleSelectionFails(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
	namespace := &proto.Namespace{Name: "default", InitialShardCount: 2, ReplicationFactor: 1}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers:    []*proto.DataServerIdentity{server},
		Namespaces: []*proto.Namespace{namespace},
	})
	calls := 0
	c := &runtime{
		ctx:              t.Context(),
		logger:           slog.Default(),
		metadata:         metadata,
		ensembleSelector: failingEnsembleSelector{succeeding: 1, calls: &calls},
		shardControllers: make(map[int64]shardcontroller.Controller),
	}

	require.ErrorContains(t, c.CreateNamespace(namespace.Name, namespace), "not enough data servers")
	require.Equal(t, 2, calls)
	require.Empty(t, c.shardControllers)
	_, exists, err := metadata.GetNamespaceStatus(namespace.Name)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestRuntimeCreateNamespaceHandlesStatusErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"already exists", fmt.Errorf("namespace creation: %w", metadatacommon.ErrAlreadyExists)},
		{"write failed", errors.New("status store unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
			namespace := &proto.Namespace{Name: "default", InitialShardCount: 1, ReplicationFactor: 1}
			var proposed *proto.NamespaceStatus
			metadata := &fakeMetadata{
				Metadata: newTestMetadata(t, &proto.ClusterConfiguration{
					Servers:    []*proto.DataServerIdentity{server},
					Namespaces: []*proto.Namespace{namespace},
				}),
				createNamespaceStatus: func(_ string, status *proto.NamespaceStatus) error {
					proposed = status
					return tc.err
				},
			}
			c := &runtime{
				ctx:              t.Context(),
				logger:           slog.Default(),
				metadata:         metadata,
				ensembleSelector: namespaceEnsembleSelector{},
				shardControllers: make(map[int64]shardcontroller.Controller),
			}

			require.ErrorIs(t, c.CreateNamespace(namespace.Name, namespace), tc.err)
			require.NotNil(t, proposed)
			require.Len(t, proposed.Shards, 1)
			require.Empty(t, c.shardControllers)
			_, exists, err := metadata.GetNamespaceStatus(namespace.Name)
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

// CreateNamespace creates nothing when it can't allocate the shard ids or
// read the status of the other namespaces.
func TestRuntimeCreateNamespaceReturnsAllocationErrors(t *testing.T) {
	readErr := errors.New("metadata unavailable")
	for _, tc := range []struct {
		name  string
		setup func(m *fakeMetadata)
	}{
		{"allocate shard ids", func(m *fakeMetadata) {
			m.allocateShardIDs = func(uint32) (int64, error) { return 0, readErr }
		}},
		{"list status", func(m *fakeMetadata) {
			m.listNamespaceStatus = func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
				return nil, readErr
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
			namespace := &proto.Namespace{Name: "default", InitialShardCount: 1, ReplicationFactor: 1}
			metadata := &fakeMetadata{Metadata: newTestMetadata(t, &proto.ClusterConfiguration{
				Servers:    []*proto.DataServerIdentity{server},
				Namespaces: []*proto.Namespace{namespace},
			})}
			tc.setup(metadata)
			c := newNamespaceTestRuntime(t, metadata)

			require.ErrorIs(t, c.CreateNamespace(namespace.Name, namespace), readErr)
			require.Empty(t, c.shardControllers)
			_, exists, err := metadata.GetNamespaceStatus(namespace.Name)
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

// The shard controllers are not started when the status can't be read back
// after it was created: the next CreateNamespace starts them.
func TestRuntimeCreateNamespaceReturnsControllerInitializationError(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "server:6648", Internal: "server:6649"}
	namespace := &proto.Namespace{Name: "default", InitialShardCount: 1, ReplicationFactor: 1}
	base := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers:    []*proto.DataServerIdentity{server},
		Namespaces: []*proto.Namespace{namespace},
	})
	readErr := errors.New("status unavailable")
	reads := 0
	c := newNamespaceTestRuntime(t, &fakeMetadata{
		Metadata: base,
		getNamespaceStatus: func(name string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
			reads++
			if reads == 2 {
				return commonobject.Borrowed[*proto.NamespaceStatus]{}, false, readErr
			}
			return base.GetNamespaceStatus(name)
		},
	})

	require.ErrorIs(t, c.CreateNamespace(namespace.Name, namespace), readErr)
	require.Empty(t, c.shardControllers)
	_, exists, err := base.GetNamespaceStatus(namespace.Name)
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, c.CreateNamespace(namespace.Name, namespace))
	require.Len(t, c.shardControllers, 1)
}

func TestRuntimeDeleteNamespaceIgnoresUnknownNamespace(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	sc := &fakeShardController{}
	c.shardControllers[0] = sc

	c.DeleteNamespace("unknown")

	assert.Zero(t, sc.deletes.Load())
}

func TestRuntimeSelectNewEnsembleReturnsDataServerListError(t *testing.T) {
	listErr := errors.New("data servers unavailable")
	c := newNamespaceTestRuntime(t, &fakeMetadata{
		Metadata: newTestMetadata(t, &proto.ClusterConfiguration{}),
		listDataServer: func() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
			return nil, listErr
		},
	})

	_, err := c.selectNewEnsemble("default", 0, &proto.Namespace{ReplicationFactor: 1}, nil, nil)
	assert.ErrorIs(t, err, listErr)
}

// The selected data servers must be known data servers with an identity.
func TestRuntimeSelectNewEnsembleRejectsUnknownDataServers(t *testing.T) {
	for _, tc := range []struct {
		name     string
		selected string
	}{
		{"not registered", "ghost"},
		{"without identity", "anonymous"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newNamespaceTestRuntime(t, &fakeMetadata{
				Metadata: newTestMetadata(t, &proto.ClusterConfiguration{}),
				listDataServer: func() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
					return map[string]commonobject.Borrowed[*proto.DataServer]{
						"anonymous": commonobject.Borrow(&proto.DataServer{}),
					}, nil
				},
			})
			c.ensembleSelector = fixedEnsembleSelector{tc.selected}

			_, err := c.selectNewEnsemble("default", 0, &proto.Namespace{ReplicationFactor: 1}, nil, nil)
			assert.ErrorContains(t, err, "failed to find node "+tc.selected)
		})
	}
}

// A draining data server that becomes unavailable is closed, even if closing
// it fails, and the shard controllers are told.
func TestRuntimeBecameUnavailableClosesDrainingNode(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	draining := &fakeDataServerController{closeErr: errors.New("close failed")}
	c.drainingNodes["s1"] = draining
	sc := &fakeShardController{}
	c.shardControllers[0] = sc

	c.BecameUnavailable(server)

	assert.NotContains(t, c.drainingNodes, "s1")
	assert.Eventually(t, func() bool {
		return draining.closes.Load() == 1
	}, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, []*proto.DataServerIdentity{server}, sc.unavailable)
}

// An action on a shard without a controller, e.g. deleted in the meantime,
// fails.
func TestRuntimeDispatchActionFailsForUnknownShard(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	ac := action.NewChangeEnsembleAction(42, &proto.DataServerIdentity{}, &proto.DataServerIdentity{})

	c.dispatchAction(ac)

	_, err := ac.Wait()
	assert.ErrorIs(t, err, constant.ErrResourceUnavailable)
}

// The clients merge the results of the shards of a namespace in the order of
// its keys, so the assignments of each namespace carry its key sorting. The
// data servers keep the keys of a namespace without a key sorting in
// hierarchical order.
func TestRuntimeComputeNewAssignmentsSetsKeySorting(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "public:6648", Internal: "internal:6649"}
	expected := map[string]proto.KeySorting{
		"natural":      proto.KeySorting_KEY_SORTING_NATURAL,
		"hierarchical": proto.KeySorting_KEY_SORTING_HIERARCHICAL,
		"":             proto.KeySorting_KEY_SORTING_HIERARCHICAL,
	}

	clusterConfig := &proto.ClusterConfiguration{Servers: []*proto.DataServerIdentity{server}}
	for keySorting := range expected {
		clusterConfig.Namespaces = append(clusterConfig.Namespaces, &proto.Namespace{
			Name:              "ns-" + keySorting,
			InitialShardCount: 1,
			ReplicationFactor: 1,
			KeySorting:        keySorting,
		})
	}
	metadata := newTestMetadata(t, clusterConfig)
	for keySorting := range expected {
		require.NoError(t, metadata.CreateNamespaceStatus("ns-"+keySorting, &proto.NamespaceStatus{
			ReplicationFactor: 1,
			Shards: map[int64]*proto.ShardMetadata{
				0: {
					Status:         proto.ShardStatusSteadyState,
					Leader:         server,
					Ensemble:       []*proto.DataServerIdentity{server},
					Int32HashRange: &proto.HashRange{Min: 0, Max: 100},
				},
			},
		}))
	}
	c := &runtime{
		RWMutex:          sync.RWMutex{},
		ctx:              t.Context(),
		metadata:         metadata,
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
	}

	c.computeNewAssignments()
	assignments := c.assignmentsWatch.Load()

	for keySorting, expectedKeySorting := range expected {
		nsAssignments, ok := assignments.Namespaces["ns-"+keySorting]
		require.True(t, ok)
		assert.Equal(t, expectedKeySorting, nsAssignments.KeySorting, "key sorting %q", keySorting)
	}
}

func TestRuntimeComputeNewAssignmentsRetriesStatusLoadError(t *testing.T) {
	server := &proto.DataServerIdentity{Public: "public:6648", Internal: "internal:6649"}
	base := newTestMetadata(t, &proto.ClusterConfiguration{
		Servers:    []*proto.DataServerIdentity{server},
		Namespaces: []*proto.Namespace{{Name: "ns", InitialShardCount: 1, ReplicationFactor: 1}},
	})
	require.NoError(t, base.CreateNamespaceStatus("ns", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Leader:         server,
				Ensemble:       []*proto.DataServerIdentity{server},
				Int32HashRange: &proto.HashRange{Min: 0, Max: 100},
			},
		},
	}))
	failures := 2
	c := &runtime{
		RWMutex: sync.RWMutex{},
		ctx:     t.Context(),
		logger:  slog.Default(),
		metadata: &fakeMetadata{
			Metadata: base,
			listNamespaceStatus: func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
				if failures > 0 {
					failures--
					return nil, errors.New("status unavailable")
				}
				return base.ListNamespaceStatus()
			},
		},
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
	}

	c.computeNewAssignments()

	require.Zero(t, failures)
	nsAssignments, ok := c.assignmentsWatch.Load().Namespaces["ns"]
	require.True(t, ok)
	require.Len(t, nsAssignments.Assignments, 1)
	assert.Equal(t, server.GetPublic(), nsAssignments.Assignments[0].Leader)
}

func TestRuntimeComputeNewAssignmentsRetriesConfigLoadErrors(t *testing.T) {
	loadErr := errors.New("config unavailable")
	for _, tc := range []struct {
		name  string
		setup func(m *fakeMetadata, fail func() bool)
	}{
		{"config", func(m *fakeMetadata, fail func() bool) {
			m.getConfig = func() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
				if fail() {
					return commonobject.Borrowed[*proto.ClusterConfiguration]{}, loadErr
				}
				return m.Metadata.GetConfig()
			}
		}},
		{"namespaces", func(m *fakeMetadata, fail func() bool) {
			m.listNamespace = func() (map[string]commonobject.Borrowed[*proto.Namespace], error) {
				if fail() {
					return nil, loadErr
				}
				return m.Metadata.ListNamespace()
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &proto.DataServerIdentity{Public: "public:6648", Internal: "internal:6649"}
			base := newTestMetadata(t, &proto.ClusterConfiguration{
				Servers:    []*proto.DataServerIdentity{server},
				Namespaces: []*proto.Namespace{{Name: "ns", InitialShardCount: 1, ReplicationFactor: 1}},
			})
			require.NoError(t, base.CreateNamespaceStatus("ns", &proto.NamespaceStatus{
				ReplicationFactor: 1,
				Shards: map[int64]*proto.ShardMetadata{
					0: {
						Status:         proto.ShardStatusSteadyState,
						Leader:         server,
						Ensemble:       []*proto.DataServerIdentity{server},
						Int32HashRange: &proto.HashRange{Min: 0, Max: 100},
					},
				},
			}))
			failures := 2
			metadata := &fakeMetadata{Metadata: base}
			tc.setup(metadata, func() bool {
				if failures > 0 {
					failures--
					return true
				}
				return false
			})
			c := &runtime{
				ctx:              t.Context(),
				logger:           slog.Default(),
				metadata:         metadata,
				assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
			}

			c.computeNewAssignments()

			require.Zero(t, failures)
			nsAssignments, ok := c.assignmentsWatch.Load().Namespaces["ns"]
			require.True(t, ok)
			require.Len(t, nsAssignments.Assignments, 1)
		})
	}
}

func TestRuntimeComputeNewAssignmentsIncludesExtraAuthorities(t *testing.T) {
	leader := &proto.DataServerIdentity{
		Public:   "tls://leader-public:6648",
		Internal: "tls://leader-internal:6649",
	}

	clusterConfig := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
		Servers: []*proto.DataServerIdentity{{
			Public:   leader.Public,
			Internal: leader.Internal,
		}},
		AllowExtraAuthorities: []string{
			"tls://bootstrap:6648",
			leader.Public,
		},
	}
	metadata := newTestMetadata(t, clusterConfig)
	require.NoError(t, metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:   proto.ShardStatusUnknown,
				Leader:   leader,
				Ensemble: []*proto.DataServerIdentity{leader},
				Int32HashRange: &proto.HashRange{
					Min: 0,
					Max: 100,
				},
			},
		},
	}))
	c := &runtime{
		RWMutex:          sync.RWMutex{},
		ctx:              t.Context(),
		metadata:         metadata,
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
	}

	c.computeNewAssignments()
	assignments := c.assignmentsWatch.Load()

	nsAssignments, ok := assignments.Namespaces["default"]
	require.True(t, ok)
	require.Len(t, nsAssignments.Assignments, 1)
	assert.Equal(t, "tls://leader-public:6648", nsAssignments.Assignments[0].Leader)
	assert.Equal(t,
		[]string{"leader-public:6648", "leader-internal:6649", "bootstrap:6648"},
		assignments.AllowedAuthorities,
	)
}

func TestRuntimeComputeNewAssignmentsKeepsRemovedShardNodeAuthorities(t *testing.T) {
	active := &proto.DataServerIdentity{
		Public:   "active-public:6648",
		Internal: "active-internal:6649",
	}
	removed := &proto.DataServerIdentity{
		Public:   "removed-public:6648",
		Internal: "removed-internal:6649",
	}

	clusterConfig := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
		Servers: []*proto.DataServerIdentity{{
			Public:   active.Public,
			Internal: active.Internal,
		}},
	}
	metadata := newTestMetadata(t, clusterConfig)
	require.NoError(t, metadata.CreateNamespaceStatus("default", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:       proto.ShardStatusUnknown,
				Leader:       removed,
				Ensemble:     []*proto.DataServerIdentity{removed},
				RemovedNodes: []*proto.DataServerIdentity{removed},
				Int32HashRange: &proto.HashRange{
					Min: 0,
					Max: 100,
				},
			},
		},
	}))
	c := &runtime{
		RWMutex:          sync.RWMutex{},
		ctx:              t.Context(),
		metadata:         metadata,
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
	}

	c.computeNewAssignments()
	assignments := c.assignmentsWatch.Load()

	assert.Equal(t,
		[]string{
			"active-public:6648",
			"active-internal:6649",
			"removed-public:6648",
			"removed-internal:6649",
		},
		assignments.AllowedAuthorities,
	)
}

// The data servers get their first assignments once the runtime computed them,
// not the empty ones the runtime starts with: a data server initialized with
// those would reject the clients, as no authority is allowed.
func TestRuntimeSubscribeShardAssignmentsWaitsForFirstComputation(t *testing.T) {
	servers := []*proto.DataServerIdentity{
		{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"},
		{Name: new("s2"), Public: "s2:9091", Internal: "s2:8191"},
		{Name: new("s3"), Public: "s3:9091", Internal: "s3:8191"},
	}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              constant.DefaultNamespace,
			InitialShardCount: 1,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	})
	_, err := metadata.AllocateShardIDs(1)
	require.NoError(t, err)
	require.NoError(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards: map[int64]*proto.ShardMetadata{0: {
			Status:         proto.ShardStatusSteadyState,
			Term:           1,
			Leader:         servers[0],
			Ensemble:       servers,
			Int32HashRange: &proto.HashRange{Min: 0, Max: math.MaxUint32},
		}},
	}))

	rpcProvider := rpctest.NewRpcProvider()
	r, err := New(metadata, func(string) rpc.Provider { return rpcProvider })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	// The data servers are ready for their assignments before the reconciler
	// computes them
	require.Eventually(t, func() bool {
		for _, server := range servers {
			if rpcProvider.GetNode(server).PushShardAssignmentsCount.Load() == 0 {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)
	r.RecomputeAssignments()

	for _, server := range servers {
		select {
		case assignments := <-rpcProvider.GetNode(server).ShardAssignmentsStream.Updates:
			assert.Contains(t, assignments.GetAllowedAuthorities(), server.Public)
		case <-time.After(10 * time.Second):
			assert.Fail(t, "no assignments", server.GetNameOrDefault())
		}
	}
}

func TestRuntimeFindDataServerFeaturesOmitsUnknownNodes(t *testing.T) {
	c := &runtime{
		dataServerControllers: map[string]dataservercontroller.Controller{},
		drainingNodes:         map[string]dataservercontroller.Controller{},
	}

	dataServer := &proto.DataServerIdentity{Internal: "s1:6649"}

	features := c.findDataServerFeatures([]*proto.DataServerIdentity{dataServer})

	assert.NotContains(t, features, dataServer.GetNameOrDefault())
}

func TestRuntimeFindDataServerFeaturesFallsBackToDrainingNodes(t *testing.T) {
	features := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}
	c := &runtime{
		dataServerControllers: map[string]dataservercontroller.Controller{},
		drainingNodes: map[string]dataservercontroller.Controller{
			"s1:6649": &fakeDataServerController{features: features},
		},
	}

	found := c.findDataServerFeatures([]*proto.DataServerIdentity{{Internal: "s1:6649"}})

	assert.Equal(t, map[string][]proto.Feature{"s1:6649": features}, found)
}

// A data server registered without metadata is a candidate without labels.
func TestDataServersToCandidatesAndMetadataDefaultsMissingMetadata(t *testing.T) {
	labels := &proto.DataServerMetadata{Labels: map[string]string{"zone": "a"}}
	candidates, metadata := dataServersToCandidatesAndMetadata(map[string]commonobject.Borrowed[*proto.DataServer]{
		"s1": commonobject.Borrow(&proto.DataServer{Metadata: labels}),
		"s2": commonobject.Borrow(&proto.DataServer{}),
	})

	assert.ElementsMatch(t, []string{"s1", "s2"}, candidates.Values())
	assert.Same(t, labels, metadata["s1"])
	require.NotNil(t, metadata["s2"])
	assert.Empty(t, metadata["s2"].GetLabels())
}

// The data servers of a shard are its leader, the members of its ensemble,
// and the ones being removed from it.
func TestDataServersFromStatusIncludesAllShardMembers(t *testing.T) {
	leader := &proto.DataServerIdentity{Public: "leader:9091", Internal: "leader:8191"}
	member := &proto.DataServerIdentity{Public: "member:9091", Internal: "member:8191"}
	removed := &proto.DataServerIdentity{Public: "removed:9091", Internal: "removed:8191"}
	pendingDelete := &proto.DataServerIdentity{Public: "pending:9091", Internal: "pending:8191"}
	status := map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
		"ns": commonobject.Borrow(&proto.NamespaceStatus{Shards: map[int64]*proto.ShardMetadata{
			0: {
				Leader:                  leader,
				Ensemble:                []*proto.DataServerIdentity{leader, member},
				RemovedNodes:            []*proto.DataServerIdentity{removed},
				PendingDeleteShardNodes: []*proto.DataServerIdentity{pendingDelete},
			},
			// Without a leader
			1: {Ensemble: []*proto.DataServerIdentity{member}},
		}}),
	}

	var names []string
	for _, server := range dataServersFromStatus(status) {
		names = append(names, server.GetNameOrDefault())
	}
	assert.ElementsMatch(t, []string{
		leader.GetNameOrDefault(), member.GetNameOrDefault(),
		removed.GetNameOrDefault(), pendingDelete.GetNameOrDefault(),
	}, names)
}

func TestRuntimeSplitCompletePublishesConsistentAssignments(t *testing.T) {
	r := newSplitTestRuntime(t)
	r.queueSplitResponses()

	r.runSplit(t, 0)

	snapshots := r.publishedSnapshots()
	for i, snapshot := range snapshots {
		ids, err := checkAssignments(snapshot)
		assert.NoError(t, err, "snapshot %d/%d publishes shards %v", i+1, len(snapshots), ids)
	}

	ids, err := checkAssignments(snapshots[len(snapshots)-1])
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{splitOtherShard, splitLeftChild, splitRightChild}, ids)
}

func TestRuntimeSplitAbortedPublishesConsistentAssignments(t *testing.T) {
	r := newSplitTestRuntime(t)
	// No data server responses: Bootstrap cannot fence the children, and the
	// split is aborted once it times out

	r.runSplit(t, 2*time.Second)

	snapshots := r.publishedSnapshots()
	for i, snapshot := range snapshots {
		ids, err := checkAssignments(snapshot)
		assert.NoError(t, err, "snapshot %d/%d publishes shards %v", i+1, len(snapshots), ids)
	}

	ids, err := checkAssignments(snapshots[len(snapshots)-1])
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{splitParentShard, splitOtherShard}, ids)
}

// TestRuntimeNewResumesSplitPastPointOfNoReturn verifies that a coordinator
// restarting with a split past the point of no return completes it. The
// parent's shard controller resumes the split, and must not elect the parent
// again: only the split fences it, and the parent then gets deleted.
func TestRuntimeNewResumesSplitPastPointOfNoReturn(t *testing.T) {
	r := newSplitTestRuntime(t)

	// The previous coordinator elected the child leaders at the parent's term,
	// then moved the split past the point of no return
	require.NoError(t, r.metadata.PatchNamespaceStatus(constant.DefaultNamespace, func(status *proto.NamespaceStatus) bool {
		shards := status.Shards
		parent := shards[splitParentShard]
		parent.Term = splitParentTerm + 1
		parent.Leader = nil
		parent.Status = proto.ShardStatusElection
		parent.Split.Phase = proto.SplitPhaseFinalize
		parent.Split.ParentTermAtBootstrap = splitParentTerm
		for childId, leader := range map[int64]*proto.DataServerIdentity{splitLeftChild: splitLs1, splitRightChild: splitRs1} {
			child := shards[childId]
			child.Term = splitParentTerm
			child.Leader = leader
			child.Split.Phase = proto.SplitPhaseFinalize
		}
		return true
	}))

	// The split fences the parent and re-elects the children, then the
	// parent's shard controller deletes the parent
	for _, node := range []*proto.DataServerIdentity{
		splitPs1, splitPs2, splitPs3, splitLs1, splitLs2, splitLs3, splitRs1, splitRs2, splitRs3,
	} {
		r.rpc.GetNode(node).NewTermResponse(splitParentTerm, 105, nil)
	}
	r.rpc.GetNode(splitLs1).BecomeLeaderResponse(nil)
	r.rpc.GetNode(splitRs1).BecomeLeaderResponse(nil)
	for _, node := range []*proto.DataServerIdentity{splitPs1, splitPs2, splitPs3} {
		r.rpc.GetNode(node).DeleteShardResponse(nil)
	}

	// The restarted coordinator starts the controllers of the split shards,
	// the way New does: the controller of the parent resumes the split
	r.Lock()
	status, err := r.metadata.ListNamespaceStatus()
	require.NoError(t, err)
	nsConfig, err := r.namespaceConfigForSplit(constant.DefaultNamespace)
	require.NoError(t, err)
	for _, shard := range []int64{splitParentShard, splitLeftChild, splitRightChild} {
		r.shardControllers[shard] = shardcontroller.NewController(constant.DefaultNamespace, shard,
			nsConfig,
			status[constant.DefaultNamespace].UnsafeBorrow().Shards[shard], r.metadata,
			r.findDataServerFeatures, r.runtime, r.rpc, shardcontroller.DefaultPeriodicTasksInterval)
	}
	r.Unlock()
	t.Cleanup(func() {
		r.Lock()
		controllers := maps.Clone(r.shardControllers)
		r.Unlock()
		for _, controller := range controllers {
			assert.NoError(t, controller.Close())
		}
	})

	require.Eventually(t, func() bool {
		r.RLock()
		defer r.RUnlock()
		_, parentExists := r.shardControllers[splitParentShard]
		return !parentExists
	}, 30*time.Second, 10*time.Millisecond, "split did not complete")

	// Only the split fenced the parent
	for _, node := range []*proto.DataServerIdentity{splitPs1, splitPs2, splitPs3} {
		requests := r.rpc.GetNode(node).NewTermRequests
		require.Len(t, requests, 1)
		assert.Equal(t, splitParentTerm+1, (<-requests).Term)
	}

	snapshots := r.publishedSnapshots()
	for i, snapshot := range snapshots {
		ids, err := checkAssignments(snapshot)
		assert.NoError(t, err, "snapshot %d/%d publishes shards %v", i+1, len(snapshots), ids)
	}

	ids, err := checkAssignments(snapshots[len(snapshots)-1])
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{splitOtherShard, splitLeftChild, splitRightChild}, ids)
}

// The leader elections of the other shards of the namespace run concurrently
// with a split: none of the split's status updates may revert them.
func TestRuntimeSplitKeepsConcurrentLeaderElections(t *testing.T) {
	r := newSplitTestRuntime(t)
	r.queueSplitResponses()

	metadata := r.metadata
	var elected *proto.ShardMetadata
	racing := &fakeMetadata{Metadata: metadata}
	racing.beforeStatusWrites(func() {
		if elected != nil {
			// The previous status update of the split kept the election
			assertElectionKept(t, metadata, splitOtherShard, elected)
		}
		elected = electLeader(t, metadata, splitOtherShard)
	})
	r.metadata = racing

	r.runSplit(t, 0)

	require.NotNil(t, elected)
	assertElectionKept(t, metadata, splitOtherShard, elected)
}

// InitiateSplit validates the parent and selects the ensembles of the children
// from the status it reads first: leader elections completed in the meantime
// must be kept when the split is written.
func TestRuntimeInitiateSplitKeepsConcurrentLeaderElections(t *testing.T) {
	r, metadata := newInitiateSplitTestRuntime(t)

	var once sync.Once
	var parentElected, otherElected *proto.ShardMetadata
	metadata.beforeStatusWrites(func() {
		once.Do(func() {
			parentElected = electLeader(t, metadata.Metadata, splitParentShard)
			otherElected = electLeader(t, metadata.Metadata, splitOtherShard)
		})
	})

	leftChild, rightChild, err := r.InitiateSplit(constant.DefaultNamespace, splitParentShard, nil)
	require.NoError(t, err)
	require.NotNil(t, otherElected)

	assertElectionKept(t, metadata, splitOtherShard, otherElected)
	assertElectionKept(t, metadata, splitParentShard, parentElected)

	parent, _, err := metadata.GetShardStatus(constant.DefaultNamespace, splitParentShard)
	require.NoError(t, err)
	assert.Equal(t, []int64{leftChild, rightChild}, parent.UnsafeBorrow().GetSplit().GetChildShardIds())
	for _, child := range []int64{leftChild, rightChild} {
		childMeta, exists, err := metadata.GetShardStatus(constant.DefaultNamespace, child)
		require.NoError(t, err)
		require.True(t, exists, "child shard %d", child)
		assert.Equal(t, splitParentShard, childMeta.UnsafeBorrow().GetSplit().GetParentShardId())
	}
}

// A leader election of the parent that starts before the split is written
// makes InitiateSplit fail, as it would have if it had started earlier.
func TestRuntimeInitiateSplitFailsIfParentElectionStarts(t *testing.T) {
	r, metadata := newInitiateSplitTestRuntime(t)

	var once sync.Once
	var election *proto.ShardMetadata
	metadata.beforeStatusWrites(func() {
		once.Do(func() {
			borrowed, _, err := metadata.GetShardStatus(constant.DefaultNamespace, splitParentShard)
			assert.NoError(t, err)
			election = pb.CloneOf(borrowed.UnsafeBorrow())
			election.Status = proto.ShardStatusElection
			election.Leader = nil
			election.Term++
			assert.NoError(t, metadata.Metadata.UpdateShardStatus(constant.DefaultNamespace, splitParentShard, election))
		})
	})

	_, _, err := r.InitiateSplit(constant.DefaultNamespace, splitParentShard, nil)
	assert.ErrorContains(t, err, "not in steady state")

	require.NotNil(t, election)
	assertElectionKept(t, metadata, splitParentShard, election)
	parent, _, err := metadata.GetShardStatus(constant.DefaultNamespace, splitParentShard)
	require.NoError(t, err)
	assert.Nil(t, parent.UnsafeBorrow().GetSplit())
	namespace, _, err := metadata.GetNamespaceStatus(constant.DefaultNamespace)
	require.NoError(t, err)
	assert.Len(t, namespace.UnsafeBorrow().GetShards(), 2)

	r.RLock()
	defer r.RUnlock()
	assert.Empty(t, r.shardControllers)
}

// The children of a split inherit the features enabled on the parent, and
// the split can't complete if the ensemble of a child doesn't support them.
// InitiateSplit selects the ensembles of the children among the data servers
// that support the features of the parent's ensemble.
func TestRuntimeInitiateSplitSelectsChildrenSupportingParentFeatures(t *testing.T) {
	r, metadata := newInitiateSplitTestRuntime(t)

	// The ls* data servers don't support ordered writes yet, e.g. during a
	// rolling upgrade, and they are the least loaded ones with the rs* ones
	older := []*proto.DataServerIdentity{splitLs1, splitLs2, splitLs3}
	r.dataServerControllers = map[string]dataservercontroller.Controller{}
	for _, server := range []*proto.DataServerIdentity{
		splitPs1, splitPs2, splitPs3, splitLs1, splitLs2, splitLs3, splitRs1, splitRs2, splitRs3,
	} {
		features := feature.SupportedFeatures()
		if slices.Contains(older, server) {
			features = slices.DeleteFunc(features, func(f proto.Feature) bool {
				return f == proto.Feature_FEATURE_ORDERED_WRITES
			})
		}
		r.dataServerControllers[server.GetNameOrDefault()] = &fakeDataServerController{features: features}
	}

	leftChild, rightChild, err := r.InitiateSplit(constant.DefaultNamespace, splitParentShard, nil)
	require.NoError(t, err)

	for _, child := range []int64{leftChild, rightChild} {
		childMeta, exists, err := metadata.GetShardStatus(constant.DefaultNamespace, child)
		require.NoError(t, err)
		require.True(t, exists, "child shard %d", child)
		for _, server := range childMeta.UnsafeBorrow().GetEnsemble() {
			assert.False(t, slices.ContainsFunc(older, func(o *proto.DataServerIdentity) bool {
				return o.GetNameOrDefault() == server.GetNameOrDefault()
			}), "child shard %d has %s in its ensemble", child, server.GetNameOrDefault())
		}
	}
}

// A change of the parent's ensemble that the balancer planned before the split
// started, and that reaches the parent's shard controller after it, is
// rejected: the parent's ensemble doesn't change until the split ends.
func TestRuntimeInitiateSplitRejectsChangeEnsemblePlannedBefore(t *testing.T) {
	r, metadata := newInitiateSplitTestRuntime(t)
	rpcProvider := r.rpc.(*rpctest.RpcProvider)

	// The parent's shard controller verifies its ensemble when it starts
	rpcProvider.GetNode(splitPs1).GetStatusResponse(splitParentTerm, proto.ServingStatus_LEADER, 0, 0)
	rpcProvider.GetNode(splitPs2).GetStatusResponse(splitParentTerm, proto.ServingStatus_FOLLOWER, 0, 0)
	rpcProvider.GetNode(splitPs3).GetStatusResponse(splitParentTerm, proto.ServingStatus_FOLLOWER, 0, 0)
	borrowed, _, err := metadata.GetShardStatus(constant.DefaultNamespace, splitParentShard)
	require.NoError(t, err)
	nsConfig, err := r.namespaceConfigForSplit(constant.DefaultNamespace)
	require.NoError(t, err)
	parentController := shardcontroller.NewController(constant.DefaultNamespace, splitParentShard,
		nsConfig, borrowed.UnsafeBorrow(), metadata,
		shardcontroller.NoOpFeaturesSupplier, r, rpcProvider, time.Hour)
	t.Cleanup(func() {
		assert.NoError(t, parentController.Close())
	})
	r.Lock()
	r.shardControllers[splitParentShard] = parentController
	r.Unlock()

	// The balancer planned to move the parent from ps3 before the split
	// started. The target is not registered, only so that the split doesn't
	// pick it for the ensemble of a child.
	spare := &proto.DataServerIdentity{Public: "spare:9091", Internal: "spare:8191"}
	swap := action.NewChangeEnsembleAction(splitParentShard, splitPs3, spare)
	_, _, err = r.InitiateSplit(constant.DefaultNamespace, splitParentShard, nil)
	require.NoError(t, err)

	// What the election that changes the ensemble would need
	rpcProvider.GetNode(splitPs1).NewTermResponse(splitParentTerm, 10, nil)
	rpcProvider.GetNode(splitPs2).NewTermResponse(splitParentTerm, 9, nil)
	rpcProvider.GetNode(splitPs3).NewTermResponse(splitParentTerm, 9, nil)
	rpcProvider.GetNode(spare).NewTermResponse(-1, -1, nil)
	rpcProvider.GetNode(splitPs1).BecomeLeaderResponse(nil)

	r.dispatchAction(swap)
	_, err = swap.Wait()
	assert.ErrorIs(t, err, shardcontroller.ErrNotReadyForChangeEnsemble)

	borrowed, _, err = metadata.GetShardStatus(constant.DefaultNamespace, splitParentShard)
	require.NoError(t, err)
	parent := borrowed.UnsafeBorrow()
	assert.NotNil(t, parent.GetSplit())
	assert.Equal(t, splitParentTerm, parent.GetTerm())
	assert.Equal(t, splitPs1.GetPublic(), parent.GetLeader().GetPublic())
	var members []string
	for _, server := range parent.GetEnsemble() {
		members = append(members, server.GetPublic())
	}
	assert.Equal(t, []string{splitPs1.GetPublic(), splitPs2.GetPublic(), splitPs3.GetPublic()}, members)
	assert.Empty(t, parent.GetPendingDeleteShardNodes())
}

func TestRuntimeInitiateSplitUsesGivenSplitPoint(t *testing.T) {
	r, metadata := newInitiateSplitTestRuntime(t)
	sp := uint32(1000)

	leftChild, rightChild, err := r.InitiateSplit(constant.DefaultNamespace, splitParentShard, &sp)
	require.NoError(t, err)

	left, _, err := metadata.GetShardStatus(constant.DefaultNamespace, leftChild)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), left.UnsafeBorrow().GetInt32HashRange().GetMin())
	assert.Equal(t, sp, left.UnsafeBorrow().GetInt32HashRange().GetMax())
	right, _, err := metadata.GetShardStatus(constant.DefaultNamespace, rightChild)
	require.NoError(t, err)
	assert.Equal(t, sp+1, right.UnsafeBorrow().GetInt32HashRange().GetMin())
	assert.Equal(t, splitParentMaxKey, right.UnsafeBorrow().GetInt32HashRange().GetMax())
}

// InitiateSplit changes nothing when the request is invalid, or when it can't
// read or write what the split needs.
func TestRuntimeInitiateSplitFailsWithoutChanges(t *testing.T) {
	metadataErr := errors.New("metadata unavailable")
	outside := splitParentMaxKey
	for _, tc := range []struct {
		name       string
		namespace  string
		splitPoint *uint32
		setup      func(r *runtime, m *fakeMetadata)
		err        string
	}{
		{name: "unknown namespace", namespace: "unknown", err: `namespace "unknown" not found`},
		{name: "split point outside the shard", splitPoint: &outside, err: "is outside shard's hash range"},
		{name: "status unavailable", setup: func(_ *runtime, m *fakeMetadata) {
			m.listNamespaceStatus = func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
				return nil, metadataErr
			}
		}, err: metadataErr.Error()},
		{name: "shard ids unavailable", setup: func(_ *runtime, m *fakeMetadata) {
			m.allocateShardIDs = func(uint32) (int64, error) { return 0, metadataErr }
		}, err: "failed to allocate the child shard ids"},
		{name: "namespace config unavailable", setup: func(_ *runtime, m *fakeMetadata) {
			m.getNamespace = func(string) (commonobject.Borrowed[*proto.Namespace], bool, error) {
				return commonobject.Borrowed[*proto.Namespace]{}, false, metadataErr
			}
		}, err: metadataErr.Error()},
		{name: "no ensemble for the left child", setup: func(r *runtime, _ *fakeMetadata) {
			r.ensembleSelector = failingEnsembleSelector{succeeding: 0, calls: new(0)}
		}, err: "failed to select ensemble for left child"},
		{name: "no ensemble for the right child", setup: func(r *runtime, _ *fakeMetadata) {
			r.ensembleSelector = failingEnsembleSelector{succeeding: 1, calls: new(0)}
		}, err: "failed to select ensemble for right child"},
		{name: "status write failed", setup: func(_ *runtime, m *fakeMetadata) {
			m.patchNamespaceStatus = func(string, func(*proto.NamespaceStatus) bool) error { return metadataErr }
		}, err: "failed to persist the split"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, metadata := newInitiateSplitTestRuntime(t)
			if tc.setup != nil {
				tc.setup(r, metadata)
			}
			namespace := cmp.Or(tc.namespace, constant.DefaultNamespace)

			_, _, err := r.InitiateSplit(namespace, splitParentShard, tc.splitPoint)
			assert.ErrorContains(t, err, tc.err)

			parent, _, err := metadata.GetShardStatus(constant.DefaultNamespace, splitParentShard)
			require.NoError(t, err)
			assert.Nil(t, parent.UnsafeBorrow().GetSplit())
			r.RLock()
			defer r.RUnlock()
			assert.Empty(t, r.shardControllers)
		})
	}
}

func TestValidateSplitParentRejectsUnsplittableShards(t *testing.T) {
	steady := func(modify func(*proto.ShardMetadata)) *proto.ShardMetadata {
		shard := &proto.ShardMetadata{
			Status:         proto.ShardStatusSteadyState,
			Int32HashRange: &proto.HashRange{Min: 0, Max: 100},
		}
		modify(shard)
		return shard
	}
	for _, tc := range []struct {
		name   string
		parent *proto.ShardMetadata
		err    string
	}{
		{"missing", nil, "not found"},
		{"pending ensemble change", steady(func(s *proto.ShardMetadata) {
			s.PendingDeleteShardNodes = []*proto.DataServerIdentity{splitPs1}
		}), "has pending ensemble changes"},
		{"single hash", steady(func(s *proto.ShardMetadata) {
			s.Int32HashRange = &proto.HashRange{Min: 7, Max: 7}
		}), "hash range is too small to split"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.ErrorContains(t, validateSplitParent("ns", 1, tc.parent), tc.err)
		})
	}
	assert.NoError(t, validateSplitParent("ns", 1, steady(func(*proto.ShardMetadata) {})))
}

// The split of a shard of a namespace removed from the configuration uses the
// default namespace configuration.
func TestRuntimeNamespaceConfigForSplitDefaultsRemovedNamespace(t *testing.T) {
	r, metadata := newInitiateSplitTestRuntime(t)

	nsConfig, err := r.namespaceConfigForSplit("removed")
	require.NoError(t, err)
	assert.True(t, pb.Equal(&proto.Namespace{}, nsConfig))

	readErr := errors.New("config unavailable")
	metadata.getNamespace = func(string) (commonobject.Borrowed[*proto.Namespace], bool, error) {
		return commonobject.Borrowed[*proto.Namespace]{}, false, readErr
	}
	_, err = r.namespaceConfigForSplit(constant.DefaultNamespace)
	assert.ErrorIs(t, err, readErr)
}

func newTestMetadata(t *testing.T, config *proto.ClusterConfiguration) coordmetadata.Metadata {
	t.Helper()

	dir := t.TempDir()
	data, err := metadatacodec.ClusterConfigCodec.MarshalYAML(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, coordoption.DefaultFileConfigName), data, 0o600))

	metadataFactory, err := coordmetadata.New(t.Context(), &coordoption.Options{
		Metadata: coordoption.MetadataOptions{
			ProviderOptions: coordoption.ProviderOptions{
				ProviderName: metadatacommon.NameFile,
				File: coordoption.FileMetadata{
					Dir: dir,
				},
			},
		},
	})
	require.NoError(t, err)

	metadata := metadataFactory.CreateMetadata(t.Context())
	_, err = metadata.WaitToBecomeLeader()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, metadataFactory.Close())
	})
	return metadata
}

func newNamespaceTestRuntime(t *testing.T, metadata coordmetadata.Metadata) *runtime {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	c := &runtime{
		ctx:                   ctx,
		ctxCancel:             cancel,
		logger:                slog.Default(),
		metadata:              metadata,
		ensembleSelector:      namespaceEnsembleSelector{},
		shardControllers:      make(map[int64]shardcontroller.Controller),
		dataServerControllers: make(map[string]dataservercontroller.Controller),
		drainingNodes:         make(map[string]dataservercontroller.Controller),
		assignmentsWatch:      commonwatch.New(&proto.ShardAssignments{}),
		rpc:                   rpctest.NewRpcProvider(),
	}
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// fakeMetadata calls the hook of each method that has one, and the embedded
// Metadata otherwise.
type fakeMetadata struct {
	coordmetadata.Metadata
	getInstanceID         func() (string, error)
	allocateShardIDs      func(count uint32) (int64, error)
	createNamespaceStatus func(namespace string, status *proto.NamespaceStatus) error
	listNamespaceStatus   func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error)
	getNamespaceStatus    func(namespace string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error)
	patchNamespaceStatus  func(namespace string, modify func(*proto.NamespaceStatus) bool) error
	updateShardStatus     func(namespace string, shard int64, shardMetadata *proto.ShardMetadata) error
	deleteShardStatus     func(namespace string, shard int64) error
	getConfig             func() (commonobject.Borrowed[*proto.ClusterConfiguration], error)
	getLoadBalancerConfig func() (commonobject.Borrowed[*proto.LoadBalancer], error)
	listNamespace         func() (map[string]commonobject.Borrowed[*proto.Namespace], error)
	getNamespace          func(name string) (commonobject.Borrowed[*proto.Namespace], bool, error)
	listDataServer        func() (map[string]commonobject.Borrowed[*proto.DataServer], error)
}

func (m *fakeMetadata) GetConfig() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
	if m.getConfig != nil {
		return m.getConfig()
	}
	return m.Metadata.GetConfig()
}

func (m *fakeMetadata) GetLoadBalancerConfig() (commonobject.Borrowed[*proto.LoadBalancer], error) {
	if m.getLoadBalancerConfig != nil {
		return m.getLoadBalancerConfig()
	}
	return m.Metadata.GetLoadBalancerConfig()
}

func (m *fakeMetadata) ListNamespace() (map[string]commonobject.Borrowed[*proto.Namespace], error) {
	if m.listNamespace != nil {
		return m.listNamespace()
	}
	return m.Metadata.ListNamespace()
}

func (m *fakeMetadata) GetNamespace(name string) (commonobject.Borrowed[*proto.Namespace], bool, error) {
	if m.getNamespace != nil {
		return m.getNamespace(name)
	}
	return m.Metadata.GetNamespace(name)
}

func (m *fakeMetadata) ListDataServer() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
	if m.listDataServer != nil {
		return m.listDataServer()
	}
	return m.Metadata.ListDataServer()
}

func (m *fakeMetadata) GetInstanceID() (string, error) {
	if m.getInstanceID != nil {
		return m.getInstanceID()
	}
	return m.Metadata.GetInstanceID()
}

func (m *fakeMetadata) AllocateShardIDs(count uint32) (int64, error) {
	if m.allocateShardIDs != nil {
		return m.allocateShardIDs(count)
	}
	return m.Metadata.AllocateShardIDs(count)
}

func (m *fakeMetadata) CreateNamespaceStatus(namespace string, status *proto.NamespaceStatus) error {
	if m.createNamespaceStatus != nil {
		return m.createNamespaceStatus(namespace, status)
	}
	return m.Metadata.CreateNamespaceStatus(namespace, status)
}

func (m *fakeMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	if m.listNamespaceStatus != nil {
		return m.listNamespaceStatus()
	}
	return m.Metadata.ListNamespaceStatus()
}

func (m *fakeMetadata) GetNamespaceStatus(namespace string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
	if m.getNamespaceStatus != nil {
		return m.getNamespaceStatus(namespace)
	}
	return m.Metadata.GetNamespaceStatus(namespace)
}

func (m *fakeMetadata) PatchNamespaceStatus(namespace string, modify func(*proto.NamespaceStatus) bool) error {
	if m.patchNamespaceStatus != nil {
		return m.patchNamespaceStatus(namespace, modify)
	}
	return m.Metadata.PatchNamespaceStatus(namespace, modify)
}

func (m *fakeMetadata) UpdateShardStatus(namespace string, shard int64, shardMetadata *proto.ShardMetadata) error {
	if m.updateShardStatus != nil {
		return m.updateShardStatus(namespace, shard, shardMetadata)
	}
	return m.Metadata.UpdateShardStatus(namespace, shard, shardMetadata)
}

func (m *fakeMetadata) DeleteShardStatus(namespace string, shard int64) error {
	if m.deleteShardStatus != nil {
		return m.deleteShardStatus(namespace, shard)
	}
	return m.Metadata.DeleteShardStatus(namespace, shard)
}

// beforeStatusWrites calls before right before every shard status update,
// once the writer has read the status it builds on, to race a concurrent
// update with it.
func (m *fakeMetadata) beforeStatusWrites(before func()) {
	m.patchNamespaceStatus = func(namespace string, modify func(*proto.NamespaceStatus) bool) error {
		before()
		return m.Metadata.PatchNamespaceStatus(namespace, modify)
	}
	m.updateShardStatus = func(namespace string, shard int64, shardMetadata *proto.ShardMetadata) error {
		before()
		return m.Metadata.UpdateShardStatus(namespace, shard, shardMetadata)
	}
}

// afterStatusWrites calls after right after every shard status write.
func (m *fakeMetadata) afterStatusWrites(after func()) {
	m.patchNamespaceStatus = func(namespace string, modify func(*proto.NamespaceStatus) bool) error {
		err := m.Metadata.PatchNamespaceStatus(namespace, modify)
		after()
		return err
	}
	m.updateShardStatus = func(namespace string, shard int64, shardMetadata *proto.ShardMetadata) error {
		err := m.Metadata.UpdateShardStatus(namespace, shard, shardMetadata)
		after()
		return err
	}
	m.deleteShardStatus = func(namespace string, shard int64) error {
		err := m.Metadata.DeleteShardStatus(namespace, shard)
		after()
		return err
	}
}

type namespaceEnsembleSelector struct{}

func (namespaceEnsembleSelector) Select(ctx *ensemble.Context) ([]string, error) {
	return ctx.Candidates.Values(), nil
}

// failingEnsembleSelector selects the candidates for the first shards, then
// fails.
type failingEnsembleSelector struct {
	succeeding int
	calls      *int
}

func (s failingEnsembleSelector) Select(ctx *ensemble.Context) ([]string, error) {
	*s.calls++
	if *s.calls > s.succeeding {
		return nil, errors.New("not enough data servers")
	}
	return ctx.Candidates.Values(), nil
}

// fixedEnsembleSelector selects the same data servers for every shard.
type fixedEnsembleSelector []string

func (s fixedEnsembleSelector) Select(*ensemble.Context) ([]string, error) {
	return s, nil
}

// fakeShardController records the calls the runtime makes. The methods that
// it doesn't implement panic.
type fakeShardController struct {
	shardcontroller.Controller
	closeErr    error
	closes      atomic.Int64
	deletes     atomic.Int64
	unavailable []*proto.DataServerIdentity
}

func (c *fakeShardController) Close() error {
	c.closes.Add(1)
	return c.closeErr
}

func (c *fakeShardController) Delete() {
	c.deletes.Add(1)
}

func (c *fakeShardController) BecameUnavailable(dataServer *proto.DataServerIdentity) {
	c.unavailable = append(c.unavailable, dataServer)
}

// fakeDataServerController reports a fixed status and the features that its
// data server supports. The methods that it doesn't implement panic.
type fakeDataServerController struct {
	dataservercontroller.Controller
	status   dataservercontroller.Status
	features []proto.Feature
	closeErr error
	closes   atomic.Int64
}

func (c *fakeDataServerController) GetStatus() dataservercontroller.Status {
	return c.status
}

func (c *fakeDataServerController) GetFeatures() []proto.Feature {
	return c.features
}

func (c *fakeDataServerController) Close() error {
	c.closes.Add(1)
	return c.closeErr
}

const (
	splitParentShard  = int64(0)
	splitOtherShard   = int64(1)
	splitLeftChild    = int64(2)
	splitRightChild   = int64(3)
	splitParentTerm   = int64(5)
	splitParentMaxKey = uint32(math.MaxUint32 / 2)
	splitPoint        = splitParentMaxKey / 2
)

var (
	splitPs1 = &proto.DataServerIdentity{Public: "ps1:9091", Internal: "ps1:8191"}
	splitPs2 = &proto.DataServerIdentity{Public: "ps2:9091", Internal: "ps2:8191"}
	splitPs3 = &proto.DataServerIdentity{Public: "ps3:9091", Internal: "ps3:8191"}
	splitLs1 = &proto.DataServerIdentity{Public: "ls1:9091", Internal: "ls1:8191"}
	splitLs2 = &proto.DataServerIdentity{Public: "ls2:9091", Internal: "ls2:8191"}
	splitLs3 = &proto.DataServerIdentity{Public: "ls3:9091", Internal: "ls3:8191"}
	splitRs1 = &proto.DataServerIdentity{Public: "rs1:9091", Internal: "rs1:8191"}
	splitRs2 = &proto.DataServerIdentity{Public: "rs2:9091", Internal: "rs2:8191"}
	splitRs3 = &proto.DataServerIdentity{Public: "rs3:9091", Internal: "rs3:8191"}
)

// electLeader writes the outcome of a new leader election of the shard, as its
// shard controller does, and returns it. The leadership moves to the next
// member of the ensemble. It can be called from any goroutine.
func electLeader(t *testing.T, metadata coordmetadata.Metadata, shard int64) *proto.ShardMetadata {
	t.Helper()
	borrowed, _, err := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.NoError(t, err)
	elected := pb.CloneOf(borrowed.UnsafeBorrow())
	leader := slices.IndexFunc(elected.Ensemble, func(server *proto.DataServerIdentity) bool {
		return server.GetPublic() == elected.GetLeader().GetPublic()
	})
	elected.Status = proto.ShardStatusSteadyState
	elected.Term++
	elected.Leader = elected.Ensemble[(leader+1)%len(elected.Ensemble)]
	assert.NoError(t, metadata.UpdateShardStatus(constant.DefaultNamespace, shard, elected))
	return elected
}

// assertElectionKept checks that the shard still has the status, the term and
// the leader of its last election. It can be called from any goroutine.
func assertElectionKept(t *testing.T, metadata coordmetadata.Metadata, shard int64, elected *proto.ShardMetadata) {
	t.Helper()
	borrowed, _, err := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.NoError(t, err)
	current := borrowed.UnsafeBorrow()
	assert.Equal(t, elected.GetStatus(), current.GetStatus(), "shard %d status", shard)
	assert.Equal(t, elected.GetTerm(), current.GetTerm(), "shard %d term", shard)
	assert.Equal(t, elected.GetLeader().GetPublic(), current.GetLeader().GetPublic(), "shard %d leader", shard)
}

// splitTestRuntime is a runtime whose shard assignments are recomputed after
// every cluster status write, recording each published snapshot.
type splitTestRuntime struct {
	*runtime
	rpc *rpctest.RpcProvider

	snapshotsLock sync.Mutex
	snapshots     []*proto.ShardAssignments
}

// newSplitTestRuntime creates a namespace with a parent shard being split into
// two children, next to a shard that is not part of the split.
func newSplitTestRuntime(t *testing.T) *splitTestRuntime {
	t.Helper()

	servers := []*proto.DataServerIdentity{
		splitPs1, splitPs2, splitPs3, splitLs1, splitLs2, splitLs3, splitRs1, splitRs2, splitRs3,
	}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              constant.DefaultNamespace,
			InitialShardCount: 2,
			ReplicationFactor: 3,
		}},
		Servers: servers,
	})
	_, err := metadata.AllocateShardIDs(4)
	require.NoError(t, err)
	parentEnsemble := []*proto.DataServerIdentity{splitPs1, splitPs2, splitPs3}
	childSplit := &proto.SplitMetadata{
		Phase:         proto.SplitPhaseBootstrap,
		ParentShardId: splitParentShard,
		SplitPoint:    splitPoint,
	}
	require.NoError(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards: map[int64]*proto.ShardMetadata{
			splitParentShard: {
				Status:         proto.ShardStatusSteadyState,
				Term:           splitParentTerm,
				Leader:         splitPs1,
				Ensemble:       parentEnsemble,
				Int32HashRange: &proto.HashRange{Min: 0, Max: splitParentMaxKey},
				Split: &proto.SplitMetadata{
					Phase:         proto.SplitPhaseBootstrap,
					ChildShardIds: []int64{splitLeftChild, splitRightChild},
					SplitPoint:    splitPoint,
				},
			},
			splitOtherShard: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         splitPs2,
				Ensemble:       parentEnsemble,
				Int32HashRange: &proto.HashRange{Min: splitParentMaxKey + 1, Max: math.MaxUint32},
			},
			splitLeftChild: {
				Status:         proto.ShardStatusSteadyState,
				Ensemble:       []*proto.DataServerIdentity{splitLs1, splitLs2, splitLs3},
				Int32HashRange: &proto.HashRange{Min: 0, Max: splitPoint},
				Split:          childSplit,
			},
			splitRightChild: {
				Status:         proto.ShardStatusSteadyState,
				Ensemble:       []*proto.DataServerIdentity{splitRs1, splitRs2, splitRs3},
				Int32HashRange: &proto.HashRange{Min: splitPoint + 1, Max: splitParentMaxKey},
				Split:          childSplit,
			},
		},
	}))

	r := &splitTestRuntime{rpc: rpctest.NewRpcProvider()}
	// Recompute the shard assignments after every cluster status write, the
	// way an unrelated shard's leader election or a configuration reconcile
	// can at any moment
	recomputing := &fakeMetadata{Metadata: metadata}
	recomputing.afterStatusWrites(func() {
		// An unrelated shard's election completing
		r.LeaderElected(splitOtherShard, splitPs2, nil)
		r.snapshotsLock.Lock()
		r.snapshots = append(r.snapshots, r.assignmentsWatch.Load())
		r.snapshotsLock.Unlock()
	})
	r.runtime = &runtime{
		logger:           slog.With(slog.String("component", "coordinator")),
		ctx:              t.Context(),
		metadata:         recomputing,
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
		shardControllers: map[int64]shardcontroller.Controller{},
		rpc:              r.rpc,
	}
	return r
}

// runSplit drives the split the way the shard controller of the parent does,
// and waits for it to report the split as completed or aborted.
func (r *splitTestRuntime) runSplit(t *testing.T, splitTimeout time.Duration) {
	t.Helper()

	split := shardcontroller.NewSplit(shardcontroller.SplitConfig{
		Namespace:   constant.DefaultNamespace,
		ShardId:     splitParentShard,
		Metadata:    r.metadata,
		RpcProvider: r.rpc,
		SplitMetadata: &proto.SplitMetadata{
			ChildShardIds: []int64{splitLeftChild, splitRightChild},
			SplitPoint:    splitPoint,
		},
		SplitTimeout: splitTimeout,
	})
	split.Start()
	select {
	case <-split.Done():
		switch split.Stop() {
		case shardcontroller.SplitCompleted:
			r.SplitComplete(splitParentShard, splitLeftChild, splitRightChild)
		case shardcontroller.SplitAborted:
			r.SplitAborted(splitParentShard, splitLeftChild, splitRightChild)
		default:
		}
	case <-time.After(30 * time.Second):
		t.Fatal("split did not finish")
	}
}

// queueSplitResponses queues the data server responses for a whole split, from
// Bootstrap to the end of Cutover.
func (r *splitTestRuntime) queueSplitResponses() {
	// Bootstrap: the *1 nodes have the highest offset and become the child leaders
	for _, child := range [][]*proto.DataServerIdentity{
		{splitLs1, splitLs2, splitLs3},
		{splitRs1, splitRs2, splitRs3},
	} {
		r.rpc.GetNode(child[0]).NewTermResponse(0, 0, nil)
		r.rpc.GetNode(child[1]).NewTermResponse(0, -1, nil)
		r.rpc.GetNode(child[2]).NewTermResponse(0, -1, nil)
		// Each member is reset to an empty copy of the child, and observes the
		// parent
		for _, member := range child {
			r.rpc.GetNode(member).DeleteShardResponse(nil)
			r.rpc.GetNode(member).NewTermResponse(-1, -1, nil)
			r.rpc.GetNode(splitPs1).AddFollowerResponse(nil)
		}
	}

	// CatchUp
	r.rpc.GetNode(splitPs1).GetStatusResponse(splitParentTerm, proto.ServingStatus_LEADER, 105, 105)
	r.queueChildrenAppliedResponses()

	// Cutover: freeze the parent, wait for the children to receive its tail,
	// fence the parent and re-elect the children in a clean term
	r.rpc.GetNode(splitPs1).FreezeShardResponse(105, nil)
	r.queueChildrenAppliedResponses()
	for _, node := range []*proto.DataServerIdentity{
		splitPs1, splitPs2, splitPs3, splitLs1, splitLs2, splitLs3, splitRs1, splitRs2, splitRs3,
	} {
		r.rpc.GetNode(node).NewTermResponse(splitParentTerm, 105, nil)
	}
	r.rpc.GetNode(splitLs1).BecomeLeaderResponse(nil)
	r.rpc.GetNode(splitRs1).BecomeLeaderResponse(nil)
}

// queueChildrenAppliedResponses queues what CatchUp or Cutover need to see that
// a majority of each child applied the parent's entries: its leader, the *1
// node, and one of its followers.
func (r *splitTestRuntime) queueChildrenAppliedResponses() {
	for _, member := range []*proto.DataServerIdentity{splitLs1, splitLs2, splitRs1, splitRs2} {
		r.rpc.GetNode(member).GetStatusResponse(splitParentTerm, proto.ServingStatus_FOLLOWER, 105, 105)
	}
}

func (r *splitTestRuntime) publishedSnapshots() []*proto.ShardAssignments {
	r.snapshotsLock.Lock()
	defer r.snapshotsLock.Unlock()
	// The split controller finished with a recompute of its own
	return append(slices.Clone(r.snapshots), r.assignmentsWatch.Load())
}

// checkAssignments verifies that the shards of a namespace partition the whole
// hash space, and that a split parent is never published together with any of
// its children. It returns the IDs of the published shards.
func checkAssignments(assignments *proto.ShardAssignments) ([]int64, error) {
	nsAssignments := assignments.GetNamespaces()[constant.DefaultNamespace]
	shards := slices.Clone(nsAssignments.GetAssignments())
	slices.SortFunc(shards, func(a, b *proto.ShardAssignment) int {
		return cmp.Compare(a.GetInt32HashRange().GetMinHashInclusive(), b.GetInt32HashRange().GetMinHashInclusive())
	})
	ids := make([]int64, 0, len(shards))
	for _, shard := range shards {
		ids = append(ids, shard.GetShard())
	}

	if slices.Contains(ids, splitParentShard) &&
		(slices.Contains(ids, splitLeftChild) || slices.Contains(ids, splitRightChild)) {
		return ids, errors.New("the split parent is published together with a child")
	}
	next := uint64(0)
	for _, shard := range shards {
		hashRange := shard.GetInt32HashRange()
		if uint64(hashRange.GetMinHashInclusive()) != next {
			return ids, fmt.Errorf("shard %d starts at %d instead of %d",
				shard.GetShard(), hashRange.GetMinHashInclusive(), next)
		}
		next = uint64(hashRange.GetMaxHashInclusive()) + 1
	}
	if next != math.MaxUint32+1 {
		return ids, fmt.Errorf("hashes from %d have no shard", next)
	}
	return ids, nil
}

// newInitiateSplitTestRuntime creates a runtime with a namespace of two shards
// that are not being split.
func newInitiateSplitTestRuntime(t *testing.T) (*runtime, *fakeMetadata) {
	t.Helper()

	metadata := newTestMetadata(t, &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              constant.DefaultNamespace,
			InitialShardCount: 2,
			ReplicationFactor: 3,
		}},
		Servers: []*proto.DataServerIdentity{
			splitPs1, splitPs2, splitPs3, splitLs1, splitLs2, splitLs3, splitRs1, splitRs2, splitRs3,
		},
	})
	_, err := metadata.AllocateShardIDs(2)
	require.NoError(t, err)
	parentEnsemble := []*proto.DataServerIdentity{splitPs1, splitPs2, splitPs3}
	require.NoError(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		ReplicationFactor: 3,
		Shards: map[int64]*proto.ShardMetadata{
			splitParentShard: {
				Status:         proto.ShardStatusSteadyState,
				Term:           splitParentTerm,
				Leader:         splitPs1,
				Ensemble:       parentEnsemble,
				Int32HashRange: &proto.HashRange{Min: 0, Max: splitParentMaxKey},
			},
			splitOtherShard: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         splitPs2,
				Ensemble:       parentEnsemble,
				Int32HashRange: &proto.HashRange{Min: splitParentMaxKey + 1, Max: math.MaxUint32},
			},
		},
	}))

	racing := &fakeMetadata{Metadata: metadata}
	loadBalancerConfig, err := racing.GetLoadBalancerConfig()
	require.NoError(t, err)
	r := &runtime{
		logger:           slog.With(slog.String("component", "coordinator")),
		ctx:              t.Context(),
		metadata:         racing,
		rpc:              rpctest.NewRpcProvider(),
		ensembleSelector: ensemble.NewSelector(),
		loadBalancer: balancer.NewLoadBalancer(balancer.Options{
			Context:             t.Context(),
			Metadata:            racing,
			LoadBalancerConfig:  loadBalancerConfig.UnsafeBorrow(),
			NodeAvailableJudger: func(string) bool { return true },
		}),
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
		shardControllers: map[int64]shardcontroller.Controller{},
	}
	t.Cleanup(func() {
		r.Lock()
		controllers := maps.Clone(r.shardControllers)
		r.Unlock()
		for _, controller := range controllers {
			assert.NoError(t, controller.Close())
		}
	})
	return r, racing
}
