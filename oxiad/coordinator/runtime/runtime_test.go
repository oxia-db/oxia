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
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	dataservercontroller "github.com/oxia-db/oxia/oxiad/coordinator/controller/dataserver"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/controller/shard"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
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

	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpcProvider })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	require.Eventually(t, func() bool {
		for _, server := range servers {
			status, ok := r.GetLiveDataServerStatus(server.GetNameOrDefault())
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
	_, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
	require.ErrorContains(t, err, "status unavailable")
}

// The runtime does not start without the status, the statuses of the data
// servers and the configuration.
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
		{"data server status", func(m *fakeMetadata) {
			m.listDataServerStatus = func() (map[string]commonobject.Borrowed[*proto.DataServerStatus], error) {
				return nil, readErr
			}
		}},
		{"config", func(m *fakeMetadata) {
			m.getConfig = func() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
				return commonobject.Borrowed[*proto.ClusterConfiguration]{}, readErr
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &fakeMetadata{Metadata: newTestMetadata(t, &proto.ClusterConfiguration{})}
			tc.setup(metadata)
			_, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
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

	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	c := r.(*runtime)
	c.RLock()
	defer c.RUnlock()
	assert.Contains(t, c.shardControllers, int64(0))
	assert.Contains(t, c.dataServerControllers, server.GetNameOrDefault())
}

// A data server stored DRAINING resumes its drain at startup: it is never seen
// RUNNING, e.g. by the leader balancer, also while the configuration has it.
func TestRuntimeNewResumesDrainOfStoredDrainingDataServer(t *testing.T) {
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{Servers: []*proto.DataServerIdentity{server}})
	require.NoError(t, metadata.CreateNamespaceStatus("ns", &proto.NamespaceStatus{
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
	require.NoError(t, metadata.PatchDataServerStatus("s1", func(status *proto.DataServerStatus) (bool, error) {
		status.State = proto.DataServerState_DATA_SERVER_STATE_DRAINING
		return true, nil
	}))

	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	status, found := r.GetLiveDataServerStatus("s1")
	require.True(t, found)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINING, status.GetState())
	assert.NotContains(t, r.ListLiveDataServer(), "s1")
}

// A DRAINED data server still referenced by a pending delete of its copy gets
// no controller: DRAINED is final.
func TestRuntimeNewSkipsStoredDrainedDataServer(t *testing.T) {
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	drained := &proto.DataServerIdentity{Name: new("s2"), Public: "s2:9091", Internal: "s2:8191"}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{Servers: []*proto.DataServerIdentity{server}})
	require.NoError(t, metadata.CreateNamespaceStatus("ns", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:                  proto.ShardStatusSteadyState,
				Term:                    1,
				Leader:                  server,
				Ensemble:                []*proto.DataServerIdentity{server},
				PendingDeleteShardNodes: []*proto.DataServerIdentity{drained},
				Int32HashRange:          &proto.HashRange{Min: 0, Max: math.MaxUint32},
			},
		},
	}))
	require.NoError(t, metadata.PatchDataServerStatus("s2", func(status *proto.DataServerStatus) (bool, error) {
		status.State = proto.DataServerState_DATA_SERVER_STATE_DRAINED
		return true, nil
	}))

	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	_, found := r.GetLiveDataServerStatus("s2")
	assert.False(t, found)
	_, found = r.GetLiveDataServerStatus("s1")
	assert.True(t, found)
}

// A data server stored RUNNING gets a controller: a configured one with its
// configured identity, and one that the configuration doesn't have, e.g.
// removed before its drain was stored, for the reconciler to drain it.
func TestRuntimeNewStartsControllersOfStoredDataServers(t *testing.T) {
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{Servers: []*proto.DataServerIdentity{server}})
	for _, name := range []string{"s1", "gone"} {
		require.NoError(t, metadata.PatchDataServerStatus(name, func(status *proto.DataServerStatus) (bool, error) {
			status.State = proto.DataServerState_DATA_SERVER_STATE_RUNNING
			return true, nil
		}))
	}

	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	dataServers := r.ListLiveDataServer()
	require.Contains(t, dataServers, "s1")
	assert.Equal(t, "s1:8191", dataServers["s1"].UnsafeBorrow().GetIdentity().GetInternal())
	assert.Contains(t, dataServers, "gone")
}

// A draining data server keeps the addresses of the shards that it drains
// from, and its stored metadata, also when the configuration has its name
// again, at other addresses and with other labels.
func TestRuntimeNewKeepsAddressesAndMetadataOfDrainingDataServerAddedAgain(t *testing.T) {
	draining := &proto.DataServerIdentity{Name: new("s1"), Public: "old:9091", Internal: "old:8191"}
	readded := &proto.DataServerIdentity{Name: new("s1"), Public: "new:9091", Internal: "new:8191"}
	stored := &proto.DataServerMetadata{Labels: map[string]string{"zone": "old"}}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{Servers: []*proto.DataServerIdentity{readded},
		ServerMetadata: map[string]*proto.DataServerMetadata{"s1": {Labels: map[string]string{"zone": "new"}}}})
	require.NoError(t, metadata.CreateNamespaceStatus("ns", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{
			0: {
				Status:         proto.ShardStatusSteadyState,
				Term:           1,
				Leader:         draining,
				Ensemble:       []*proto.DataServerIdentity{draining},
				Int32HashRange: &proto.HashRange{Min: 0, Max: math.MaxUint32},
			},
		},
	}))
	require.NoError(t, metadata.PatchDataServerStatus("s1", func(status *proto.DataServerStatus) (bool, error) {
		status.State = proto.DataServerState_DATA_SERVER_STATE_DRAINING
		status.Metadata = stored
		return true, nil
	}))

	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpctest.NewRpcProvider() })
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, r.Close()) })

	c := r.(*runtime)
	c.RLock()
	defer c.RUnlock()
	require.Contains(t, c.dataServerControllers, "s1")
	dataServer := c.dataServerControllers["s1"].GetDataServer().UnsafeBorrow()
	assert.Equal(t, "old:8191", dataServer.GetIdentity().GetInternal())
	assert.Equal(t, stored.GetLabels(), dataServer.GetMetadata().GetLabels())
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
func TestRuntimeGetDataServerStatusReportsDrainingDataServer(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	c.dataServerControllers["s1"] = &fakeDataServerController{
		status:   dataservercontroller.NewStatus(proto.DataServerState_DATA_SERVER_STATE_DRAINING),
		features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM},
	}

	status, found := c.GetLiveDataServerStatus("s1")
	require.True(t, found)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINING, status.GetState())
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, status.GetSupportedFeatures())

	_, found = c.GetLiveDataServerStatus("unknown")
	assert.False(t, found)
}

func TestRuntimeCreateDataServerRejectsMissingIdentity(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))

	assert.False(t, c.CreateDataServer("s1", &proto.DataServer{}))
	assert.Empty(t, c.dataServerControllers)
}

// A data server that has a controller, also a draining one, keeps it.
func TestRuntimeCreateDataServerKeepsExistingController(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	draining := newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_DRAINING)
	c.dataServerControllers["s1"] = draining
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}

	assert.False(t, c.CreateDataServer("s1", &proto.DataServer{Identity: server}))
	assert.Same(t, draining, c.dataServerControllers["s1"])
	assert.Zero(t, draining.closes.Load())
}

func TestRuntimeDeleteDataServerIgnoresUnknownDataServer(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))

	c.DeleteDataServer("unknown")

	assert.Empty(t, c.dataServerControllers)
}

// A deleted data server keeps its controller, which drains it, and is left out
// of the data servers of the configuration.
func TestRuntimeDeleteDataServerDrainsDataServer(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	running := newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_RUNNING)
	c.dataServerControllers["s1"] = running
	require.Contains(t, c.ListLiveDataServer(), "s1")

	c.DeleteDataServer("s1")
	c.DeleteDataServer("s1")

	assert.Same(t, running, c.dataServerControllers["s1"])
	assert.EqualValues(t, 1, running.drains.Load())
	assert.NotContains(t, c.ListLiveDataServer(), "s1")
}

// A drained data server that left the configuration is forgotten, with its
// pending deletes; a configured, a draining or an unknown one isn't, and a
// retry once forgotten is ErrNotFound.
func TestRuntimeDrainedDataServer(t *testing.T) {
	configured := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	drained := &proto.DataServerIdentity{Name: new("drained"), Public: "d:9091", Internal: "d:8191"}
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{Servers: []*proto.DataServerIdentity{configured}})
	require.NoError(t, metadata.CreateNamespaceStatus("ns", &proto.NamespaceStatus{
		ReplicationFactor: 1,
		Shards: map[int64]*proto.ShardMetadata{0: {
			Ensemble:                []*proto.DataServerIdentity{configured},
			PendingDeleteShardNodes: []*proto.DataServerIdentity{drained},
			Int32HashRange:          &proto.HashRange{Min: 0, Max: math.MaxUint32},
		}},
	}))
	for name, state := range map[string]proto.DataServerState{
		"s1":       proto.DataServerState_DATA_SERVER_STATE_RUNNING,
		"draining": proto.DataServerState_DATA_SERVER_STATE_DRAINING,
		"drained":  proto.DataServerState_DATA_SERVER_STATE_DRAINED,
	} {
		require.NoError(t, metadata.PatchDataServerStatus(name, func(status *proto.DataServerStatus) (bool, error) {
			status.State = state
			return true, nil
		}))
	}
	c := newNamespaceTestRuntime(t, metadata)

	require.ErrorIs(t, c.DrainedDataServer("s1"), errs.ErrFailedPrecondition)
	require.ErrorIs(t, c.DrainedDataServer("draining"), errs.ErrFailedPrecondition)
	require.ErrorIs(t, c.DrainedDataServer("unknown"), errs.ErrNotFound)

	require.NoError(t, c.DrainedDataServer("drained"))
	_, found, err := metadata.GetDataServerStatus("drained")
	require.NoError(t, err)
	assert.False(t, found)
	shard, _, err := metadata.GetShardStatus("ns", 0)
	require.NoError(t, err)
	assert.Empty(t, shard.UnsafeBorrow().GetPendingDeleteShardNodes())
	assert.ErrorIs(t, c.DrainedDataServer("drained"), errs.ErrNotFound)
}

func TestRuntimeDrainedDataServerReturnsMetadataErrors(t *testing.T) {
	metadataErr := errors.New("metadata unavailable")
	for name, fake := range map[string]*fakeMetadata{
		"configuration": {getDataServer: func(string) (commonobject.Borrowed[*proto.DataServer], bool, error) {
			return commonobject.Borrowed[*proto.DataServer]{}, false, metadataErr
		}},
		"delete": {deleteDrainedDataServerStatus: func(string) error { return metadataErr }},
	} {
		fake.Metadata = newTestMetadata(t, &proto.ClusterConfiguration{})
		c := newNamespaceTestRuntime(t, fake)

		assert.ErrorIs(t, c.DrainedDataServer("drained"), metadataErr, name)
	}
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
	createErr := fmt.Errorf("competing creation: %w", errs.ErrAlreadyExists)
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
			c.dataServerControllers[server.GetNameOrDefault()] = newFakeDataServerController(
				proto.DataServerState_DATA_SERVER_STATE_RUNNING)
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
				require.ErrorIs(t, err, errs.ErrConflict)
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
		planner:          failingPlanner(1, &calls),
		shardControllers: make(map[int64]shardcontroller.Controller),
		dataServerControllers: map[string]dataservercontroller.Controller{
			server.GetNameOrDefault(): newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_RUNNING),
		},
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
		{"already exists", fmt.Errorf("namespace creation: %w", errs.ErrAlreadyExists)},
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
				planner:          defaultPlanner(),
				shardControllers: make(map[int64]shardcontroller.Controller),
				dataServerControllers: map[string]dataservercontroller.Controller{
					server.GetNameOrDefault(): newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_RUNNING),
				},
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

func TestRuntimePlaceShardReturnsDataServerListError(t *testing.T) {
	listErr := errors.New("data servers unavailable")
	c := newNamespaceTestRuntime(t, &fakeMetadata{
		Metadata: newTestMetadata(t, &proto.ClusterConfiguration{}),
		listDataServer: func() (map[string]commonobject.Borrowed[*proto.DataServer], error) {
			return nil, listErr
		},
	})

	_, err := c.placeShard("default", &proto.Namespace{ReplicationFactor: 1}, nil, nil)
	assert.ErrorIs(t, err, listErr)
}

// Only a live data server takes shards: a configured one without a controller,
// or with a controller that drains, e.g. added again after it left, is skipped.
func TestRuntimePlaceShardSkipsDataServersNotLive(t *testing.T) {
	servers := make([]*proto.DataServerIdentity, 0, 4)
	for _, name := range []string{"running", "draining", "drained", "unreconciled"} {
		servers = append(servers, &proto.DataServerIdentity{Name: &name, Public: name, Internal: name})
	}
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{Servers: servers}))
	c.dataServerControllers["running"] = newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_RUNNING)
	c.dataServerControllers["draining"] = newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_DRAINING)
	c.dataServerControllers["drained"] = newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_DRAINED)
	delete(c.dataServerControllers, "unreconciled")
	c.RLock()
	defer c.RUnlock()

	ensemble, err := c.placeShard("default", &proto.Namespace{ReplicationFactor: 1}, nil, nil)
	require.NoError(t, err)
	require.Len(t, ensemble, 1)
	assert.Equal(t, "running", ensemble[0].GetNameOrDefault())

	_, err = c.placeShard("default", &proto.Namespace{ReplicationFactor: 2}, nil, nil)
	assert.ErrorIs(t, err, errs.ErrNotFound)
}

// Close closes the split controllers: a split that didn't reach its point of
// no return is aborted.
func TestRuntimeCloseAbortsSplits(t *testing.T) {
	r, metadata := newInitiateSplitTestRuntime(t)
	r.ctx, r.ctxCancel = context.WithCancel(t.Context())
	_, _, err := r.InitiateSplit(constant.DefaultNamespace, splitParentShard, nil)
	require.NoError(t, err)

	require.NoError(t, r.Close())

	r.RLock()
	assert.Empty(t, r.splitControllers)
	r.RUnlock()
	parent, _, err := metadata.GetShardStatus(constant.DefaultNamespace, splitParentShard)
	require.NoError(t, err)
	assert.Nil(t, parent.UnsafeBorrow().GetSplit())
}

// A draining data server that becomes unavailable keeps its controller, which
// stores it DRAINED, and the shard controllers are told.
func TestRuntimeBecameUnavailableKeepsDrainingNode(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	draining := newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_DRAINING)
	c.dataServerControllers["s1"] = draining
	sc := &fakeShardController{}
	c.shardControllers[0] = sc

	c.BecameUnavailable(server)

	assert.Contains(t, c.dataServerControllers, "s1")
	assert.Zero(t, draining.closes.Load())
	assert.Equal(t, []*proto.DataServerIdentity{server}, sc.unavailable)
}

// A drained data server's controller is closed, even if closing it fails.
func TestRuntimeBecameDrainedClosesDrainingNode(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	draining := newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_DRAINED)
	draining.closeErr = errors.New("close failed")
	c.dataServerControllers["s1"] = draining

	c.BecameDrained(server)

	assert.NotContains(t, c.dataServerControllers, "s1")
	assert.Eventually(t, func() bool {
		return draining.closes.Load() == 1
	}, 10*time.Second, 10*time.Millisecond)

	// Already closed: nothing left to do
	c.BecameDrained(server)
	assert.EqualValues(t, 1, draining.closes.Load())
}

// Close waits for the close of a drained controller, which runs on a detached
// goroutine, before it closes the RPC provider that the controller uses.
func TestRuntimeCloseWaitsForDrainedDataServerClose(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	release := make(chan struct{})
	drained := newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_DRAINED)
	drained.closeHook = func() { <-release }
	c.dataServerControllers["s1"] = drained
	c.BecameDrained(server)

	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while the drained controller was still closing")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-closed)
	assert.EqualValues(t, 1, drained.closes.Load())
}

// The controller of a data server added again since it drained isn't closed by
// the late notification of the drained one.
func TestRuntimeBecameDrainedKeepsControllerOfDataServerAddedAgain(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	server := &proto.DataServerIdentity{Name: new("s1"), Public: "s1:9091", Internal: "s1:8191"}
	running := newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_RUNNING)
	c.dataServerControllers["s1"] = running

	c.BecameDrained(server)

	assert.Same(t, running, c.dataServerControllers["s1"])
	assert.Zero(t, running.closes.Load())
}

// An action on a shard without a controller, e.g. deleted in the meantime,
// fails.
func TestRuntimeDispatchActionFailsForUnknownShard(t *testing.T) {
	c := newNamespaceTestRuntime(t, newTestMetadata(t, &proto.ClusterConfiguration{}))
	ac := action.NewChangeEnsembleAction(42, &proto.DataServerIdentity{}, &proto.DataServerIdentity{})

	c.dispatchAction(ac)

	_, err := ac.Wait(t.Context())
	assert.ErrorIs(t, err, constant.ErrResourceUnavailable)
}

// A split runs in the runtime: it completes with its children once it starts,
// or with the error that kept it from starting.
func TestRuntimeDispatchActionInitiatesASplit(t *testing.T) {
	r, _ := newInitiateSplitTestRuntime(t)
	split := action.NewSplitAction(constant.DefaultNamespace, splitParentShard)

	r.dispatchAction(split)

	children, err := split.Wait(context.Background())
	require.NoError(t, err)
	assert.Len(t, children, 2)

	failed := action.NewSplitAction("unknown", splitParentShard)
	r.dispatchAction(failed)
	_, err = failed.Wait(context.Background())
	assert.Error(t, err)
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
				Status:   proto.ShardStatusUnknown,
				Leader:   removed,
				Ensemble: []*proto.DataServerIdentity{removed},
				EnsembleChange: &proto.EnsembleChangeMetadata{
					From: &proto.DataServerIdentity{Public: "changed-public:6648", Internal: "changed-internal:6649"},
					To:   removed,
				},
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
			"changed-public:6648",
			"changed-internal:6649",
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
	r, err := New(t.Context(), metadata, func(string) rpc.ControllerRPCProvider { return rpcProvider })
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
	}

	dataServer := &proto.DataServerIdentity{Internal: "s1:6649"}

	features := c.findDataServerFeatures([]*proto.DataServerIdentity{dataServer})

	assert.NotContains(t, features, dataServer.GetNameOrDefault())
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
		scheduler:             fakeScheduler{},
		planner:               defaultPlanner(),
		shardControllers:      make(map[int64]shardcontroller.Controller),
		splitControllers:      make(map[int64]*shardcontroller.SplitController),
		dataServerControllers: make(map[string]dataservercontroller.Controller),
		assignmentsWatch:      commonwatch.New(&proto.ShardAssignments{}),
		rpc:                   rpctest.NewRpcProvider(),
	}
	// Every configured data server is live, so that it takes shards
	if dataServers, err := metadata.ListDataServer(); err == nil {
		for name := range dataServers {
			c.dataServerControllers[name] = newFakeDataServerController(proto.DataServerState_DATA_SERVER_STATE_RUNNING)
		}
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
	listDataServerStatus  func() (map[string]commonobject.Borrowed[*proto.DataServerStatus], error)
	getNamespaceStatus    func(namespace string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error)
	patchNamespaceStatus  func(namespace string, modify func(*proto.NamespaceStatus) (bool, error)) error
	updateShardStatus     func(namespace string, shard int64, shardMetadata *proto.ShardMetadata) error
	deleteShardStatus     func(namespace string, shard int64) error
	getConfig             func() (commonobject.Borrowed[*proto.ClusterConfiguration], error)
	listNamespace         func() (map[string]commonobject.Borrowed[*proto.Namespace], error)
	getNamespace          func(name string) (commonobject.Borrowed[*proto.Namespace], bool, error)
	listDataServer        func() (map[string]commonobject.Borrowed[*proto.DataServer], error)
	getDataServer         func(name string) (commonobject.Borrowed[*proto.DataServer], bool, error)

	deleteDrainedDataServerStatus func(name string) error
}

func (m *fakeMetadata) GetDataServer(name string) (commonobject.Borrowed[*proto.DataServer], bool, error) {
	if m.getDataServer != nil {
		return m.getDataServer(name)
	}
	return m.Metadata.GetDataServer(name)
}

func (m *fakeMetadata) DeleteDrainedDataServerStatus(name string) error {
	if m.deleteDrainedDataServerStatus != nil {
		return m.deleteDrainedDataServerStatus(name)
	}
	return m.Metadata.DeleteDrainedDataServerStatus(name)
}

func (m *fakeMetadata) GetConfig() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
	if m.getConfig != nil {
		return m.getConfig()
	}
	return m.Metadata.GetConfig()
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

func (m *fakeMetadata) ListDataServerStatus() (map[string]commonobject.Borrowed[*proto.DataServerStatus], error) {
	if m.listDataServerStatus != nil {
		return m.listDataServerStatus()
	}
	return m.Metadata.ListDataServerStatus()
}

func (m *fakeMetadata) GetNamespaceStatus(namespace string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
	if m.getNamespaceStatus != nil {
		return m.getNamespaceStatus(namespace)
	}
	return m.Metadata.GetNamespaceStatus(namespace)
}

func (m *fakeMetadata) PatchNamespaceStatus(namespace string, modify func(*proto.NamespaceStatus) (bool, error)) error {
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
	m.patchNamespaceStatus = func(namespace string, modify func(*proto.NamespaceStatus) (bool, error)) error {
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
	m.patchNamespaceStatus = func(namespace string, modify func(*proto.NamespaceStatus) (bool, error)) error {
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

// fakeScheduler sends no action, for the runtime to close.
type fakeScheduler struct{}

func (fakeScheduler) Close() error {
	return nil
}

func (fakeScheduler) GetActions() <-chan action.Action {
	return nil
}

func defaultPlanner() planner.Planner {
	return planner.New(filter.Defaults(), tier.Defaults())
}

// failingPlanner places the first succeeding shards, then fails, and counts the
// calls.
func failingPlanner(succeeding int, calls *int) planner.Planner {
	return fakePlanner{Planner: defaultPlanner(), succeeding: succeeding, calls: calls}
}

type fakePlanner struct {
	planner.Planner
	succeeding int
	calls      *int
}

func (f fakePlanner) Place(s *stats.ClusterStats, namespace string, replicas int, antiAffinities []*proto.AntiAffinity,
	eligible func(*stats.DataServerStats) bool) ([]string, error) {
	*f.calls++
	if *f.calls > f.succeeding {
		return nil, errors.New("not enough data servers")
	}
	return f.Planner.Place(s, namespace, replicas, antiAffinities, eligible)
}

// fakeShardController records the calls the runtime makes. The methods that
// it doesn't implement panic.
type fakeShardController struct {
	shardcontroller.Controller
	closeErr    error
	closes      atomic.Int64
	deletes     atomic.Int64
	unavailable []*proto.DataServerIdentity
	restarted   []*proto.DataServerIdentity
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

func (c *fakeShardController) MaybeRestarted(dataServer *proto.DataServerIdentity) {
	c.restarted = append(c.restarted, dataServer)
}

// fakeDataServerController reports a fixed status and the features that its
// data server supports. The methods that it doesn't implement panic.
type fakeDataServerController struct {
	dataservercontroller.Controller
	status   dataservercontroller.Status
	features []proto.Feature
	closeErr error
	// closeHook, when set, runs in Close, e.g. to block it
	closeHook func()
	closes    atomic.Int64
	drains    atomic.Int64
}

func newFakeDataServerController(state proto.DataServerState) *fakeDataServerController {
	return &fakeDataServerController{status: dataservercontroller.NewStatus(state)}
}

func (*fakeDataServerController) GetDataServer() commonobject.Borrowed[*proto.DataServer] {
	return commonobject.Borrow(&proto.DataServer{})
}

func (c *fakeDataServerController) GetStatus() dataservercontroller.Status {
	return c.status
}

func (c *fakeDataServerController) GetFeatures() []proto.Feature {
	return c.features
}

func (c *fakeDataServerController) Drain() {
	c.drains.Add(1)
	c.status = dataservercontroller.NewStatus(proto.DataServerState_DATA_SERVER_STATE_DRAINING)
}

func (c *fakeDataServerController) Close() error {
	if c.closeHook != nil {
		c.closeHook()
	}
	c.closes.Add(1)
	return c.closeErr
}
