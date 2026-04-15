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
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/rpc"
	coordinatorpkg "github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
)

func TestCoordinator_RaftMetadataRepeatedNodeRestarts(t *testing.T) {
	coordinators := newRaftMetadataCoordinatorCluster(t, model.ClusterConfig{})
	defer coordinators.close(t)

	_, initialCoordinator := coordinators.requireState(t, model.NewClusterStatus())

	expectedStatus := testCoordinatorClusterStatus(1)
	require.NoError(t, initialCoordinator.StatusResource().Update(expectedStatus))
	activeIdx, _ := coordinators.requireState(t, expectedStatus)

	followerA := nextCoordinatorNode(activeIdx, -1)
	followerB := nextCoordinatorNode(activeIdx, followerA)
	restartOrder := []int{followerA, followerB, followerA, followerB, followerA}

	seed := 2
	var activeCoordinator coordinatorpkg.Coordinator
	for _, nodeIdx := range restartOrder {
		coordinators.restartNode(t, nodeIdx)
		_, activeCoordinator = coordinators.requireState(t, expectedStatus)

		expectedStatus = testCoordinatorClusterStatus(seed)
		require.NoError(t, activeCoordinator.StatusResource().Update(expectedStatus))
		coordinators.requireState(t, expectedStatus)
		seed++
	}

	for i := 0; i < 5; i++ {
		coordinators.restartNode(t, activeIdx)
		_, activeCoordinator = coordinators.requireState(t, expectedStatus)

		expectedStatus = testCoordinatorClusterStatus(seed)
		require.NoError(t, activeCoordinator.StatusResource().Update(expectedStatus))
		activeIdx, _ = coordinators.requireState(t, expectedStatus)
		seed++

		follower := nextCoordinatorNode(activeIdx, -1)
		coordinators.restartNode(t, follower)
		_, activeCoordinator = coordinators.requireState(t, expectedStatus)

		expectedStatus = testCoordinatorClusterStatus(seed)
		require.NoError(t, activeCoordinator.StatusResource().Update(expectedStatus))
		activeIdx, _ = coordinators.requireState(t, expectedStatus)
		seed++
	}
}

type raftMetadataCoordinatorCluster struct {
	mu sync.Mutex

	bootstrapNodes []string
	dataDirs       []string
	clusterConfig  model.ClusterConfig
	clientPool     rpc.ClientPool
	rpcProvider    rpc2.Provider

	nodes  []raftMetadataCoordinatorNode
	active map[int]coordinatorpkg.Coordinator
	errors map[int]error
}

type raftMetadataCoordinatorNode struct {
	provider   metadata.Provider
	done       chan struct{}
	generation uint64
}

func newRaftMetadataCoordinatorCluster(t *testing.T, clusterConfig model.ClusterConfig) *raftMetadataCoordinatorCluster {
	t.Helper()

	baseDir := t.TempDir()
	bootstrapNodes := []string{
		newCoordinatorRaftAddress(t),
		newCoordinatorRaftAddress(t),
		newCoordinatorRaftAddress(t),
	}

	clientPool := rpc.NewClientPool(nil, nil)
	cluster := &raftMetadataCoordinatorCluster{
		bootstrapNodes: bootstrapNodes,
		dataDirs: []string{
			filepath.Join(baseDir, "node-0"),
			filepath.Join(baseDir, "node-1"),
			filepath.Join(baseDir, "node-2"),
		},
		clusterConfig: clusterConfig,
		clientPool:    clientPool,
		rpcProvider:   rpc2.NewRpcProvider(clientPool),
		nodes:         make([]raftMetadataCoordinatorNode, len(bootstrapNodes)),
		active:        make(map[int]coordinatorpkg.Coordinator),
		errors:        make(map[int]error),
	}

	for i := range bootstrapNodes {
		cluster.startNode(t, i)
	}

	return cluster
}

func (c *raftMetadataCoordinatorCluster) startNode(t *testing.T, idx int) {
	t.Helper()

	provider, err := metadata.NewMetadataProviderRaft(c.bootstrapNodes[idx], c.bootstrapNodes, c.dataDirs[idx])
	require.NoError(t, err)

	done := make(chan struct{})
	c.mu.Lock()
	node := &c.nodes[idx]
	node.generation++
	generation := node.generation
	node.provider = provider
	node.done = done
	delete(c.errors, idx)
	c.mu.Unlock()

	go func() {
		defer close(done)

		coordinatorInstance, err := coordinatorpkg.NewCoordinator(
			provider,
			func() (model.ClusterConfig, error) { return c.clusterConfig, nil },
			nil,
			c.rpcProvider,
		)

		c.mu.Lock()
		current := c.nodes[idx].generation == generation && c.nodes[idx].provider == provider
		if err != nil {
			if current {
				c.errors[idx] = err
			}
			c.mu.Unlock()
			return
		}
		if !current {
			c.mu.Unlock()
			_ = coordinatorInstance.Close()
			return
		}
		c.active[idx] = coordinatorInstance
		c.mu.Unlock()
	}()
}

func (c *raftMetadataCoordinatorCluster) restartNode(t *testing.T, idx int) {
	t.Helper()

	c.stopNode(t, idx)
	c.startNode(t, idx)
}

func (c *raftMetadataCoordinatorCluster) stopNode(t *testing.T, idx int) {
	t.Helper()

	c.mu.Lock()
	node := &c.nodes[idx]
	node.generation++
	provider := node.provider
	done := node.done
	coordinatorInstance := c.active[idx]
	node.provider = nil
	node.done = nil
	delete(c.active, idx)
	delete(c.errors, idx)
	c.mu.Unlock()

	if coordinatorInstance != nil {
		assert.NoError(t, coordinatorInstance.Close())
	}
	if provider != nil {
		assert.NoError(t, provider.Close())
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for coordinator node %d to stop", idx)
		}
	}
}

func (c *raftMetadataCoordinatorCluster) close(t *testing.T) {
	t.Helper()

	for i := range c.nodes {
		c.stopNode(t, i)
	}
	assert.NoError(t, c.clientPool.Close())
}

func (c *raftMetadataCoordinatorCluster) requireState(
	t *testing.T,
	expected *model.ClusterStatus,
) (int, coordinatorpkg.Coordinator) {
	t.Helper()

	var activeIdx int
	var activeCoordinator coordinatorpkg.Coordinator
	require.Eventually(t, func() bool {
		idx, coordinatorInstance, ok := c.singleActiveCoordinator()
		if !ok {
			return false
		}

		leaderCount, metadataState := c.metadataLeaderState()
		if leaderCount != 1 {
			return false
		}

		if !reflect.DeepEqual(normalizeClusterStatus(metadataState), normalizeClusterStatus(expected)) {
			return false
		}

		if !reflect.DeepEqual(
			normalizeClusterStatus(coordinatorInstance.StatusResource().Load()),
			normalizeClusterStatus(expected),
		) {
			return false
		}

		activeIdx = idx
		activeCoordinator = coordinatorInstance
		return true
	}, 45*time.Second, 100*time.Millisecond)

	return activeIdx, activeCoordinator
}

func (c *raftMetadataCoordinatorCluster) singleActiveCoordinator() (int, coordinatorpkg.Coordinator, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.errors) > 0 {
		return 0, nil, false
	}
	if len(c.active) != 1 {
		return 0, nil, false
	}
	for idx, coordinatorInstance := range c.active {
		return idx, coordinatorInstance, true
	}
	return 0, nil, false
}

func (c *raftMetadataCoordinatorCluster) metadataLeaderState() (int, *model.ClusterStatus) {
	c.mu.Lock()
	providers := make([]metadata.Provider, 0, len(c.nodes))
	for i := range c.nodes {
		if c.nodes[i].provider != nil {
			providers = append(providers, c.nodes[i].provider)
		}
	}
	c.mu.Unlock()

	leaders := 0
	var leaderState *model.ClusterStatus
	for _, provider := range providers {
		state, _, err := provider.Get()
		if err != nil {
			continue
		}
		leaders++
		leaderState = state
	}
	return leaders, leaderState
}

func normalizeClusterStatus(status *model.ClusterStatus) *model.ClusterStatus {
	if status == nil {
		return model.NewClusterStatus()
	}
	return status.Clone()
}

func nextCoordinatorNode(current int, excluded int) int {
	for i := 0; i < 3; i++ {
		if i != current && i != excluded {
			return i
		}
	}
	panic("failed to find coordinator node")
}

func newCoordinatorRaftAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, listener.Close())
	}()
	return listener.Addr().String()
}

func testCoordinatorClusterStatus(seed int) *model.ClusterStatus {
	return &model.ClusterStatus{
		Namespaces:       map[string]model.NamespaceStatus{},
		ShardIdGenerator: int64(seed * 100),
		ServerIdx:        uint32(seed),
	}
}
