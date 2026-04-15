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

package metadata

import (
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

type testRaftClusterProvider struct {
	cluster *testRaftCluster
	leader  Provider
}

func (t testRaftClusterProvider) Close() error {
	return t.cluster.Close()
}

func (t testRaftClusterProvider) Get() (cs *model.ClusterStatus, version Version, err error) {
	return t.leader.Get()
}

func (t testRaftClusterProvider) Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error) {
	return t.leader.Store(cs, expectedVersion)
}

func (t testRaftClusterProvider) WaitToBecomeLeader() error {
	return t.leader.WaitToBecomeLeader()
}

func newTestRaftClusterProvider(t *testing.T) Provider {
	t.Helper()

	cluster := newTestRaftCluster(t)
	return &testRaftClusterProvider{
		cluster: cluster,
		leader:  cluster.waitForLeader(t),
	}
}

func TestMetadataProviderRaftGetReturnsClone(t *testing.T) {
	provider := newTestRaftClusterProvider(t)
	t.Cleanup(func() {
		require.NoError(t, provider.Close())
	})

	leaderName := "leader"
	_, err := provider.Store(&model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"default": {
				Shards: map[int64]model.ShardMetadata{
					1: {
						Leader: &model.Server{
							Name:     &leaderName,
							Public:   "public-1",
							Internal: "internal-1",
						},
					},
				},
			},
		},
	}, NotExists)
	require.NoError(t, err)

	clusterStatus, _, err := provider.Get()
	require.NoError(t, err)
	shard := clusterStatus.Namespaces["default"].Shards[1]
	shard.Leader.Public = "mutated"

	clusterStatus, _, err = provider.Get()
	require.NoError(t, err)
	assert.Equal(t, "public-1", clusterStatus.Namespaces["default"].Shards[1].Leader.Public)
}

func TestMetadataProviderRaftCloseReleasesTransport(t *testing.T) {
	addr := newTestTCPAddress(t)
	dataDir := t.TempDir()

	provider, err := NewMetadataProviderRaft(addr, []string{addr}, dataDir)
	require.NoError(t, err)
	require.NoError(t, provider.WaitToBecomeLeader())
	require.NoError(t, provider.Close())

	provider, err = NewMetadataProviderRaft(addr, []string{addr}, dataDir)
	require.NoError(t, err)
	require.NoError(t, provider.Close())
}

func TestMetadataProviderRaftWatchLeadershipChangesNotifiesLossTransitions(t *testing.T) {
	mpr := &metadataProviderRaft{
		leadershipLostCh:       make(chan struct{}, 4),
		leadershipNotifyCh:     make(chan bool, 4),
		leadershipNotifyStopCh: make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		mpr.watchLeadershipChanges(false)
	}()

	mpr.leadershipNotifyCh <- true
	mpr.leadershipNotifyCh <- false
	requireReceiveLeadershipLoss(t, mpr.leadershipLostCh)

	mpr.leadershipNotifyCh <- false
	requireNoLeadershipLoss(t, mpr.leadershipLostCh)

	mpr.leadershipNotifyCh <- true
	mpr.leadershipNotifyCh <- false
	requireReceiveLeadershipLoss(t, mpr.leadershipLostCh)

	close(mpr.leadershipNotifyStopCh)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for leadership watcher to stop")
	}
}

func TestMetadataProviderRaftLeadershipLostOnLeaderRestart(t *testing.T) {
	cluster := newTestRaftCluster(t)
	t.Cleanup(func() {
		require.NoError(t, cluster.Close())
	})

	expected := testClusterStatus(1)
	leader := cluster.waitForLeader(t)
	version, err := leader.Store(expected, NotExists)
	require.NoError(t, err)
	cluster.requireSingleLeader(t)
	cluster.requireReplicatedState(t, expected, version)

	leaderIdx := cluster.indexOf(leader)
	require.NotEqual(t, -1, leaderIdx)
	lostCh := leader.LeadershipLost()

	cluster.restartNode(t, leaderIdx)

	requireReceiveLeadershipLoss(t, lostCh)
	cluster.requireSingleLeader(t)
	cluster.requireReplicatedState(t, expected, version)
}

func TestMetadataProviderRaftThreeNodeClusterMaintainsStateAcrossRepeatedRestarts(t *testing.T) {
	cluster := newTestRaftCluster(t)
	t.Cleanup(func() {
		require.NoError(t, cluster.Close())
	})

	leader := cluster.waitForLeader(t)
	expected := testClusterStatus(1)
	version, err := leader.Store(expected, NotExists)
	require.NoError(t, err)
	cluster.requireSingleLeader(t)
	cluster.requireReplicatedState(t, expected, version)

	restartOrder := []int{0, 1, 2, 2, 1, 0, 1, 0, 2}
	for i, nodeIdx := range restartOrder {
		currentLeader := cluster.waitForLeader(t)
		if cluster.indexOf(currentLeader) == nodeIdx {
			cluster.restartNode(t, nodeIdx)
			requireReceiveLeadershipLoss(t, currentLeader.LeadershipLost())
		} else {
			cluster.restartNode(t, nodeIdx)
		}

		cluster.requireSingleLeader(t)
		cluster.requireReplicatedState(t, expected, version)

		expected = testClusterStatus(i + 2)
		leader = cluster.waitForLeader(t)
		version, err = leader.Store(expected, version)
		require.NoError(t, err)

		cluster.requireSingleLeader(t)
		cluster.requireReplicatedState(t, expected, version)
	}
}

func newTestTCPAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().String()
}

func requireReceiveLeadershipLoss(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for leadership loss notification")
	}
}

func requireNoLeadershipLoss(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
		t.Fatal("received unexpected leadership loss notification")
	case <-time.After(200 * time.Millisecond):
	}
}

type testRaftCluster struct {
	bootstrapServers []string
	dataDirs         []string
	providers        []*metadataProviderRaft
}

func newTestRaftCluster(t *testing.T) *testRaftCluster {
	t.Helper()

	baseDir := t.TempDir()
	bootstrapServers := []string{
		newTestTCPAddress(t),
		newTestTCPAddress(t),
		newTestTCPAddress(t),
	}

	cluster := &testRaftCluster{
		bootstrapServers: bootstrapServers,
		dataDirs: []string{
			filepath.Join(baseDir, fmt.Sprintf("data-%d", 0)),
			filepath.Join(baseDir, fmt.Sprintf("data-%d", 1)),
			filepath.Join(baseDir, fmt.Sprintf("data-%d", 2)),
		},
		providers: make([]*metadataProviderRaft, len(bootstrapServers)),
	}

	for i := range bootstrapServers {
		cluster.providers[i] = cluster.newProvider(t, i)
	}

	return cluster
}

func (c *testRaftCluster) Close() error {
	var closeErr error
	for i, provider := range c.providers {
		if provider == nil {
			continue
		}
		closeErr = multierr.Append(closeErr, provider.Close())
		c.providers[i] = nil
	}
	return closeErr
}

func (c *testRaftCluster) newProvider(t *testing.T, idx int) *metadataProviderRaft {
	t.Helper()

	provider, err := NewMetadataProviderRaft(c.bootstrapServers[idx], c.bootstrapServers, c.dataDirs[idx])
	require.NoError(t, err)

	mpr, ok := provider.(*metadataProviderRaft)
	require.True(t, ok)
	return mpr
}

func (c *testRaftCluster) restartNode(t *testing.T, idx int) {
	t.Helper()

	if c.providers[idx] != nil {
		require.NoError(t, c.providers[idx].Close())
		c.providers[idx] = nil
	}
	c.providers[idx] = c.newProvider(t, idx)
}

func (c *testRaftCluster) waitForLeader(t *testing.T) *metadataProviderRaft {
	t.Helper()
	return c.requireSingleLeader(t)
}

func (c *testRaftCluster) requireSingleLeader(t *testing.T) *metadataProviderRaft {
	t.Helper()

	var leader *metadataProviderRaft
	require.Eventually(t, func() bool {
		leaders := make([]*metadataProviderRaft, 0, len(c.providers))
		for _, provider := range c.providers {
			if provider == nil {
				continue
			}
			if provider.raft.State() == raft.Leader && provider.raft.VerifyLeader().Error() == nil {
				leaders = append(leaders, provider)
			}
		}
		if len(leaders) != 1 {
			return false
		}
		leader = leaders[0]
		return true
	}, 30*time.Second, 100*time.Millisecond)
	return leader
}

func (c *testRaftCluster) requireReplicatedState(t *testing.T, expected *model.ClusterStatus, version Version) {
	t.Helper()

	expectedVersion := fromVersion(version)
	require.Eventually(t, func() bool {
		for _, provider := range c.providers {
			if provider == nil {
				continue
			}
			state, currentVersion := provider.sc.CloneState()
			if currentVersion != expectedVersion {
				return false
			}
			if !reflect.DeepEqual(state, expected) {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)
}

func (c *testRaftCluster) indexOf(target *metadataProviderRaft) int {
	for i, provider := range c.providers {
		if provider == target {
			return i
		}
	}
	return -1
}

func testClusterStatus(seed int) *model.ClusterStatus {
	leaderName := fmt.Sprintf("leader-%d", seed)
	leader := model.Server{
		Name:     &leaderName,
		Public:   fmt.Sprintf("public-%d", seed),
		Internal: fmt.Sprintf("internal-%d", seed),
	}
	follower1 := model.Server{
		Public:   fmt.Sprintf("follower-%d-a-public", seed),
		Internal: fmt.Sprintf("follower-%d-a-internal", seed),
	}
	follower2 := model.Server{
		Public:   fmt.Sprintf("follower-%d-b-public", seed),
		Internal: fmt.Sprintf("follower-%d-b-internal", seed),
	}

	status := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			"default": {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusSteadyState,
						Term:     int64(seed),
						Leader:   &leader,
						Ensemble: []model.Server{leader, follower1, follower2},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: uint32(100 + seed),
						},
					},
				},
			},
			fmt.Sprintf("ns-%d", seed): {
				ReplicationFactor: 1,
				Shards: map[int64]model.ShardMetadata{
					int64(seed): {
						Status: model.ShardStatusUnknown,
						Term:   int64(seed * 10),
						Int32HashRange: model.Int32HashRange{
							Min: uint32(seed),
							Max: uint32(1000 + seed),
						},
					},
				},
			},
		},
		ShardIdGenerator: int64(seed * 100),
		ServerIdx:        uint32(seed),
	}
	return status.Clone()
}
