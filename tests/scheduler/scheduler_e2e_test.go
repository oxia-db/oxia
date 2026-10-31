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

package scheduler

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/tests/mock"
)

const (
	balanceTimeout = 3 * time.Minute
	balanceTick    = 100 * time.Millisecond
	// writesBeforeMoves is the writes that each namespace acknowledges before
	// a case changes the cluster: their data must survive the moves.
	writesBeforeMoves = 500
	// stableChecks is the checks in a row, a tick apart, that a balanced
	// cluster passes.
	stableChecks = 10
)

// The cases run in parallel, each on its own cluster. Each one writes while
// the scheduler moves the replicas and the leaders, then checks that every
// acknowledged write survived, and that the cluster is balanced.
func TestScheduler(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		run  func(t *testing.T)
	}{
		{"scale out", testScaleOut},
		{"drain a removed data server", testDrain},
		{"replace a data server", testReplace},
		{"scale out with anti-affinity", testScaleOutWithAntiAffinity},
		{"replication factors 1 to 3", testReplicationFactors},
		{"scale out at scale", testScaleOutAtScale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testScaleOut(t *testing.T) {
	t.Helper()
	c := newCluster(t, nil)
	c.addDataServers("ds-0", "ds-1", "ds-2")
	c.createNamespace(&proto.Namespace{Name: "ns-a", InitialShardCount: 6, ReplicationFactor: 3})
	c.createNamespace(&proto.Namespace{Name: "ns-b", InitialShardCount: 4, ReplicationFactor: 2})
	c.waitBalanced()

	writers := c.startWriters("ds-0", "ns-a", "ns-b")
	c.addDataServers("ds-3", "ds-4", "ds-5")
	c.waitBalanced()
	writers.stopAndVerify()

	c.waitBalanced()
	require.Len(t, c.ensembleMembers(), 6, "every data server holds replicas")
}

func testDrain(t *testing.T) {
	t.Helper()
	c := newCluster(t, nil)
	c.addDataServers("ds-0", "ds-1", "ds-2", "ds-3", "ds-4")
	c.createNamespace(&proto.Namespace{Name: "ns-a", InitialShardCount: 6, ReplicationFactor: 3})
	c.createNamespace(&proto.Namespace{Name: "ns-b", InitialShardCount: 4, ReplicationFactor: 3})
	c.waitBalanced()

	writers := c.startWriters("ds-0", "ns-a", "ns-b")
	c.removeDataServer("ds-4")
	c.waitBalanced()
	writers.stopAndVerify()

	c.waitBalanced()
	require.NotContains(t, c.ensembleMembers(), "ds-4")
}

func testReplace(t *testing.T) {
	t.Helper()
	c := newCluster(t, nil)
	c.addDataServers("ds-0", "ds-1", "ds-2", "ds-3")
	c.createNamespace(&proto.Namespace{Name: "ns-a", InitialShardCount: 8, ReplicationFactor: 3})
	c.waitBalanced()

	writers := c.startWriters("ds-0", "ns-a")
	c.removeDataServer("ds-3")
	c.addDataServers("ds-4")
	c.waitBalanced()
	writers.stopAndVerify()

	c.waitBalanced()
	members := c.ensembleMembers()
	require.NotContains(t, members, "ds-3")
	require.Contains(t, members, "ds-4")
}

func testScaleOutWithAntiAffinity(t *testing.T) {
	t.Helper()
	c := newCluster(t, nil)
	zones := map[string]string{"ds-0": "z0", "ds-1": "z1", "ds-2": "z2", "ds-3": "z0", "ds-4": "z1", "ds-5": "z2"}
	for _, name := range []string{"ds-0", "ds-1", "ds-2"} {
		c.addDataServer(name, map[string]string{"zone": zones[name]})
	}
	c.createNamespace(&proto.Namespace{Name: "ns-a", InitialShardCount: 6, ReplicationFactor: 3,
		AntiAffinities: []*proto.AntiAffinity{{Labels: []string{"zone"}, Mode: proto.AntiAffinityModeStrict}}})
	c.waitBalanced()

	writers := c.startWriters("ds-0", "ns-a")
	for _, name := range []string{"ds-3", "ds-4", "ds-5"} {
		c.addDataServer(name, map[string]string{"zone": zones[name]})
	}
	c.waitBalanced()
	writers.stopAndVerify()

	c.waitBalanced()
	for id, shard := range c.status().GetNamespaces()["ns-a"].GetShards() {
		seen := make(map[string]bool)
		for _, member := range shard.GetEnsemble() {
			zone := zones[member.GetNameOrDefault()]
			require.False(t, seen[zone], "shard %d has two replicas in zone %s", id, zone)
			seen[zone] = true
		}
	}
}

func testReplicationFactors(t *testing.T) {
	t.Helper()
	c := newCluster(t, nil)
	c.addDataServers("ds-0", "ds-1", "ds-2")
	for rf := uint32(1); rf <= 3; rf++ {
		c.createNamespace(&proto.Namespace{Name: fmt.Sprintf("rf-%d", rf), InitialShardCount: 4, ReplicationFactor: rf})
	}
	c.waitBalanced("rf-2", "rf-3")

	writers := c.startWriters("ds-0", "rf-1", "rf-2", "rf-3")
	c.addDataServers("ds-3", "ds-4")
	c.waitBalanced("rf-2", "rf-3")
	writers.stopAndVerify()

	// A single replica can't move: only the other namespaces must be balanced
	c.waitBalanced("rf-2", "rf-3")
}

func testScaleOutAtScale(t *testing.T) {
	t.Helper()
	c := newCluster(t, nil)
	c.addDataServers("ds-0", "ds-1", "ds-2", "ds-3")
	for _, name := range []string{"ns-a", "ns-b", "ns-c"} {
		c.createNamespace(&proto.Namespace{Name: name, InitialShardCount: 12, ReplicationFactor: 3})
	}
	c.waitBalanced()

	writers := c.startWriters("ds-0", "ns-a", "ns-b", "ns-c")
	c.addDataServers("ds-4", "ds-5", "ds-6", "ds-7")
	c.waitBalanced()
	writers.stopAndVerify()

	c.waitBalanced()
	require.Len(t, c.ensembleMembers(), 8, "every data server holds replicas")
}

// cluster is a coordinator with its own data servers, whose scheduler policy
// acts in milliseconds rather than minutes.
type cluster struct {
	t           *testing.T
	runtime     coordruntime.Runtime
	mu          sync.Mutex
	dataServers map[string]*dataserver.Server
	identities  map[string]*proto.DataServerIdentity
}

func newCluster(t *testing.T, policy *proto.SchedulerPolicy) *cluster {
	t.Helper()
	if policy == nil {
		policy = &proto.SchedulerPolicy{Interval: "200ms", StableTime: "1ms", MoveBackoff: "200ms",
			MoveTimeout: "30s", MaxMovesPerDataServer: 2, MaxMovesPerNamespace: 4}
	}
	c := &cluster{t: t, dataServers: make(map[string]*dataserver.Server),
		identities: make(map[string]*proto.DataServerIdentity)}
	c.runtime = mock.NewCoordinator(t, mock.NewConfigProvider(t, &proto.ClusterConfiguration{SchedulerPolicy: policy}))
	t.Cleanup(func() {
		assert.NoError(t, c.runtime.Close())
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, s := range c.dataServers {
			assert.NoError(t, s.Close())
		}
	})
	return c
}

func (c *cluster) addDataServers(names ...string) {
	for _, name := range names {
		c.addDataServer(name, nil)
	}
}

func (c *cluster) addDataServer(name string, labels map[string]string) {
	c.t.Helper()
	s, identity := mock.NewServer(c.t, name)
	c.mu.Lock()
	c.dataServers[name] = s
	c.identities[name] = identity
	c.mu.Unlock()
	require.NoError(c.t, c.runtime.Metadata().CreateDataServer(&proto.DataServer{Identity: identity,
		Metadata: &proto.DataServerMetadata{Labels: labels}}))
}

// removeDataServer removes a data server from the configuration: the scheduler
// drains it, and the data server keeps running meanwhile.
func (c *cluster) removeDataServer(name string) {
	c.t.Helper()
	_, err := c.runtime.Metadata().DeleteDataServer(name)
	require.NoError(c.t, err)
}

func (c *cluster) createNamespace(namespace *proto.Namespace) {
	c.t.Helper()
	namespace.KeySorting = "natural"
	require.NoError(c.t, c.runtime.Metadata().CreateNamespace(namespace))
}

func (c *cluster) status() *proto.ClusterStatus {
	return mock.StatusSnapshot(c.t, c.runtime.Metadata())
}

// waitBalanced waits until the cluster stays balanced for stableChecks checks
// in a row, as far as the namespaces go, every one if none is given: the
// metadata shows a move only once its election persists it.
func (c *cluster) waitBalanced(namespaces ...string) {
	c.t.Helper()
	var err error
	checks := 0
	require.Eventually(c.t, func() bool {
		if err = c.balanced(namespaces...); err != nil {
			checks = 0
			return false
		}
		checks++
		return checks >= stableChecks
	}, balanceTimeout, balanceTick, "not balanced: %v", &err)
}

// ensembleMembers returns the data servers in an ensemble of a shard.
func (c *cluster) ensembleMembers() []string {
	members := make(map[string]bool)
	for _, ns := range c.status().GetNamespaces() {
		for _, shard := range ns.GetShards() {
			for _, member := range shard.GetEnsemble() {
				members[member.GetNameOrDefault()] = true
			}
		}
	}
	return slices.Sorted(maps.Keys(members))
}

// balanced returns the first reason why the cluster isn't balanced, reading
// only the metadata. Every namespace of the configuration has its shards, each
// in steady state with a leader, no pending change, and an ensemble of
// distinct data servers of the configuration, of the replication factor. And
// for the namespaces, every one if none is given: the replicas of each one,
// and in total, spread within 1 over the data servers, and no leader could
// move to a member of its ensemble with at least 2 fewer leaders of the
// namespace. A leader can only move within its ensemble, so the leaders can't
// always spread within 1 over every data server.
func (c *cluster) balanced(namespaces ...string) error {
	borrowed, err := c.runtime.Metadata().GetConfig()
	if err != nil {
		return err
	}
	config := borrowed.UnsafeBorrow()
	configured := make(map[string]bool)
	for _, dataServer := range config.ListDataServers() {
		configured[dataServer.GetIdentity().GetNameOrDefault()] = true
	}
	statuses := c.status().GetNamespaces()
	for _, namespace := range config.GetNamespaces() {
		if len(statuses[namespace.GetName()].GetShards()) == 0 {
			return fmt.Errorf("namespace %s has no shards yet", namespace.GetName())
		}
	}
	total := make(map[string]int)
	for _, name := range slices.Sorted(maps.Keys(statuses)) {
		ns := statuses[name]
		checked := len(namespaces) == 0 || slices.Contains(namespaces, name)
		replicas, leaders := make(map[string]int), make(map[string]int)
		for id, shard := range ns.GetShards() {
			if err := checkShard(name, id, shard, int(ns.GetReplicationFactor()), configured); err != nil {
				return err
			}
			for _, member := range shard.GetEnsemble() {
				replicas[member.GetNameOrDefault()]++
				total[member.GetNameOrDefault()]++
			}
			leaders[shard.GetLeader().GetNameOrDefault()]++
		}
		if !checked {
			continue
		}
		if spread(configured, replicas) > 1 {
			return fmt.Errorf("the replicas of %s spread over more than 1: %v", name, replicas)
		}
		for id, shard := range ns.GetShards() {
			leader := shard.GetLeader().GetNameOrDefault()
			for _, member := range shard.GetEnsemble() {
				if leaders[leader]-leaders[member.GetNameOrDefault()] >= 2 {
					return fmt.Errorf("the leader of shard %d of %s could move to %s: %v", id, name,
						member.GetNameOrDefault(), leaders)
				}
			}
		}
	}
	if len(namespaces) == 0 && spread(configured, total) > 1 {
		return fmt.Errorf("the replicas spread over more than 1: %v", total)
	}
	return nil
}

func checkShard(namespace string, id int64, shard *proto.ShardMetadata, replicationFactor int,
	configured map[string]bool) error {
	switch {
	case shard.GetStatusOrDefault() != proto.ShardStatusSteadyState || shard.GetLeader() == nil:
		return fmt.Errorf("shard %d of %s isn't led yet", id, namespace)
	case shard.GetEnsembleChange() != nil:
		return fmt.Errorf("shard %d of %s has a pending change", id, namespace)
	case len(shard.GetEnsemble()) != replicationFactor:
		return fmt.Errorf("shard %d of %s has %d members", id, namespace, len(shard.GetEnsemble()))
	}
	members := make(map[string]bool)
	for _, member := range shard.GetEnsemble() {
		name := member.GetNameOrDefault()
		if !configured[name] || members[name] {
			return fmt.Errorf("shard %d of %s has the member %s twice, or removed", id, namespace, name)
		}
		members[name] = true
	}
	return nil
}

func spread(dataServers map[string]bool, counts map[string]int) int {
	lowest, highest := -1, 0
	for name := range dataServers {
		if lowest < 0 || counts[name] < lowest {
			lowest = counts[name]
		}
		highest = max(highest, counts[name])
	}
	return highest - lowest
}

// writers write unique keys to namespaces until stopped, and record the ones
// that the cluster acknowledged.
type writers struct {
	t        *testing.T
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	clients  map[string]oxia.SyncClient
	mu       sync.Mutex
	acked    map[string]map[string]string
	attempts int
}

// startWriters starts writing to namespaces through a data server, and returns
// once each namespace has acknowledged writes to move.
func (c *cluster) startWriters(through string, namespaces ...string) *writers {
	c.t.Helper()
	ctx, cancel := context.WithCancel(c.t.Context())
	w := &writers{t: c.t, cancel: cancel, clients: make(map[string]oxia.SyncClient),
		acked: make(map[string]map[string]string)}
	for _, namespace := range namespaces {
		client, err := oxia.NewSyncClient(c.identities[through].GetPublic(), oxia.WithNamespace(namespace),
			oxia.WithRequestTimeout(5*time.Second))
		require.NoError(c.t, err)
		w.clients[namespace] = client
		w.acked[namespace] = make(map[string]string)
		w.wg.Go(func() { w.write(ctx, namespace, client) })
	}
	require.Eventually(c.t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		for _, writes := range w.acked {
			if len(writes) < writesBeforeMoves {
				return false
			}
		}
		return true
	}, balanceTimeout, balanceTick)
	return w
}

func (w *writers) write(ctx context.Context, namespace string, client oxia.SyncClient) {
	for i := 0; ctx.Err() == nil; i++ {
		key, value := fmt.Sprintf("key-%06d", i), fmt.Sprintf("%s-%d", namespace, i)
		_, _, err := client.Put(ctx, key, []byte(value))
		w.mu.Lock()
		w.attempts++
		if err == nil {
			w.acked[namespace][key] = value
		}
		w.mu.Unlock()
	}
}

// stopAndVerify stops the writers, and reads every namespace back: each
// acknowledged write is there with its value. A write that failed may or may
// not be there.
func (w *writers) stopAndVerify() {
	w.t.Helper()
	w.cancel()
	w.wg.Wait()
	acked := 0
	for namespace, writes := range w.acked {
		client := w.clients[namespace]
		stored := make(map[string]string)
		for result := range client.RangeScan(context.Background(), "", "") {
			require.NoError(w.t, result.Err, "namespace %s", namespace)
			stored[result.Key] = string(result.Value)
		}
		for key, value := range writes {
			require.Equal(w.t, value, stored[key], "namespace %s lost the write of %s", namespace, key)
		}
		acked += len(writes)
		require.NoError(w.t, client.Close())
	}
	w.t.Logf("%d of %d writes acknowledged, and read back", acked, w.attempts)
	require.Positive(w.t, acked)
}
