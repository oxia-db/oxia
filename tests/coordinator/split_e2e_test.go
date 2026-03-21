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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonoption "github.com/oxia-db/oxia/oxiad/common/option"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

func TestCoordinator_ShardSplit(t *testing.T) {
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

	coordinatorInstance, _, err := coordinator.NewCoordinator(
		metadataProvider,
		func() (model.ClusterConfig, error) { return clusterConfig, nil },
		nil,
		rpc2.NewRpcProvider(clientPool),
	)
	require.NoError(t, err)

	statusResource := coordinatorInstance.StatusResource()

	// Wait for initial shard to be in steady state
	require.Eventually(t, func() bool {
		shard := statusResource.Load().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 30*time.Second, 100*time.Millisecond)

	slog.Info("Initial cluster is ready")

	// Create a client connected through one of the dataservers
	client, err := oxia.NewSyncClient(sa1.Public)
	require.NoError(t, err)

	ctx := context.Background()

	// Write a set of keys that will span the hash range. We use a large number
	// to ensure we have keys on both sides of whatever split point is chosen.
	numKeys := 100
	writtenKeys := make(map[string][]byte)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := []byte(fmt.Sprintf("value-%04d", i))
		_, _, err := client.Put(ctx, key, value)
		require.NoError(t, err)
		writtenKeys[key] = value
	}

	slog.Info("Written all keys", slog.Int("count", numKeys))

	// Trigger shard split (use default split point = midpoint of hash range)
	leftChild, rightChild, err := coordinatorInstance.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)
	slog.Info("Split initiated",
		slog.Int64("left-child", leftChild),
		slog.Int64("right-child", rightChild),
	)

	// Wait for split to complete: parent shard (0) should be removed,
	// and both children should be in steady state with leaders.
	require.Eventually(t, func() bool {
		status := statusResource.Load()
		ns, ok := status.Namespaces[constant.DefaultNamespace]
		if !ok {
			t.Log("Namespace not found in status")
			return false
		}

		// Parent should be deleted
		if _, parentExists := ns.Shards[0]; parentExists {
			t.Log("Parent shard 0 still exists")
			return false
		}

		// Left child should be in steady state with a leader
		left, leftExists := ns.Shards[leftChild]
		if !leftExists {
			t.Logf("Left child shard %d not found", leftChild)
			return false
		}
		if left.Status != model.ShardStatusSteadyState {
			t.Logf("Left child shard %d status: %v", leftChild, left.Status)
			return false
		}
		if left.Leader == nil {
			t.Logf("Left child shard %d has no leader", leftChild)
			return false
		}

		// Right child should be in steady state with a leader
		right, rightExists := ns.Shards[rightChild]
		if !rightExists {
			t.Logf("Right child shard %d not found", rightChild)
			return false
		}
		if right.Status != model.ShardStatusSteadyState {
			t.Logf("Right child shard %d status: %v", rightChild, right.Status)
			return false
		}
		if right.Leader == nil {
			t.Logf("Right child shard %d has no leader", rightChild)
			return false
		}

		// Children should have no split metadata
		if left.Split != nil {
			t.Logf("Left child shard %d still has split metadata: %+v", leftChild, left.Split)
			return false
		}
		if right.Split != nil {
			t.Logf("Right child shard %d still has split metadata: %+v", rightChild, right.Split)
			return false
		}

		return true
	}, 2*time.Minute, 500*time.Millisecond)

	slog.Info("Split complete")

	// Verify hash ranges: children should cover the entire original range
	status := statusResource.Load()
	ns := status.Namespaces[constant.DefaultNamespace]
	leftMeta := ns.Shards[leftChild]
	rightMeta := ns.Shards[rightChild]

	assert.EqualValues(t, 0, leftMeta.Int32HashRange.Min)
	assert.EqualValues(t, math.MaxUint32, rightMeta.Int32HashRange.Max)
	assert.EqualValues(t, leftMeta.Int32HashRange.Max+1, rightMeta.Int32HashRange.Min)

	slog.Info("Hash ranges verified",
		slog.Any("left-range", leftMeta.Int32HashRange),
		slog.Any("right-range", rightMeta.Int32HashRange),
	)

	// Close the old client and create a new one that will receive
	// the updated shard assignments (with children instead of parent)
	assert.NoError(t, client.Close())

	// The client needs to reconnect to get updated shard assignments
	require.Eventually(t, func() bool {
		client, err = oxia.NewSyncClient(sa1.Public)
		if err != nil {
			return false
		}
		// Try a read to confirm the client has working assignments
		_, _, _, err = client.Get(ctx, "key-0000")
		if err != nil {
			_ = client.Close()
			return false
		}
		return true
	}, 30*time.Second, 500*time.Millisecond)

	// Verify all data is still accessible
	for key, expectedValue := range writtenKeys {
		_, value, _, err := client.Get(ctx, key)
		if !assert.NoError(t, err, "failed to get key %s", key) {
			continue
		}
		assert.Equal(t, expectedValue, value, "value mismatch for key %s", key)
	}

	slog.Info("All data verified after split")

	// ---- Per-shard key duplication verification ----
	// Connect directly to each child shard's leader and list all keys.
	// Every key's hash must fall within that shard's hash range, and the
	// union of keys across children must equal the original set (no duplication).
	allShardKeys := make(map[string]struct{})
	for _, childId := range []int64{leftChild, rightChild} {
		childMeta := ns.Shards[childId]
		leaderTarget := childMeta.Leader.Public

		rpcClient, err := clientPool.GetClientRpc(leaderTarget)
		require.NoError(t, err, "connect to child %d leader", childId)

		shardIdVal := childId
		listStream, err := rpcClient.List(ctx, &proto.ListRequest{
			Shard:          &shardIdVal,
			StartInclusive: "",
			EndExclusive:   "",
		})
		require.NoError(t, err, "list keys on child %d", childId)

		var shardKeys []string
		for {
			resp, err := listStream.Recv()
			if err != nil {
				break
			}
			shardKeys = append(shardKeys, resp.Keys...)
		}

		slog.Info("Listed keys on child shard",
			slog.Int64("shard", childId),
			slog.Int("key-count", len(shardKeys)),
			slog.Any("hash-range", childMeta.Int32HashRange),
		)

		// Every key on this shard must hash within the shard's range
		for _, key := range shardKeys {
			h := hash.Xxh332(key)
			assert.True(t, h >= childMeta.Int32HashRange.Min && h <= childMeta.Int32HashRange.Max,
				"key %q (hash=%d) is outside shard %d range [%d, %d]",
				key, h, childId, childMeta.Int32HashRange.Min, childMeta.Int32HashRange.Max)
			allShardKeys[key] = struct{}{}
		}
	}

	// Total unique keys across both children should equal what we wrote
	assert.Equal(t, numKeys, len(allShardKeys),
		"expected %d unique keys across children, got %d (possible duplication)", numKeys, len(allShardKeys))
	slog.Info("Per-shard key verification passed — no duplication detected",
		slog.Int("total-unique-keys", len(allShardKeys)))

	// Verify we can write new data to both children
	_, _, err = client.Put(ctx, "post-split-key-1", []byte("new-value-1"))
	assert.NoError(t, err)

	_, _, err = client.Put(ctx, "post-split-key-2", []byte("new-value-2"))
	assert.NoError(t, err)

	// Read back the new keys
	_, val, _, err := client.Get(ctx, "post-split-key-1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("new-value-1"), val)

	_, val, _, err = client.Get(ctx, "post-split-key-2")
	assert.NoError(t, err)
	assert.Equal(t, []byte("new-value-2"), val)

	slog.Info("Post-split writes verified")

	assert.NoError(t, client.Close())
	assert.NoError(t, coordinatorInstance.Close())
	assert.NoError(t, clientPool.Close())

	for _, serverObj := range servers {
		assert.NoError(t, serverObj.Close())
	}
}

// splitTestCluster holds references to a 3-node test cluster with a
// coordinator, used by the shard-split integration tests.
type splitTestCluster struct {
	servers             map[model.Server]*dataserver.Server
	sa1                 model.Server
	coordinator         coordinator.Coordinator
	statusResource      resource.StatusResource
	clientPool          rpc.ClientPool
	leftChild           int64
	rightChild          int64
	leftMeta, rightMeta model.ShardMetadata
}

// setupSplitCluster creates a 3-node cluster, waits for the initial shard to
// be ready, and returns the cluster handle. Callers must call close() when done.
func setupSplitCluster(t *testing.T) *splitTestCluster {
	t.Helper()

	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[model.Server]*dataserver.Server{
		sa1: s1, sa2: s2, sa3: s3,
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

	coordinatorInstance, _, err := coordinator.NewCoordinator(
		metadataProvider,
		func() (model.ClusterConfig, error) { return clusterConfig, nil },
		nil,
		rpc2.NewRpcProvider(clientPool),
	)
	require.NoError(t, err)

	statusResource := coordinatorInstance.StatusResource()
	require.Eventually(t, func() bool {
		shard := statusResource.Load().Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.Status == model.ShardStatusSteadyState
	}, 30*time.Second, 100*time.Millisecond)
	slog.Info("Initial cluster is ready")

	return &splitTestCluster{
		servers:        servers,
		sa1:            sa1,
		coordinator:    coordinatorInstance,
		statusResource: statusResource,
		clientPool:     clientPool,
	}
}

// splitAndWait triggers a shard split on shard 0 and waits for both children
// to reach steady state with leaders and no split metadata.
func (c *splitTestCluster) splitAndWait(t *testing.T) {
	t.Helper()

	var err error
	c.leftChild, c.rightChild, err = c.coordinator.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)
	slog.Info("Split initiated",
		slog.Int64("left-child", c.leftChild),
		slog.Int64("right-child", c.rightChild),
	)

	require.Eventually(t, func() bool {
		status := c.statusResource.Load()
		ns := status.Namespaces[constant.DefaultNamespace]
		if _, parentExists := ns.Shards[0]; parentExists {
			return false
		}
		for _, child := range []int64{c.leftChild, c.rightChild} {
			sm, ok := ns.Shards[child]
			if !ok || sm.Status != model.ShardStatusSteadyState || sm.Leader == nil || sm.Split != nil {
				return false
			}
		}
		return true
	}, 60*time.Second, 500*time.Millisecond)

	status := c.statusResource.Load()
	ns := status.Namespaces[constant.DefaultNamespace]
	c.leftMeta = ns.Shards[c.leftChild]
	c.rightMeta = ns.Shards[c.rightChild]
	slog.Info("Split complete",
		slog.Any("left-range", c.leftMeta.Int32HashRange),
		slog.Any("right-range", c.rightMeta.Int32HashRange),
	)
}

// reconnectClient closes an existing client (if non-nil) and creates a new
// SyncClient that picks up the post-split shard assignments. testKey is any
// key known to exist — it is used to verify the client has working assignments.
func (c *splitTestCluster) reconnectClient(t *testing.T, old oxia.SyncClient, testKey string,
	opts ...oxia.ClientOption) oxia.SyncClient {
	t.Helper()
	if old != nil {
		assert.NoError(t, old.Close())
	}

	var client oxia.SyncClient
	require.Eventually(t, func() bool {
		var err error
		client, err = oxia.NewSyncClient(c.sa1.Public, opts...)
		if err != nil {
			return false
		}
		_, _, _, err = client.Get(context.Background(), testKey)
		if err != nil {
			_ = client.Close()
			return false
		}
		return true
	}, 30*time.Second, 500*time.Millisecond)
	return client
}

func (c *splitTestCluster) close(t *testing.T) {
	t.Helper()
	assert.NoError(t, c.coordinator.Close())
	assert.NoError(t, c.clientPool.Close())
	for _, s := range c.servers {
		assert.NoError(t, s.Close())
	}
}

// ---- Notifications test ----

func TestCoordinator_ShardSplit_Notifications(t *testing.T) {
	cluster := setupSplitCluster(t)
	defer cluster.close(t)

	ctx := context.Background()
	client, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)

	// Write keys before the split
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("notif-key-%04d", i)
		_, _, err := client.Put(ctx, key, []byte(fmt.Sprintf("v-%04d", i)))
		require.NoError(t, err)
	}
	slog.Info("Written pre-split keys", slog.Int("count", numKeys))

	// Perform split
	cluster.splitAndWait(t)

	// Reconnect to get updated shard assignments
	client = cluster.reconnectClient(t, client, "notif-key-0000")
	defer func() { assert.NoError(t, client.Close()) }()

	// Subscribe to notifications after the split.
	notifications, err := client.GetNotifications()
	require.NoError(t, err)
	defer func() { assert.NoError(t, notifications.Close()) }()

	// Wait for notification streams to be fully established against BOTH
	// child shards. We write a trigger key to each child shard and wait
	// until we receive a notification for both.
	triggersReceived := make(map[string]bool)
	require.Eventually(t, func() bool {
		// Write triggers that hash to different shards
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("trigger-%04d", i)
			_, _, _ = client.Put(ctx, key, []byte("t"))
		}
		// Drain notifications
		for {
			select {
			case n := <-notifications.Ch():
				if n != nil && strings.HasPrefix(n.Key, "trigger-") {
					// Track which child shard this key belongs to
					h := hash.Xxh332(n.Key)
					if h <= cluster.leftMeta.Int32HashRange.Max {
						triggersReceived["left"] = true
					} else {
						triggersReceived["right"] = true
					}
				}
			default:
				return triggersReceived["left"] && triggersReceived["right"]
			}
		}
	}, 30*time.Second, 1*time.Second, "notification streams not established for both shards")
	slog.Info("Notification streams established for both child shards")

	// Write new keys after the split — these should generate notifications
	numPostSplitKeys := 20
	postSplitKeys := make(map[string]string) // key -> value
	for i := 0; i < numPostSplitKeys; i++ {
		key := fmt.Sprintf("post-notif-%04d", i)
		value := fmt.Sprintf("post-v-%04d", i)
		_, _, err := client.Put(ctx, key, []byte(value))
		require.NoError(t, err)
		postSplitKeys[key] = value
	}

	// Also update a pre-split key
	_, _, err = client.Put(ctx, "notif-key-0000", []byte("updated"))
	require.NoError(t, err)

	// Delete a pre-split key
	err = client.Delete(ctx, "notif-key-0010")
	require.NoError(t, err)

	slog.Info("Written post-split changes for notification test")

	// Collect notifications — we expect the post-split creates + modify + delete.
	receivedKeys := make(map[string]oxia.NotificationType)
	expectedEvents := numPostSplitKeys + 2 // creates + modify + delete
	deadline := time.After(30 * time.Second)
	collected := 0
collectLoop:
	for collected < expectedEvents {
		select {
		case n := <-notifications.Ch():
			if n == nil {
				break collectLoop
			}
			if strings.HasPrefix(n.Key, "trigger-") {
				continue // skip trigger notifications
			}
			receivedKeys[n.Key] = n.Type
			collected++
		case <-deadline:
			break collectLoop
		}
	}

	slog.Info("Collected notifications",
		slog.Int("count", collected),
	)

	// Verify all post-split creates were received
	for key := range postSplitKeys {
		_, found := receivedKeys[key]
		assert.True(t, found, "missing notification for created key %s", key)
	}

	// Verify modify and delete notifications were received
	_, modFound := receivedKeys["notif-key-0000"]
	assert.True(t, modFound, "missing notification for modified key notif-key-0000")
	_, delFound := receivedKeys["notif-key-0010"]
	assert.True(t, delFound, "missing notification for deleted key notif-key-0010")

	// Verify we can still read all surviving pre-split keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("notif-key-%04d", i)
		if i == 10 {
			continue // deleted
		}
		_, _, _, err := client.Get(ctx, key)
		assert.NoError(t, err, "failed to read surviving pre-split key %s", key)
	}

	// Verify the updated key has the new value
	_, val, _, err := client.Get(ctx, "notif-key-0000")
	assert.NoError(t, err)
	assert.Equal(t, []byte("updated"), val)

	// Verify the deleted key is gone
	_, _, _, err = client.Get(ctx, "notif-key-0010")
	assert.ErrorIs(t, err, oxia.ErrKeyNotFound)

	slog.Info("Notifications test passed")
}

// ---- Ephemeral records test ----

func TestCoordinator_ShardSplit_EphemeralRecords(t *testing.T) {
	cluster := setupSplitCluster(t)
	defer cluster.close(t)

	ctx := context.Background()

	// Create a client with a session for ephemeral records
	ephemeralClient, err := oxia.NewSyncClient(cluster.sa1.Public,
		oxia.WithSessionTimeout(30*time.Second),
		oxia.WithIdentity("ephemeral-test-client"),
	)
	require.NoError(t, err)

	// Write regular (non-ephemeral) keys
	numRegular := 30
	for i := 0; i < numRegular; i++ {
		key := fmt.Sprintf("regular-%04d", i)
		_, _, err := ephemeralClient.Put(ctx, key, []byte(fmt.Sprintf("rv-%04d", i)))
		require.NoError(t, err)
	}

	// Write ephemeral keys
	numEphemeral := 20
	ephemeralKeys := make(map[string]string) // key -> value
	for i := 0; i < numEphemeral; i++ {
		key := fmt.Sprintf("ephemeral-%04d", i)
		value := fmt.Sprintf("ev-%04d", i)
		_, version, err := ephemeralClient.Put(ctx, key, []byte(value), oxia.Ephemeral())
		require.NoError(t, err)
		assert.True(t, version.Ephemeral, "key %s should be ephemeral", key)
		assert.Equal(t, "ephemeral-test-client", version.ClientIdentity)
		ephemeralKeys[key] = value
	}
	slog.Info("Written ephemeral keys", slog.Int("count", numEphemeral))

	// Perform split
	cluster.splitAndWait(t)

	// Reconnect the ephemeral client to pick up new shard assignments
	// We must NOT close the old client first (that would delete ephemeral
	// records), so we create a new one and close the old one only after.
	var newEphemeralClient oxia.SyncClient
	require.Eventually(t, func() bool {
		var err2 error
		newEphemeralClient, err2 = oxia.NewSyncClient(cluster.sa1.Public,
			oxia.WithSessionTimeout(30*time.Second),
			oxia.WithIdentity("ephemeral-test-client"),
		)
		if err2 != nil {
			return false
		}
		_, _, _, err2 = newEphemeralClient.Get(ctx, "regular-0000")
		if err2 != nil {
			_ = newEphemeralClient.Close()
			return false
		}
		return true
	}, 30*time.Second, 500*time.Millisecond)
	// Close old client — may error because shard 0 no longer exists for
	// session cleanup, but that's expected after a split.
	_ = ephemeralClient.Close()
	ephemeralClient = newEphemeralClient

	// Verify all regular keys survived the split
	for i := 0; i < numRegular; i++ {
		key := fmt.Sprintf("regular-%04d", i)
		_, _, version, err := ephemeralClient.Get(ctx, key)
		assert.NoError(t, err, "regular key %s should still exist", key)
		assert.False(t, version.Ephemeral, "regular key %s should not be ephemeral", key)
	}
	slog.Info("Regular keys verified after split")

	// Verify all ephemeral keys survived the split and retained their
	// ephemeral property
	for key, expectedValue := range ephemeralKeys {
		_, value, version, err := ephemeralClient.Get(ctx, key)
		if !assert.NoError(t, err, "ephemeral key %s should still exist after split", key) {
			continue
		}
		assert.Equal(t, []byte(expectedValue), value, "value mismatch for %s", key)
		assert.True(t, version.Ephemeral, "key %s should still be ephemeral after split", key)
	}
	slog.Info("Ephemeral keys verified after split")

	// Write new ephemeral records to child shards
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("post-split-eph-%04d", i)
		_, version, err := ephemeralClient.Put(ctx, key, []byte("post"), oxia.Ephemeral())
		assert.NoError(t, err, "post-split ephemeral put should succeed")
		assert.True(t, version.Ephemeral)
	}
	slog.Info("Post-split ephemeral writes verified")

	// Close the ephemeral client — all ephemeral records should eventually
	// be deleted by session expiry. Verify using a separate non-ephemeral client.
	assert.NoError(t, ephemeralClient.Close())

	readerClient, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)
	defer func() { assert.NoError(t, readerClient.Close()) }()

	// Wait for ephemeral records to be cleaned up (session expiry)
	require.Eventually(t, func() bool {
		for key := range ephemeralKeys {
			_, _, _, err := readerClient.Get(ctx, key)
			if err == nil {
				return false // still exists
			}
		}
		return true
	}, 60*time.Second, 500*time.Millisecond, "ephemeral records should be deleted after client close")

	slog.Info("Ephemeral records cleaned up after client close")

	// Regular keys should still exist
	for i := 0; i < numRegular; i++ {
		key := fmt.Sprintf("regular-%04d", i)
		_, _, _, err := readerClient.Get(ctx, key)
		assert.NoError(t, err, "regular key %s should survive ephemeral client close", key)
	}

	slog.Info("Ephemeral records test passed")
}

// ---- Secondary indexes test ----

func TestCoordinator_ShardSplit_SecondaryIndexes(t *testing.T) {
	cluster := setupSplitCluster(t)
	defer cluster.close(t)

	ctx := context.Background()
	client, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)

	// Write records with secondary indexes before the split.
	// We use two secondary indexes:
	//   "category" — groups keys by category (few distinct values)
	//   "priority" — a numeric priority for ordering
	type record struct {
		key      string
		value    string
		category string
		priority string
	}

	var records []record
	categories := []string{"alpha", "beta", "gamma"}
	for i := 0; i < 60; i++ {
		r := record{
			key:      fmt.Sprintf("idx-key-%04d", i),
			value:    fmt.Sprintf("idx-val-%04d", i),
			category: categories[i%len(categories)],
			priority: fmt.Sprintf("%04d", i),
		}
		records = append(records, r)
		_, _, err := client.Put(ctx, r.key, []byte(r.value),
			oxia.SecondaryIndex("category", r.category),
			oxia.SecondaryIndex("priority", r.priority),
		)
		require.NoError(t, err)
	}
	slog.Info("Written records with secondary indexes", slog.Int("count", len(records)))

	// Verify secondary indexes work before split (baseline)
	alphaKeysBefore, err := client.List(ctx, "alpha", "alpha\xff", oxia.UseIndex("category"))
	require.NoError(t, err)
	assert.Equal(t, 20, len(alphaKeysBefore), "expected 20 alpha keys before split")

	// Perform split
	cluster.splitAndWait(t)

	// Reconnect client
	client = cluster.reconnectClient(t, client, "idx-key-0000")
	defer func() { assert.NoError(t, client.Close()) }()

	// ---- Verify secondary index "category" ----
	// List all keys for each category via secondary index
	for _, cat := range categories {
		keys, err := client.List(ctx, cat, cat+"\xff", oxia.UseIndex("category"))
		require.NoError(t, err, "list by category %q", cat)

		// Count expected keys for this category
		var expected []string
		for _, r := range records {
			if r.category == cat {
				expected = append(expected, r.key)
			}
		}

		assert.Equal(t, len(expected), len(keys),
			"category %q: expected %d keys, got %d", cat, len(expected), len(keys))

		slog.Info("Category index verified",
			slog.String("category", cat),
			slog.Int("expected", len(expected)),
			slog.Int("actual", len(keys)),
		)
	}

	// ---- Verify secondary index "priority" with range query ----
	// Query a range of priorities: "0010" to "0030" (exclusive)
	priorityKeys, err := client.List(ctx, "0010", "0030", oxia.UseIndex("priority"))
	require.NoError(t, err)
	assert.Equal(t, 20, len(priorityKeys), "expected 20 keys in priority range [0010, 0030)")

	// Verify the returned keys match the expected primary keys
	expectedPriorityKeys := make(map[string]bool)
	for i := 10; i < 30; i++ {
		expectedPriorityKeys[fmt.Sprintf("idx-key-%04d", i)] = true
	}
	for _, key := range priorityKeys {
		assert.True(t, expectedPriorityKeys[key],
			"unexpected key %q in priority range query", key)
	}

	// ---- Verify RangeScan with secondary index ----
	rangeScanResults := make(map[string]string) // primary key -> value
	resCh := client.RangeScan(ctx, "0050", "0060", oxia.UseIndex("priority"))
	for res := range resCh {
		require.NoError(t, res.Err)
		rangeScanResults[res.Key] = string(res.Value)
	}
	assert.Equal(t, 10, len(rangeScanResults), "expected 10 results in priority range scan [0050, 0060)")
	for i := 50; i < 60; i++ {
		key := fmt.Sprintf("idx-key-%04d", i)
		expectedVal := fmt.Sprintf("idx-val-%04d", i)
		assert.Equal(t, expectedVal, rangeScanResults[key],
			"range scan value mismatch for %s", key)
	}

	// ---- Verify new writes with secondary indexes after split ----
	for i := 60; i < 70; i++ {
		key := fmt.Sprintf("idx-key-%04d", i)
		value := fmt.Sprintf("idx-val-%04d", i)
		cat := categories[i%len(categories)]
		_, _, err := client.Put(ctx, key, []byte(value),
			oxia.SecondaryIndex("category", cat),
			oxia.SecondaryIndex("priority", fmt.Sprintf("%04d", i)),
		)
		require.NoError(t, err)
	}

	// Verify new post-split records appear in secondary index queries
	allAlphaKeys, err := client.List(ctx, "alpha", "alpha\xff", oxia.UseIndex("category"))
	require.NoError(t, err)
	// Original: 20 alpha keys (i % 3 == 0 for i in [0,59]) + new: i=60,63,66,69 → 4 more
	// Actually: alpha is categories[0], so i%3==0 → indices 0,3,6,...,57 = 20 keys
	// New: i=60,63,66,69 → 4 more alpha keys
	assert.Equal(t, 24, len(allAlphaKeys), "expected 24 alpha keys after adding post-split records")

	slog.Info("Secondary indexes test passed")
}

func TestCoordinator_KeySorting(t *testing.T) {
	for _, test := range []struct {
		sorting string
	}{
		{"hierarchical"},
		{"natural"},
	} {
		t.Run(test.sorting, func(t *testing.T) {
			dataServerOption := option.NewDefaultOptions()
			dataServerOption.Server.Public.BindAddress = "localhost:0"
			dataServerOption.Server.Internal.BindAddress = "localhost:0"
			dataServerOption.Observability.Metric.Enabled = &constant.FlagFalse
			dataServerOption.Storage.Database.Dir = t.TempDir()
			dataServerOption.Storage.WAL.Dir = t.TempDir()
			s1, err := dataserver.New(t.Context(), commonoption.NewWatch(dataServerOption))
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

			coordinatorInstance, _, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
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

// --- Split Failure E2E Tests ---

// waitForSplitPhase waits until the parent shard's split metadata reaches the
// given phase, or fails the test if the timeout is exceeded.
func waitForSplitPhase(t *testing.T, statusRes resource.StatusResource, parentShardId int64, phase model.SplitPhase, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := statusRes.Load()
		ns := status.Namespaces[constant.DefaultNamespace]
		parentMeta, exists := ns.Shards[parentShardId]
		if !exists || parentMeta.Split == nil {
			return false
		}
		return parentMeta.Split.Phase >= phase
	}, timeout, 100*time.Millisecond)
}

func TestCoordinator_ShardSplit_ParentLeaderKillDuringSplit(t *testing.T) {
	t.Skip("TODO: split controller retries AddFollower to dead parent; needs shard controller to detect " +
		"failure and elect new parent leader faster")
	cluster := setupSplitCluster(t)
	defer cluster.close(t)

	// Write some keys before split
	client, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_, _, err = client.Put(ctx, fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf("value-%d", i)))
		require.NoError(t, err)
	}
	assert.NoError(t, client.Close())

	// Find the parent leader before initiating the split
	status := cluster.statusResource.Load()
	parentLeader := *status.Namespaces[constant.DefaultNamespace].Shards[0].Leader
	slog.Info("Parent leader identified", slog.Any("leader", parentLeader))

	// Initiate split (don't wait for completion)
	leftChild, rightChild, err := cluster.coordinator.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)
	cluster.leftChild = leftChild
	cluster.rightChild = rightChild
	slog.Info("Split initiated")

	// Wait briefly for Bootstrap to start, then kill the parent leader.
	// This simulates a leader crash during the observer snapshot transfer.
	waitForSplitPhase(t, cluster.statusResource, 0, model.SplitPhaseBootstrap, 30*time.Second)
	slog.Info("Kill parent leader during Bootstrap/CatchUp", slog.Any("leader", parentLeader))

	assert.NoError(t, cluster.servers[parentLeader].Close())
	delete(cluster.servers, parentLeader)

	// The split controller should recover: the coordinator will elect a new
	// parent leader, the split controller detects the term change and falls
	// back to Bootstrap to re-add observers, then completes.
	slog.Info("Waiting for split to complete after parent leader kill")

	require.Eventually(t, func() bool {
		st := cluster.statusResource.Load()
		ns := st.Namespaces[constant.DefaultNamespace]
		if _, parentExists := ns.Shards[0]; parentExists {
			return false
		}
		for _, child := range []int64{leftChild, rightChild} {
			sm, ok := ns.Shards[child]
			if !ok || sm.Status != model.ShardStatusSteadyState || sm.Leader == nil || sm.Split != nil {
				return false
			}
		}
		return true
	}, 2*time.Minute, 500*time.Millisecond)
	slog.Info("Split completed after parent leader kill")

	// Verify all 50 keys are still accessible
	var survivingAddr model.Server
	for addr := range cluster.servers {
		survivingAddr = addr
		break
	}

	require.Eventually(t, func() bool {
		client, err = oxia.NewSyncClient(survivingAddr.Public)
		if err != nil {
			return false
		}
		_, _, _, err = client.Get(ctx, "key-0000")
		if err != nil {
			_ = client.Close()
			return false
		}
		return true
	}, 30*time.Second, 500*time.Millisecond)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		_, value, _, err := client.Get(ctx, key)
		assert.NoError(t, err, "key %s not found after split with leader kill", key)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
	assert.NoError(t, client.Close())
}

func TestCoordinator_ShardSplit_FollowerKillDuringSplit(t *testing.T) {
	cluster := setupSplitCluster(t)
	defer cluster.close(t)

	// Write keys before split
	client, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		_, _, err = client.Put(ctx, fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf("value-%d", i)))
		require.NoError(t, err)
	}
	assert.NoError(t, client.Close())

	// Find a follower (non-leader) server
	status := cluster.statusResource.Load()
	parentMeta := status.Namespaces[constant.DefaultNamespace].Shards[0]
	parentLeader := *parentMeta.Leader
	var follower model.Server
	for addr := range cluster.servers {
		if addr != parentLeader {
			follower = addr
			break
		}
	}

	// Kill a follower before initiating split
	slog.Info("Killing follower before split", slog.Any("follower", follower))
	assert.NoError(t, cluster.servers[follower].Close())
	delete(cluster.servers, follower)

	// The split should still complete with 2/3 servers (quorum)
	slog.Info("Initiating split with one follower down")
	cluster.leftChild, cluster.rightChild, err = cluster.coordinator.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st := cluster.statusResource.Load()
		ns := st.Namespaces[constant.DefaultNamespace]
		if _, parentExists := ns.Shards[0]; parentExists {
			return false
		}
		for _, child := range []int64{cluster.leftChild, cluster.rightChild} {
			sm, ok := ns.Shards[child]
			if !ok || sm.Status != model.ShardStatusSteadyState || sm.Leader == nil || sm.Split != nil {
				return false
			}
		}
		return true
	}, 2*time.Minute, 500*time.Millisecond)
	slog.Info("Split completed with one follower down")

	// Verify keys accessible through a surviving server
	var survivingAddr model.Server
	for addr := range cluster.servers {
		survivingAddr = addr
		break
	}

	require.Eventually(t, func() bool {
		client, err = oxia.NewSyncClient(survivingAddr.Public)
		if err != nil {
			return false
		}
		_, _, _, err = client.Get(ctx, "key-0000")
		if err != nil {
			_ = client.Close()
			return false
		}
		return true
	}, 30*time.Second, 500*time.Millisecond)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		_, value, _, err := client.Get(ctx, key)
		assert.NoError(t, err, "key %s not found", key)
		assert.Equal(t, []byte(fmt.Sprintf("value-%d", i)), value)
	}
	assert.NoError(t, client.Close())
}

func TestCoordinator_ShardSplit_ConcurrentSplitRejected(t *testing.T) {
	cluster := setupSplitCluster(t)
	defer cluster.close(t)

	// Initiate first split
	leftChild, rightChild, err := cluster.coordinator.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)
	cluster.leftChild = leftChild
	cluster.rightChild = rightChild
	slog.Info("First split initiated", slog.Int64("left", leftChild), slog.Int64("right", rightChild))

	// Immediately try a second split on the same parent — should be rejected
	_, _, err = cluster.coordinator.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already has an active split")
	slog.Info("Second split correctly rejected", slog.Any("error", err))

	// First split should still complete
	require.Eventually(t, func() bool {
		st := cluster.statusResource.Load()
		ns := st.Namespaces[constant.DefaultNamespace]
		if _, parentExists := ns.Shards[0]; parentExists {
			return false
		}
		for _, child := range []int64{leftChild, rightChild} {
			sm, ok := ns.Shards[child]
			if !ok || sm.Status != model.ShardStatusSteadyState || sm.Leader == nil || sm.Split != nil {
				return false
			}
		}
		return true
	}, 2*time.Minute, 500*time.Millisecond)
	slog.Info("First split completed successfully")
}
