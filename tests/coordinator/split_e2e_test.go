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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpcmetadata "google.golang.org/grpc/metadata"

	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/common/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/hash"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxia"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	commonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	manifestpkg "github.com/oxia-db/oxia/oxiad/dataserver/manifest"
	dataserverrpc "github.com/oxia-db/oxia/oxiad/dataserver/rpc"
	"github.com/oxia-db/oxia/tests/mock"
)

func TestCoordinator_ShardSplit(t *testing.T) {
	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}

	metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              constant.DefaultNamespace,
		ReplicationFactor: 3,
		InitialShardCount: 1,
	}}, []*proto.DataServerIdentity{sa1, sa2, sa3})

	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   clusterConfig,
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))
	clientPool := rpc.NewClientPool(nil, nil)

	metadata := coordinatorInstance.Metadata()

	// Wait for initial shard to be in steady state
	require.Eventually(t, func() bool {
		shard := mock.StatusSnapshot(t, metadata).Namespaces[constant.DefaultNamespace].Shards[0]
		return shard.GetStatusOrDefault() == proto.ShardStatusSteadyState
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
		status := mock.StatusSnapshot(t, metadata)
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
		if left.GetStatusOrDefault() != proto.ShardStatusSteadyState {
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
		if right.GetStatusOrDefault() != proto.ShardStatusSteadyState {
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
	}, 3*time.Minute, 500*time.Millisecond)

	slog.Info("Split complete")

	// Verify hash ranges: children should cover the entire original range
	status := mock.StatusSnapshot(t, metadata)
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

// TestCoordinator_ShardSplit_WritesDuringSplit exercises the cutover tail: a
// client writes continuously to the parent throughout the whole split. The
// parent's head keeps advancing right up to cutover, so there is always a tail
// of entries the observer cursors have not yet delivered to the children when
// the parent is quiesced. The freeze-then-fence cutover must drain that tail
// (freeze stops writes but keeps observers streaming) before fencing the
// parent — otherwise the children could never reach the parent's final offset
// and the split would hang, or acknowledged writes would be lost.
//
// The children must also have applied the tail by then: they apply the
// parent's entries with the split filter only while they observe the parent,
// and a child leader replays the entries it did not apply yet without it. A
// child would then hold the records of the other one.
func TestCoordinator_ShardSplit_WritesDuringSplit(t *testing.T) {
	// The children apply an entry once the parent advertises a commit offset
	// that covers it. Hold the advertisement back, unless a new entry carries
	// it: once the parent is frozen, the children receive its tail long before
	// they can apply it.
	lead.SetObserverCommitAdvertisementDelay(2 * time.Second)
	defer lead.SetObserverCommitAdvertisementDelay(0)

	c := setupSplitCluster(t)
	defer c.close(t)

	ctx := context.Background()

	client, err := oxia.NewSyncClient(c.sa1.Public)
	require.NoError(t, err)

	// Seed some initial data so both children start non-empty.
	initialKeys := make(map[string][]byte)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("seed-%04d", i)
		value := []byte(fmt.Sprintf("seed-value-%04d", i))
		_, _, err := client.Put(ctx, key, value)
		require.NoError(t, err)
		initialKeys[key] = value
	}

	// A partition key on each side of the split point, the midpoint of the
	// hash range
	var leftPk, rightPk string
	for i := 0; leftPk == "" || rightPk == ""; i++ {
		pk := fmt.Sprintf("pk-%d", i)
		if h := hash.Xxh332(pk); h < math.MaxUint32/4 {
			leftPk = pk
		} else if h > math.MaxUint32/4*3 {
			rightPk = pk
		}
	}

	// Background writer: keep writing new keys for the entire duration of the
	// split, recording only the writes the server acknowledged. Besides the
	// records routed by their key, it writes records under each partition key,
	// both with a secondary index, and records of a sequence.
	writerClient, err := oxia.NewSyncClient(c.sa1.Public)
	require.NoError(t, err)

	var (
		mu      sync.Mutex
		written = make(map[string][]byte)
		stop    atomic.Bool
		wg      sync.WaitGroup
	)
	put := func(key string, value []byte, opts ...oxia.PutOption) {
		putCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		// The key of a record of a sequence is the one the server assigned
		key, _, putErr := writerClient.Put(putCtx, key, value, opts...)
		if putErr == nil {
			mu.Lock()
			written[key] = value
			mu.Unlock()
		}
	}
	wg.Go(func() {
		for i := 0; !stop.Load(); i++ {
			value := []byte(fmt.Sprintf("live-value-%06d", i))
			put(fmt.Sprintf("live-%06d", i), value, oxia.SecondaryIndex("live", fmt.Sprintf("%06d", i)))
			for _, pk := range []string{leftPk, rightPk} {
				put(fmt.Sprintf("%s/rec-%06d", pk, i), value, oxia.PartitionKey(pk), oxia.SecondaryIndex("pk", pk))
				put(pk+"/seq", value, oxia.PartitionKey(pk), oxia.SequenceKeysDeltas(1))
			}
			time.Sleep(2 * time.Millisecond)
		}
	})

	// Run the split while writes are in flight, then stop the writer.
	c.splitAndWait(t)
	stop.Store(true)
	wg.Wait()
	assert.NoError(t, writerClient.Close())

	mu.Lock()
	liveCount := len(written)
	mu.Unlock()
	require.Positive(t, liveCount, "expected some acknowledged writes during the split")
	slog.Info("Split completed with concurrent writes",
		slog.Int("seed-keys", len(initialKeys)),
		slog.Int("live-keys-acked", liveCount),
	)

	// Reconnect to pick up the post-split assignments, then verify EVERY
	// acknowledged write (seed + live) is readable from the children with the
	// correct value. A lost cutover tail would surface here as a missing key.
	client = c.reconnectClient(t, client, "seed-0000")

	for key, expected := range initialKeys {
		_, value, _, err := client.Get(ctx, key)
		if assert.NoError(t, err, "seed key %s missing after split", key) {
			assert.Equal(t, expected, value, "seed value mismatch for %s", key)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for key, expected := range written {
		_, value, _, err := client.Get(ctx, key, oxia.PartitionKey(splitPartitionKey(key)))
		if assert.NoError(t, err, "acknowledged live key %s missing after split", key) {
			assert.Equal(t, expected, value, "live value mismatch for %s", key)
		}
	}

	slog.Info("All acknowledged writes survived the split", slog.Int("verified", len(written)+len(initialKeys)))
	assert.NoError(t, client.Close())

	c.assertChildrenHoldOnlyTheirRecords(t)
}

// splitPartitionKey returns the partition key of a record written by the split
// tests: the part of the key before the first "/", if any, or else the key.
func splitPartitionKey(key string) string {
	partitionKey, _, _ := strings.Cut(key, "/")
	return partitionKey
}

// assertChildrenHoldOnlyTheirRecords checks that each child holds only the
// records in its hash range, with their secondary index entries, and that no
// key is in both children, like the key of a record of a sequence of the other
// child.
func (c *splitTestCluster) assertChildrenHoldOnlyTheirRecords(t *testing.T) {
	t.Helper()
	childOfKey := make(map[string]int64)
	for child, childMeta := range map[int64]*proto.ShardMetadata{c.leftChild: c.leftMeta, c.rightChild: c.rightMeta} {
		for _, key := range c.listShardKeys(t, child) {
			record := key
			if strings.HasPrefix(key, constant.InternalKeyPrefix) {
				primaryKey, _, err := database.ParseSecondaryIndexKey(key)
				if err != nil {
					// Not a secondary index entry
					continue
				}
				record = primaryKey
			} else {
				if other, found := childOfKey[key]; found {
					assert.Fail(t, "key in both children", "key %q is in shard %d and shard %d", key, other, child)
				}
				childOfKey[key] = child
			}

			hashRange := childMeta.Int32HashRange
			partitionKey := splitPartitionKey(record)
			h := hash.Xxh332(partitionKey)
			assert.True(t, h >= hashRange.Min && h <= hashRange.Max,
				"shard %d [%d, %d] holds %q, of partition key %q (hash %d)",
				child, hashRange.Min, hashRange.Max, key, partitionKey, h)
		}
	}
}

// listShardKeys lists all the keys of a shard on its leader, the internal keys
// included.
func (c *splitTestCluster) listShardKeys(t *testing.T, shard int64) []string {
	t.Helper()
	clientPool := rpc.NewClientPool(nil, nil)
	defer func() { assert.NoError(t, clientPool.Close()) }()

	rpcClient, err := clientPool.GetClientRpc(c.shardStatus(t, shard).Leader.Public)
	require.NoError(t, err)
	stream, err := rpcClient.List(context.Background(), &proto.ListRequest{
		Shard:               &shard,
		IncludeInternalKeys: true,
	})
	require.NoError(t, err)

	var keys []string
	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return keys
		}
		require.NoError(t, err, "list the keys of shard %d", shard)
		keys = append(keys, res.Keys...)
	}
}

// TestCoordinator_ShardSplit_WriteOrder checks that the puts a client issues
// to the same key are applied in the order they were issued, across a split.
// The async client pipelines the puts: when the split cuts over, some of them
// are still queued or in flight to the parent and get rerouted to the
// children, while the puts issued after the client received the post-split
// assignments go to the children directly.
func TestCoordinator_ShardSplit_WriteOrder(t *testing.T) {
	c := setupSplitCluster(t)
	defer c.close(t)

	client, err := oxia.NewAsyncClient(c.sa1.Public)
	require.NoError(t, err)

	type putOp struct {
		key    string
		seq    int
		result <-chan oxia.PutResult
	}
	const numKeys = 16
	// Large enough not to throttle the writer: it must keep issuing puts
	// while the rerouted ones are still pending
	const maxOutstanding = 64 * 1024

	var stop atomic.Bool
	ops := make(chan putOp, maxOutstanding)
	go func() {
		defer close(ops)
		for seq := 0; !stop.Load(); seq++ {
			key := fmt.Sprintf("key-%02d", seq%numKeys)
			ops <- putOp{key, seq, client.Put(key, []byte(fmt.Sprintf("%d", seq)))}
		}
	}()

	// Among the successful puts to a key, taken in issue order, the
	// modifications count reports the order in which they were applied
	type applied struct {
		seq   int
		count int64
	}
	var (
		succeeded, failed int
		firstErr          error
		outOfOrder        []string
		lastApplied       = make(map[string]applied)
		checked           = make(chan struct{})
	)
	go func() {
		defer close(checked)
		for op := range ops {
			result := <-op.result
			if result.Err != nil {
				if failed == 0 {
					firstErr = result.Err
				}
				failed++
				continue
			}
			succeeded++
			if last, ok := lastApplied[op.key]; ok && result.Version.ModificationsCount <= last.count {
				outOfOrder = append(outOfOrder, fmt.Sprintf("%s: put #%d applied as modification %d, before put #%d (modification %d)",
					op.key, op.seq, result.Version.ModificationsCount, last.seq, last.count))
			}
			lastApplied[op.key] = applied{op.seq, result.Version.ModificationsCount}
		}
	}()

	c.splitAndWait(t)
	// Keep writing to the children for a while
	time.Sleep(time.Second)
	stop.Store(true)
	<-checked

	slog.Info("Checked the order of the puts across the split",
		slog.Int("succeeded", succeeded),
		slog.Int("failed", failed),
		slog.Any("first-error", firstErr),
		slog.Int("out-of-order", len(outOfOrder)),
	)
	assert.Positive(t, succeeded)
	assert.Empty(t, outOfOrder, "puts applied out of the order they were issued")
	assert.NoError(t, client.Close())
}

// TestCoordinator_ShardSplit_VersionIDs checks that the records written to the
// parent while the split is in progress keep, on the children, the version the
// parent returned to the writer, although each child applies only part of the
// parent's puts. The writer updates every record it creates with a conditional
// write on the version it got back, as a read-modify-write client does; after
// the split, reads, conditional writes and the notifications of the children
// must all see the versions the writer saw.
func TestCoordinator_ShardSplit_VersionIDs(t *testing.T) {
	c := setupSplitCluster(t)
	defer c.close(t)

	ctx := context.Background()

	client, err := oxia.NewSyncClient(c.sa1.Public)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		_, _, err := client.Put(ctx, fmt.Sprintf("seed-%04d", i), []byte("seed"))
		require.NoError(t, err)
	}

	writerClient, err := oxia.NewSyncClient(c.sa1.Public)
	require.NoError(t, err)

	var (
		mu sync.Mutex
		// The versions of the acknowledged writes of each record, in order
		versions = make(map[string][]oxia.Version)
		// The conditional writes rejected although they used the version
		// that the previous write of the record returned
		rejected []string
		stop     atomic.Bool
		wg       sync.WaitGroup
	)
	wg.Go(func() {
		for i := 0; !stop.Load(); i++ {
			key := fmt.Sprintf("live-%06d", i)
			putCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			_, created, err := writerClient.Put(putCtx, key, []byte("created"))
			if err == nil {
				_, updated, updateErr := writerClient.Put(putCtx, key, []byte("updated"),
					oxia.ExpectedVersionId(created.VersionId))
				mu.Lock()
				switch {
				case updateErr == nil:
					versions[key] = []oxia.Version{created, updated}
				case errors.Is(updateErr, oxia.ErrUnexpectedVersionId):
					rejected = append(rejected, fmt.Sprintf("%s: version %d", key, created.VersionId))
				default:
					// The update may or may not have been applied: the record
					// is not checked
				}
				mu.Unlock()
			}
			cancel()
			time.Sleep(2 * time.Millisecond)
		}
	})

	c.splitAndWait(t)
	stop.Store(true)
	wg.Wait()
	assert.NoError(t, writerClient.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Positive(t, len(versions), "expected some acknowledged writes during the split")
	assert.Empty(t, rejected, "conditional writes rejected with the version the previous write returned")

	client = c.reconnectClient(t, client, "seed-0000")
	defer func() { assert.NoError(t, client.Close()) }()

	for key, acked := range versions {
		_, value, version, err := client.Get(ctx, key)
		if assert.NoError(t, err, "record %s", key) {
			last := acked[len(acked)-1]
			assert.Equal(t, "updated", string(value), "value of %s", key)
			assert.Equal(t, last.VersionId, version.VersionId, "version id of %s", key)
			assert.Equal(t, last.ModificationsCount, version.ModificationsCount, "modifications count of %s", key)
		}
	}

	for _, childId := range []int64{c.leftChild, c.rightChild} {
		c.assertNotificationVersions(t, childId, versions)
	}

	for key, acked := range versions {
		_, _, err := client.Put(ctx, key, []byte("after-split"), oxia.ExpectedVersionId(acked[len(acked)-1].VersionId))
		assert.NoError(t, err, "conditional write on %s after the split", key)
	}
}

// assertNotificationVersions checks that the notifications of a child carry,
// for each of the records in its hash range, the versions of its writes.
func (c *splitTestCluster) assertNotificationVersions(t *testing.T, childId int64, versions map[string][]oxia.Version) {
	t.Helper()

	childMeta := c.shardStatus(t, childId)
	expected := make(map[string][]int64)
	for key, acked := range versions {
		h := hash.Xxh332(key)
		if h >= childMeta.Int32HashRange.Min && h <= childMeta.Int32HashRange.Max {
			for _, version := range acked {
				expected[key] = append(expected[key], version.VersionId)
			}
		}
	}

	clientPool := rpc.NewClientPool(nil, nil)
	defer func() { assert.NoError(t, clientPool.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	received := make(map[string][]int64)
	pending := len(expected)
	// All the retained notifications, from those of the parent in the snapshot
	// the child was seeded with
	lastOffset := int64(-1)
	for pending > 0 {
		// The leader of the child can move: subscribe again, to the current
		// leader, from where the previous subscription got to
		require.NoError(t, ctx.Err(), "notifications of child %d: %d records still missing", childId, pending)
		leader := c.shardStatus(t, childId).GetLeader()
		if leader == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		rpcClient, err := clientPool.GetClientRpc(leader.Public)
		require.NoError(t, err)
		stream, err := rpcClient.GetNotifications(ctx, &proto.NotificationsRequest{
			Shard:                childId,
			StartOffsetExclusive: &lastOffset,
		})
		require.NoError(t, err)

		for pending > 0 {
			batch, err := stream.Recv()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				break
			}
			lastOffset = batch.Offset
			for _, n := range batch.Notifications {
				want, ok := expected[n.GetKey()]
				if !ok {
					continue
				}
				received[n.GetKey()] = append(received[n.GetKey()], n.GetValue().GetVersionId())
				if len(received[n.GetKey()]) == len(want) {
					pending--
				}
			}
		}
	}
	for key, want := range expected {
		assert.Equal(t, want, received[key], "version ids in the notifications of %s", key)
	}
}

// splitTestCluster holds references to a 3-node test cluster with a
// coordinator, used by the shard-split integration tests.
type splitTestCluster struct {
	servers             map[string]*dataserver.Server
	addresses           []*proto.DataServerIdentity
	sa1                 *proto.DataServerIdentity
	coordinator         coordruntime.Runtime
	metadata            coordmetadata.Metadata
	leftChild           int64
	rightChild          int64
	leftMeta, rightMeta *proto.ShardMetadata
}

// setupSplitCluster creates a 3-node cluster, waits for the initial shard to
// be ready, and returns the cluster handle. Callers must call close() when done.
func setupSplitCluster(t *testing.T) *splitTestCluster {
	t.Helper()
	return setupSplitClusterWithRpc(t, rpc2.NewRpcProviderFactory(nil))
}

// setupSplitClusterWithRpc is setupSplitCluster, with a coordinator that uses
// the given rpc provider factory.
func setupSplitClusterWithRpc(t *testing.T, rpcProviderFactory rpc2.ProviderFactory) *splitTestCluster {
	t.Helper()
	return setupSplitClusterWithRpcAndShardCount(t, rpcProviderFactory, 1)
}

// setupSplitClusterWithShardCount is setupSplitCluster, with a namespace of
// initialShardCount shards.
func setupSplitClusterWithShardCount(t *testing.T, initialShardCount uint32) *splitTestCluster {
	t.Helper()
	return setupSplitClusterWithRpcAndShardCount(t, rpc2.NewRpcProviderFactory(nil), initialShardCount)
}

func setupSplitClusterWithRpcAndShardCount(t *testing.T, rpcProviderFactory rpc2.ProviderFactory,
	initialShardCount uint32) *splitTestCluster {
	t.Helper()

	s1, sa1 := newServer(t)
	s2, sa2 := newServer(t)
	s3, sa3 := newServer(t)
	servers := map[string]*dataserver.Server{
		sa1.GetNameOrDefault(): s1,
		sa2.GetNameOrDefault(): s2,
		sa3.GetNameOrDefault(): s3,
	}
	return setupSplitClusterWithShards(t, rpcProviderFactory, servers, []*proto.DataServerIdentity{sa1, sa2, sa3},
		initialShardCount)
}

// setupSplitClusterWith is setupSplitClusterWithRpc, on the given data servers.
func setupSplitClusterWith(t *testing.T, rpcProviderFactory rpc2.ProviderFactory,
	servers map[string]*dataserver.Server, addresses []*proto.DataServerIdentity) *splitTestCluster {
	t.Helper()
	return setupSplitClusterWithShards(t, rpcProviderFactory, servers, addresses, 1)
}

// setupSplitClusterWithShards is setupSplitClusterWith, with a namespace of
// initialShardCount shards.
func setupSplitClusterWithShards(t *testing.T, rpcProviderFactory rpc2.ProviderFactory,
	servers map[string]*dataserver.Server, addresses []*proto.DataServerIdentity,
	initialShardCount uint32) *splitTestCluster {
	t.Helper()

	metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
	clusterConfig := newClusterConfig([]*proto.Namespace{{
		Name:              constant.DefaultNamespace,
		ReplicationFactor: 3,
		InitialShardCount: initialShardCount,
	}}, addresses)
	configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   clusterConfig,
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)

	coordinatorInstance := newCoordinatorInstance(
		t,
		metadataProvider,
		configProvider,
		rpcProviderFactory,
	)

	metadata := coordinatorInstance.Metadata()
	require.Eventually(t, func() bool {
		shards := mock.StatusSnapshot(t, metadata).Namespaces[constant.DefaultNamespace].Shards
		if len(shards) != int(initialShardCount) {
			return false
		}
		for _, shard := range shards {
			if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)
	slog.Info("Initial cluster is ready")

	return &splitTestCluster{
		servers:     servers,
		addresses:   addresses,
		sa1:         addresses[0],
		coordinator: coordinatorInstance,
		metadata:    metadata,
	}
}

// splitAndWait triggers a shard split on shard 0 and waits for both children
// to reach steady state with leaders and no split metadata.
func (c *splitTestCluster) splitAndWait(t *testing.T) {
	t.Helper()
	c.initiateSplit(t)
	c.waitForSplit(t)
}

// initiateSplit triggers a shard split on shard 0.
func (c *splitTestCluster) initiateSplit(t *testing.T) {
	t.Helper()

	var err error
	c.leftChild, c.rightChild, err = c.coordinator.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)
	slog.Info("Split initiated",
		slog.Int64("left-child", c.leftChild),
		slog.Int64("right-child", c.rightChild),
	)
}

// waitForSplit waits for both children of the split to reach steady state with
// leaders and no split metadata.
func (c *splitTestCluster) waitForSplit(t *testing.T) {
	t.Helper()

	require.Eventually(t, func() bool {
		status := mock.StatusSnapshot(t, c.metadata)
		ns := status.Namespaces[constant.DefaultNamespace]
		if _, parentExists := ns.Shards[0]; parentExists {
			return false
		}
		for _, child := range []int64{c.leftChild, c.rightChild} {
			sm, ok := ns.Shards[child]
			if !ok || sm.GetStatusOrDefault() != proto.ShardStatusSteadyState || sm.Leader == nil || sm.Split != nil {
				return false
			}
		}
		return true
	}, 60*time.Second, 500*time.Millisecond)

	status := mock.StatusSnapshot(t, c.metadata)
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
	for _, s := range c.servers {
		assert.NoError(t, s.Close())
	}
}

func (c *splitTestCluster) liveAddressExcluding(excludedIDs ...string) *proto.DataServerIdentity {
	excluded := make(map[string]struct{}, len(excludedIDs))
	for _, id := range excludedIDs {
		excluded[id] = struct{}{}
	}
	for _, addr := range c.addresses {
		id := addr.GetNameOrDefault()
		if _, skip := excluded[id]; skip {
			continue
		}
		if _, ok := c.servers[id]; ok {
			return addr
		}
	}
	return nil
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

// notificationEvent is what a notification reports: the record, its change,
// and the version the change produced, or -1 for a deletion.
type notificationEvent struct {
	key       string
	eventType oxia.NotificationType
	versionId int64
}

// TestCoordinator_ShardSplit_SubscriptionFollowsSplit checks that a
// subscription created before a split delivers every notification once across
// it: the notifications of the parent that it hasn't read when the split
// completes, which both children inherited, and those of the writes to each
// child afterwards.
func TestCoordinator_ShardSplit_SubscriptionFollowsSplit(t *testing.T) {
	c := setupSplitCluster(t)
	defer c.close(t)

	checkSubscriptionFollowsSplit(t, c, nil)
}

// TestCoordinator_ShardSplit_SubscriptionFollowsSplitPartitionKey is
// TestCoordinator_ShardSplit_SubscriptionFollowsSplit in a namespace of two
// shards, where the records written to the split shard are placed by a
// partition key: their keys hash anywhere, inside the range of the shard and
// outside of it. Each child delivers the notifications it inherited whose key
// hash is on its side of the split, the ones outside the range of the parent
// included: the child that reaches neither bound of the hash space tells its
// side from the range of the parent.
func TestCoordinator_ShardSplit_SubscriptionFollowsSplitPartitionKey(t *testing.T) {
	c := setupSplitClusterWithShardCount(t, 2)
	defer c.close(t)

	parentRange := c.shardStatus(t, 0).Int32HashRange
	var partitionKey string
	for i := 0; ; i++ {
		partitionKey = fmt.Sprintf("partition-key-%d", i)
		if h := hash.Xxh332(partitionKey); h >= parentRange.Min && h <= parentRange.Max {
			break
		}
	}
	checkSubscriptionFollowsSplit(t, c, &partitionKey)
}

// checkSubscriptionFollowsSplit splits shard 0 with a subscription created
// before, and checks that it delivers the notification of every write once:
// before the split, while it doesn't read them, during the split, and to both
// children afterwards. The records written to shard 0 before the split are
// placed by partitionKey, if not nil.
func checkSubscriptionFollowsSplit(t *testing.T, c *splitTestCluster, partitionKey *string) {
	t.Helper()
	ctx := context.Background()
	client, err := oxia.NewSyncClient(c.sa1.Public)
	require.NoError(t, err)
	defer func() { assert.NoError(t, client.Close()) }()

	notifications, err := client.GetNotifications()
	require.NoError(t, err)
	defer func() { assert.NoError(t, notifications.Close()) }()

	var (
		mu sync.Mutex
		// The notifications of the acknowledged writes
		expected []notificationEvent
	)
	record := func(event notificationEvent) {
		mu.Lock()
		defer mu.Unlock()
		expected = append(expected, event)
	}
	// put and del write the record at key, placed by partitionKey if
	// partitioned, and record the notification of the write
	put := func(key string, partitioned bool) bool {
		var options []oxia.PutOption
		if partitioned && partitionKey != nil {
			options = append(options, oxia.PartitionKey(*partitionKey))
		}
		putCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		_, version, err := client.Put(putCtx, key, []byte(key), options...)
		if err != nil {
			return false
		}
		eventType := oxia.KeyModified
		if version.ModificationsCount == 0 {
			eventType = oxia.KeyCreated
		}
		record(notificationEvent{key: key, eventType: eventType, versionId: version.VersionId})
		return true
	}
	del := func(key string, partitioned bool) {
		var options []oxia.DeleteOption
		if partitioned && partitionKey != nil {
			options = append(options, oxia.PartitionKey(*partitionKey))
		}
		delCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := client.Delete(delCtx, key, options...); err == nil {
			record(notificationEvent{key: key, eventType: oxia.KeyDeleted, versionId: -1})
		}
	}

	received := make(map[notificationEvent]int)
	allReceived := func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, event := range expected {
			if received[event] == 0 {
				return false
			}
		}
		return true
	}
	// receive reads the notifications until it got all the expected ones, and
	// then for as long as quiet, to catch duplicates
	receive := func(timeout time.Duration, quiet time.Duration) {
		deadline := time.After(timeout)
		var quietCh <-chan time.Time
		for {
			if quietCh == nil && allReceived() {
				if quiet == 0 {
					return
				}
				quietCh = time.After(quiet)
			}
			select {
			case n := <-notifications.Ch():
				require.NotNil(t, n, "the subscription was closed")
				received[notificationEvent{key: n.Key, eventType: n.Type, versionId: n.VersionId}]++
			case <-quietCh:
				return
			case <-deadline:
				return
			}
		}
	}

	// The subscription reads the notifications of these writes from the parent
	for i := 0; i < 20; i++ {
		require.True(t, put(fmt.Sprintf("before-%03d", i), true))
	}
	receive(30*time.Second, 0)
	require.True(t, allReceived(), "notifications before the split")

	// The subscription stops reading: the notifications of these writes fill
	// its channel, and the flow-control window of its stream, with their long
	// keys, so that the rest of them are still on the parent when the split
	// completes, along with those of the writes during the split
	padding := strings.Repeat("x", 1000)
	for i := 0; i < 300; i++ {
		require.True(t, put(fmt.Sprintf("unread-%03d-%s", i, padding), true))
	}
	var (
		stop atomic.Bool
		wg   sync.WaitGroup
	)
	wg.Go(func() {
		for i := 0; !stop.Load(); i++ {
			key := fmt.Sprintf("during-%05d", i)
			if put(key, true) && i%3 == 0 && put(key, true) && i%2 == 0 {
				del(key, true)
			}
			time.Sleep(2 * time.Millisecond)
		}
	})
	c.splitAndWait(t)
	stop.Store(true)
	wg.Wait()

	// Writes to both children, of new records and of records of the parent
	for i := 0; i < 30; i++ {
		for _, child := range []int64{c.leftChild, c.rightChild} {
			key := c.childKey(child, fmt.Sprintf("after-%03d", i))
			require.True(t, put(key, false))
			if i%3 == 0 {
				require.True(t, put(key, false))
			}
			if i%4 == 0 {
				del(key, false)
			}
		}
	}
	for i := 0; i < 20; i += 2 {
		require.True(t, put(fmt.Sprintf("before-%03d", i), true))
		del(fmt.Sprintf("before-%03d", i+1), true)
	}

	receive(60*time.Second, 2*time.Second)

	mu.Lock()
	defer mu.Unlock()
	var missing, duplicated []string
	for _, event := range expected {
		if received[event] == 0 {
			missing = append(missing, fmt.Sprintf("%s %v %d", strings.TrimSuffix(event.key, padding),
				event.eventType, event.versionId))
		}
	}
	for event, count := range received {
		if count > 1 {
			duplicated = append(duplicated, fmt.Sprintf("%s %v %d: %d times", strings.TrimSuffix(event.key, padding),
				event.eventType, event.versionId, count))
		}
	}
	slices.Sort(missing)
	slices.Sort(duplicated)
	t.Logf("%d notifications expected, %d received, %d missing, %d duplicated",
		len(expected), len(received), len(missing), len(duplicated))
	assert.Empty(t, missing, "notifications not received")
	assert.Empty(t, duplicated, "notifications received more than once")
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

	// Ephemeral records written with a partition key are placed by it,
	// whatever the hash of their keys, and their session shadow keys, which
	// delete them with the session, must follow them.
	partitionKey := "pk-7"
	var partitionedEphemeralKeys []string
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("%s/ephemeral-%06d", partitionKey, i)
		_, _, err := ephemeralClient.Put(ctx, key, []byte(key), oxia.Ephemeral(), oxia.PartitionKey(partitionKey))
		require.NoError(t, err)
		partitionedEphemeralKeys = append(partitionedEphemeralKeys, key)
	}

	// Perform split
	cluster.splitAndWait(t)

	require.Eventually(t, func() bool {
		_, _, _, err := ephemeralClient.Get(ctx, "regular-0000")
		return err == nil
	}, 30*time.Second, 500*time.Millisecond)

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
	for _, key := range partitionedEphemeralKeys {
		_, _, version, err := ephemeralClient.Get(ctx, key, oxia.PartitionKey(partitionKey))
		if assert.NoError(t, err, "ephemeral key %s should still exist after split", key) {
			assert.True(t, version.Ephemeral, "key %s should still be ephemeral after split", key)
		}
	}
	slog.Info("Ephemeral keys verified after split")

	// Verify all regular keys survived the split
	for i := 0; i < numRegular; i++ {
		key := fmt.Sprintf("regular-%04d", i)
		_, _, version, err := ephemeralClient.Get(ctx, key)
		assert.NoError(t, err, "regular key %s should still exist", key)
		assert.False(t, version.Ephemeral, "regular key %s should not be ephemeral", key)
	}
	slog.Info("Regular keys verified after split")

	// Write new ephemeral records to child shards
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("post-split-eph-%04d", i)
		_, version, err := ephemeralClient.Put(ctx, key, []byte("post"), oxia.Ephemeral())
		assert.NoError(t, err, "post-split ephemeral put should succeed")
		assert.True(t, version.Ephemeral)
	}
	slog.Info("Post-split ephemeral writes verified")

	// Close the ephemeral client: it closes its session on both child shards,
	// which deletes all its ephemeral records. Verify using a separate
	// non-ephemeral client.
	assert.NoError(t, ephemeralClient.Close())

	readerClient, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)
	defer func() { assert.NoError(t, readerClient.Close()) }()

	require.Eventually(t, func() bool {
		for key := range ephemeralKeys {
			_, _, _, err := readerClient.Get(ctx, key)
			if err == nil {
				return false // still exists
			}
		}
		for _, key := range partitionedEphemeralKeys {
			_, _, _, err := readerClient.Get(ctx, key, oxia.PartitionKey(partitionKey))
			if err == nil {
				return false // still exists
			}
		}
		return true
	}, 10*time.Second, 500*time.Millisecond, "ephemeral records should be deleted after client close")

	slog.Info("Ephemeral records cleaned up after client close")

	// Regular keys should still exist
	for i := 0; i < numRegular; i++ {
		key := fmt.Sprintf("regular-%04d", i)
		_, _, _, err := readerClient.Get(ctx, key)
		assert.NoError(t, err, "regular key %s should survive ephemeral client close", key)
	}

	slog.Info("Ephemeral records test passed")
}

// Both children of a split inherit the sessions of the parent, with the
// ephemeral records of each session that fall in their hash range. The records
// must live as long as the client that wrote them, and go away when it closes.
func TestCoordinator_ShardSplit_InheritedSessionKeptAlive(t *testing.T) {
	cluster := setupSplitCluster(t)
	defer cluster.close(t)

	ctx := context.Background()
	sessionTimeout := 10 * time.Second
	client, err := oxia.NewSyncClient(cluster.sa1.Public, oxia.WithSessionTimeout(sessionTimeout))
	require.NoError(t, err)

	var keys []string
	var sessionId int64
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("ephemeral-%04d", i)
		_, version, err := client.Put(ctx, key, []byte("value"), oxia.Ephemeral())
		require.NoError(t, err)
		keys = append(keys, key)
		sessionId = version.SessionId
	}

	cluster.splitAndWait(t)
	keysPerChild := map[int64]int{}
	for _, key := range keys {
		if hash.Xxh332(key) <= cluster.leftMeta.Int32HashRange.Max {
			keysPerChild[cluster.leftChild]++
		} else {
			keysPerChild[cluster.rightChild]++
		}
	}
	require.Len(t, keysPerChild, 2, "the records must be in both children")

	// The children restart the timeout of the sessions they inherit when they
	// are elected, before the split completes
	time.Sleep(2 * sessionTimeout)

	for _, key := range keys {
		_, _, version, err := client.Get(ctx, key)
		if assert.NoError(t, err, "ephemeral record %s lost while its client is alive", key) {
			assert.True(t, version.Ephemeral)
			assert.Equal(t, sessionId, version.SessionId)
		}
	}

	// The new ephemeral records of the client on the children belong to the
	// session they inherited
	for _, child := range []int64{cluster.leftChild, cluster.rightChild} {
		key := cluster.childKey(child, "post-split")
		_, version, err := client.Put(ctx, key, []byte("value"), oxia.Ephemeral())
		require.NoError(t, err)
		assert.Equal(t, sessionId, version.SessionId)
		keys = append(keys, key)
	}

	require.NoError(t, client.Close())

	reader, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()

	// Closing the client deletes them, without waiting for the session to expire
	assert.Eventually(t, func() bool {
		for _, key := range keys {
			if _, _, _, err := reader.Get(ctx, key); !errors.Is(err, oxia.ErrKeyNotFound) {
				return false
			}
		}
		return true
	}, sessionTimeout/2, 100*time.Millisecond, "ephemeral records left after the client closed")
}

// fenceHookRpcProvider calls afterFence once a data server is fenced in a new
// term of a shard. The new term waits for it to return.
type fenceHookRpcProvider struct {
	rpc2.Provider
	afterFence func(req *proto.NewTermRequest)
}

func (p *fenceHookRpcProvider) factory(instanceID string) rpc2.Provider {
	p.Provider = rpc2.NewRpcProvider(nil, instanceID)
	return p
}

func (p *fenceHookRpcProvider) NewTerm(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	response, err := p.Provider.NewTerm(ctx, node, req)
	if err == nil {
		p.afterFence(req)
	}
	return response, err
}

// The Finalize of a split fences the parent, then it re-elects the children and
// publishes them: meanwhile, the clients keep their sessions alive on the
// parent. The fenced parent must reject those keep-alives like a node that is
// not the leader, which the client retries until it moves the session to the
// children. If it answers that the session doesn't exist, the client gives up
// the session, and the children expire it, with its ephemeral records.
func TestCoordinator_ShardSplit_KeepAliveOnFencedParent(t *testing.T) {
	// Once the split started, the only new term of the parent is the fence of
	// the Finalize: hold it for longer than the client waits between two
	// keep-alives, 2s at least
	var splitting atomic.Bool
	holdFinalize := sync.OnceFunc(func() { time.Sleep(3 * time.Second) })
	rpcProvider := &fenceHookRpcProvider{afterFence: func(req *proto.NewTermRequest) {
		if splitting.Load() && req.Shard == 0 {
			holdFinalize()
		}
	}}
	c := setupSplitClusterWithRpc(t, rpcProvider.factory)
	defer c.close(t)

	ctx := context.Background()
	sessionTimeout := 5 * time.Second
	client, err := oxia.NewSyncClient(c.sa1.Public, oxia.WithSessionTimeout(sessionTimeout))
	require.NoError(t, err)
	defer func() { assert.NoError(t, client.Close()) }()

	var keys []string
	var sessionId int64
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("ephemeral-%04d", i)
		_, version, err := client.Put(ctx, key, []byte("value"), oxia.Ephemeral())
		require.NoError(t, err)
		keys = append(keys, key)
		sessionId = version.SessionId
	}

	splitting.Store(true)
	c.splitAndWait(t)

	// The client kept the session: its new ephemeral records belong to it
	for _, child := range []int64{c.leftChild, c.rightChild} {
		_, version, err := client.Put(ctx, c.childKey(child, "post-split"), []byte("value"), oxia.Ephemeral())
		require.NoError(t, err)
		assert.Equal(t, sessionId, version.SessionId, "new session on shard %d", child)
	}

	// The children restart the timeout of the session when they are re-elected
	time.Sleep(2 * sessionTimeout)

	for _, key := range keys {
		_, _, version, err := client.Get(ctx, key)
		if assert.NoError(t, err, "ephemeral record %s lost while its client is alive", key) {
			assert.Equal(t, sessionId, version.SessionId)
		}
	}
}

// TestCoordinator_ShardSplit_SessionCreatedDuringCatchUp checks that a session
// created on the parent while the children catch up from its log reaches both
// children, like the sessions in the parent's snapshot. A child without it
// rejects the ephemeral records of the session in its hash range, which the
// parent acknowledged.
func TestCoordinator_ShardSplit_SessionCreatedDuringCatchUp(t *testing.T) {
	// The cutover freezes the parent once both children have loaded its
	// snapshot and caught up with its log. Hold it there: what is written
	// before it resumes reaches the children through the parent's log only.
	reachedFreeze := make(chan struct{})
	resumeFreeze := make(chan struct{})
	rpcProvider := &freezeHookRpcProvider{beforeFreeze: sync.OnceFunc(func() {
		close(reachedFreeze)
		<-resumeFreeze
	})}
	c := setupSplitClusterWithRpc(t, rpcProvider.factory)
	defer c.close(t)
	resume := sync.OnceFunc(func() { close(resumeFreeze) })
	defer resume()

	ctx := context.Background()

	client, err := oxia.NewSyncClient(c.sa1.Public)
	require.NoError(t, err)
	_, _, err = client.Put(ctx, "seed", []byte("seed"))
	require.NoError(t, err)

	c.initiateSplit(t)
	select {
	case <-reachedFreeze:
	case <-time.After(time.Minute):
		require.FailNow(t, "the split did not reach the cutover")
	}

	// A record on each side of the split point: one of them is in the child
	// that the session key doesn't hash to, whatever the session id
	var leftKey, rightKey string
	for i := 0; leftKey == "" || rightKey == ""; i++ {
		key := fmt.Sprintf("catch-up-%d", i)
		if h := hash.Xxh332(key); h < math.MaxUint32/4 {
			leftKey = key
		} else if h > math.MaxUint32/4*3 {
			rightKey = key
		}
	}
	keys := []string{leftKey, rightKey}

	sessionClient, err := oxia.NewSyncClient(c.sa1.Public,
		oxia.WithSessionTimeout(time.Minute),
		oxia.WithIdentity("catch-up-client"),
	)
	require.NoError(t, err)
	var sessionId int64
	for _, key := range keys {
		_, version, err := sessionClient.Put(ctx, key, []byte(key), oxia.Ephemeral())
		require.NoError(t, err)
		sessionId = version.SessionId
	}

	resume()
	c.waitForSplit(t)

	// Each child holds the session, and the records of the session in its hash
	// range, with the shadow keys that delete them with the session
	sessionKey := lead.SessionKey(lead.SessionId(sessionId))
	for child, childMeta := range map[int64]*proto.ShardMetadata{c.leftChild: c.leftMeta, c.rightChild: c.rightMeta} {
		childKeys := make(map[string]bool)
		for _, key := range c.listShardKeys(t, child) {
			childKeys[key] = true
		}
		assert.True(t, childKeys[sessionKey], "shard %d lacks session %d", child, sessionId)

		hashRange := childMeta.Int32HashRange
		for _, key := range keys {
			h := hash.Xxh332(key)
			inRange := h >= hashRange.Min && h <= hashRange.Max
			assert.Equal(t, inRange, childKeys[key], "record %q in shard %d", key, child)
			assert.Equal(t, inRange, childKeys[lead.ShadowKey(lead.SessionId(sessionId), key)],
				"shadow key of %q in shard %d", key, child)
		}
	}

	// The records that the parent acknowledged are readable from the children
	client = c.reconnectClient(t, client, "seed")
	for _, key := range keys {
		_, value, version, err := client.Get(ctx, key)
		if assert.NoError(t, err, "acknowledged record %q missing after the split", key) {
			assert.Equal(t, []byte(key), value)
			assert.True(t, version.Ephemeral)
			assert.Equal(t, sessionId, version.SessionId)
		}
	}
	assert.NoError(t, client.Close())

	// The client closes the session on both children
	assert.NoError(t, sessionClient.Close())
}

// freezeHookRpcProvider calls beforeFreeze before freezing a shard, as the
// cutover of a split does with the parent, and afterFreeze once the shard is
// frozen. The cutover waits for them to return.
type freezeHookRpcProvider struct {
	rpc2.Provider
	beforeFreeze func()
	afterFreeze  func()
}

func (p *freezeHookRpcProvider) factory(instanceID string) rpc2.Provider {
	p.Provider = rpc2.NewRpcProvider(nil, instanceID)
	return p
}

func (p *freezeHookRpcProvider) FreezeShard(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
	if req.Frozen && p.beforeFreeze != nil {
		p.beforeFreeze()
	}
	response, err := p.Provider.FreezeShard(ctx, node, req)
	if err == nil && req.Frozen && p.afterFreeze != nil {
		p.afterFreeze()
	}
	return response, err
}

// The first ephemeral put of a client creates its session, on the shard of the
// key, and the next ephemeral puts wait for it. A put issued while the cutover
// of a split freezes the parent can't create it there: the parent rejects it,
// then the parent is deleted. The put must create the session on the child that
// took over its key instead.
func TestCoordinator_ShardSplit_SessionCreatedDuringCutover(t *testing.T) {
	// Hold the cutover once it has frozen the parent
	frozen := make(chan struct{})
	resumeCutover := make(chan struct{})
	rpcProvider := &freezeHookRpcProvider{afterFreeze: sync.OnceFunc(func() {
		close(frozen)
		<-resumeCutover
	})}
	c := setupSplitClusterWithRpc(t, rpcProvider.factory)
	defer c.close(t)
	resume := sync.OnceFunc(func() { close(resumeCutover) })
	defer resume()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := oxia.NewSyncClient(c.sa1.Public)
	require.NoError(t, err)
	defer func() { assert.NoError(t, client.Close()) }()
	_, _, err = client.Put(ctx, "seed", []byte("seed"))
	require.NoError(t, err)

	c.initiateSplit(t)
	select {
	case <-frozen:
	case <-time.After(time.Minute):
		require.FailNow(t, "the split did not reach the cutover")
	}

	// A key on each side of the split point
	var leftKey, rightKey string
	for i := 0; leftKey == "" || rightKey == ""; i++ {
		key := fmt.Sprintf("cutover-%d", i)
		if h := hash.Xxh332(key); h < math.MaxUint32/4 {
			leftKey = key
		} else if h > math.MaxUint32/4*3 {
			rightKey = key
		}
	}

	type putResult struct {
		version oxia.Version
		err     error
	}
	put := func(key string) <-chan putResult {
		ch := make(chan putResult, 1)
		go func() {
			_, version, err := client.Put(ctx, key, []byte(key), oxia.Ephemeral())
			ch <- putResult{version, err}
		}()
		return ch
	}
	await := func(ch <-chan putResult, key string) oxia.Version {
		select {
		case result := <-ch:
			require.NoError(t, result.err, "ephemeral put of %s", key)
			assert.True(t, result.version.Ephemeral)
			return result.version
		case <-time.After(30 * time.Second):
			require.FailNow(t, "ephemeral put stuck after the split", "key %s", key)
			return oxia.Version{}
		}
	}

	leftPut := put(leftKey)
	select {
	case result := <-leftPut:
		require.FailNow(t, "ephemeral put completed on the frozen parent", "%+v", result)
	case <-time.After(time.Second):
	}

	resume()
	c.waitForSplit(t)
	rightPut := put(rightKey)
	versions := map[string]oxia.Version{
		leftKey:  await(leftPut, leftKey),
		rightKey: await(rightPut, rightKey),
	}

	for key, expected := range versions {
		_, value, version, err := client.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, []byte(key), value)
		assert.Equal(t, expected.SessionId, version.SessionId)
	}
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

	// Records written with a partition key are placed by it, whatever the
	// hash of their keys, and their index entries must follow them.
	partitionKey := "pk-7"
	var partitionedKeys []string
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("%s/rec-%06d", partitionKey, i)
		_, _, err := client.Put(ctx, key, []byte(key),
			oxia.PartitionKey(partitionKey),
			oxia.SecondaryIndex("pk", partitionKey),
		)
		require.NoError(t, err)
		partitionedKeys = append(partitionedKeys, key)
	}

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

	// ---- Verify the index of the records written with a partition key ----
	// Queried in the child that holds the partition, the index must have an
	// entry for every record, and each entry must resolve to its record.
	partitionIndexKeys, err := client.List(ctx, partitionKey, partitionKey+"\xff",
		oxia.UseIndex("pk"), oxia.PartitionKey(partitionKey))
	require.NoError(t, err)
	assert.ElementsMatch(t, partitionedKeys, partitionIndexKeys)

	var partitionScanKeys []string
	for res := range client.RangeScan(ctx, partitionKey, partitionKey+"\xff",
		oxia.UseIndex("pk"), oxia.PartitionKey(partitionKey)) {
		require.NoError(t, res.Err)
		assert.Equal(t, res.Key, string(res.Value))
		partitionScanKeys = append(partitionScanKeys, res.Key)
	}
	assert.ElementsMatch(t, partitionedKeys, partitionScanKeys)

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
			s1, err := dataserver.New(t.Context(), commonwatch.New(dataServerOption))
			assert.NoError(t, err)

			sa1 := &proto.DataServerIdentity{
				Public:   fmt.Sprintf("localhost:%d", s1.PublicPort()),
				Internal: fmt.Sprintf("localhost:%d", s1.InternalPort()),
			}

			metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
			var keySorting proto.KeySortingType
			if test.sorting == "natural" {
				keySorting = proto.KeySortingType_NATURAL
			} else {
				keySorting = proto.KeySortingType_HIERARCHICAL
			}
			keySortingValue := "hierarchical"
			if keySorting == proto.KeySortingType_NATURAL {
				keySortingValue = "natural"
			}
			clusterConfig := newClusterConfig([]*proto.Namespace{{
				Name:              constant.DefaultNamespace,
				ReplicationFactor: 1,
				InitialShardCount: 1,
				KeySorting:        keySortingValue,
			}}, []*proto.DataServerIdentity{sa1})

			configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
			_, err = configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
				Value:   clusterConfig,
				Version: metadatacommon.NotExists,
			})
			require.NoError(t, err)
			coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))

			metadata := coordinatorInstance.Metadata()
			status := mock.StatusSnapshot(t, metadata)

			assert.EqualValues(t, 1, len(status.Namespaces))
			nsStatus := status.Namespaces[constant.DefaultNamespace]
			assert.EqualValues(t, 1, len(nsStatus.Shards))
			assert.EqualValues(t, 1, nsStatus.ReplicationFactor)

			assert.Eventually(t, func() bool {
				shard := mock.StatusSnapshot(t, metadata).Namespaces[constant.DefaultNamespace].Shards[0]
				return shard.GetStatusOrDefault() == proto.ShardStatusSteadyState
			}, 10*time.Second, 10*time.Millisecond)

			client, err := oxia.NewSyncClient(sa1.Public)
			assert.NoError(t, err)

			_, _, _ = client.Put(context.Background(), "/a", []byte("a"))
			_, _, _ = client.Put(context.Background(), "/b", []byte("b"))
			_, _, _ = client.Put(context.Background(), "/a/b", []byte("a/b"))

			list, err := client.List(context.Background(), "", "")
			assert.NoError(t, err)

			if keySorting == proto.KeySortingType_HIERARCHICAL {
				assert.Equal(t, []string{"/a", "/b", "/a/b"}, list)
			} else {
				assert.Equal(t, []string{"/a", "/a/b", "/b"}, list)
			}

			assert.NoError(t, client.Close())
			assert.NoError(t, coordinatorInstance.Close())

			assert.NoError(t, s1.Close())
		})
	}
}

// A range scan without a partition key goes to every shard, and the client
// merges the records of the shards in the key sorting of the namespace, which
// the coordinator sends through the data servers with the shard assignments.
func TestCoordinator_MultiShardKeySorting(t *testing.T) {
	keys := []string{"b", "a/y/z", "ab/y", "a0", "a/x"}
	for _, test := range []struct {
		keySorting string
		expected   []string
	}{
		// Natural sorting compares the bytes: '/' sorts before '0'
		{"natural", []string{"a/x", "a/y/z", "a0", "ab/y", "b"}},
		// Hierarchical sorting puts the keys with fewer '/' first, then sorts '/'
		// after any other byte
		{"hierarchical", []string{"a0", "b", "ab/y", "a/x", "a/y/z"}},
	} {
		t.Run(test.keySorting, func(t *testing.T) {
			s1, sa1 := newServer(t)
			defer s1.Close()

			metadataProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, "")
			configProvider := memory.NewProvider(metadatacodec.ClusterConfigCodec, metadatacommon.WatchEnabled, "")
			_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
				Value: newClusterConfig([]*proto.Namespace{{
					Name:              constant.DefaultNamespace,
					ReplicationFactor: 1,
					InitialShardCount: 4,
					KeySorting:        test.keySorting,
				}}, []*proto.DataServerIdentity{sa1}),
				Version: metadatacommon.NotExists,
			})
			require.NoError(t, err)
			coordinatorInstance := newCoordinatorInstance(t, metadataProvider, configProvider, rpc2.NewRpcProviderFactory(nil))
			defer coordinatorInstance.Close()

			require.Eventually(t, func() bool {
				shards := mock.StatusSnapshot(t, coordinatorInstance.Metadata()).Namespaces[constant.DefaultNamespace].Shards
				for _, shard := range shards {
					if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
						return false
					}
				}
				return len(shards) == 4
			}, 10*time.Second, 10*time.Millisecond)

			client, err := oxia.NewSyncClient(sa1.Public, oxia.WithBatchLinger(0))
			require.NoError(t, err)
			defer client.Close()

			for _, key := range keys {
				_, _, err = client.Put(t.Context(), key, []byte(key))
				require.NoError(t, err)
			}

			var scanned []string
			for result := range client.RangeScan(t.Context(), "", "") {
				require.NoError(t, result.Err)
				scanned = append(scanned, result.Key)
			}
			assert.Equal(t, test.expected, scanned)
		})
	}
}

// --- Split Failure E2E Tests ---

// waitForSplitPhase waits until the parent shard's split metadata reaches the
// given phase, or fails the test if the timeout is exceeded.
func waitForSplitPhase(t *testing.T, metadata coordmetadata.Metadata, parentShardId int64, phase proto.SplitPhase, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := mock.StatusSnapshot(t, metadata)
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
	status := mock.StatusSnapshot(t, cluster.metadata)
	parentLeader := status.Namespaces[constant.DefaultNamespace].Shards[0].Leader
	slog.Info("Parent leader identified", slog.Any("leader", parentLeader))

	// Initiate split (don't wait for completion)
	leftChild, rightChild, err := cluster.coordinator.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)
	cluster.leftChild = leftChild
	cluster.rightChild = rightChild
	slog.Info("Split initiated")

	// Wait briefly for Bootstrap to start, then kill the parent leader.
	// This simulates a leader crash during the observer snapshot transfer.
	waitForSplitPhase(t, cluster.metadata, 0, proto.SplitPhaseBootstrap, 30*time.Second)
	slog.Info("Kill parent leader during Bootstrap/CatchUp", slog.Any("leader", parentLeader))

	assert.NoError(t, cluster.servers[parentLeader.GetNameOrDefault()].Close())
	delete(cluster.servers, parentLeader.GetNameOrDefault())

	// The split controller should recover: the coordinator will elect a new
	// parent leader, the split controller detects the term change and falls
	// back to Bootstrap to re-add observers, then completes.
	slog.Info("Waiting for split to complete after parent leader kill")

	require.Eventually(t, func() bool {
		st := mock.StatusSnapshot(t, cluster.metadata)
		ns := st.Namespaces[constant.DefaultNamespace]
		if _, parentExists := ns.Shards[0]; parentExists {
			return false
		}
		for _, child := range []int64{leftChild, rightChild} {
			sm, ok := ns.Shards[child]
			if !ok || sm.GetStatusOrDefault() != proto.ShardStatusSteadyState || sm.Leader == nil || sm.Split != nil {
				return false
			}
		}
		return true
	}, 2*time.Minute, 500*time.Millisecond)
	slog.Info("Split completed after parent leader kill")

	// Verify all 50 keys are still accessible
	survivingAddr := cluster.liveAddressExcluding(parentLeader.GetNameOrDefault())
	require.NotNil(t, survivingAddr)

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

// cutoverHoldingRpcProvider holds the first cutover of a split before it
// freezes the parent, and records the BecomeLeader requests that data servers
// refused because of the features enabled on the shard.
//
// Once delayChildSnapshots is called, it also delays the snapshots that the
// parent sends to the members of the children when the split adds them as
// observers again: the parent gets a member as observer only once the member
// reports that it has no data, or once the split fences the parent past the
// point of no return. That fence then waits until the members are installing
// the snapshot, or have installed it. This is the worst timing for a member
// that still reports the data that it received in an earlier term of the
// parent.
type cutoverHoldingRpcProvider struct {
	rpc2.Provider
	cutoverReached chan struct{}
	resumeCutover  chan struct{}
	holdOnce       sync.Once
	resumeOnce     sync.Once
	fenceOnce      sync.Once

	mu                  sync.Mutex
	refusedBecomeLeader map[int64]int
	// parentTerm is the term of the parent whose observers are delayed, -1
	// until delayChildSnapshots
	parentTerm       int64
	delayedObservers map[childMember]*delayedObserver
	childMembers     map[childMember]bool
}

// childMember is a member of the ensemble of a split child.
type childMember struct {
	child    int64
	internal string
}

type delayedObserver struct {
	parentLeader *proto.DataServerIdentity
	req          *proto.AddFollowerRequest
}

func newCutoverHoldingRpcProvider() *cutoverHoldingRpcProvider {
	return &cutoverHoldingRpcProvider{
		cutoverReached:      make(chan struct{}),
		resumeCutover:       make(chan struct{}),
		refusedBecomeLeader: make(map[int64]int),
		parentTerm:          -1,
		delayedObservers:    make(map[childMember]*delayedObserver),
		childMembers:        make(map[childMember]bool),
	}
}

func (p *cutoverHoldingRpcProvider) factory(instanceID string) rpc2.Provider {
	p.Provider = rpc2.NewRpcProvider(nil, instanceID)
	return p
}

func (p *cutoverHoldingRpcProvider) resume() {
	p.resumeOnce.Do(func() { close(p.resumeCutover) })
}

// delayChildSnapshots delays the snapshots that the parent, led in the given
// term, sends to the children.
func (p *cutoverHoldingRpcProvider) delayChildSnapshots(parentTerm int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.parentTerm = parentTerm
}

func (p *cutoverHoldingRpcProvider) AddFollower(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	p.mu.Lock()
	if req.Observer && p.parentTerm >= 0 && req.Term == p.parentTerm {
		member := childMember{child: req.GetTargetShard(), internal: req.FollowerName}
		p.delayedObservers[member] = &delayedObserver{parentLeader: node, req: req}
		p.childMembers[member] = true
		p.mu.Unlock()
		return &proto.AddFollowerResponse{}, nil
	}
	p.mu.Unlock()
	return p.Provider.AddFollower(ctx, node, req)
}

func (p *cutoverHoldingRpcProvider) GetStatus(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	res, err := p.Provider.GetStatus(ctx, node, req)
	if err == nil && res.CommitOffset < 0 {
		// The member of the child has no data: the parent must send it a
		// snapshot
		p.addDelayedObserver(ctx, childMember{child: req.Shard, internal: node.GetInternal()})
	}
	return res, err
}

func (p *cutoverHoldingRpcProvider) NewTerm(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
	p.mu.Lock()
	finalizeFence := req.Shard == 0 && p.parentTerm >= 0 && req.Term > p.parentTerm
	p.mu.Unlock()
	if finalizeFence {
		p.fenceOnce.Do(func() { p.startChildSnapshots(ctx) })
	}
	return p.Provider.NewTerm(ctx, node, req)
}

func (p *cutoverHoldingRpcProvider) addDelayedObserver(ctx context.Context, member childMember) {
	p.mu.Lock()
	observer := p.delayedObservers[member]
	delete(p.delayedObservers, member)
	p.mu.Unlock()
	if observer == nil {
		return
	}
	if _, err := p.Provider.AddFollower(ctx, observer.parentLeader, observer.req); err != nil {
		slog.Warn("Failed to add the delayed observer", slog.Int64("child-shard", member.child),
			slog.String("member", member.internal), slog.Any("error", err))
	}
}

// startChildSnapshots lets the parent send the delayed snapshots, and waits
// until the members of the children are installing them, or have installed
// them.
func (p *cutoverHoldingRpcProvider) startChildSnapshots(ctx context.Context) {
	p.mu.Lock()
	childMembers := maps.Clone(p.childMembers)
	p.mu.Unlock()
	for member := range childMembers {
		p.addDelayedObserver(ctx, member)
	}
	deadline := time.Now().Add(30 * time.Second)
	for member := range childMembers {
		node := &proto.DataServerIdentity{Internal: member.internal}
		for ; time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			res, err := p.Provider.GetStatus(ctx, node, &proto.GetStatusRequest{Shard: member.child})
			if err == nil && (res.Status == proto.ServingStatus_FOLLOWER || res.CommitOffset < 0) {
				break
			}
		}
	}
}

func (p *cutoverHoldingRpcProvider) FreezeShard(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.FreezeShardRequest) (*proto.FreezeShardResponse, error) {
	if req.Frozen {
		p.holdOnce.Do(func() {
			close(p.cutoverReached)
			select {
			case <-p.resumeCutover:
			case <-ctx.Done():
			}
		})
	}
	return p.Provider.FreezeShard(ctx, node, req)
}

func (p *cutoverHoldingRpcProvider) BecomeLeader(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	res, err := p.Provider.BecomeLeader(ctx, node, req)
	if errors.Is(err, constant.ErrUnsupportedFeatures) {
		p.mu.Lock()
		p.refusedBecomeLeader[req.Shard]++
		p.mu.Unlock()
	}
	return res, err
}

func (p *cutoverHoldingRpcProvider) refusedBecomeLeaderCount(shard int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refusedBecomeLeader[shard]
}

// A leader election of the parent, once the children have received its data,
// sends the split back to Bootstrap. It fences the children again in the
// parent's new term, and the parent sends their members a new snapshot. The
// split must not pass the point of no return before a majority of each child
// has installed it: fencing the parent aborts the installation, which would
// leave the members without their data. The child leaders must also accept to
// lead the new term, though they got the features enabled on the parent with
// the first snapshot.
func TestCoordinator_ShardSplit_ParentElectionAfterSnapshot(t *testing.T) {
	rpcProvider := newCutoverHoldingRpcProvider()
	cluster := setupSplitClusterWithRpc(t, rpcProvider.factory)
	defer cluster.close(t)
	// Runs before close: the split controller must not stay held
	defer rpcProvider.resume()

	ctx := context.Background()
	client, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)
	// Incompressible values, so that installing a snapshot of the parent
	// takes a while
	random := rand.New(rand.NewPCG(1, 2))
	keys := make(map[string][]byte)
	for i := 0; i < 50; i++ {
		key, value := fmt.Sprintf("key-%04d", i), make([]byte, 128*1024)
		for j := range value {
			value[j] = byte(random.Uint32())
		}
		_, _, err = client.Put(ctx, key, value)
		require.NoError(t, err)
		keys[key] = value
	}

	cluster.leftChild, cluster.rightChild, err = cluster.coordinator.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)

	// The children have caught up with the parent, starting from its snapshot
	select {
	case <-rpcProvider.cutoverReached:
	case <-time.After(60 * time.Second):
		require.FailNow(t, "the split did not reach the cutover")
	}

	// A write after the snapshot, which the members of the children receive as
	// observers
	_, _, err = client.Put(ctx, "key-after-snapshot", []byte("value"))
	require.NoError(t, err)
	keys["key-after-snapshot"] = []byte("value")
	assert.NoError(t, client.Close())
	parent := cluster.shardStatus(t, 0)
	parentStatus, err := rpcProvider.GetStatus(ctx, parent.Leader, &proto.GetStatusRequest{Shard: 0})
	require.NoError(t, err)
	for _, child := range []int64{cluster.leftChild, cluster.rightChild} {
		childLeader := cluster.shardStatus(t, child).Leader
		require.Eventually(t, func() bool {
			childStatus, err := rpcProvider.GetStatus(ctx, childLeader, &proto.GetStatusRequest{Shard: child})
			return err == nil && childStatus.HeadOffset >= parentStatus.HeadOffset
		}, 30*time.Second, 100*time.Millisecond)
	}

	// A new leader election of the parent sends the split back to Bootstrap.
	// The new snapshots reach the children as late as possible, right before
	// the split fences the parent, unless a child reports that it needs one.
	slog.Info("Electing a new parent leader during the cutover", slog.Any("leader", parent.Leader))
	cluster.coordinator.BecameUnavailable(parent.Leader)
	cluster.waitForNewTerm(t, 0, parent.Term)
	rpcProvider.delayChildSnapshots(cluster.shardStatus(t, 0).Term)
	rpcProvider.resume()

	require.Eventually(t, func() bool {
		ns := mock.StatusSnapshot(t, cluster.metadata).Namespaces[constant.DefaultNamespace]
		parentShard, exists := ns.Shards[0]
		if exists && parentShard.GetStatusOrDefault() != proto.ShardStatusDeleting {
			return false
		}
		for _, child := range []int64{cluster.leftChild, cluster.rightChild} {
			if sm, ok := ns.Shards[child]; !ok || sm.Split != nil {
				return false
			}
		}
		return true
	}, 2*time.Minute, 500*time.Millisecond, "the split did not complete")
	for _, child := range []int64{cluster.leftChild, cluster.rightChild} {
		assert.Zero(t, rpcProvider.refusedBecomeLeaderCount(child),
			"the leader of child %d refused to lead because of the features enabled on it", child)
	}

	// Reads of a child without a leader give up, instead of retrying until
	// the client is closed
	reader, err := oxia.NewSyncClient(cluster.sa1.Public, oxia.WithRequestTimeout(5*time.Second))
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	var unreadable []string
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Second) {
		if unreadable = unreadableKeys(ctx, reader, keys); len(unreadable) == 0 || time.Now().After(deadline) {
			break
		}
	}
	assert.Empty(t, unreadable, "keys lost by the split")
}

// unreadableKeys returns the keys that can't be read with their expected value,
// with the reason.
func unreadableKeys(ctx context.Context, client oxia.SyncClient, expected map[string][]byte) []string {
	var (
		mu         sync.Mutex
		unreadable []string
		wg         sync.WaitGroup
	)
	for key, value := range expected {
		wg.Go(func() {
			getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, actual, _, err := client.Get(getCtx, key)
			if err == nil && bytes.Equal(actual, value) {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			unreadable = append(unreadable, fmt.Sprintf("%s (%d bytes read, error: %v)", key, len(actual), err))
		})
	}
	wg.Wait()
	slices.Sort(unreadable)
	return unreadable
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
	status := mock.StatusSnapshot(t, cluster.metadata)
	parentMeta := status.Namespaces[constant.DefaultNamespace].Shards[0]
	parentLeader := parentMeta.Leader
	follower := cluster.liveAddressExcluding(parentLeader.GetNameOrDefault())
	require.NotNil(t, follower)

	// Kill a follower before initiating split
	slog.Info("Killing follower before split", slog.Any("follower", follower))
	assert.NoError(t, cluster.servers[follower.GetNameOrDefault()].Close())
	delete(cluster.servers, follower.GetNameOrDefault())

	// The split should still complete with 2/3 servers (quorum)
	slog.Info("Initiating split with one follower down")
	cluster.leftChild, cluster.rightChild, err = cluster.coordinator.InitiateSplit(constant.DefaultNamespace, 0, nil)
	require.NoError(t, err)

	// With a dead follower, the shard controller's DeleteShard retries
	// indefinitely (can't reach the dead node). So we accept the parent
	// being either fully deleted OR marked Deleting with split metadata cleared.
	require.Eventually(t, func() bool {
		st := mock.StatusSnapshot(t, cluster.metadata)
		ns := st.Namespaces[constant.DefaultNamespace]
		if parentMeta, parentExists := ns.Shards[0]; parentExists {
			if parentMeta.GetStatusOrDefault() != proto.ShardStatusDeleting {
				return false
			}
		}
		for _, child := range []int64{cluster.leftChild, cluster.rightChild} {
			sm, ok := ns.Shards[child]
			if !ok || sm.GetStatusOrDefault() != proto.ShardStatusSteadyState || sm.Leader == nil || sm.Split != nil {
				return false
			}
		}
		return true
	}, 2*time.Minute, 500*time.Millisecond)
	slog.Info("Split completed with one follower down")

	// Verify keys accessible through a surviving server
	survivingAddr := cluster.liveAddressExcluding()
	require.NotNil(t, survivingAddr)

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
		st := mock.StatusSnapshot(t, cluster.metadata)
		ns := st.Namespaces[constant.DefaultNamespace]
		if _, parentExists := ns.Shards[0]; parentExists {
			return false
		}
		for _, child := range []int64{leftChild, rightChild} {
			sm, ok := ns.Shards[child]
			if !ok || sm.GetStatusOrDefault() != proto.ShardStatusSteadyState || sm.Leader == nil || sm.Split != nil {
				return false
			}
		}
		return true
	}, 2*time.Minute, 500*time.Millisecond)
	slog.Info("First split completed successfully")
}

// ---- Writes to split children ----

// splitWithKeys writes some keys, splits the shard, and returns a client that
// uses the children's assignments.
func (c *splitTestCluster) splitWithKeys(t *testing.T) oxia.SyncClient {
	t.Helper()
	client, err := oxia.NewSyncClient(c.sa1.Public)
	require.NoError(t, err)
	for i := 0; i < 30; i++ {
		_, _, err = client.Put(context.Background(), fmt.Sprintf("key-%04d", i), []byte("value"))
		require.NoError(t, err)
	}
	c.splitAndWait(t)
	return c.reconnectClient(t, client, "key-0000")
}

// childKey returns a key, starting with prefix, that belongs to the given child.
func (c *splitTestCluster) childKey(child int64, prefix string) string {
	hashRange := c.leftMeta.Int32HashRange
	if child == c.rightChild {
		hashRange = c.rightMeta.Int32HashRange
	}
	for i := 0; ; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if h := hash.Xxh332(key); h >= hashRange.Min && h <= hashRange.Max {
			return key
		}
	}
}

func (c *splitTestCluster) shardStatus(t *testing.T, shard int64) *proto.ShardMetadata {
	t.Helper()
	return mock.StatusSnapshot(t, c.metadata).Namespaces[constant.DefaultNamespace].Shards[shard]
}

// waitForNewTerm waits for the shard to be led again in a term after the given one.
func (c *splitTestCluster) waitForNewTerm(t *testing.T, shard int64, term int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		sm := c.shardStatus(t, shard)
		return sm.Term > term && sm.Leader != nil && sm.GetStatusOrDefault() == proto.ShardStatusSteadyState
	}, 30*time.Second, 100*time.Millisecond)
}

// The first leader of a split child is seeded from a snapshot of the parent,
// with an empty WAL. The child must keep committing writes after it gets
// re-elected, like leader balancing does.
func TestCoordinator_ShardSplit_ChildWritesAfterReelection(t *testing.T) {
	cluster := setupSplitCluster(t)
	defer cluster.close(t)
	client := cluster.splitWithKeys(t)
	defer func() { assert.NoError(t, client.Close()) }()

	_, _, err := client.Put(context.Background(), cluster.childKey(cluster.leftChild, "before"), []byte("value"))
	require.NoError(t, err)

	// Reporting the leader as unavailable runs the same election as leader balancing
	before := cluster.shardStatus(t, cluster.leftChild)
	cluster.coordinator.BecameUnavailable(before.Leader)
	cluster.waitForNewTerm(t, cluster.leftChild, before.Term)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _, err = client.Put(ctx, cluster.childKey(cluster.leftChild, "after"), []byte("value"))
	require.NoError(t, err, "write to the split child after its re-election")
}

// A write acknowledged by a split child must be on a quorum of the child's
// ensemble, and survive the loss of the child's leader.
func TestCoordinator_ShardSplit_ChildWriteSurvivesLeaderLoss(t *testing.T) {
	cluster := setupSplitCluster(t)
	defer cluster.close(t)
	client := cluster.splitWithKeys(t)

	ctx := context.Background()
	key := cluster.childKey(cluster.leftChild, "acked")
	_, _, err := client.Put(ctx, key, []byte("value"))
	require.NoError(t, err)
	assert.NoError(t, client.Close())

	before := cluster.shardStatus(t, cluster.leftChild)
	leader := before.Leader.GetNameOrDefault()
	assert.NoError(t, cluster.servers[leader].Close())
	delete(cluster.servers, leader)
	cluster.waitForNewTerm(t, cluster.leftChild, before.Term)

	var reader oxia.SyncClient
	require.Eventually(t, func() bool {
		reader, err = oxia.NewSyncClient(cluster.liveAddressExcluding(leader).Public)
		if err != nil {
			return false
		}
		if _, _, _, err = reader.Get(ctx, "key-0000"); err != nil {
			_ = reader.Close()
			return false
		}
		return true
	}, 30*time.Second, 500*time.Millisecond)
	defer func() { assert.NoError(t, reader.Close()) }()

	_, value, _, err := reader.Get(ctx, key)
	require.NoError(t, err, "write acknowledged by the split child lost with its leader")
	assert.Equal(t, []byte("value"), value)
}

// leaderOnlyChildObservers adds only the leader of each split child as an
// observer of the parent, and reports the other members of the child as
// holding the parent's data while the split runs: the split completes with
// the data of each child on its leader only, as if the other members had
// missed it. They get it from a snapshot of the child leader once the split
// elects it in a clean term.
type leaderOnlyChildObservers struct {
	rpc2.Provider

	// The coordinator's metadata, once the cluster is up
	metadata atomic.Value
}

// splittingChildLeader returns the leader of a split child, or nil for a shard
// that isn't the child of a running split.
func (p *leaderOnlyChildObservers) splittingChildLeader(shard int64) *proto.DataServerIdentity {
	metadata, _ := p.metadata.Load().(coordmetadata.Metadata)
	if metadata == nil {
		return nil
	}
	status, exists := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	if !exists {
		return nil
	}
	child := status.UnsafeBorrow()
	if child.GetSplit() == nil || len(child.GetSplit().GetChildShardIds()) > 0 {
		return nil
	}
	return child.GetLeader()
}

func (p *leaderOnlyChildObservers) AddFollower(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	if leader := p.splittingChildLeader(req.GetTargetShard()); req.Observer && leader != nil &&
		leader.GetInternal() != req.FollowerName {
		return &proto.AddFollowerResponse{}, nil
	}
	return p.Provider.AddFollower(ctx, node, req)
}

func (p *leaderOnlyChildObservers) GetStatus(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
	res, err := p.Provider.GetStatus(ctx, node, req)
	if leader := p.splittingChildLeader(req.Shard); err == nil && leader != nil &&
		leader.GetNameOrDefault() != node.GetNameOrDefault() {
		res = res.CloneVT()
		res.CommitOffset = math.MaxInt64
	}
	return res, err
}

// followerlessChildRpcProvider elects the leaders of the split children
// without their followers, which never get the children's data: the children
// stay as they are right after the split, while the followers are still
// installing the snapshot that the child leader sends them.
type followerlessChildRpcProvider struct {
	leaderOnlyChildObservers
}

func (p *followerlessChildRpcProvider) factory(instanceID string) rpc2.Provider {
	p.Provider = rpc2.NewRpcProvider(nil, instanceID)
	return p
}

func (p *followerlessChildRpcProvider) BecomeLeader(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	if req.Shard != 0 {
		req = req.CloneVT()
		req.FollowerMaps = nil
	}
	return p.Provider.BecomeLeader(ctx, node, req)
}

func (p *followerlessChildRpcProvider) AddFollower(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
	if req.Shard != 0 {
		return &proto.AddFollowerResponse{}, nil
	}
	return p.leaderOnlyChildObservers.AddFollower(ctx, node, req)
}

// The leader of a split child can be the only member seeded from a snapshot of
// the parent, e.g. when the other members were down during the split, and it
// sends one to the child's followers once the split elects it in a clean term.
// A leader election of the child in the meantime, like the one that
// BecameUnavailable runs, fences the leader, which aborts the installation of
// the snapshot on the followers. The election must still elect the child
// leader, the only member with the child's data, though the wal of a member
// seeded from a snapshot is as empty as the one of a member without any data.
func TestCoordinator_ShardSplit_ChildElectionBeforeFollowersSeeded(t *testing.T) {
	rpcProvider := &followerlessChildRpcProvider{}
	cluster := setupSplitClusterWithRpc(t, rpcProvider.factory)
	defer cluster.close(t)
	rpcProvider.metadata.Store(cluster.metadata)

	ctx := context.Background()
	client, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)
	keys := make(map[string][]byte)
	for i := 0; i < 30; i++ {
		key, value := fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf("value-%d", i))
		_, _, err = client.Put(ctx, key, value)
		require.NoError(t, err)
		keys[key] = value
	}
	assert.NoError(t, client.Close())
	cluster.splitAndWait(t)

	// A read that a child can't serve fails after a while, instead of retrying
	// until the client is closed
	reader, err := oxia.NewSyncClient(cluster.sa1.Public, oxia.WithRequestTimeout(5*time.Second))
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()

	// Among the members with the highest head entry, the election can pick any:
	// elect the leader of each child a few times. Check the keys after every
	// election, before the next one: an election that moves the leader of a child
	// to the data server that leads the other child would get it elected again
	// by the next BecameUnavailable
	for range 3 {
		for _, child := range []int64{cluster.leftChild, cluster.rightChild} {
			// The children can have the same leader, and get elected together
			var before *proto.ShardMetadata
			require.Eventually(t, func() bool {
				before = cluster.shardStatus(t, child)
				return before.Leader != nil && before.GetStatusOrDefault() == proto.ShardStatusSteadyState
			}, 30*time.Second, 100*time.Millisecond)
			cluster.coordinator.BecameUnavailable(before.Leader)
			cluster.waitForNewTerm(t, child, before.Term)

			for key, expected := range keys {
				_, value, _, err := reader.Get(ctx, key)
				require.NoError(t, err, "key %s lost by a leader election of a split child", key)
				assert.Equal(t, expected, value)
			}
		}
	}
}

// slowChildSnapshotProvider delays the end of the snapshots that the leader of
// a split child sends to the followers of the child, as if the child were big.
// The snapshots that the parent sends to the children, its observers, are not
// delayed.
type slowChildSnapshotProvider struct {
	dataserverrpc.ReplicationRpcProvider
	delay time.Duration
}

func (p *slowChildSnapshotProvider) SendSnapshot(ctx context.Context, follower string, namespace string,
	shard int64, term int64) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	stream, err := p.ReplicationRpcProvider.SendSnapshot(ctx, follower, namespace, shard, term)
	md, _ := grpcmetadata.FromOutgoingContext(ctx)
	if err != nil || shard == 0 || len(md.Get(constant.MetadataSplitHashRangeMin)) > 0 {
		return stream, err
	}
	return &slowSnapshotStream{OxiaLogReplication_SendSnapshotClient: stream, ctx: ctx, delay: p.delay}, nil
}

type slowSnapshotStream struct {
	proto.OxiaLogReplication_SendSnapshotClient
	ctx   context.Context
	delay time.Duration
}

func (s *slowSnapshotStream) CloseAndRecv() (*proto.SnapshotResponse, error) {
	select {
	case <-time.After(s.delay):
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
	return s.OxiaLogReplication_SendSnapshotClient.CloseAndRecv()
}

// newServerWithSlowChildSnapshots is newServer, with the snapshots of the split
// children delayed by slowChildSnapshotProvider, and a checksum of each shard
// recorded every second: a split child writes an entry soon after it starts
// leading.
func newServerWithSlowChildSnapshots(t *testing.T, delay time.Duration) (*dataserver.Server,
	*proto.DataServerIdentity) {
	t.Helper()
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()
	options.Scheduler.Checksum.Interval = commonoption.Duration(time.Second)

	manifest, err := manifestpkg.NewManifest(options.Storage.Database.Dir)
	require.NoError(t, err)
	replication, err := dataserverrpc.NewReplicationRpcProvider(&options.Replication.TLS, manifest)
	require.NoError(t, err)
	s, err := dataserver.NewWithGrpcProvider(t.Context(), commonwatch.New(options), commonrpc.Default,
		&slowChildSnapshotProvider{ReplicationRpcProvider: replication, delay: delay}, manifest)
	require.NoError(t, err)
	return s, &proto.DataServerIdentity{
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}
}

// finalizeRetryRpcProvider makes a split run Finalize again once it elected the
// left child in its clean term: it fails the first election of the right child,
// as soon as the left child leader has an entry that its followers didn't ack.
// It counts the elections of each child by Finalize.
type finalizeRetryRpcProvider struct {
	leaderOnlyChildObservers

	mu sync.Mutex
	// The term of the parent when the split started: Finalize elects the
	// children in later terms
	parentTerm            int64
	leftChild, rightChild int64
	leftLeader            *proto.DataServerIdentity
	failedRight           bool
	leftPendingEntry      bool
	finalizeElections     map[int64]int
}

func (p *finalizeRetryRpcProvider) initiateSplit(t *testing.T, c *splitTestCluster) {
	t.Helper()
	p.mu.Lock()
	p.parentTerm = c.shardStatus(t, 0).Term
	p.mu.Unlock()
	c.initiateSplit(t)
	p.mu.Lock()
	p.leftChild, p.rightChild = c.leftChild, c.rightChild
	p.mu.Unlock()
}

func (p *finalizeRetryRpcProvider) BecomeLeader(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
	p.mu.Lock()
	finalize := req.Shard != 0 && req.Term > p.parentTerm
	if finalize {
		p.finalizeElections[req.Shard]++
	}
	if req.Shard == p.leftChild {
		p.leftLeader = node
	}
	failRight := finalize && req.Shard == p.rightChild && !p.failedRight
	p.failedRight = p.failedRight || failRight
	leftChild, leftLeader := p.leftChild, p.leftLeader
	p.mu.Unlock()
	if !failRight {
		return p.Provider.BecomeLeader(ctx, node, req)
	}

	// The left child leader records a checksum within a second, an entry that
	// its followers can only ack once they installed the snapshot of the child
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		res, err := p.GetStatus(ctx, leftLeader, &proto.GetStatusRequest{Shard: leftChild})
		if err == nil && res.Status == proto.ServingStatus_LEADER && res.HeadOffset > res.CommitOffset {
			p.mu.Lock()
			p.leftPendingEntry = true
			p.mu.Unlock()
			break
		}
	}
	return nil, errors.New("injected failure of the election of the right child")
}

func (p *finalizeRetryRpcProvider) elections(child int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.finalizeElections[child]
}

func (p *finalizeRetryRpcProvider) sawLeftPendingEntry() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.leftPendingEntry
}

// Finalize elects each child of a split in a clean term. A child leader that
// has entries left to commit through followers that miss the child's data,
// e.g. because they were down during the split, only leads once one of them
// installed the snapshot of the child, which takes as long as the child is big.
// The leader of the left child has such an entry when Finalize runs again after
// electing it: a checksum it recorded while its followers were installing the
// snapshot. Bounded by the rpc timeout, the election of a child that takes
// longer to install failed, and the next attempt of Finalize fenced the child
// in a new term, which aborted the installation: the split completed only once
// the retry backoff left a whole installation the time to complete, if ever,
// and the parent's hash range was unavailable until then.
func TestCoordinator_ShardSplit_ChildElectionSlowerThanRpcTimeout(t *testing.T) {
	// The rpc timeout of the coordinator, and the time the followers of a child
	// take to install its snapshot, scaled down
	const rpcTimeout = 2 * time.Second
	servers := make(map[string]*dataserver.Server)
	var addresses []*proto.DataServerIdentity
	for range 3 {
		s, addr := newServerWithSlowChildSnapshots(t, 3*rpcTimeout)
		servers[addr.GetNameOrDefault()] = s
		addresses = append(addresses, addr)
	}
	rpcProvider := &finalizeRetryRpcProvider{finalizeElections: make(map[int64]int)}
	cluster := setupSplitClusterWith(t, func(instanceID string) rpc2.Provider {
		rpcProvider.Provider = rpc2.NewRpcProviderWithTimeout(nil, instanceID, rpcTimeout)
		return rpcProvider
	}, servers, addresses)
	defer cluster.close(t)
	rpcProvider.metadata.Store(cluster.metadata)

	ctx := context.Background()
	client, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)
	keys := make(map[string][]byte)
	for i := 0; i < 30; i++ {
		key, value := fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf("value-%d", i))
		_, _, err = client.Put(ctx, key, value)
		require.NoError(t, err)
		keys[key] = value
	}
	assert.NoError(t, client.Close())

	// Finalize elects the left child twice: before and after the failed
	// election of the right child
	rpcProvider.initiateSplit(t, cluster)
	require.Eventually(t, func() bool {
		parent := cluster.shardStatus(t, 0)
		return rpcProvider.elections(cluster.leftChild) > 2 ||
			parent == nil || parent.GetStatusOrDefault() == proto.ShardStatusDeleting
	}, time.Minute, 100*time.Millisecond, "the split did not complete")
	require.True(t, rpcProvider.sawLeftPendingEntry(), "the left child leader had no entry left to commit")
	require.LessOrEqual(t, rpcProvider.elections(cluster.leftChild), 2,
		"Finalize kept electing the left child, and its followers kept installing the snapshot over")
	cluster.waitForSplit(t)

	reader, err := oxia.NewSyncClient(cluster.sa1.Public, oxia.WithRequestTimeout(5*time.Second))
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	var unreadable []string
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Second) {
		if unreadable = unreadableKeys(ctx, reader, keys); len(unreadable) == 0 || time.Now().After(deadline) {
			break
		}
	}
	assert.Empty(t, unreadable, "keys lost by the split")
}

// The split deletes the parent once it completes: from then on, the data of a
// child must survive the loss of the child's leader, even right away. A
// majority of the child must hold it by then, which takes as long as the child
// is big: the members of the child get it as observers of the parent, and the
// child leader must not send them the whole child again once the split elects
// it in a clean term.
func TestCoordinator_ShardSplit_ChildDataSurvivesLeaderLossAfterSplit(t *testing.T) {
	// The followers of a child take a few seconds to install a snapshot of the
	// child leader, as if the child were big
	servers := make(map[string]*dataserver.Server)
	var addresses []*proto.DataServerIdentity
	for range 3 {
		s, addr := newServerWithSlowChildSnapshots(t, 3*time.Second)
		servers[addr.GetNameOrDefault()] = s
		addresses = append(addresses, addr)
	}
	cluster := setupSplitClusterWith(t, rpc2.NewRpcProviderFactory(nil), servers, addresses)
	defer cluster.close(t)

	ctx := context.Background()
	client, err := oxia.NewSyncClient(cluster.sa1.Public)
	require.NoError(t, err)
	keys := make(map[string][]byte)
	for i := 0; i < 30; i++ {
		key, value := fmt.Sprintf("key-%04d", i), []byte(fmt.Sprintf("value-%d", i))
		_, _, err = client.Put(ctx, key, value)
		require.NoError(t, err)
		keys[key] = value
	}
	assert.NoError(t, client.Close())

	// Lose the leader of the left child as soon as the split publishes the
	// children
	assignments := cluster.coordinator.SubscribeShardAssignments()
	cluster.initiateSplit(t)
	leftAssigned := func() bool {
		return slices.ContainsFunc(assignments.Load().GetNamespaces()[constant.DefaultNamespace].GetAssignments(),
			func(assignment *proto.ShardAssignment) bool { return assignment.GetShard() == cluster.leftChild })
	}
	for !leftAssigned() {
		select {
		case <-assignments.Changed():
		case <-time.After(time.Minute):
			require.FailNow(t, "the split did not complete")
		}
	}
	left := cluster.shardStatus(t, cluster.leftChild)
	leader := left.Leader.GetNameOrDefault()
	slog.Info("Losing the leader of the left child right after the split", slog.String("leader", leader))
	assert.NoError(t, cluster.servers[leader].Close())
	delete(cluster.servers, leader)
	cluster.coordinator.BecameUnavailable(left.Leader)
	cluster.waitForNewTerm(t, cluster.leftChild, left.Term)

	reader, err := oxia.NewSyncClient(cluster.liveAddressExcluding(leader).Public,
		oxia.WithRequestTimeout(5*time.Second))
	require.NoError(t, err)
	defer func() { assert.NoError(t, reader.Close()) }()
	var unreadable []string
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Second) {
		if unreadable = unreadableKeys(ctx, reader, keys); len(unreadable) == 0 || time.Now().After(deadline) {
			break
		}
	}
	assert.Empty(t, unreadable, "keys lost with the leader of the left child")
}
