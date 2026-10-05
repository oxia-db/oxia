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

package kvscript

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	coordreconciler "github.com/oxia-db/oxia/oxiad/coordinator/reconciler"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/oxia-db/oxia/tests/mock"
)

// These are independent servers, storage directories, and gRPC endpoints in
// one Go test process. Stopping a node closes it gracefully; it is not a crash.
type scriptCluster struct {
	ctx      context.Context
	nodes    []*scriptNode
	metadata coordmetadata.Metadata
	shards   uint32
	sorting  proto.KeySorting
	// Discovery excludes a stopped node after close and includes it only
	// after its restart and assignment streams are ready.
	updateSeeds func([]string)
}

type scriptNode struct {
	identity *proto.DataServerIdentity
	options  *option.Options
	server   *dataserver.Server
}

func newScriptCluster(t *testing.T, shards uint32, sorting proto.KeySortingType) (*scriptCluster, string) {
	t.Helper()
	cluster := &scriptCluster{ctx: t.Context(), shards: shards, sorting: sorting.ToKeySorting()}
	identities := make([]*proto.DataServerIdentity, 0, 3)
	for index := range 3 {
		node := newScriptNode(t, fmt.Sprintf("node-%d", index+1))
		cluster.nodes = append(cluster.nodes, node)
		identities = append(identities, node.identity)
	}
	namespace := &proto.Namespace{
		Name: constant.DefaultNamespace, InitialShardCount: shards, ReplicationFactor: 3,
	}
	namespace.SetKeySortingType(sorting)
	config := &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{namespace}, Servers: identities}
	configProvider := mock.NewConfigProvider(t, config)
	t.Cleanup(func() { require.NoError(t, configProvider.Close()) })
	statusProvider := memory.NewProvider(metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled, mock.RandomCoordinatorName())
	t.Cleanup(func() { require.NoError(t, statusProvider.Close()) })
	factory, metadata := mock.NewMetadataFromProviders(t, statusProvider, configProvider)
	cluster.metadata = metadata
	t.Cleanup(func() { require.NoError(t, factory.Close()) })
	t.Cleanup(func() { require.NoError(t, metadata.Close()) })
	coordinator, err := coordruntime.New(metadata, coordrpc.NewRpcProviderFactory(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
	reconciler := coordreconciler.New(t.Context(), coordinator)
	t.Cleanup(func() { require.NoError(t, reconciler.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), clusterTimeout)
	defer cancel()
	c := command{t: t, ctx: ctx}
	cluster.wait(c, "initial RF3 readiness", func() string {
		return cluster.health(t, 3)
	})
	cluster.waitAssignments(c)
	return cluster, cluster.liveAddress()
}

func newScriptNode(t *testing.T, name string) *scriptNode {
	t.Helper()
	options := option.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()
	server, err := dataserver.New(t.Context(), commonwatch.New(options))
	require.NoError(t, err)
	restartOptions := *options
	node := &scriptNode{options: &restartOptions, server: server, identity: &proto.DataServerIdentity{
		Name: &name, Public: fmt.Sprintf("localhost:%d", server.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", server.InternalPort()),
	}}
	// Reopening uses the same addresses and the same WAL/database directories.
	restartOptions.Server.Public.BindAddress = node.identity.Public
	restartOptions.Server.Internal.BindAddress = node.identity.Internal
	t.Cleanup(func() {
		if node.server != nil {
			require.NoError(t, node.server.Close())
			node.server = nil
		}
	})
	return node
}

func (cluster *scriptCluster) seedAddresses() []string {
	addresses := make([]string, 0, len(cluster.nodes))
	for _, node := range cluster.nodes {
		addresses = append(addresses, node.identity.Public)
	}
	return addresses
}

func (cluster *scriptCluster) liveSeeds() []string {
	addresses := make([]string, 0, len(cluster.nodes))
	for _, node := range cluster.nodes {
		if node.server != nil {
			addresses = append(addresses, node.identity.Public)
		}
	}
	return addresses
}

func (cluster *scriptCluster) liveAddress() string {
	for _, node := range cluster.nodes {
		if node.server != nil {
			return node.identity.Public
		}
	}
	return ""
}

func (cluster *scriptCluster) node(name string) *scriptNode {
	for _, node := range cluster.nodes {
		if node.identity.GetNameOrDefault() == name {
			return node
		}
	}
	return nil
}

func (cluster *scriptCluster) snapshot(t *testing.T) map[int64]*proto.ShardMetadata {
	t.Helper()
	return mock.StatusSnapshot(t, cluster.metadata).Namespaces[constant.DefaultNamespace].GetShards()
}

func (cluster *scriptCluster) health(t *testing.T, live int) string {
	t.Helper()
	count := 0
	for _, node := range cluster.nodes {
		if node.server != nil {
			count++
		}
	}
	if count != live {
		return fmt.Sprintf("live nodes=%d, need %d", count, live)
	}
	shards := cluster.snapshot(t)
	if len(shards) != int(cluster.shards) {
		return fmt.Sprintf("shards=%d, need %d", len(shards), cluster.shards)
	}
	for id, shard := range shards {
		if reason := cluster.shardHealth(id, shard); reason != "" {
			return reason
		}
	}
	return ""
}

func (cluster *scriptCluster) shardHealth(id int64, shard *proto.ShardMetadata) string {
	if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState || len(shard.Ensemble) != 3 {
		return fmt.Sprintf("shard %d is not steady RF3: %v", id, shard)
	}
	leaderName := shard.GetLeader().GetNameOrDefault()
	leaderNode := cluster.node(leaderName)
	if leaderNode == nil || leaderNode.server == nil {
		return fmt.Sprintf("shard %d has no live leader", id)
	}
	leader, err := leaderNode.server.GetShardDirector().GetLeader(id)
	if err != nil || leader.Status() != proto.ServingStatus_LEADER || leader.Term() != shard.Term {
		return fmt.Sprintf("shard %d leader is not serving term %d: %v", id, shard.Term, err)
	}
	for _, supported := range feature.SupportedFeatures() {
		if !leader.IsFeatureEnabled(supported) {
			return fmt.Sprintf("shard %d leader has not enabled feature %s", id, supported)
		}
	}
	seen := make(map[string]bool, 3)
	for _, identity := range shard.Ensemble {
		name := identity.GetNameOrDefault()
		node := cluster.node(name)
		if node == nil || seen[name] {
			return fmt.Sprintf("shard %d has unknown or duplicate member %q", id, name)
		}
		seen[name] = true
		if node.server == nil || name == leaderName {
			continue
		}
		if reason := followerHealth(node, id, shard.Term); reason != "" {
			return reason
		}
	}
	if !seen[leaderName] {
		return fmt.Sprintf("shard %d leader is outside ensemble", id)
	}
	return ""
}

func followerHealth(node *scriptNode, id, term int64) string {
	follower, err := node.server.GetShardDirector().GetFollower(id)
	if err != nil {
		return fmt.Sprintf("shard %d follower %s unavailable: %v", id, node.identity.GetNameOrDefault(), err)
	}
	// NewTerm fences an idle follower until the first append in that term.
	// A topology barrier must allow that state rather than requiring a KV write
	// to happen before it can complete. The replication barrier checks FOLLOWER.
	status := follower.Status()
	if (status != proto.ServingStatus_FOLLOWER && status != proto.ServingStatus_FENCED) || follower.Term() != term {
		return fmt.Sprintf("shard %d follower %s is not ready at term %d: status=%s",
			id, node.identity.GetNameOrDefault(), term, status)
	}
	return ""
}

func (cluster *scriptCluster) wait(c command, description string, observe func() string) {
	c.t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		reason := observe()
		if reason == "" {
			return
		}
		select {
		case <-c.ctx.Done():
			c.t.Fatalf("%s failed: %s (%v)", description, reason, c.ctx.Err())
		case <-ticker.C:
		}
	}
}

// Readiness includes the assignment streams served by every surviving seed.
// Controller state alone can precede the coordinator's assignment broadcast.
func (cluster *scriptCluster) waitAssignments(c command) {
	c.t.Helper()
	clients := make(map[string]proto.OxiaClientClient, len(cluster.nodes))
	connections := make([]*grpc.ClientConn, 0, len(cluster.nodes))
	defer func() {
		for _, connection := range connections {
			require.NoError(c.t, connection.Close())
		}
	}()
	for _, node := range cluster.nodes {
		if node.server == nil {
			continue
		}
		connection, err := grpc.NewClient(node.identity.Public,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(c.t, err)
		connections = append(connections, connection)
		clients[node.identity.GetNameOrDefault()] = proto.NewOxiaClientClient(connection)
	}
	cluster.wait(c, "all live seeds advertise the current shard leaders", func() string {
		shards := cluster.snapshot(c.t)
		for name, client := range clients {
			if reason := cluster.assignmentHealth(c.ctx, client, shards); reason != "" {
				return fmt.Sprintf("seed %s: %s", name, reason)
			}
		}
		return ""
	})
}

func (cluster *scriptCluster) assignmentHealth(ctx context.Context, client proto.OxiaClientClient,
	shards map[int64]*proto.ShardMetadata) string {
	probeCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	stream, err := client.GetShardAssignments(probeCtx,
		&proto.ShardAssignmentsRequest{Namespace: constant.DefaultNamespace})
	if err != nil {
		return fmt.Sprintf("assignment stream not ready: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		return fmt.Sprintf("assignment snapshot not ready: %v", err)
	}
	assignments := response.GetNamespaces()[constant.DefaultNamespace]
	if assignments.GetShardKeyRouter() != proto.ShardKeyRouter_XXHASH3 ||
		assignments.GetKeySorting() != cluster.sorting || len(assignments.GetAssignments()) != len(shards) {
		return "assignment topology, router, or sorting is not current"
	}
	seen := make(map[int64]bool, len(shards))
	for _, assignment := range assignments.Assignments {
		shard := shards[assignment.Shard]
		if shard == nil || seen[assignment.Shard] || !assignmentMatchesShard(assignment, shard) {
			return fmt.Sprintf("shard %d assignment is not current", assignment.Shard)
		}
		seen[assignment.Shard] = true
	}
	return ""
}

func assignmentMatchesShard(assignment *proto.ShardAssignment, shard *proto.ShardMetadata) bool {
	actualRange, expectedRange := assignment.GetInt32HashRange(), shard.GetInt32HashRange()
	return assignment.Leader == shard.GetLeader().Public && actualRange != nil && expectedRange != nil &&
		actualRange.MinHashInclusive == expectedRange.Min && actualRange.MaxHashInclusive == expectedRange.Max
}

func (cluster *scriptCluster) routedShard(t *testing.T, key string, partition *string) (int64, *proto.ShardMetadata) {
	t.Helper()
	route := key
	if partition != nil {
		route = *partition
	}
	hash := uint32(xxh3.HashString(route))
	for id, shard := range cluster.snapshot(t) {
		rangeForShard := shard.GetInt32HashRange()
		if rangeForShard != nil && hash >= rangeForShard.Min && hash <= rangeForShard.Max {
			return id, shard
		}
	}
	t.Fatalf("no shard covers key %q routing hash %d", key, hash)
	return 0, nil
}

func (cluster *scriptCluster) stop(c command, role, key string, partition *string) string {
	c.t.Helper()
	if reason := cluster.health(c.t, 3); reason != "" {
		c.t.Fatalf("stop requires three healthy replicas: %s", reason)
	}
	id, shard := cluster.routedShard(c.t, key, partition)
	name := cluster.selectNode(c, role, shard)
	before := cluster.snapshot(c.t)
	node := cluster.node(name)
	require.NoError(c.t, node.server.Close())
	node.server = nil
	if cluster.updateSeeds != nil {
		cluster.updateSeeds(cluster.liveSeeds())
	}
	cluster.wait(c, "RF3 availability after node stop", func() string {
		if reason := cluster.health(c.t, 2); reason != "" {
			return reason
		}
		return checkStoppedLeaders(name, before, cluster.snapshot(c.t))
	})
	cluster.waitAssignments(c)
	c.t.Logf("stopped %s %s selected by shard %d", role, name, id)
	return name
}

func (cluster *scriptCluster) selectNode(c command, role string, shard *proto.ShardMetadata) string {
	c.t.Helper()
	leader := shard.GetLeader().GetNameOrDefault()
	switch role {
	case "leader":
		return leader
	case "follower":
		for _, node := range cluster.nodes {
			if node.identity.GetNameOrDefault() != leader {
				return node.identity.GetNameOrDefault()
			}
		}
	default:
		c.t.Fatalf("stop role must be leader or follower, got %q", role)
	}
	c.t.Fatal("no follower found")
	return ""
}

func checkStoppedLeaders(name string, before, after map[int64]*proto.ShardMetadata) string {
	for id, old := range before {
		if old.GetLeader().GetNameOrDefault() != name {
			continue
		}
		current := after[id]
		if current.GetLeader().GetNameOrDefault() == name || current.GetTerm() <= old.Term {
			return fmt.Sprintf("shard %d has not elected a different leader above term %d", id, old.Term)
		}
	}
	return ""
}

func (cluster *scriptCluster) restart(c command, name string) {
	c.t.Helper()
	node := cluster.node(name)
	if node == nil || node.server != nil {
		c.t.Fatalf("restart needs a stopped node, got %q", name)
	}
	server, err := dataserver.New(cluster.ctx, commonwatch.New(node.options))
	require.NoError(c.t, err)
	node.server = server
	cluster.wait(c, "RF3 readiness after persistent node restart", func() string {
		return cluster.health(c.t, 3)
	})
	cluster.waitAssignments(c)
	if cluster.updateSeeds != nil {
		cluster.updateSeeds(cluster.liveSeeds())
	}
	c.t.Logf("restarted %s with its original storage and endpoints", name)
}

type replicationTarget struct {
	leader string
	term   int64
	offset int64
}

func (cluster *scriptCluster) waitReplicated(c command) {
	c.t.Helper()
	if reason := cluster.health(c.t, 3); reason != "" {
		c.t.Fatalf("replication barrier requires three healthy replicas: %s", reason)
	}
	targets := cluster.replicationTargets(c)
	// Regular followers learn the commit offset on the next append. Empty
	// writes advertise the captured commit without adding user-visible keys.
	for id, target := range targets {
		cluster.appendBarrier(c, id, target)
	}
	cluster.wait(c, "all replicas applied captured quorum commits", func() string {
		return cluster.replicationHealth(c, targets)
	})
}

func (cluster *scriptCluster) replicationTargets(c command) map[int64]replicationTarget {
	c.t.Helper()
	targets := make(map[int64]replicationTarget, cluster.shards)
	for id, shard := range cluster.snapshot(c.t) {
		name := shard.GetLeader().GetNameOrDefault()
		leader, err := cluster.node(name).server.GetShardDirector().GetLeader(id)
		require.NoError(c.t, err)
		status, err := leader.GetStatus(&proto.GetStatusRequest{Shard: id})
		require.NoError(c.t, err)
		if status.Status != proto.ServingStatus_LEADER || status.Term != shard.Term {
			c.t.Fatalf("shard %d changed term during replication checkpoint", id)
		}
		targets[id] = replicationTarget{leader: name, term: status.Term, offset: status.CommitOffset}
		c.t.Logf("replication checkpoint shard=%d term=%d offset=%d", id, status.Term, status.CommitOffset)
	}
	return targets
}

func (cluster *scriptCluster) appendBarrier(c command, id int64, target replicationTarget) {
	c.t.Helper()
	connection, err := grpc.NewClient(cluster.node(target.leader).identity.Public,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(c.t, err)
	defer func() { require.NoError(c.t, connection.Close()) }()
	response, err := proto.NewOxiaClientClient(connection).Write(c.ctx, &proto.WriteRequest{Shard: &id})
	require.NoError(c.t, err)
	if response == nil || len(response.Puts)+len(response.Deletes)+len(response.DeleteRanges) != 0 {
		c.t.Fatalf("shard %d returned invalid empty-write barrier response: %v", id, response)
	}
}

func (cluster *scriptCluster) replicationHealth(c command, targets map[int64]replicationTarget) string {
	c.t.Helper()
	if reason := cluster.health(c.t, 3); reason != "" {
		return reason
	}
	for id, target := range targets {
		shard := cluster.snapshot(c.t)[id]
		if shard.Term != target.term || shard.GetLeader().GetNameOrDefault() != target.leader {
			c.t.Fatalf("shard %d changed leader or term during replication barrier", id)
		}
		for _, node := range cluster.nodes {
			if reason := replicaApplied(node, id, target); reason != "" {
				return reason
			}
		}
	}
	return ""
}

func replicaApplied(node *scriptNode, id int64, target replicationTarget) string {
	var applied int64
	if node.identity.GetNameOrDefault() == target.leader {
		leader, err := node.server.GetShardDirector().GetLeader(id)
		if err != nil {
			return fmt.Sprintf("shard %d leader unavailable: %v", id, err)
		}
		if leader.Status() != proto.ServingStatus_LEADER || leader.Term() != target.term {
			return fmt.Sprintf("shard %d leader is not serving checkpoint term %d", id, target.term)
		}
		applied = leader.CommitOffset()
	} else {
		follower, err := node.server.GetShardDirector().GetFollower(id)
		if err != nil {
			return fmt.Sprintf("shard %d follower unavailable: %v", id, err)
		}
		if follower.Status() != proto.ServingStatus_FOLLOWER || follower.Term() != target.term {
			return fmt.Sprintf("shard %d follower %s has not accepted the term %d append", id,
				node.identity.GetNameOrDefault(), target.term)
		}
		applied = follower.CommitOffset()
	}
	if applied < target.offset {
		return fmt.Sprintf("shard %d replica %s applied=%d, need >=%d", id,
			node.identity.GetNameOrDefault(), applied, target.offset)
	}
	return ""
}
