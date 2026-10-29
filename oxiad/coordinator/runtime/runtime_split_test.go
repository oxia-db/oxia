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
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/common/sharding"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector/ensemble"
	dataservercontroller "github.com/oxia-db/oxia/oxiad/coordinator/controller/dataserver"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/controller/shard"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
)

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
		splitControllers: map[int64]*shardcontroller.SplitController{},
		rpc:              r.rpc,
	}
	return r
}

// runSplit starts the split controller the way InitiateSplit does, and waits
// for it to report the split as completed or aborted.
func (r *splitTestRuntime) runSplit(t *testing.T, splitTimeout time.Duration) {
	t.Helper()

	r.Lock()
	sc := shardcontroller.NewSplitController(shardcontroller.SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: splitParentShard,
		Metadata:      r.metadata,
		RpcProvider:   r.rpc,
		EventListener: r.runtime,
		Split: &proto.SplitMetadata{
			ChildShardIds: []int64{splitLeftChild, splitRightChild},
			SplitPoint:    splitPoint,
		},
		SplitTimeout: splitTimeout,
	})
	r.splitControllers[splitParentShard] = sc
	r.Unlock()
	t.Cleanup(sc.Close)

	require.Eventually(t, func() bool {
		r.RLock()
		defer r.RUnlock()
		return len(r.splitControllers) == 0
	}, 30*time.Second, 10*time.Millisecond, "split did not finish")
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

func TestRuntimeSplitPublishesConsistentAssignments(t *testing.T) {
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

func TestRuntimeSplitAbortPublishesConsistentAssignments(t *testing.T) {
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

// TestRuntimeRestartInProgressSplitsResumesPastPointOfNoReturn verifies that a coordinator restarting
// with a split past the point of no return completes it. The parent's shard
// controller must not elect the parent again: only the split controller fences
// it, and the parent then gets deleted.
func TestRuntimeRestartInProgressSplitsResumesPastPointOfNoReturn(t *testing.T) {
	r := newSplitTestRuntime(t)

	// The previous coordinator elected the child leaders at the parent's term,
	// then moved the split past the point of no return
	require.NoError(t, r.metadata.PatchNamespaceStatus(constant.DefaultNamespace, func(status *proto.NamespaceStatus) (bool, error) {
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
		return true, nil
	}))

	// The split controller fences the parent and re-elects the children, then
	// the parent's shard controller deletes the parent
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
	// then resumes the split, the way New does
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
	r.restartInProgressSplits(status)
	sc := r.splitControllers[splitParentShard]
	r.Unlock()
	t.Cleanup(func() {
		sc.Close()
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
		return len(r.splitControllers) == 0 && !parentExists
	}, 30*time.Second, 10*time.Millisecond, "split did not complete")

	// Only the split controller fenced the parent
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
		splitControllers: map[int64]*shardcontroller.SplitController{},
	}
	t.Cleanup(func() {
		// No data server answers, so closing the split controller aborts the
		// split, which closes the shard controllers of the children
		r.Lock()
		splitControllers := maps.Clone(r.splitControllers)
		r.Unlock()
		for _, sc := range splitControllers {
			sc.Close()
		}
	})
	return r, racing
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
	assert.Empty(t, r.splitControllers)
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
			assert.False(t, slices.ContainsFunc(older, sharding.SameDataServer(server)),
				"child shard %d has %s in its ensemble", child, server.GetNameOrDefault())
		}
	}
}

// A change of the parent's ensemble that the balancer planned before the split
// started, and that reaches the parent's shard controller after it, is
// rejected: the parent's ensemble doesn't change until the split ends.
func TestRuntimeInitiateSplitRejectsChangeEnsemblePlannedBefore(t *testing.T) {
	r, metadata := newInitiateSplitTestRuntime(t)
	rpc := r.rpc.(*rpctest.RpcProvider)

	// The parent's shard controller verifies its ensemble when it starts
	rpc.GetNode(splitPs1).GetStatusResponse(splitParentTerm, proto.ServingStatus_LEADER, 0, 0)
	rpc.GetNode(splitPs2).GetStatusResponse(splitParentTerm, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(splitPs3).GetStatusResponse(splitParentTerm, proto.ServingStatus_FOLLOWER, 0, 0)
	borrowed, _, err := metadata.GetShardStatus(constant.DefaultNamespace, splitParentShard)
	require.NoError(t, err)
	nsConfig, err := r.namespaceConfigForSplit(constant.DefaultNamespace)
	require.NoError(t, err)
	parentController := shardcontroller.NewController(constant.DefaultNamespace, splitParentShard,
		nsConfig, borrowed.UnsafeBorrow(), metadata,
		feature.NoOpSupplier, r, rpc, time.Hour)
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
	rpc.GetNode(splitPs1).NewTermResponse(splitParentTerm, 10, nil)
	rpc.GetNode(splitPs2).NewTermResponse(splitParentTerm, 9, nil)
	rpc.GetNode(splitPs3).NewTermResponse(splitParentTerm, 9, nil)
	rpc.GetNode(spare).NewTermResponse(-1, -1, nil)
	rpc.GetNode(splitPs1).BecomeLeaderResponse(nil)

	r.dispatchAction(swap)
	_, err = swap.Wait()
	assert.ErrorIs(t, err, errs.ErrUnavailable)

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
			m.patchNamespaceStatus = func(string, func(*proto.NamespaceStatus) (bool, error)) error { return metadataErr }
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
			assert.Empty(t, r.splitControllers)
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
		{"pending change of the ensemble", steady(func(s *proto.ShardMetadata) {
			s.EnsembleChange = &proto.EnsembleChangeMetadata{From: splitPs1, To: splitPs1}
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
