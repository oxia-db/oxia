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
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/mockutils"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/shard"
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

// recomputingMetadata recomputes the shard assignments after every cluster
// status write, the way an unrelated shard's leader election or a
// configuration reconcile can at any moment.
type recomputingMetadata struct {
	coordmetadata.Metadata
	afterWrite func()
}

func (m *recomputingMetadata) UpdateNamespaceStatus(name string, status *proto.NamespaceStatus) error {
	err := m.Metadata.UpdateNamespaceStatus(name, status)
	m.afterWrite()
	return err
}

func (m *recomputingMetadata) UpdateShardStatus(namespace string, shard int64, shardMetadata *proto.ShardMetadata) error {
	err := m.Metadata.UpdateShardStatus(namespace, shard, shardMetadata)
	m.afterWrite()
	return err
}

func (m *recomputingMetadata) UpdateShardStatuses(namespace string, shardsMetadata map[int64]*proto.ShardMetadata) error {
	err := m.Metadata.UpdateShardStatuses(namespace, shardsMetadata)
	m.afterWrite()
	return err
}

func (m *recomputingMetadata) DeleteShardStatus(namespace string, shard int64) error {
	err := m.Metadata.DeleteShardStatus(namespace, shard)
	m.afterWrite()
	return err
}

// splitTestRuntime is a runtime whose shard assignments are recomputed after
// every cluster status write, recording each published snapshot.
type splitTestRuntime struct {
	*runtime
	rpc *mockutils.RpcProvider

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
	_, err := metadata.ReserveShardIDs(4)
	require.NoError(t, err)
	parentEnsemble := []*proto.DataServerIdentity{splitPs1, splitPs2, splitPs3}
	childSplit := &proto.SplitMetadata{
		Phase:         proto.SplitPhaseBootstrap,
		ParentShardId: splitParentShard,
		SplitPoint:    splitPoint,
	}
	require.True(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
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

	r := &splitTestRuntime{rpc: mockutils.NewRpcProvider()}
	r.runtime = &runtime{
		logger: slog.With(slog.String("component", "coordinator")),
		metadata: &recomputingMetadata{
			Metadata: metadata,
			afterWrite: func() {
				// An unrelated shard's election completing
				r.LeaderElected(splitOtherShard, splitPs2, nil)
				r.snapshotsLock.Lock()
				r.snapshots = append(r.snapshots, r.assignmentsWatch.Load())
				r.snapshotsLock.Unlock()
			},
		},
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
		shardControllers: map[int64]shardcontroller.Controller{},
		splitControllers: map[int64]*shardcontroller.SplitController{},
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
		SplitTimeout:  splitTimeout,
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
		r.rpc.GetNode(child[0]).BecomeLeaderResponse(nil)
	}
	r.rpc.GetNode(splitPs1).AddFollowerResponse(nil)
	r.rpc.GetNode(splitPs1).AddFollowerResponse(nil)

	// CatchUp
	r.rpc.GetNode(splitPs1).GetStatusResponse(splitParentTerm, proto.ServingStatus_LEADER, 105, 105)
	r.rpc.GetNode(splitLs1).GetStatusResponse(splitParentTerm, proto.ServingStatus_LEADER, 105, 105)
	r.rpc.GetNode(splitRs1).GetStatusResponse(splitParentTerm, proto.ServingStatus_LEADER, 105, 105)

	// Cutover: freeze the parent, wait for the children to receive its tail,
	// fence the parent and re-elect the children in a clean term
	r.rpc.GetNode(splitPs1).FreezeShardResponse(105, nil)
	r.rpc.GetNode(splitLs1).GetStatusResponse(splitParentTerm, proto.ServingStatus_LEADER, 105, 105)
	r.rpc.GetNode(splitRs1).GetStatusResponse(splitParentTerm, proto.ServingStatus_LEADER, 105, 105)
	for _, node := range []*proto.DataServerIdentity{
		splitPs1, splitPs2, splitPs3, splitLs1, splitLs2, splitLs3, splitRs1, splitRs2, splitRs3,
	} {
		r.rpc.GetNode(node).NewTermResponse(splitParentTerm, 105, nil)
	}
	r.rpc.GetNode(splitLs1).BecomeLeaderResponse(nil)
	r.rpc.GetNode(splitRs1).BecomeLeaderResponse(nil)
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

func TestSplit_PublishedAssignmentsAreConsistent(t *testing.T) {
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

func TestSplit_AbortPublishesConsistentAssignments(t *testing.T) {
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
