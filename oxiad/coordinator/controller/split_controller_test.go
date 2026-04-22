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

package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"

	"github.com/oxia-db/oxia/common/proto"
)

type mockSplitEventListener struct {
	completions chan splitEvent
	aborts      chan splitEvent
}

type splitEvent struct {
	parentShard int64
	leftChild   int64
	rightChild  int64
}

func newMockSplitEventListener() *mockSplitEventListener {
	return &mockSplitEventListener{
		completions: make(chan splitEvent, 10),
		aborts:      make(chan splitEvent, 10),
	}
}

func (m *mockSplitEventListener) SplitComplete(parentShard int64, leftChild int64, rightChild int64) {
	m.completions <- splitEvent{parentShard, leftChild, rightChild}
}

func (m *mockSplitEventListener) SplitAborted(parentShard int64, leftChild int64, rightChild int64) {
	m.aborts <- splitEvent{parentShard, leftChild, rightChild}
}

var (
	ps1 = model.Server{Public: "ps1:9091", Internal: "ps1:8191"}
	ps2 = model.Server{Public: "ps2:9091", Internal: "ps2:8191"}
	ps3 = model.Server{Public: "ps3:9091", Internal: "ps3:8191"}
	ls1 = model.Server{Public: "ls1:9091", Internal: "ls1:8191"}
	ls2 = model.Server{Public: "ls2:9091", Internal: "ls2:8191"}
	ls3 = model.Server{Public: "ls3:9091", Internal: "ls3:8191"}
	rs1 = model.Server{Public: "rs1:9091", Internal: "rs1:8191"}
	rs2 = model.Server{Public: "rs2:9091", Internal: "rs2:8191"}
	rs3 = model.Server{Public: "rs3:9091", Internal: "rs3:8191"}
)

// setupSplitTest creates a cluster status with a parent shard in SteadyState,
// two child shards ready for split, and returns the test resources.
func setupSplitTest(t *testing.T, phase model.SplitPhase) (
	*mockRpcProvider,
	coordmetadata.Metadata,
	*mockSplitEventListener,
) {
	t.Helper()

	rpcMock := newMockRpcProvider()
	metaProvider := memory.NewProvider()
	metadata := coordmetadata.New(t.Context(), metaProvider, func() (*proto.ClusterConfiguration, error) {
		return &proto.ClusterConfiguration{}, nil
	}, nil)
	t.Cleanup(func() { assert.NoError(t, metadata.Close()) })

	clusterStatus := &model.ClusterStatus{
		Namespaces: map[string]model.NamespaceStatus{
			constant.DefaultNamespace: {
				ReplicationFactor: 3,
				Shards: map[int64]model.ShardMetadata{
					0: {
						Status:   model.ShardStatusSteadyState,
						Term:     5,
						Leader:   &ps1,
						Ensemble: []model.Server{ps1, ps2, ps3},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: 1000,
						},
						Split: &model.SplitMetadata{
							Phase:         phase,
							ChildShardIDs: []int64{1, 2},
							SplitPoint:    500,
						},
					},
					1: {
						Status:   model.ShardStatusSteadyState,
						Term:     0,
						Ensemble: []model.Server{ls1, ls2, ls3},
						Int32HashRange: model.Int32HashRange{
							Min: 0,
							Max: 500,
						},
						Split: &model.SplitMetadata{
							Phase:         phase,
							ParentShardId: 0,
							SplitPoint:    500,
						},
					},
					2: {
						Status:   model.ShardStatusSteadyState,
						Term:     0,
						Ensemble: []model.Server{rs1, rs2, rs3},
						Int32HashRange: model.Int32HashRange{
							Min: 501,
							Max: 1000,
						},
						Split: &model.SplitMetadata{
							Phase:         phase,
							ParentShardId: 0,
							SplitPoint:    500,
						},
					},
				},
			},
		},
		ShardIdGenerator: 3,
	}

	metadata.UpdateStatus(clusterStatus)
	listener := newMockSplitEventListener()

	return rpcMock, metadata, listener
}

// queueBootstrapResponses queues all responses needed for the bootstrap phase.
// The *1 nodes report a higher offset so pickLeader deterministically chooses them.
func queueBootstrapResponses(rpcMock *mockRpcProvider) {
	// Fence left child ensemble: ls1 has highest offset -> becomes leader
	rpcMock.GetNode(ls1).NewTermResponse(0, 0, nil)
	rpcMock.GetNode(ls2).NewTermResponse(0, -1, nil)
	rpcMock.GetNode(ls3).NewTermResponse(0, -1, nil)
	// BecomeLeader for left child
	rpcMock.GetNode(ls1).BecomeLeaderResponse(nil)

	// Fence right child ensemble: rs1 has highest offset -> becomes leader
	rpcMock.GetNode(rs1).NewTermResponse(0, 0, nil)
	rpcMock.GetNode(rs2).NewTermResponse(0, -1, nil)
	rpcMock.GetNode(rs3).NewTermResponse(0, -1, nil)
	// BecomeLeader for right child
	rpcMock.GetNode(rs1).BecomeLeaderResponse(nil)

	// AddFollower on parent leader for each child leader (observer)
	rpcMock.GetNode(ps1).AddFollowerResponse(nil)
	rpcMock.GetNode(ps1).AddFollowerResponse(nil)
}

// queueCatchUpResponses queues all responses needed for the catch-up phase.
func queueCatchUpResponses(rpcMock *mockRpcProvider) {
	// Parent commitOffset as target
	rpcMock.GetNode(ps1).GetStatusResponse(5, proto.ServingStatus_LEADER, 105, 105)
	// Children commitOffset >= target
	rpcMock.GetNode(ls1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)
	rpcMock.GetNode(rs1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)
}

// queueCutoverResponses queues all responses needed for the cutover phase.
func queueCutoverResponses(rpcMock *mockRpcProvider) {
	// Fence parent ensemble
	rpcMock.GetNode(ps1).NewTermResponse(5, 105, nil)
	rpcMock.GetNode(ps2).NewTermResponse(5, 105, nil)
	rpcMock.GetNode(ps3).NewTermResponse(5, 105, nil)

	// Children commit offset >= parentFinalOffset
	rpcMock.GetNode(ls1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)
	rpcMock.GetNode(rs1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)

	// Re-elect children in clean term
	rpcMock.GetNode(ls1).NewTermResponse(1, 106, nil)
	rpcMock.GetNode(ls2).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(ls3).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(rs1).NewTermResponse(1, 106, nil)
	rpcMock.GetNode(rs2).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(rs3).NewTermResponse(1, 105, nil)

	rpcMock.GetNode(ls1).BecomeLeaderResponse(nil)
	rpcMock.GetNode(rs1).BecomeLeaderResponse(nil)
}

// queueCleanupResponses queues all responses needed for the cleanup phase.
func TestSplitController_FullLifecycle(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseBootstrap)

	queueBootstrapResponses(rpcMock)
	queueCatchUpResponses(rpcMock)
	queueCutoverResponses(rpcMock)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
	})
	defer sc.Close()

	// Wait for split completion
	select {
	case completion := <-listener.completions:
		assert.Equal(t, int64(0), completion.parentShard)
		assert.Equal(t, int64(1), completion.leftChild)
		assert.Equal(t, int64(2), completion.rightChild)
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete in time")
	}

	// Verify final state: parent is Deleting, children have no split metadata
	status := statusRes.LoadStatus()
	ns := status.Namespaces[constant.DefaultNamespace]

	parentMeta, parentExists := ns.Shards[0]
	require.True(t, parentExists)
	assert.Equal(t, model.ShardStatusDeleting, parentMeta.Status)
	assert.Nil(t, parentMeta.Split, "parent split metadata should be cleared")

	leftMeta, leftExists := ns.Shards[1]
	require.True(t, leftExists)
	assert.Nil(t, leftMeta.Split, "left child split metadata should be cleared")

	rightMeta, rightExists := ns.Shards[2]
	require.True(t, rightExists)
	assert.Nil(t, rightMeta.Split, "right child split metadata should be cleared")
}

func TestSplitController_ResumeFromBootstrap(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseBootstrap)

	queueBootstrapResponses(rpcMock)
	queueCatchUpResponses(rpcMock)
	queueCutoverResponses(rpcMock)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
	})
	defer sc.Close()

	select {
	case completion := <-listener.completions:
		assert.Equal(t, int64(0), completion.parentShard)
		assert.Equal(t, int64(1), completion.leftChild)
		assert.Equal(t, int64(2), completion.rightChild)
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete in time")
	}
}

func TestSplitController_ResumeFromCatchUp(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseCatchUp)

	// For CatchUp, we need child leaders to already be set (they were set during bootstrap)
	// Update child metadata to have leaders
	status := statusRes.LoadStatus()
	cloned := status.Clone()
	ns := cloned.Namespaces[constant.DefaultNamespace]
	leftMeta := ns.Shards[1]
	leftMeta.Leader = &ls1
	leftMeta.Term = 1
	ns.Shards[1] = leftMeta
	rightMeta := ns.Shards[2]
	rightMeta.Leader = &rs1
	rightMeta.Term = 1
	ns.Shards[2] = rightMeta
	statusRes.UpdateStatus(cloned)

	queueCatchUpResponses(rpcMock)
	queueCutoverResponses(rpcMock)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
	})
	defer sc.Close()

	select {
	case completion := <-listener.completions:
		assert.Equal(t, int64(0), completion.parentShard)
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete in time")
	}
}

func TestSplitController_PhaseTransitions(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseBootstrap)

	queueBootstrapResponses(rpcMock)
	queueCatchUpResponses(rpcMock)
	queueCutoverResponses(rpcMock)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
	})
	defer sc.Close()

	// Wait for completion
	select {
	case <-listener.completions:
		// Verify final state: parent marked Deleting, children elected
		status := statusRes.LoadStatus()
		ns := status.Namespaces[constant.DefaultNamespace]

		parentMeta := ns.Shards[0]
		assert.Equal(t, model.ShardStatusDeleting, parentMeta.Status)

		leftMeta := ns.Shards[1]
		assert.NotNil(t, leftMeta.Leader)
		assert.Equal(t, model.ShardStatusSteadyState, leftMeta.Status)

		rightMeta := ns.Shards[2]
		assert.NotNil(t, rightMeta.Leader)
		assert.Equal(t, model.ShardStatusSteadyState, rightMeta.Status)
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete in time")
	}
}

// --- Abort / Timeout Tests ---

func TestSplitController_TimeoutDuringBootstrap(t *testing.T) {
	// Don't queue any RPC responses -- bootstrap will hang on fenceEnsemble.
	_, statusRes, listener := setupSplitTest(t, model.SplitPhaseBootstrap)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   newMockRpcProvider(), // fresh mock, no responses queued
		EventListener: listener,
		SplitTimeout:  2 * time.Second,
	})
	defer sc.Close()

	// Wait for abort
	select {
	case evt := <-listener.aborts:
		assert.Equal(t, int64(0), evt.parentShard)
		assert.Equal(t, int64(1), evt.leftChild)
		assert.Equal(t, int64(2), evt.rightChild)
	case <-time.After(10 * time.Second):
		t.Fatal("Split was not aborted in time")
	}

	// Verify: parent.Split cleared, children deleted
	status := statusRes.LoadStatus()
	ns := status.Namespaces[constant.DefaultNamespace]

	parentMeta, parentExists := ns.Shards[0]
	assert.True(t, parentExists, "parent should still exist")
	assert.Nil(t, parentMeta.Split, "parent split metadata should be cleared")

	_, leftExists := ns.Shards[1]
	assert.False(t, leftExists, "left child should be deleted")

	_, rightExists := ns.Shards[2]
	assert.False(t, rightExists, "right child should be deleted")
}

func TestSplitController_TimeoutDuringCatchUp(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseCatchUp)

	// Set child leaders (as if Bootstrap completed) and ParentTermAtBootstrap
	status := statusRes.LoadStatus()
	cloned := status.Clone()
	ns := cloned.Namespaces[constant.DefaultNamespace]

	leftMeta := ns.Shards[1]
	leftMeta.Term = 1
	leftMeta.Leader = &ls1
	ns.Shards[1] = leftMeta

	rightMeta := ns.Shards[2]
	rightMeta.Term = 1
	rightMeta.Leader = &rs1
	ns.Shards[2] = rightMeta

	parentMeta := ns.Shards[0]
	parentMeta.Split.ParentTermAtBootstrap = 5
	ns.Shards[0] = parentMeta

	statusRes.UpdateStatus(cloned)

	// Queue RemoveObserver responses (abort will call these)
	rpcMock.GetNode(ps1).RemoveObserverResponse(nil)
	rpcMock.GetNode(ps1).RemoveObserverResponse(nil)

	// Don't queue CatchUp responses -- it will hang and timeout

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
		SplitTimeout:  2 * time.Second,
	})
	defer sc.Close()

	// Wait for abort
	select {
	case evt := <-listener.aborts:
		assert.Equal(t, int64(0), evt.parentShard)
	case <-time.After(10 * time.Second):
		t.Fatal("Split was not aborted in time")
	}

	// Verify: parent restored, children deleted
	finalStatus := statusRes.LoadStatus()
	finalNs := finalStatus.Namespaces[constant.DefaultNamespace]

	pm, exists := finalNs.Shards[0]
	assert.True(t, exists)
	assert.Nil(t, pm.Split)

	_, leftExists := finalNs.Shards[1]
	assert.False(t, leftExists)
}

// --- Parent Leader Election Tests ---

func TestSplitController_ParentTermChangeDuringCatchUp(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseCatchUp)

	// Set child leaders and ParentTermAtBootstrap = 5
	status := statusRes.LoadStatus()
	cloned := status.Clone()
	ns := cloned.Namespaces[constant.DefaultNamespace]

	leftMeta := ns.Shards[1]
	leftMeta.Term = 1
	leftMeta.Leader = &ls1
	ns.Shards[1] = leftMeta

	rightMeta := ns.Shards[2]
	rightMeta.Term = 1
	rightMeta.Leader = &rs1
	ns.Shards[2] = rightMeta

	// Simulate parent leader election: term changed from 5 to 6
	parentMeta := ns.Shards[0]
	parentMeta.Term = 6 // Changed!
	parentMeta.Leader = &ps2
	parentMeta.Split.ParentTermAtBootstrap = 5 // Was 5 during Bootstrap
	ns.Shards[0] = parentMeta

	statusRes.UpdateStatus(cloned)

	// The controller should detect term change and reset to Bootstrap.
	// Queue Bootstrap responses -- note: parent leader is now ps2 (after election)
	// Child fencing: ls1/rs1 have higher offset -> deterministically become leader
	rpcMock.GetNode(ls1).NewTermResponse(1, 101, nil)
	rpcMock.GetNode(ls2).NewTermResponse(1, 100, nil)
	rpcMock.GetNode(ls3).NewTermResponse(1, 100, nil)
	// BecomeLeader for left child
	rpcMock.GetNode(ls1).BecomeLeaderResponse(nil)

	rpcMock.GetNode(rs1).NewTermResponse(1, 101, nil)
	rpcMock.GetNode(rs2).NewTermResponse(1, 100, nil)
	rpcMock.GetNode(rs3).NewTermResponse(1, 100, nil)
	// BecomeLeader for right child
	rpcMock.GetNode(rs1).BecomeLeaderResponse(nil)

	// AddFollower on ps2 (new parent leader)
	rpcMock.GetNode(ps2).AddFollowerResponse(nil)
	rpcMock.GetNode(ps2).AddFollowerResponse(nil)

	// CatchUp: parent on ps2, children caught up
	rpcMock.GetNode(ps2).GetStatusResponse(6, proto.ServingStatus_LEADER, 105, 105)
	rpcMock.GetNode(ls1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)
	rpcMock.GetNode(rs1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)

	queueCutoverResponses(rpcMock)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
		SplitTimeout:  30 * time.Second,
	})
	defer sc.Close()

	// Should complete successfully after re-bootstrap
	select {
	case <-listener.completions:
		// Success -- split recovered from parent election
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete after parent term change")
	}
}

// --- Child Failure Tests ---

func TestSplitController_ChildFencingPartialSuccess(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseBootstrap)

	// Left child: only 2/3 respond (quorum OK)
	// ls1 has higher offset -> deterministically becomes leader
	rpcMock.GetNode(ls1).NewTermResponse(1, 101, nil)
	rpcMock.GetNode(ls2).NewTermResponse(1, 100, nil)
	rpcMock.GetNode(ls3).NewTermResponse(0, 0, errors.New("connection refused"))
	// BecomeLeader for left child
	rpcMock.GetNode(ls1).BecomeLeaderResponse(nil)

	// Right child: all 3 respond, rs1 has higher offset
	rpcMock.GetNode(rs1).NewTermResponse(1, 101, nil)
	rpcMock.GetNode(rs2).NewTermResponse(1, 100, nil)
	rpcMock.GetNode(rs3).NewTermResponse(1, 100, nil)
	// BecomeLeader for right child
	rpcMock.GetNode(rs1).BecomeLeaderResponse(nil)

	// AddFollower on parent for each child
	rpcMock.GetNode(ps1).AddFollowerResponse(nil)
	rpcMock.GetNode(ps1).AddFollowerResponse(nil)

	queueCatchUpResponses(rpcMock)
	queueCutoverResponses(rpcMock)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
		SplitTimeout:  30 * time.Second,
	})
	defer sc.Close()

	select {
	case <-listener.completions:
		// Success -- 2/3 quorum was sufficient
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete with partial child fencing")
	}
}

func TestSplitController_ParentFencingPartialFailure(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseBootstrap)

	queueBootstrapResponses(rpcMock)
	queueCatchUpResponses(rpcMock)

	// Cutover: parent fencing with 1/3 failure (still quorum)
	rpcMock.GetNode(ps1).NewTermResponse(5, 105, nil)
	rpcMock.GetNode(ps2).NewTermResponse(5, 105, nil)
	rpcMock.GetNode(ps3).NewTermResponse(0, 0, errors.New("connection refused"))

	// Children reach final offset
	rpcMock.GetNode(ls1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)
	rpcMock.GetNode(rs1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)

	// Re-elect children in clean term
	rpcMock.GetNode(ls1).NewTermResponse(1, 106, nil)
	rpcMock.GetNode(ls2).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(ls3).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(rs1).NewTermResponse(1, 106, nil)
	rpcMock.GetNode(rs2).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(rs3).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(ls1).BecomeLeaderResponse(nil)
	rpcMock.GetNode(rs1).BecomeLeaderResponse(nil)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
		SplitTimeout:  30 * time.Second,
	})
	defer sc.Close()

	select {
	case <-listener.completions:
		// Success -- parent fencing with 2/3 quorum was sufficient
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete with partial parent fencing")
	}
}

func TestSplitController_ChildQuorumCommitRetry(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseBootstrap)

	queueBootstrapResponses(rpcMock)
	queueCatchUpResponses(rpcMock)

	// Cutover
	rpcMock.GetNode(ps1).NewTermResponse(5, 105, nil)
	rpcMock.GetNode(ps2).NewTermResponse(5, 105, nil)
	rpcMock.GetNode(ps3).NewTermResponse(5, 105, nil)

	// First commit check: left child not caught up yet
	rpcMock.GetNode(ls1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 50)

	// Second commit check: left child caught up
	rpcMock.GetNode(ls1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)

	// Right child commits immediately
	rpcMock.GetNode(rs1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)

	// Re-elect children in clean term
	rpcMock.GetNode(ls1).NewTermResponse(1, 106, nil)
	rpcMock.GetNode(ls2).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(ls3).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(rs1).NewTermResponse(1, 106, nil)
	rpcMock.GetNode(rs2).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(rs3).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(ls1).BecomeLeaderResponse(nil)
	rpcMock.GetNode(rs1).BecomeLeaderResponse(nil)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
		SplitTimeout:  30 * time.Second,
	})
	defer sc.Close()

	select {
	case <-listener.completions:
		// Success -- quorum commit retried and succeeded
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete after quorum commit retry")
	}
}

func TestSplitController_ChildLeaderDiesTimesOutAndAborts(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseCatchUp)

	// Set child leaders (as if Bootstrap completed) and ParentTermAtBootstrap
	status := statusRes.LoadStatus()
	cloned := status.Clone()
	ns := cloned.Namespaces[constant.DefaultNamespace]

	leftMeta := ns.Shards[1]
	leftMeta.Term = 1
	leftMeta.Leader = &ls1
	ns.Shards[1] = leftMeta

	rightMeta := ns.Shards[2]
	rightMeta.Term = 1
	rightMeta.Leader = &rs1
	ns.Shards[2] = rightMeta

	parentMeta := ns.Shards[0]
	parentMeta.Split.ParentTermAtBootstrap = 5
	ns.Shards[0] = parentMeta

	statusRes.UpdateStatus(cloned)

	// Queue RemoveObserver responses (abort will call these)
	rpcMock.GetNode(ps1).RemoveObserverResponse(nil)
	rpcMock.GetNode(ps1).RemoveObserverResponse(nil)

	// Simulate dead child leader: GetStatus returns errors
	rpcMock.GetNode(ps1).GetStatusResponse(5, proto.ServingStatus_LEADER, 100, 100) // parent OK
	rpcMock.GetNode(ls1).EnqueueGetStatusError(errors.New("connection refused"))    // child-L dead

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
		SplitTimeout:  2 * time.Second,
	})
	defer sc.Close()

	// Should time out and abort
	select {
	case evt := <-listener.aborts:
		assert.Equal(t, int64(0), evt.parentShard)
	case <-listener.completions:
		t.Fatal("Split should have been aborted, not completed")
	case <-time.After(10 * time.Second):
		t.Fatal("Split was not aborted in time")
	}

	// Verify: parent restored, children deleted
	finalStatus := statusRes.LoadStatus()
	finalNs := finalStatus.Namespaces[constant.DefaultNamespace]

	pm, exists := finalNs.Shards[0]
	assert.True(t, exists)
	assert.Nil(t, pm.Split)

	_, leftExists := finalNs.Shards[1]
	assert.False(t, leftExists)
	_, rightExists := finalNs.Shards[2]
	assert.False(t, rightExists)
}

func TestSplitController_ChildFollowersDeadCommitStalls(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseCatchUp)

	// Set child leaders (as if Bootstrap completed) and ParentTermAtBootstrap
	status := statusRes.LoadStatus()
	cloned := status.Clone()
	ns := cloned.Namespaces[constant.DefaultNamespace]

	leftMeta := ns.Shards[1]
	leftMeta.Term = 1
	leftMeta.Leader = &ls1
	ns.Shards[1] = leftMeta

	rightMeta := ns.Shards[2]
	rightMeta.Term = 1
	rightMeta.Leader = &rs1
	ns.Shards[2] = rightMeta

	parentMeta := ns.Shards[0]
	parentMeta.Split.ParentTermAtBootstrap = 5
	ns.Shards[0] = parentMeta

	statusRes.UpdateStatus(cloned)

	// Queue RemoveObserver responses (abort will call these)
	rpcMock.GetNode(ps1).RemoveObserverResponse(nil)
	rpcMock.GetNode(ps1).RemoveObserverResponse(nil)

	// Parent status: commitOffset=100
	rpcMock.GetNode(ps1).GetStatusResponse(5, proto.ServingStatus_LEADER, 100, 100)

	// Child leaders are alive but headOffset advances while commitOffset stays
	// stuck at -1 (both followers are dead, no quorum ack)
	for i := 0; i < 10; i++ {
		rpcMock.GetNode(ls1).GetStatusResponse(1, proto.ServingStatus_LEADER, 100, -1) // headOffset=100, commitOffset=-1
		rpcMock.GetNode(rs1).GetStatusResponse(1, proto.ServingStatus_LEADER, 100, -1)
	}

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
		SplitTimeout:  2 * time.Second,
	})
	defer sc.Close()

	// Should time out and abort because children can't reach quorum commit
	select {
	case evt := <-listener.aborts:
		assert.Equal(t, int64(0), evt.parentShard)
	case <-listener.completions:
		t.Fatal("Split should have been aborted, not completed")
	case <-time.After(10 * time.Second):
		t.Fatal("Split was not aborted in time")
	}

	// Verify: parent restored, children deleted
	finalStatus := statusRes.LoadStatus()
	finalNs := finalStatus.Namespaces[constant.DefaultNamespace]

	pm, exists := finalNs.Shards[0]
	assert.True(t, exists)
	assert.Nil(t, pm.Split)

	_, leftExists := finalNs.Shards[1]
	assert.False(t, leftExists)
	_, rightExists := finalNs.Shards[2]
	assert.False(t, rightExists)
}

func TestSplitController_ChildLeaderChangeDuringCatchUp(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseCatchUp)

	// Set child leaders and ParentTermAtBootstrap as if Bootstrap completed.
	// Also set ChildLeadersAtBootstrap with the ORIGINAL leaders.
	status := statusRes.LoadStatus()
	cloned := status.Clone()
	ns := cloned.Namespaces[constant.DefaultNamespace]

	leftMeta := ns.Shards[1]
	leftMeta.Term = 2      // term=2 because the shard controller re-elected
	leftMeta.Leader = &ls2 // NEW leader (was ls1 at bootstrap)
	ns.Shards[1] = leftMeta

	rightMeta := ns.Shards[2]
	rightMeta.Term = 1
	rightMeta.Leader = &rs1
	ns.Shards[2] = rightMeta

	parentMeta := ns.Shards[0]
	parentMeta.Split.ParentTermAtBootstrap = 5
	parentMeta.Split.ChildLeadersAtBootstrap = map[int64]string{
		1: ls1.Internal, // OLD leader
		2: rs1.Internal,
	}
	ns.Shards[0] = parentMeta

	statusRes.UpdateStatus(cloned)

	// CatchUp detects left child leader changed (ls1 -> ls2).
	// It RemoveObserver(old leader) on parent, then falls back to Bootstrap.
	rpcMock.GetNode(ps1).RemoveObserverResponse(nil) // RemoveObserver for stale observer

	// Bootstrap re-run: children already have leaders (skipped).
	// Step 2: re-read parent (still has leader), AddFollower for new child leaders.
	rpcMock.GetNode(ps1).AddFollowerResponse(nil) // AddFollower for new left child leader (ls2)
	rpcMock.GetNode(ps1).AddFollowerResponse(nil) // AddFollower for right child leader (rs1, unchanged)

	// CatchUp (second attempt): parent and children caught up.
	rpcMock.GetNode(ps1).GetStatusResponse(5, proto.ServingStatus_LEADER, 105, 105)
	rpcMock.GetNode(ls2).GetStatusResponse(2, proto.ServingStatus_LEADER, 105, 105) // ls2 is now the leader
	rpcMock.GetNode(rs1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)

	// Cutover: fence parent
	rpcMock.GetNode(ps1).NewTermResponse(5, 105, nil)
	rpcMock.GetNode(ps2).NewTermResponse(5, 105, nil)
	rpcMock.GetNode(ps3).NewTermResponse(5, 105, nil)

	// Cutover: wait for children commit (using current leaders)
	rpcMock.GetNode(ls2).GetStatusResponse(2, proto.ServingStatus_LEADER, 105, 105)
	rpcMock.GetNode(rs1).GetStatusResponse(1, proto.ServingStatus_LEADER, 105, 105)

	// Cutover: re-elect children in clean term
	rpcMock.GetNode(ls1).NewTermResponse(2, 105, nil)
	rpcMock.GetNode(ls2).NewTermResponse(2, 106, nil) // ls2 has higher offset
	rpcMock.GetNode(ls3).NewTermResponse(2, 105, nil)
	rpcMock.GetNode(rs1).NewTermResponse(1, 106, nil)
	rpcMock.GetNode(rs2).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(rs3).NewTermResponse(1, 105, nil)
	rpcMock.GetNode(ls2).BecomeLeaderResponse(nil)
	rpcMock.GetNode(rs1).BecomeLeaderResponse(nil)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
		SplitTimeout:  30 * time.Second,
	})
	defer sc.Close()

	select {
	case <-listener.completions:
		// Success — split recovered from child leader change and completed
	case <-listener.aborts:
		t.Fatal("Split should not have been aborted")
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete in time")
	}

	// Verify parent is Deleting and children are healthy
	finalStatus := statusRes.LoadStatus()
	finalNs := finalStatus.Namespaces[constant.DefaultNamespace]

	pm := finalNs.Shards[0]
	assert.Equal(t, model.ShardStatusDeleting, pm.Status)

	lm := finalNs.Shards[1]
	assert.Nil(t, lm.Split)
	assert.NotNil(t, lm.Leader)

	rm := finalNs.Shards[2]
	assert.Nil(t, rm.Split)
	assert.NotNil(t, rm.Leader)
}

func TestSplitController_ChildEnsembleMemberDiesDuringBootstrap(t *testing.T) {
	rpcMock, statusRes, listener := setupSplitTest(t, model.SplitPhaseBootstrap)

	// Left child: 2/3 respond, 1 dead — quorum still met.
	// ls1 has higher offset so pickLeader deterministically chooses it.
	rpcMock.GetNode(ls1).NewTermResponse(0, 0, nil)
	rpcMock.GetNode(ls2).NewTermResponse(0, -1, nil)
	rpcMock.GetNode(ls3).NewTermResponse(0, 0, errors.New("connection refused"))
	rpcMock.GetNode(ls1).BecomeLeaderResponse(nil)

	// Right child: 2/3 respond, 1 dead.
	// rs1 has higher offset so pickLeader deterministically chooses it.
	rpcMock.GetNode(rs1).NewTermResponse(0, 0, nil)
	rpcMock.GetNode(rs2).NewTermResponse(0, 0, errors.New("connection refused"))
	rpcMock.GetNode(rs3).NewTermResponse(0, -1, nil)
	rpcMock.GetNode(rs1).BecomeLeaderResponse(nil)

	// AddFollower on parent for each child
	rpcMock.GetNode(ps1).AddFollowerResponse(nil)
	rpcMock.GetNode(ps1).AddFollowerResponse(nil)

	// Queue CatchUp and Cutover responses for completion
	queueCatchUpResponses(rpcMock)
	queueCutoverResponses(rpcMock)

	sc := NewSplitController(SplitControllerConfig{
		Namespace:     constant.DefaultNamespace,
		ParentShardId: 0,
		Metadata:      statusRes,
		RpcProvider:   rpcMock,
		EventListener: listener,
		SplitTimeout:  30 * time.Second,
	})
	defer sc.Close()

	select {
	case <-listener.completions:
		// Success — split completed despite dead ensemble members
	case <-listener.aborts:
		t.Fatal("Split should not have been aborted")
	case <-time.After(30 * time.Second):
		t.Fatal("Split did not complete in time")
	}
}
