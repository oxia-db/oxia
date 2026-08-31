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

package shard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/mockutils"
)

func splitParentMetadata() *proto.ShardMetadata {
	return &proto.ShardMetadata{
		Status:         proto.ShardStatusSteadyState,
		Term:           1,
		Leader:         ps1,
		Ensemble:       []*proto.DataServerIdentity{ps1, ps2, ps3},
		Int32HashRange: &proto.HashRange{Min: 0, Max: 99},
	}
}

func TestSplitterMergesChildrenIntoLatestNamespaceStatus(t *testing.T) {
	metadata := newTestMetadata(
		t,
		memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		nil,
	)
	parentShardID := metadata.ReserveShardIDs(1)
	storeTestShardMetadata(
		t,
		metadata,
		constant.DefaultNamespace,
		parentShardID,
		namespaceConfig,
		splitParentMetadata(),
	)
	unrelatedShardID := metadata.ReserveShardIDs(1)
	metadata.UpdateShardStatus(constant.DefaultNamespace, unrelatedShardID, &proto.ShardMetadata{
		Status:         proto.ShardStatusSteadyState,
		Term:           1,
		Int32HashRange: &proto.HashRange{Min: 100, Max: 199},
	})

	listener := newMockShardSplitEventListener()
	selectorCalls := 0
	selector := func(
		namespace string,
		_ int64,
		status map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	) ([]*proto.DataServerIdentity, error) {
		selectorCalls++
		if selectorCalls == 2 {
			assert.Len(t, status[namespace].UnsafeBorrow().Shards, 3, "left child must affect right-child placement")
			unrelated := requireShardMetadata(t, metadata, constant.DefaultNamespace, unrelatedShardID)
			unrelated.Term = 2
			metadata.UpdateShardStatus(constant.DefaultNamespace, unrelatedShardID, unrelated)
			return []*proto.DataServerIdentity{rs1, rs2, rs3}, nil
		}
		return []*proto.DataServerIdentity{ls1, ls2, ls3}, nil
	}

	splitter := NewSplitter(
		constant.DefaultNamespace,
		parentShardID,
		metadata,
		SplitterConfig{
			EnsembleSelector: selector,
			EventListener:    listener,
		},
	)
	leftChild, rightChild, err := splitter.Split(nil)
	require.NoError(t, err)
	assert.Equal(t, leftChild+1, rightChild)
	assert.Equal(t, 2, selectorCalls)

	parent := requireShardMetadata(t, metadata, constant.DefaultNamespace, parentShardID)
	require.NotNil(t, parent.Split)
	assert.Equal(t, []int64{leftChild, rightChild}, parent.Split.ChildShardIds)
	assert.Equal(t, uint32(49), parent.Split.SplitPoint)

	left := requireShardMetadata(t, metadata, constant.DefaultNamespace, leftChild)
	assert.Equal(t, &proto.HashRange{Min: 0, Max: 49}, left.Int32HashRange)
	assert.Equal(t, []*proto.DataServerIdentity{ls1, ls2, ls3}, left.Ensemble)

	right := requireShardMetadata(t, metadata, constant.DefaultNamespace, rightChild)
	assert.Equal(t, &proto.HashRange{Min: 50, Max: 99}, right.Int32HashRange)
	assert.Equal(t, []*proto.DataServerIdentity{rs1, rs2, rs3}, right.Ensemble)
	assert.Equal(t, int64(2), requireShardMetadata(t, metadata, constant.DefaultNamespace, unrelatedShardID).Term)

	select {
	case event := <-listener.starts:
		assert.Equal(t, splitEvent{parentShardID, leftChild, rightChild}, event)
	default:
		t.Fatal("expected split-started event")
	}
}

func TestSplitterMergesSplitIntoLatestParentStatus(t *testing.T) {
	metadata := newTestMetadata(
		t,
		memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		nil,
	)
	parentShardID := metadata.ReserveShardIDs(1)
	storeTestShardMetadata(
		t,
		metadata,
		constant.DefaultNamespace,
		parentShardID,
		namespaceConfig,
		splitParentMetadata(),
	)

	selectorCalls := 0
	selector := func(
		_ string,
		_ int64,
		_ map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	) ([]*proto.DataServerIdentity, error) {
		selectorCalls++
		if selectorCalls == 2 {
			parent := requireShardMetadata(t, metadata, constant.DefaultNamespace, parentShardID)
			parent.Term++
			metadata.UpdateShardStatus(constant.DefaultNamespace, parentShardID, parent)
		}
		return []*proto.DataServerIdentity{ls1, ls2, ls3}, nil
	}
	splitter := NewSplitter(
		constant.DefaultNamespace,
		parentShardID,
		metadata,
		SplitterConfig{
			EnsembleSelector: selector,
			EventListener:    newMockShardSplitEventListener(),
		},
	)

	leftChild, rightChild, err := splitter.Split(nil)

	require.NoError(t, err)
	parent := requireShardMetadata(t, metadata, constant.DefaultNamespace, parentShardID)
	assert.Equal(t, int64(2), parent.Term)
	require.NotNil(t, parent.Split)
	assert.Equal(t, []int64{leftChild, rightChild}, parent.Split.ChildShardIds)
	namespace, exists := metadata.GetNamespaceStatus(constant.DefaultNamespace)
	require.True(t, exists)
	assert.Len(t, namespace.UnsafeBorrow().Shards, 3)
}

func TestControllerSerializesSplitActionsOnEventLoop(t *testing.T) {
	metadata := newTestMetadata(
		t,
		memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		nil,
	)
	parentShardID := metadata.ReserveShardIDs(1)
	parent := splitParentMetadata()
	parent.Ensemble = nil
	storeTestShardMetadata(
		t,
		metadata,
		constant.DefaultNamespace,
		parentShardID,
		namespaceConfig,
		parent,
	)

	selectorEntered := make(chan struct{})
	releaseSelector := make(chan struct{})
	selectorCalls := 0
	selector := func(
		_ string,
		_ int64,
		_ map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	) ([]*proto.DataServerIdentity, error) {
		selectorCalls++
		if selectorCalls == 1 {
			close(selectorEntered)
			<-releaseSelector
		}
		return []*proto.DataServerIdentity{ls1, ls2, ls3}, nil
	}
	listener := newMockShardSplitEventListener()
	shardController := NewController(
		constant.DefaultNamespace,
		parentShardID,
		namespaceConfig,
		parent,
		metadata,
		NoOpSupportedFeaturesSupplier,
		nil,
		SplitterConfig{
			EnsembleSelector: selector,
			EventListener:    listener,
		},
		mockutils.NewRpcProvider(),
		time.Hour,
	).(*controller)
	t.Cleanup(func() { assert.NoError(t, shardController.Close()) })

	firstDone := make(chan error, 1)
	go func() {
		_, err := shardController.Split(action.NewSplitAction(parentShardID, nil))
		firstDone <- err
	}()
	<-selectorEntered

	secondDone := make(chan error, 1)
	go func() {
		_, err := shardController.Split(action.NewSplitAction(parentShardID, nil))
		secondDone <- err
	}()

	select {
	case err := <-secondDone:
		t.Fatalf("second split bypassed the event loop: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseSelector)
	require.NoError(t, <-firstDone)
	assert.ErrorContains(t, <-secondDone, "already has an active split")
}

func TestControllerRejectsSplitActionForDifferentShard(t *testing.T) {
	shardController := &controller{shard: 1}

	_, err := shardController.Split(action.NewSplitAction(2, nil))

	assert.EqualError(t, err, "split action for shard 2 sent to controller for shard 1")
}

func TestControllerCloseDuringSplitInitialization(t *testing.T) {
	metadata := newTestMetadata(
		t,
		memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		nil,
	)
	parentShardID := metadata.ReserveShardIDs(1)
	parent := splitParentMetadata()
	parent.Ensemble = nil
	storeTestShardMetadata(
		t,
		metadata,
		constant.DefaultNamespace,
		parentShardID,
		namespaceConfig,
		parent,
	)

	selectorEntered := make(chan struct{})
	releaseSelector := make(chan struct{})
	selectorCalls := 0
	selector := func(
		_ string,
		_ int64,
		_ map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	) ([]*proto.DataServerIdentity, error) {
		selectorCalls++
		if selectorCalls == 1 {
			close(selectorEntered)
			<-releaseSelector
		}
		return []*proto.DataServerIdentity{ls1, ls2, ls3}, nil
	}
	shardController := NewController(
		constant.DefaultNamespace,
		parentShardID,
		namespaceConfig,
		parent,
		metadata,
		NoOpSupportedFeaturesSupplier,
		nil,
		SplitterConfig{
			EnsembleSelector: selector,
			EventListener:    newMockShardSplitEventListener(),
		},
		mockutils.NewRpcProvider(),
		time.Hour,
	).(*controller)

	splitDone := make(chan error, 1)
	go func() {
		_, err := shardController.Split(action.NewSplitAction(parentShardID, nil))
		splitDone <- err
	}()
	<-selectorEntered

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- shardController.Close()
	}()
	<-shardController.ctx.Done()
	close(releaseSelector)

	select {
	case err := <-splitDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("split did not finish during controller close")
	}
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("controller close did not finish")
	}
	assert.Nil(t, shardController.currentSplitting)
}

func TestControllerSerializesSplitMetadataUpdatesOnEventLoop(t *testing.T) {
	metadata := newTestMetadata(
		t,
		memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		nil,
	)
	parentShardID := metadata.ReserveShardIDs(1)
	parent := splitParentMetadata()
	parent.Ensemble = nil
	storeTestShardMetadata(
		t,
		metadata,
		constant.DefaultNamespace,
		parentShardID,
		namespaceConfig,
		parent,
	)

	shardController := NewController(
		constant.DefaultNamespace,
		parentShardID,
		namespaceConfig,
		parent,
		metadata,
		NoOpSupportedFeaturesSupplier,
		nil,
		SplitterConfig{},
		mockutils.NewRpcProvider(),
		time.Hour,
	).(*controller)
	t.Cleanup(func() { assert.NoError(t, shardController.Close()) })
	splitting := NewSplitting(
		shardController.ctx,
		shardController.logger,
		shardController.namespace,
		shardController.shard,
		shardController.metadataStore,
		shardController.rpc,
		shardController.splitMetadataOp,
		shardController.splittingConfig,
	)
	t.Cleanup(splitting.Stop)

	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
	})
	firstDone := make(chan struct{})
	shardController.splitMetadataOp <- func() {
		borrowedParent, exists := metadata.GetShardStatus(constant.DefaultNamespace, parentShardID)
		if !exists {
			panic("parent shard metadata is missing")
		}
		parentMeta := gproto.Clone(borrowedParent.UnsafeBorrow()).(*proto.ShardMetadata)
		parentMeta.Split = &proto.SplitMetadata{Phase: proto.SplitPhaseBootstrap}
		metadata.UpdateShardStatus(constant.DefaultNamespace, parentShardID, parentMeta)
		close(firstEntered)
		<-releaseFirst
		close(firstDone)
	}
	<-firstEntered

	phaseUpdated := make(chan error, 1)
	go func() {
		phaseUpdated <- splitting.updatePhase(proto.SplitPhaseCatchUp)
	}()

	select {
	case err := <-phaseUpdated:
		t.Fatalf("split phase update bypassed the shard controller event loop: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	assert.Equal(t, proto.SplitPhaseBootstrap,
		requireShardMetadata(t, metadata, constant.DefaultNamespace, parentShardID).Split.Phase)

	close(releaseFirst)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first metadata update did not complete")
	}
	select {
	case err := <-phaseUpdated:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("split phase update did not complete")
	}
	assert.Equal(t, proto.SplitPhaseCatchUp,
		requireShardMetadata(t, metadata, constant.DefaultNamespace, parentShardID).Split.Phase)
}
