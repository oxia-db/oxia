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

func TestSplitterPersistsChildrenBeforeSelectingSecondEnsemble(t *testing.T) {
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

	listener := newMockShardSplitEventListener()
	selectorCalls := 0
	selector := func(
		namespace string,
		_ int64,
		status map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	) ([]*proto.DataServerIdentity, error) {
		selectorCalls++
		if selectorCalls == 2 {
			assert.Len(t, status[namespace].UnsafeBorrow().Shards, 2, "left child must affect right-child placement")
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

	select {
	case event := <-listener.starts:
		assert.Equal(t, splitEvent{parentShardID, leftChild, rightChild}, event)
	default:
		t.Fatal("expected split-started event")
	}
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
