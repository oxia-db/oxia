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
	"context"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/mockutils"

	"github.com/oxia-db/oxia/common/concurrent"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxiad/common/feature"
)

var namespaceConfig = &proto.Namespace{
	Name:              "my-namespace",
	InitialShardCount: 1,
	ReplicationFactor: 3,
}

func newTestMetadata(t *testing.T, metadataProvider provider.Provider[*proto.ClusterStatus], clusterConfig *proto.ClusterConfiguration) coordmetadata.Metadata {
	t.Helper()

	if clusterConfig == nil {
		clusterConfig = &proto.ClusterConfiguration{}
	}
	if len(clusterConfig.Namespaces) == 0 {
		clusterConfig.Namespaces = []*proto.Namespace{namespaceConfig}
	}
	if len(clusterConfig.Servers) == 0 {
		clusterConfig.Servers = []*proto.DataServerIdentity{
			{Public: "seed-public-1:6648", Internal: "seed-internal-1:6649"},
			{Public: "seed-public-2:7648", Internal: "seed-internal-2:7649"},
			{Public: "seed-public-3:8648", Internal: "seed-internal-3:8649"},
		}
	}

	dir := t.TempDir()
	statusData, err := metadatacodec.ClusterStatusCodec.MarshalYAML(metadataProvider.Watch().Load().Value)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(dir, coordoption.DefaultFileStatusName), statusData, 0o600))
	configData, err := metadatacodec.ClusterConfigCodec.MarshalYAML(clusterConfig)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(dir, coordoption.DefaultFileConfigName), configData, 0o600))
	metadataFactory, err := coordmetadata.New(t.Context(), &coordoption.Options{
		Metadata: coordoption.MetadataOptions{
			ProviderOptions: coordoption.ProviderOptions{
				ProviderName: metadatacommon.NameFile,
				File: coordoption.FileMetadata{
					Dir: dir,
				},
			},
		},
	})
	assert.NoError(t, err)
	metadata, err := metadataFactory.CreateMetadata(t.Context())
	assert.NoError(t, err)
	_, err = metadata.WaitToBecomeLeader()
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, metadata.Close())
		assert.NoError(t, metadataFactory.Close())
	})
	return metadata
}

func newTestController( //nolint:revive // Test helper mirrors NewController and adds metadata setup.
	t *testing.T,
	metadata coordmetadata.Metadata,
	namespace string,
	shard int64,
	namespaceConfig *proto.Namespace,
	shardMetadata *proto.ShardMetadata,
	dataServerSupportedFeaturesSupplier DataServerSupportedFeaturesSupplier,
	rpcProvider coordrpc.Provider,
	periodicTasksInterval time.Duration,
) Controller {
	t.Helper()
	storeTestShardMetadata(t, metadata, namespace, shard, namespaceConfig, shardMetadata)
	return NewController(
		namespace,
		shard,
		namespaceConfig,
		shardMetadata,
		metadata,
		dataServerSupportedFeaturesSupplier,
		nil,
		rpcProvider,
		periodicTasksInterval,
	)
}

func storeTestShardMetadata(
	t *testing.T,
	metadata coordmetadata.Metadata,
	namespace string,
	shard int64,
	namespaceConfig *proto.Namespace,
	shardMetadata *proto.ShardMetadata,
) {
	t.Helper()
	namespaceStatus := &proto.NamespaceStatus{
		ReplicationFactor: namespaceConfig.GetReplicationFactor(),
		Shards: map[int64]*proto.ShardMetadata{
			shard: gproto.CloneOf(shardMetadata),
		},
	}
	assert.True(t, metadata.CreateNamespaceStatus(namespace, namespaceStatus))
}

func testShardMetadata(metadata coordmetadata.Metadata, namespace string, shard int64) (*proto.ShardMetadata, bool) {
	borrowedMeta, exists := metadata.GetShardStatus(namespace, shard)
	if !exists {
		return nil, false
	}
	return gproto.CloneOf(borrowedMeta.UnsafeBorrow()), true
}

func requireShardMetadata(t *testing.T, metadata coordmetadata.Metadata, namespace string, shard int64) *proto.ShardMetadata {
	t.Helper()
	shardMetadata, exists := testShardMetadata(metadata, namespace, shard)
	require.True(t, exists)
	return shardMetadata
}

func shardStatus(metadata coordmetadata.Metadata, namespace string, shard int64) proto.ShardStatus {
	shardMetadata, exists := testShardMetadata(metadata, namespace, shard)
	if !exists {
		return proto.ShardStatusUnknown
	}
	return shardMetadata.GetStatusOrDefault()
}

func shardTerm(metadata coordmetadata.Metadata, namespace string, shard int64) int64 {
	shardMetadata, exists := testShardMetadata(metadata, namespace, shard)
	if !exists {
		return 0
	}
	return shardMetadata.Term
}

func shardLeader(metadata coordmetadata.Metadata, namespace string, shard int64) *proto.DataServerIdentity {
	shardMetadata, exists := testShardMetadata(metadata, namespace, shard)
	if !exists {
		return nil
	}
	return shardMetadata.Leader
}

func assertShardLeader(
	t *testing.T,
	metadata coordmetadata.Metadata,
	namespace string,
	shard int64,
	expected *proto.DataServerIdentity,
) {
	t.Helper()
	assert.True(t, gproto.Equal(expected, shardLeader(metadata, namespace, shard)))
}

func assertShardEnsemble(
	t *testing.T,
	actual []*proto.DataServerIdentity,
	expected ...*proto.DataServerIdentity,
) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for i, expectedServer := range expected {
		assert.True(t, gproto.Equal(expectedServer, actual[i]))
	}
}

func TestLeaderElection_ShouldChooseHighestTerm(t *testing.T) {
	tests := []struct {
		name           string
		candidates     map[*proto.DataServerIdentity]*proto.EntryId
		expectedLeader *proto.DataServerIdentity
	}{
		{
			name: "Choose highest term",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 2480},
				{Public: "2", Internal: "2"}: {Term: 200, Offset: 2500},
				{Public: "3", Internal: "3"}: {Term: 198, Offset: 3000},
			},
			expectedLeader: &proto.DataServerIdentity{Public: "2", Internal: "2"},
		},
		{
			name: "Same term, different offsets",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 1000},
				{Public: "2", Internal: "2"}: {Term: 200, Offset: 2000},
				{Public: "3", Internal: "3"}: {Term: 200, Offset: 1500},
			},
			expectedLeader: &proto.DataServerIdentity{Public: "2", Internal: "2"},
		},
		{
			name: "Different terms, same offsets",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 1500},
				{Public: "2", Internal: "2"}: {Term: 198, Offset: 1500},
				{Public: "3", Internal: "3"}: {Term: 199, Offset: 1500},
			},
			expectedLeader: &proto.DataServerIdentity{Public: "1", Internal: "1"},
		},
		{
			name: "Single candidate",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 1500},
			},
			expectedLeader: &proto.DataServerIdentity{Public: "1", Internal: "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates := chooseCandidates(tt.candidates)

			// Check candidates
			assert.Equal(t, 1, len(candidates))
			for _, candidate := range candidates {
				assert.Equal(t, tt.expectedLeader, candidate)
			}
		})
	}
}

func TestController(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	// Shard controller should initiate a leader election
	// and newTerm each server
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader, because it has the highest offset
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 3)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, shardTerm(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	rpc.GetNode(s2).NewTermResponse(2, 0, nil)
	rpc.GetNode(s3).NewTermResponse(2, -1, nil)

	rpc.GetNode(s2).BecomeLeaderResponse(nil)

	// Simulate the failure of the leader
	rpc.FailNode(s1, errors.New("failed to connect"))
	sc.BecameUnavailable(s1)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 3, true)

	// s2 should be selected as new leader, because it has the highest offset
	rpc.GetNode(s2).ExpectBecomeLeaderRequest(t, shard, 3, 3)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 3, shardTerm(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s2)

	// Simulate the failure of the leader
	sc.BecameUnavailable(s2)

	rpc.FailNode(s2, errors.New("failed to connect"))
	rpc.GetNode(s3).NewTermResponse(2, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 4, true)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 4, true)
	assert.NoError(t, sc.Close())
}

func TestController_StartingWithLeaderAlreadyPresent(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	n1 := rpc.GetNode(s1)
	n2 := rpc.GetNode(s2)
	n3 := rpc.GetNode(s3)

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     1,
		Leader:   s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	n1.ExpectGetStatusRequest(t, shard)
	n1.GetStatusResponse(1, proto.ServingStatus_LEADER, 0, 0)
	n2.ExpectGetStatusRequest(t, shard)
	n2.GetStatusResponse(1, proto.ServingStatus_FOLLOWER, 0, 0)
	n3.ExpectGetStatusRequest(t, shard)
	n3.GetStatusResponse(1, proto.ServingStatus_FOLLOWER, 0, 0)

	n1.ExpectNoMoreNewTermRequest(t)
	n2.ExpectNoMoreNewTermRequest(t)
	n3.ExpectNoMoreNewTermRequest(t)

	assert.NoError(t, sc.Close())
}

func TestController_RetriesElectionWhenDataServerNotInitialized(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	rpc.GetNode(s1).NewTermResponse(0, 0, constant.ErrNotInitialized)
	rpc.GetNode(s2).NewTermResponse(0, 0, constant.ErrNotInitialized)
	rpc.GetNode(s3).NewTermResponse(0, 0, constant.ErrNotInitialized)

	rpc.GetNode(s1).NewTermResponse(2, 0, nil)
	rpc.GetNode(s2).NewTermResponse(2, -1, nil)
	rpc.GetNode(s3).NewTermResponse(2, -1, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 3, true)

	// s1 has the highest offset, so it becomes the leader.
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 3, 3)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 3, shardTerm(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	assert.NoError(t, sc.Close())
}

func TestController_NewTermWithNonRespondingServer(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	timeStart := time.Now()

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	// Shard controller should initiate a leader election
	// and newTerm each server
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	// s3 is not responding

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader, without waiting for s3 to timeout
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 3)

	assert.WithinDuration(t, timeStart, time.Now(), 1*time.Second)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, proto.ShardStatusSteadyState, shardStatus(metadata, constant.DefaultNamespace, shard))
	assert.EqualValues(t, 2, shardTerm(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	assert.NoError(t, sc.Close())
}

func TestController_NewTermFollowerUntilItRecovers(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	// s3 is failing, though we can still elect a leader
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, errors.New("fails"))

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader, without waiting for s3 to timeout
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 3)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, shardTerm(metadata, constant.DefaultNamespace, shard))
	assert.NotNil(t, shardLeader(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	// One more failure from s1
	rpc.GetNode(s3).NewTermResponse(1, -1, errors.New("fails"))
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	// Now it succeeds
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	// Leader should be notified
	rpc.GetNode(s1).AddFollowerResponse(nil)
	rpc.GetNode(s1).ExpectAddFollowerRequest(t, shard, 2)

	assert.NoError(t, sc.Close())
}

func TestController_VerifyFollowersWereAllFenced(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	n1 := rpc.GetNode(s1)
	n2 := rpc.GetNode(s2)
	n3 := rpc.GetNode(s3)

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     4,
		Leader:   s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	n1.ExpectGetStatusRequest(t, 5)
	n1.GetStatusResponse(4, proto.ServingStatus_LEADER, 0, 0)

	n2.ExpectGetStatusRequest(t, 5)
	n2.GetStatusResponse(4, proto.ServingStatus_FOLLOWER, 0, 0)

	// The `s3` server was not properly fenced and it's stuck term 3
	// It needs to be fenced again
	n3.ExpectGetStatusRequest(t, 5)
	n3.GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 0, 0)

	// This should have triggered a new election, since s3 was in the wrong term
	n1.ExpectNewTermRequest(t, shard, 5, true)
	n2.ExpectNewTermRequest(t, shard, 5, true)
	n3.ExpectNewTermRequest(t, shard, 5, true)

	assert.NoError(t, sc.Close())
}

func TestController_NotificationsDisabled(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	notificationsEnabled := false
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{
			{
				Name:                 "default",
				InitialShardCount:    1,
				ReplicationFactor:    1,
				NotificationsEnabled: &notificationsEnabled,
			},
		},
	})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	// Shard controller should initiate a leader election
	// and newTerm each server
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, false)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, false)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, false)

	assert.NoError(t, sc.Close())
}

func TestController_SwapNodeWithLeaderElectionFailure(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	s4 := &proto.DataServerIdentity{Public: "s4:9091", Internal: "s4:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	// Do initial election
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader, without waiting for s3 to timeout
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 3)

	// caught-up the leader election entry
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(s3).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, shardTerm(metadata, constant.DefaultNamespace, shard))
	assert.NotNil(t, shardLeader(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	wg := concurrent.NewWaitGroup(1)

	wg.Go(func() error {
		// Retry until the shard controller is ready for ensemble change.
		// After an election, follower catch-up runs in a background
		// goroutine; ChangeEnsemble is rejected until it completes.
		for {
			a := action.NewChangeEnsembleAction(shard, s1, s4)
			sc.ChangeEnsemble(a)
			_, err := a.Wait()
			if err == nil {
				return nil
			}
			if !errors.Is(err, ErrNotReadyForChangeEnsemble) {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	// First leader election before swap will fail
	rpc.GetNode(s1).NewTermResponse(2, 0, nil)
	rpc.GetNode(s2).NewTermResponse(2, 0, errors.New("fails"))
	rpc.GetNode(s3).NewTermResponse(2, 0, errors.New("fails"))
	rpc.GetNode(s4).NewTermResponse(2, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s4).ExpectNewTermRequest(t, shard, 3, true)

	// Shard controller should retry and eventually succeed
	rpc.GetNode(s1).NewTermResponse(2, 2, nil)
	rpc.GetNode(s2).NewTermResponse(2, 0, errors.New("fails"))
	rpc.GetNode(s3).NewTermResponse(2, 1, nil)
	rpc.GetNode(s4).NewTermResponse(2, 0, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s4).ExpectNewTermRequest(t, shard, 4, true)

	// s3 should be selected as new leader
	rpc.GetNode(s3).BecomeLeaderResponse(nil)
	rpc.GetNode(s3).ExpectBecomeLeaderRequest(t, shard, 4, 3)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 4, shardTerm(metadata, constant.DefaultNamespace, shard))
	assert.NotNil(t, shardLeader(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s3)

	assert.NoError(t, sc.Close())
}

func TestController_LeaderElectionShouldNotFailIfRemoveFails(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	s4 := &proto.DataServerIdentity{Public: "s4:9091", Internal: "s4:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, 1*time.Second)

	// Do initial election
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 3)

	// caught-up the leader election entry
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(s3).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, shardTerm(metadata, constant.DefaultNamespace, shard))
	assert.NotNil(t, shardLeader(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	// Now start the swap dataServer, which will trigger a new election
	wg := concurrent.NewWaitGroup(1)
	wg.Go(func() error {
		// Retry until the shard controller is ready for ensemble change.
		// After an election, follower catch-up runs in a background
		// goroutine; ChangeEnsemble is rejected until it completes.
		for {
			a := action.NewChangeEnsembleAction(shard, s1, s4)
			sc.ChangeEnsemble(a)
			_, err := a.Wait()
			if err == nil {
				return nil
			}
			if !errors.Is(err, ErrNotReadyForChangeEnsemble) {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	rpc.GetNode(s1).NewTermResponse(2, 1, nil)
	rpc.GetNode(s2).NewTermResponse(2, 2, nil)
	rpc.GetNode(s3).NewTermResponse(2, 1, nil)
	rpc.GetNode(s4).NewTermResponse(2, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s4).ExpectNewTermRequest(t, shard, 3, true)

	// s2 should be selected as new leader
	rpc.GetNode(s2).BecomeLeaderResponse(nil)
	rpc.GetNode(s2).ExpectBecomeLeaderRequest(t, shard, 3, 3)

	rpc.GetNode(s2).GetStatusResponse(3, proto.ServingStatus_LEADER, 1, 1)
	rpc.GetNode(s3).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 1, 1)
	rpc.GetNode(s4).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, -1, -1)

	rpc.GetNode(s3).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s4).ExpectGetStatusRequest(t, shard)

	// s1 fails in removing the shard the first time
	rpc.GetNode(s1).DeleteShardResponse(errors.New("could not delete shard"))

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 3, shardTerm(metadata, constant.DefaultNamespace, shard))
	assert.NotNil(t, shardLeader(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s2)

	// The swap dataServer should be free to complete as well
	assert.NoError(t, wg.Wait(context.Background()))

	// Eventually, the shard should get deleted
	rpc.GetNode(s1).ExpectDeleteShardRequest(t, shard, 3)
	assert.Eventually(t, func() bool {
		shardMeta := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
		return len(shardMeta.PendingDeleteShardNodes) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Next attempt wlll succeed
	rpc.GetNode(s1).DeleteShardResponse(nil)
	rpc.GetNode(s1).ExpectDeleteShardRequest(t, shard, 3)

	// s1 should be completely removed from list
	assert.Eventually(t, func() bool {
		shardMeta := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
		return len(shardMeta.PendingDeleteShardNodes) == 0
	}, 10*time.Second, 100*time.Millisecond)

	assert.NoError(t, sc.Close())
}

func TestController_ShardsDataLostWithChangeEnsemble(t *testing.T) {
	var shardId = rand.Int63()
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	s4 := &proto.DataServerIdentity{Public: "s4:9091", Internal: "s4:8191"}
	s5 := &proto.DataServerIdentity{Public: "s5:9091", Internal: "s5:8191"}
	s6 := &proto.DataServerIdentity{Public: "s6:9091", Internal: "s6:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})
	sc := newTestController(t, metadata, constant.DefaultNamespace, shardId, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, NoOpSupportedFeaturesSupplier, rpc, 1*time.Second)

	// Do initial election
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shardId, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shardId, 2, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shardId, 2, true)

	// s1 should be selected as new leader
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shardId, 2, 3)

	// test newTerm stage error would not change metadata ensemble
	action1 := action.NewChangeEnsembleAction(shardId, s1, s4)
	sc.ChangeEnsemble(action1)
	action2 := action.NewChangeEnsembleAction(shardId, s2, s5)
	sc.ChangeEnsemble(action2)
	action3 := action.NewChangeEnsembleAction(shardId, s3, s6)
	sc.ChangeEnsemble(action3)

	_, err := action1.Wait()
	assert.Error(t, err)
	_, err = action2.Wait()
	assert.Error(t, err)
	_, err = action3.Wait()
	assert.Error(t, err)

	metaSnap := requireShardMetadata(t, metadata, constant.DefaultNamespace, shardId)
	assertShardEnsemble(t, metaSnap.Ensemble, s1, s2, s3)

	// test become leader would not change metadata ensemble
	wait := sync.WaitGroup{}
	wait.Go(func() {
		action1 := action.NewChangeEnsembleAction(shardId, s1, s4)
		sc.ChangeEnsemble(action1)
		_, err := action1.Wait()
		assert.Error(t, err)
	})

	rpc.GetNode(s1).NewTermResponse(2, 3, nil)
	rpc.GetNode(s2).NewTermResponse(2, 3, nil)
	rpc.GetNode(s3).NewTermResponse(2, 3, nil)
	rpc.GetNode(s4).NewTermResponse(2, -1, nil)
	wait.Wait()

	wait = sync.WaitGroup{}
	wait.Go(func() {
		action1 := action.NewChangeEnsembleAction(shardId, s2, s5)
		sc.ChangeEnsemble(action1)
		_, err := action1.Wait()
		assert.Error(t, err)
	})
	rpc.GetNode(s2).NewTermResponse(3, 3, nil)
	rpc.GetNode(s3).NewTermResponse(3, 3, nil)
	rpc.GetNode(s4).NewTermResponse(3, -1, nil)
	rpc.GetNode(s5).NewTermResponse(3, 3, nil)
	wait.Wait()

	wait = sync.WaitGroup{}
	wait.Go(func() {
		action1 := action.NewChangeEnsembleAction(shardId, s3, s6)
		sc.ChangeEnsemble(action1)
		_, err := action1.Wait()
		assert.Error(t, err)
	})
	rpc.GetNode(s3).NewTermResponse(4, 3, nil)
	rpc.GetNode(s4).NewTermResponse(4, -1, nil)
	rpc.GetNode(s5).NewTermResponse(4, 3, nil)
	rpc.GetNode(s6).NewTermResponse(4, 3, nil)
	wait.Wait()

	metaSnap = requireShardMetadata(t, metadata, constant.DefaultNamespace, shardId)
	assertShardEnsemble(t, metaSnap.Ensemble, s1, s2, s3)
}

// Test feature negotiation with all nodes supporting the same features.
func TestController_FeatureNegotiation_AllNodesSupport(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	// All nodes support FINGERPRINT feature
	rpc.GetNode(s1).SetNodeFeatures(feature.SupportedFeatures())
	rpc.GetNode(s2).SetNodeFeatures(feature.SupportedFeatures())
	rpc.GetNode(s3).SetNodeFeatures(feature.SupportedFeatures())

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	// Create a feature supplier that queries the mock RPC
	featureSupplier := func(servers []*proto.DataServerIdentity) map[string][]proto.Feature {
		result := make(map[string][]proto.Feature)
		for _, server := range servers {
			info, err := rpc.GetInfo(context.Background(), server, &proto.GetInfoRequest{})
			if err == nil {
				result[server.Internal] = info.FeaturesSupported
			}
		}
		return result
	}

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, featureSupplier, rpc, DefaultPeriodicTasksInterval)

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	// Verify BecomeLeader includes the DB Checksum feature
	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 2, 3, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	sc.Close()
}

// Test feature negotiation with mixed node versions (one old node).
func TestController_FeatureNegotiation_MixedVersions(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	// s1 and s2 are new nodes with DB checksum support
	rpc.GetNode(s1).SetNodeFeatures([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})
	rpc.GetNode(s2).SetNodeFeatures([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})
	// s3 is an old node without feature support
	rpc.GetNode(s3).SetNodeFeatures([]proto.Feature{})

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	featureSupplier := func(servers []*proto.DataServerIdentity) map[string][]proto.Feature {
		result := make(map[string][]proto.Feature)
		for _, server := range servers {
			info, err := rpc.GetInfo(context.Background(), server, &proto.GetInfoRequest{})
			if err == nil {
				result[server.Internal] = info.FeaturesSupported
			}
		}
		return result
	}

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, featureSupplier, rpc, DefaultPeriodicTasksInterval)

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 2, true)

	// No features should be negotiated because s3 does not support FINGERPRINT
	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 2, 3, []proto.Feature{})

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	sc.Close()
}

func TestController_ChangeEnsembleRejectsTargetMissingCurrentFeatures(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	s4 := &proto.DataServerIdentity{Public: "s4:9091", Internal: "s4:8191"}

	featuresByNode := map[string][]proto.Feature{
		s1.GetNameOrDefault(): {proto.Feature_FEATURE_DB_CHECKSUM},
		s2.GetNameOrDefault(): {proto.Feature_FEATURE_DB_CHECKSUM},
		s3.GetNameOrDefault(): {proto.Feature_FEATURE_DB_CHECKSUM},
		s4.GetNameOrDefault(): {},
	}
	featureSupplier := func(servers []*proto.DataServerIdentity) map[string][]proto.Feature {
		result := make(map[string][]proto.Feature, len(servers))
		for _, server := range servers {
			result[server.GetNameOrDefault()] = featuresByNode[server.GetNameOrDefault()]
		}
		return result
	}

	metadata := newTestMetadata(
		t,
		memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{},
	)

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     2,
		Leader:   s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, featureSupplier, rpc, DefaultPeriodicTasksInterval)

	rpc.GetNode(s1).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 0, 0)
	rpc.GetNode(s2).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(s3).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s3).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)

	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)

	swap := action.NewChangeEnsembleAction(shard, s3, s4)
	sc.ChangeEnsemble(swap)
	_, err := swap.Wait()
	assert.ErrorIs(t, err, ErrIncompatibleChangeEnsemble)

	rpc.GetNode(s4).ExpectNoMoreNewTermRequest(t)

	assert.NoError(t, sc.Close())
}

// Each UpdateShardStatus persists the full cluster status, so re-writing it
// for every shard on every periodic tick costs O(shards^2) bytes per interval
// on the metadata backend. The periodic task must not persist when it has no
// pending-delete bookkeeping to apply.
func TestController_PeriodicTasksSkipUnchangedPersist(t *testing.T) {
	var shard int64 = 5
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	shardMeta := &proto.ShardMetadata{
		Status: proto.ShardStatusSteadyState,
		Term:   1,
		Leader: &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"},
	}
	assert.True(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{shard: shardMeta},
	}))

	// Built by hand instead of through NewController so the run loop's
	// own periodic timer cannot race the direct handlePeriodicTasks calls
	s := &controller{
		namespace:     constant.DefaultNamespace,
		shard:         shard,
		metadataStore: metadata,
		logger:        slog.Default(),
	}

	borrowedBefore, exists := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.True(t, exists)

	// Steady state without pending-delete nodes: no persist
	s.handlePeriodicTasks()
	borrowedAfter, _ := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.Same(t, borrowedBefore.UnsafeBorrow(), borrowedAfter.UnsafeBorrow())
}

func TestController_ElectionDiscardsWhenQueueIsFull(t *testing.T) {
	var shard int64 = 5
	s := &controller{
		shard:      shard,
		electionOp: make(chan *action.ElectionAction, chanBufferSize),
		logger:     slog.Default(),
	}
	for range chanBufferSize {
		group := &sync.WaitGroup{}
		group.Add(1)
		s.electionOp <- &action.ElectionAction{
			Shard:  shard,
			Waiter: group,
		}
	}

	done := make(chan string)
	go func() {
		done <- s.Election(&action.ElectionAction{Shard: shard})
	}()

	select {
	case newLeader := <-done:
		assert.Empty(t, newLeader)
	case <-time.After(time.Second):
		t.Fatal("Election blocked on a full election queue")
	}
	assert.Len(t, s.electionOp, chanBufferSize)
}

func TestController_SyncServerAddressDiscardsElectionWhenQueueIsFull(t *testing.T) {
	var shard int64 = 5
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	serverName := "s1"
	oldIdentity := &proto.DataServerIdentity{Name: &serverName, Public: "old-public:9091", Internal: "old-internal:8191"}
	newIdentity := &proto.DataServerIdentity{Name: &serverName, Public: "new-public:9091", Internal: "new-internal:8191"}
	assert.NoError(t, metadata.CreateDataServer(&proto.DataServer{Identity: newIdentity}))
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     1,
		Leader:   oldIdentity,
		Ensemble: []*proto.DataServerIdentity{oldIdentity},
	})

	s := &controller{
		namespace:     constant.DefaultNamespace,
		shard:         shard,
		metadataStore: metadata,
		electionOp:    make(chan *action.ElectionAction, chanBufferSize),
		logger:        slog.Default(),
	}
	for range chanBufferSize {
		group := &sync.WaitGroup{}
		group.Add(1)
		s.electionOp <- &action.ElectionAction{
			Shard:  shard,
			Waiter: group,
		}
	}

	done := make(chan struct{})
	go func() {
		s.SyncServerAddress()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SyncServerAddress blocked on a full election queue")
	}
	assert.Len(t, s.electionOp, chanBufferSize)
}

func TestController_DeleteShardAllowsNilEventListener(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	assert.True(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{
			shard: &proto.ShardMetadata{
				Status:   proto.ShardStatusDeleting,
				Term:     1,
				Leader:   leader,
				Ensemble: []*proto.DataServerIdentity{leader},
			},
		},
	}))

	s := &controller{
		namespace:     constant.DefaultNamespace,
		shard:         shard,
		metadataStore: metadata,
		rpc:           rpc,
		logger:        slog.Default(),
	}
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
	defer s.ctxCancel()

	done := make(chan struct{})
	go func() {
		s.deleteShardWithRetries()
		close(done)
	}()

	rpc.GetNode(leader).ExpectDeleteShardRequest(t, shard, 1)
	rpc.GetNode(leader).DeleteShardResponse(nil)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deleteShardWithRetries did not complete")
	}
	_, exists := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.False(t, exists)
	assert.True(t, s.terminating.Load())
}
