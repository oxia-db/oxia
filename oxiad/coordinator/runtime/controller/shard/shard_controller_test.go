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
	"fmt"
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
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	leaderselector "github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector/leader"
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

func TestController_OnElectLeaderReturnsNewLeader(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1},
	})

	labels := metric.LabelsForShard(constant.DefaultNamespace, shard)
	s := &controller{
		namespace:                           constant.DefaultNamespace,
		shard:                               shard,
		metadataStore:                       metadata,
		dataServerSupportedFeaturesSupplier: NoOpSupportedFeaturesSupplier,
		leaderSelector:                      leaderselector.NewSelector(),
		rpc:                                 rpc,
		logger:                              slog.Default(),
		leaderElectionLatency: metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency",
			"The time it takes to elect a leader for the shard", labels),
		leaderElectionsFailed: metric.NewCounter("oxia_coordinator_leader_election_failed",
			"The number of failed leader elections", "count", labels),
		newTermQuorumLatency: metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency",
			"The time it takes to take the ensemble of data servers to a new term", labels),
		becomeLeaderLatency: metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency",
			"The time it takes for the new elected leader to start", labels),
	}
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
	defer s.ctxCancel()

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	newLeader := s.onElectLeader(nil)
	require.NotNil(t, newLeader)
	assert.True(t, gproto.Equal(s1, newLeader))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 1)
}

// statusWriteFailingMetadata fails the shard status writes, as the metadata
// does when it gives up retrying them because the coordinator is closing.
type statusWriteFailingMetadata struct {
	coordmetadata.Metadata
}

func (statusWriteFailingMetadata) UpdateShardStatus(string, int64, *proto.ShardMetadata) error {
	return context.Canceled
}

func TestController_ElectionDoesNotFenceWithUnpersistedTerm(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})
	sc := newTestController(t, statusWriteFailingMetadata{metadata}, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	// The election fails to persist its new term: it must not fence the
	// ensemble with it
	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)
	assert.EqualValues(t, 1, shardTerm(metadata, constant.DefaultNamespace, shard))

	assert.NoError(t, sc.Close())
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

	// Shard controller should retry and eventually succeed. s3 has all the
	// entries of s1, which is swapped out
	rpc.GetNode(s1).NewTermResponse(2, 2, nil)
	rpc.GetNode(s2).NewTermResponse(2, 0, errors.New("fails"))
	rpc.GetNode(s3).NewTermResponse(2, 2, nil)
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

// The removed data server counts toward the fencing majority of a change
// ensemble election, but it is neither a leader nor a follower candidate. With
// RF=2, the leader and the removed follower reach that majority on their own:
// the election must still wait for the new member, otherwise the leader gets
// no follower to commit its head entry and BecomeLeader cannot complete.
func TestController_ChangeEnsembleWaitsForNewEnsembleQuorum(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})
	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, &proto.Namespace{
		Name:              "my-namespace",
		InitialShardCount: 1,
		ReplicationFactor: 2,
	}, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2},
	}, NoOpSupportedFeaturesSupplier, rpc, DefaultPeriodicTasksInterval)

	// Do initial election
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 2, true)

	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 2)

	// caught-up the leader election entry
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)

	wg := concurrent.NewWaitGroup(1)
	wg.Go(func() error {
		// Retry until the shard controller is ready for ensemble change.
		for {
			a := action.NewChangeEnsembleAction(shard, s2, s3)
			sc.ChangeEnsemble(a)
			_, err := a.Wait()
			if !errors.Is(err, ErrNotReadyForChangeEnsemble) {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	// s1 appended an entry that s2 never acked: it can only commit it, and so
	// complete BecomeLeader, with the ack of a follower of the new ensemble
	rpc.GetNode(s1).NewTermResponse(2, 1, nil)
	rpc.GetNode(s2).NewTermResponse(1, 0, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 3, true)

	// s3 is still opening the shard and answers after the grace period: the
	// election must wait for it
	rpc.GetNode(s1).ExpectNoBecomeLeaderRequest(t)
	rpc.GetNode(s3).NewTermResponse(-1, -1, nil)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFollowers(t, shard, 3, 2, s3)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assert.NoError(t, wg.Wait(ctx))
	metaSnap := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
	assert.Equal(t, proto.ShardStatusSteadyState, metaSnap.GetStatusOrDefault())
	assert.EqualValues(t, 3, metaSnap.Term)
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)
	assertShardEnsemble(t, metaSnap.Ensemble, s1, s3)

	assert.NoError(t, sc.Close())
}

// A change ensemble election fences the removed data server as well, but it
// is not a leader candidate: it can be the only responder that holds a
// committed entry. RF=3: s1 committed the entry at offset 5 with the ack of s2,
// while s3 is still at offset 4. Swapping s2 out while s1 misses the fence must
// not elect s3: s1 would be truncated when it rejoins, and s2 gets deleted.
func TestController_ChangeEnsembleKeepsEntryOfRemovedFollower(t *testing.T) {
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

	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 3)

	// caught-up the leader election entry
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(s3).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)

	wg := concurrent.NewWaitGroup(1)
	wg.Go(func() error {
		// Retry until the shard controller is ready for ensemble change.
		for {
			a := action.NewChangeEnsembleAction(shard, s2, s4)
			sc.ChangeEnsemble(a)
			_, err := a.Wait()
			if !errors.Is(err, ErrNotReadyForChangeEnsemble) {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	// s1 fails to answer the fence of the new term
	rpc.GetNode(s1).NewTermResponse(-1, -1, errors.New("fails"))
	rpc.GetNode(s2).NewTermResponse(2, 5, nil)
	rpc.GetNode(s3).NewTermResponse(2, 4, nil)
	rpc.GetNode(s4).NewTermResponse(-1, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s4).ExpectNewTermRequest(t, shard, 3, true)

	// s3 misses the committed entry: it must not become the leader
	rpc.GetNode(s3).ExpectNoBecomeLeaderRequest(t)

	// The change ensemble is aborted and the election goes on with the current
	// ensemble, where s2 is a candidate again
	rpc.GetNode(s1).NewTermResponse(-1, -1, errors.New("fails"))
	rpc.GetNode(s2).NewTermResponse(2, 5, nil)
	rpc.GetNode(s3).NewTermResponse(2, 4, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 4, true)

	rpc.GetNode(s2).ExpectBecomeLeaderRequestWithFollowers(t, shard, 4, 3, s3)
	rpc.GetNode(s2).BecomeLeaderResponse(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assert.ErrorIs(t, wg.Wait(ctx), ErrChangeEnsembleLosesEntries)
	metaSnap := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
	assert.Equal(t, proto.ShardStatusSteadyState, metaSnap.GetStatusOrDefault())
	assert.EqualValues(t, 4, metaSnap.Term)
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s2)
	assertShardEnsemble(t, metaSnap.Ensemble, s1, s2, s3)
	// s4 was fenced already: its copy of the shard gets deleted
	require.Len(t, metaSnap.PendingDeleteShardNodes, 1)
	assert.True(t, gproto.Equal(s4, metaSnap.PendingDeleteShardNodes[0]))

	assert.NoError(t, sc.Close())
}

// Same as TestController_ChangeEnsembleKeepsEntryOfRemovedFollower, when the
// swapped out data server is the leader: s1 committed the entry at offset 5
// with the ack of s2, and s2 is slow to answer the fence of the new term.
func TestController_ChangeEnsembleKeepsEntryOfRemovedLeader(t *testing.T) {
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

	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 3)

	// caught-up the leader election entry
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(s3).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)

	wg := concurrent.NewWaitGroup(1)
	wg.Go(func() error {
		// Retry until the shard controller is ready for ensemble change.
		for {
			a := action.NewChangeEnsembleAction(shard, s1, s4)
			sc.ChangeEnsemble(a)
			_, err := a.Wait()
			if !errors.Is(err, ErrNotReadyForChangeEnsemble) {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	// s2 does not answer the fence of the new term yet
	rpc.GetNode(s1).NewTermResponse(2, 5, nil)
	rpc.GetNode(s3).NewTermResponse(2, 4, nil)
	rpc.GetNode(s4).NewTermResponse(-1, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s4).ExpectNewTermRequest(t, shard, 3, true)

	// s3 misses the committed entry: it must not become the leader
	rpc.GetNode(s3).ExpectNoBecomeLeaderRequest(t)

	// The change ensemble is aborted and the election goes on with the current
	// ensemble, where s1 is a candidate again
	rpc.GetNode(s1).NewTermResponse(2, 5, nil)
	rpc.GetNode(s3).NewTermResponse(2, 4, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 4, true)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFollowers(t, shard, 4, 3, s3)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assert.ErrorIs(t, wg.Wait(ctx), ErrChangeEnsembleLosesEntries)
	metaSnap := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
	assert.Equal(t, proto.ShardStatusSteadyState, metaSnap.GetStatusOrDefault())
	assert.EqualValues(t, 4, metaSnap.Term)
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)
	assertShardEnsemble(t, metaSnap.Ensemble, s1, s2, s3)
	// s4 was fenced already: its copy of the shard gets deleted
	require.Len(t, metaSnap.PendingDeleteShardNodes, 1)
	assert.True(t, gproto.Equal(s4, metaSnap.PendingDeleteShardNodes[0]))

	assert.NoError(t, sc.Close())
}

// Once BecomeLeader was sent to a member of the new ensemble, the change
// ensemble must go on even if the removed data server may hold entries that no
// candidate has: the leader of the failed attempt may have committed entries on
// the new ensemble alone, which a leader of the current ensemble could miss.
func TestController_ChangeEnsembleNotAbortedAfterBecomeLeader(t *testing.T) {
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

	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).ExpectBecomeLeaderRequest(t, shard, 2, 3)

	// caught-up the leader election entry
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(s3).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 0, 0)

	wg := concurrent.NewWaitGroup(1)
	wg.Go(func() error {
		// Retry until the shard controller is ready for ensemble change.
		for {
			a := action.NewChangeEnsembleAction(shard, s2, s4)
			sc.ChangeEnsemble(a)
			_, err := a.Wait()
			if !errors.Is(err, ErrNotReadyForChangeEnsemble) {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	})

	rpc.GetNode(s1).NewTermResponse(2, 5, nil)
	rpc.GetNode(s2).NewTermResponse(2, 5, nil)
	rpc.GetNode(s3).NewTermResponse(2, 4, nil)
	rpc.GetNode(s4).NewTermResponse(-1, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s4).ExpectNewTermRequest(t, shard, 3, true)

	// The coordinator sees BecomeLeader fail, though s1 may lead the term
	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFollowers(t, shard, 3, 3, s3, s4)
	rpc.GetNode(s1).BecomeLeaderResponse(errors.New("fails"))

	// s1 misses the fence of the next attempt
	rpc.GetNode(s1).NewTermResponse(-1, -1, errors.New("fails"))
	rpc.GetNode(s2).NewTermResponse(2, 5, nil)
	rpc.GetNode(s3).NewTermResponse(2, 4, nil)
	rpc.GetNode(s4).NewTermResponse(-1, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s4).ExpectNewTermRequest(t, shard, 4, true)

	rpc.GetNode(s3).ExpectNoBecomeLeaderRequest(t)

	// The next attempt still fences the new ensemble
	rpc.GetNode(s1).NewTermResponse(2, 5, nil)
	rpc.GetNode(s2).NewTermResponse(2, 5, nil)
	rpc.GetNode(s3).NewTermResponse(2, 4, nil)
	rpc.GetNode(s4).NewTermResponse(-1, -1, nil)

	rpc.GetNode(s1).ExpectNewTermRequest(t, shard, 5, true)
	rpc.GetNode(s2).ExpectNewTermRequest(t, shard, 5, true)
	rpc.GetNode(s3).ExpectNewTermRequest(t, shard, 5, true)
	rpc.GetNode(s4).ExpectNewTermRequest(t, shard, 5, true)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFollowers(t, shard, 5, 3, s3, s4)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	assert.NoError(t, wg.Wait(ctx))
	metaSnap := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
	assert.Equal(t, proto.ShardStatusSteadyState, metaSnap.GetStatusOrDefault())
	assert.EqualValues(t, 5, metaSnap.Term)
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)
	assertShardEnsemble(t, metaSnap.Ensemble, s1, s3, s4)

	assert.NoError(t, sc.Close())
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

	// Verify BecomeLeader includes all features supported by the ensemble.
	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 2, 3, feature.SupportedFeatures())

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

func TestController_ChangeEnsembleRejectsFeatureSupportRegression(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	s4 := &proto.DataServerIdentity{Public: "s4:9091", Internal: "s4:8191"}

	rpc.GetNode(s1).SetNodeFeatures([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})
	rpc.GetNode(s2).SetNodeFeatures([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})
	rpc.GetNode(s3).SetNodeFeatures([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})
	rpc.GetNode(s4).SetNodeFeatures([]proto.Feature{})

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	featureSupplier := func(servers []*proto.DataServerIdentity) map[string][]proto.Feature {
		result := make(map[string][]proto.Feature)
		for _, server := range servers {
			info, err := rpc.GetInfo(context.Background(), server, &proto.GetInfoRequest{})
			if err == nil {
				result[server.GetNameOrDefault()] = info.FeaturesSupported
			}
		}
		return result
	}

	rpc.GetNode(s1).GetStatusResponse(2, proto.ServingStatus_LEADER, 1, 1)
	rpc.GetNode(s2).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 1, 1)
	rpc.GetNode(s3).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 1, 1)

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     2,
		Leader:   s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, featureSupplier, rpc, DefaultPeriodicTasksInterval)
	defer sc.Close()

	rpc.GetNode(s1).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s2).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s3).ExpectGetStatusRequest(t, shard)

	change := action.NewChangeEnsembleAction(shard, s1, s4)
	sc.ChangeEnsemble(change)

	waitResult := make(chan error, 1)
	go func() {
		_, err := change.Wait()
		waitResult <- err
	}()

	select {
	case err := <-waitResult:
		assert.ErrorIs(t, err, ErrChangeEnsembleLosesFeatureSupport)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for change ensemble to be rejected")
	}

	metaSnap := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
	assertShardEnsemble(t, metaSnap.Ensemble, s1, s2, s3)
}

func TestController_ChangeEnsembleRejectsMissingFeatureInfo(t *testing.T) {
	var shard int64 = 5

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	s4 := &proto.DataServerIdentity{Public: "s4:9091", Internal: "s4:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     2,
		Leader:   s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	})

	tests := []struct {
		name     string
		missing  string
		features []proto.Feature
	}{
		{name: "current ensemble member", missing: s3.GetNameOrDefault(), features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}},
		{name: "target ensemble member", missing: s4.GetNameOrDefault(), features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}},
		{name: "target ensemble member with no negotiated features", missing: s4.GetNameOrDefault(), features: []proto.Feature{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &controller{
				namespace:     constant.DefaultNamespace,
				shard:         shard,
				metadataStore: metadata,
				dataServerSupportedFeaturesSupplier: func(servers []*proto.DataServerIdentity) map[string][]proto.Feature {
					result := make(map[string][]proto.Feature)
					for _, server := range servers {
						if server.GetNameOrDefault() == tt.missing {
							continue
						}
						result[server.GetNameOrDefault()] = tt.features
					}
					return result
				},
				logger: slog.Default(),
			}

			err := s.validateChangeEnsembleFeatures(action.NewChangeEnsembleAction(shard, s1, s4))
			assert.ErrorIs(t, err, ErrNotReadyForChangeEnsemble)
		})
	}
}

func TestController_ChangeEnsembleRejectsInvalidAction(t *testing.T) {
	var shard int64 = 5

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	s4 := &proto.DataServerIdentity{Public: "s4:9091", Internal: "s4:8191"}
	s5 := &proto.DataServerIdentity{Public: "s5:9091", Internal: "s5:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     2,
		Leader:   s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	})

	s := &controller{
		namespace:                           constant.DefaultNamespace,
		shard:                               shard,
		metadataStore:                       metadata,
		dataServerSupportedFeaturesSupplier: NoOpSupportedFeaturesSupplier,
		logger:                              slog.Default(),
	}

	tests := []struct {
		name string
		from *proto.DataServerIdentity
		to   *proto.DataServerIdentity
	}{
		{name: "nil from", from: nil, to: s4},
		{name: "nil to", from: s1, to: nil},
		{name: "missing from", from: s4, to: s5},
		{name: "duplicate to", from: s1, to: s2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.validateChangeEnsembleFeatures(action.NewChangeEnsembleAction(shard, tt.from, tt.to))
			assert.ErrorIs(t, err, ErrInvalidChangeEnsemble)
		})
	}

	assert.NoError(t, s.validateChangeEnsembleFeatures(action.NewChangeEnsembleAction(shard, s1, s4)))
}

// Each UpdateShardStatus persists the full cluster status. A periodic tick
// should only persist after it changes shard state.
func TestController_PeriodicTasksPersistOnlyDirtyState(t *testing.T) {
	var shard int64 = 5
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})
	rpc := mockutils.NewRpcProvider()
	pendingDeleteNode := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}

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
		rpc:           rpc,
		ctx:           t.Context(),
		logger:        slog.Default(),
	}

	borrowedBefore, exists := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.True(t, exists)

	// Steady state without pending-delete nodes: no persist
	s.handlePeriodicTasks()
	borrowedAfter, _ := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.Same(t, borrowedBefore.UnsafeBorrow(), borrowedAfter.UnsafeBorrow())

	updatedShardMeta := gproto.CloneOf(shardMeta)
	updatedShardMeta.PendingDeleteShardNodes = []*proto.DataServerIdentity{pendingDeleteNode}
	require.NoError(t, metadata.UpdateShardStatus(constant.DefaultNamespace, shard, updatedShardMeta))
	borrowedBefore, _ = metadata.GetShardStatus(constant.DefaultNamespace, shard)

	rpc.GetNode(pendingDeleteNode).DeleteShardResponse(nil)
	s.handlePeriodicTasks()
	rpc.GetNode(pendingDeleteNode).ExpectDeleteShardRequest(t, shard, shardMeta.Term)

	borrowedAfter, _ = metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.NotSame(t, borrowedBefore.UnsafeBorrow(), borrowedAfter.UnsafeBorrow())
	assert.Empty(t, borrowedAfter.UnsafeBorrow().PendingDeleteShardNodes)

	borrowedBefore = borrowedAfter
	s.handlePeriodicTasks()
	borrowedAfter, _ = metadata.GetShardStatus(constant.DefaultNamespace, shard)
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

// testFeaturesSupplier is a mutable DataServerSupportedFeaturesSupplier for
// simulating nodes running different binary versions.
type testFeaturesSupplier struct {
	sync.Mutex
	features map[string][]proto.Feature
}

func newTestFeaturesSupplier() *testFeaturesSupplier {
	return &testFeaturesSupplier{features: make(map[string][]proto.Feature)}
}

func (s *testFeaturesSupplier) set(node *proto.DataServerIdentity, features ...proto.Feature) {
	s.Lock()
	defer s.Unlock()
	s.features[node.GetNameOrDefault()] = features
}

func (s *testFeaturesSupplier) supply(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature {
	s.Lock()
	defer s.Unlock()
	out := make(map[string][]proto.Feature, len(dataServers))
	for _, ds := range dataServers {
		out[ds.GetNameOrDefault()] = s.features[ds.GetNameOrDefault()]
	}
	return out
}

// An ensemble that contains a node not supporting a feature already enabled
// on the shard must not be able to elect a leader: the old node would apply
// entries with different semantics and silently diverge.
func TestController_ElectionRejectsEnsembleMissingEnabledFeature(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s3) // old binary: no features supported

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, supplier.supply, rpc, DefaultPeriodicTasksInterval)

	// s1 reports FEATURE_DB_CHECKSUM as already enabled in its database, but
	// the negotiated set is empty because s3 does not support it. Enqueue the
	// responses for the first attempt and its retry.
	for i := 0; i < 2; i++ {
		rpc.GetNode(s1).NewTermResponseWithFeatures(1, 0, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, nil)
		rpc.GetNode(s2).NewTermResponse(1, -1, nil)
		rpc.GetNode(s3).NewTermResponse(1, -1, nil)
	}

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)

	// The election must abort before electing a leader...
	rpc.GetNode(s1).ExpectNoBecomeLeaderRequest(t)
	// ...and keep retrying (loudly) with a new term.
	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 3, nil)

	assert.NotEqual(t, proto.ShardStatusSteadyState, shardStatus(metadata, constant.DefaultNamespace, shard))
	assert.NoError(t, sc.Close())
}

// When the whole ensemble supports the negotiated features, the election pins
// them in the NewTerm options of every member and passes them to the leader.
func TestController_ElectionPinsNegotiatedFeatures(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s3, proto.Feature_FEATURE_DB_CHECKSUM)

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, supplier.supply, rpc, DefaultPeriodicTasksInterval)

	checksum := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}

	rpc.GetNode(s1).NewTermResponseWithFeatures(1, 0, checksum, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 2, 3, checksum)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, shardTerm(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	assert.NoError(t, sc.Close())
}

// A member whose features are not known yet (it has not completed a handshake
// since the coordinator started, e.g. because it is down) must not block the
// election of a shard that already has features enabled: the election pins
// the enabled features, and the member is checked when it joins the term.
func TestController_ElectionPinsEnabledFeaturesWithUnknownMember(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	// s3's features are unknown

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, supplier.supply, rpc, DefaultPeriodicTasksInterval)

	checksum := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}

	// s3 is down. The first attempt finds the feature already enabled on the
	// shard, and the second one pins it.
	for i := 0; i < 2; i++ {
		rpc.GetNode(s1).NewTermResponseWithFeatures(1, 0, checksum, nil)
		rpc.GetNode(s2).NewTermResponse(1, -1, nil)
		rpc.GetNode(s3).NewTermResponse(0, 0, errors.New("node not available"))
	}
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 3, 3, checksum)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 3, shardTerm(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	assert.NoError(t, sc.Close())
}

// A member counts as not supporting any feature until it completes its
// handshake, which can happen while the election is fencing the ensemble
// (e.g. at coordinator startup). The election must then start over to pin
// the features the whole ensemble supports, instead of leaving them disabled
// until the next election.
func TestController_ElectionPinsFeaturesOfHandshakeDuringFencing(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	// s3 completes its handshake right after the election negotiated the
	// features of the new term
	var handshake sync.Once
	supply := func(dataServers []*proto.DataServerIdentity) map[string][]proto.Feature {
		features := supplier.supply(dataServers)
		handshake.Do(func() {
			supplier.set(s3, proto.Feature_FEATURE_DB_CHECKSUM)
		})
		return features
	}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, supply, rpc, DefaultPeriodicTasksInterval)

	checksum := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}

	for i := 0; i < 2; i++ {
		rpc.GetNode(s1).NewTermResponse(1, 0, nil)
		rpc.GetNode(s2).NewTermResponse(1, -1, nil)
		rpc.GetNode(s3).NewTermResponse(1, -1, nil)
	}
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 3, 3, checksum)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 3, shardTerm(metadata, constant.DefaultNamespace, shard))
	assertShardLeader(t, metadata, constant.DefaultNamespace, shard, s1)

	assert.NoError(t, sc.Close())
}

// A member can reject the NewTerm because it has not completed its handshake
// yet, and complete it only after the rest of the ensemble elected a leader:
// the term was negotiated while its features were unknown, and a new election
// must pin the features that the whole ensemble supports once they are known.
func TestController_ElectionPinsFeaturesDiscoveredAfterElection(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	// s3's features are unknown

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, supplier.supply, rpc, DefaultPeriodicTasksInterval)

	checksum := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(0, 0, constant.ErrNotInitialized)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 2, 3, nil)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	// s3 completes its handshake. s3 is left without responses from now on,
	// so the retries of the term 2 fence can't rejoin it.
	rpc.GetNode(s1).NewTermResponse(2, 0, nil)
	rpc.GetNode(s2).NewTermResponse(2, -1, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	supplier.set(s3, proto.Feature_FEATURE_DB_CHECKSUM)
	sc.FeaturesDiscovered(s3)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 3, 3, checksum)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState &&
			shardTerm(metadata, constant.DefaultNamespace, shard) == 3
	}, 10*time.Second, 100*time.Millisecond)

	assert.NoError(t, sc.Close())
}

// Discovering the features of a member only starts a new election if the
// whole ensemble then supports more features than the term pinned. The first
// handshake of a data server outside of the ensemble doesn't either, even if
// the ensemble supports more features by then: e.g. after a rolling upgrade,
// the new features are only enabled by the next election.
func TestController_FeaturesDiscoveredWithoutNewFeaturesKeepsTerm(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	s4 := &proto.DataServerIdentity{Public: "s4:9091", Internal: "s4:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	// s3's features are unknown

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, supplier.supply, rpc, DefaultPeriodicTasksInterval)

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(0, 0, constant.ErrNotInitialized)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 2, 3, nil)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	// s3 turns out to run an old binary, which doesn't support any feature
	supplier.set(s3)
	sc.FeaturesDiscovered(s3)
	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)

	// s3 is upgraded, then s4 completes its first handshake
	supplier.set(s3, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s4, proto.Feature_FEATURE_DB_CHECKSUM)
	sc.FeaturesDiscovered(s4)
	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)

	assert.EqualValues(t, 2, shardTerm(metadata, constant.DefaultNamespace, shard))
	assert.NoError(t, sc.Close())
}

// Discovering the features of a member doesn't start a new election when the
// term pins them already: e.g. at coordinator startup, when the handshake
// completes before the election negotiates the features of the term.
func TestController_FeaturesDiscoveredAlreadyPinnedKeepsTerm(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s3, proto.Feature_FEATURE_DB_CHECKSUM)

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, supplier.supply, rpc, DefaultPeriodicTasksInterval)

	// s3 completes its handshake while the election is running: the shard
	// controller handles it once the election is done
	sc.FeaturesDiscovered(s3)

	checksum := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 2, 3, checksum)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)

	assert.EqualValues(t, 2, shardTerm(metadata, constant.DefaultNamespace, shard))
	assert.NoError(t, sc.Close())
}

// The features of a data server are discovered only once, so the shard
// controller must not lose the notification while it is busy with an
// election, however many data servers complete their first handshake in the
// meantime (e.g. all of them at coordinator startup).
func TestController_FeaturesDiscoveredDuringElectionAreNotDropped(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	// s3's features are unknown

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, supplier.supply, rpc, DefaultPeriodicTasksInterval)

	checksum := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(0, 0, constant.ErrNotInitialized)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, nil)

	// While the election waits for the leader, many other data servers
	// complete their first handshake, and then s3
	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 2, 3, nil)
	for i := range chanBufferSize {
		sc.FeaturesDiscovered(&proto.DataServerIdentity{
			Public:   fmt.Sprintf("other-%d:9091", i),
			Internal: fmt.Sprintf("other-%d:8191", i),
		})
	}
	supplier.set(s3, proto.Feature_FEATURE_DB_CHECKSUM)
	sc.FeaturesDiscovered(s3)

	// s3 is left without responses, so the retries of the term 2 fence can't
	// rejoin it
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).NewTermResponse(2, 0, nil)
	rpc.GetNode(s2).NewTermResponse(2, -1, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 3, checksum)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 3, 3, checksum)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState &&
			shardTerm(metadata, constant.DefaultNamespace, shard) == 3
	}, 10*time.Second, 100*time.Millisecond)

	assert.NoError(t, sc.Close())
}

// A follower that failed the election fence rejoins later through
// AddFollower: the coordinator must report the joiner's supported features so
// the leader can validate them.
func TestController_RejoiningFollowerFeaturesReported(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s3, proto.Feature_FEATURE_DB_CHECKSUM)

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := newTestController(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, supplier.supply, rpc, DefaultPeriodicTasksInterval)

	checksum := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}

	// s3 fails the initial fence and rejoins through the retry path.
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(0, 0, errors.New("node not available"))
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).AddFollowerResponse(nil)

	rpc.GetNode(s1).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)
	rpc.GetNode(s2).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)

	rpc.GetNode(s1).ExpectBecomeLeaderRequestWithFeatures(t, shard, 2, 3, checksum)

	// Rejoin: NewTerm to s3 pins the same feature set, and the AddFollower
	// to the leader carries s3's supported features.
	rpc.GetNode(s3).ExpectNewTermRequestWithFeatures(t, shard, 2, checksum)
	rpc.GetNode(s1).ExpectAddFollowerRequestWithFeatures(t, shard, 2, checksum)

	assert.Eventually(t, func() bool {
		return shardStatus(metadata, constant.DefaultNamespace, shard) == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.NoError(t, sc.Close())
}
