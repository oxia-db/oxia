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

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"

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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.Equal(t, s1, sc.Metadata().Leader())

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 3, sc.Metadata().Term())
	assert.Equal(t, s2, sc.Metadata().Leader())

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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     1,
		Leader:   s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 3, sc.Metadata().Term())
	assert.Equal(t, s1, sc.Metadata().Leader())

	assert.NoError(t, sc.Close())
}

func TestController_NewTermWithNonRespondingServer(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, proto.ShardStatusSteadyState, sc.Metadata().Status())
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.Equal(t, s1, sc.Metadata().Leader())

	assert.NoError(t, sc.Close())
}

func TestController_NewTermFollowerUntilItRecovers(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s1, sc.Metadata().Leader())

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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     4,
		Leader:   s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s1, sc.Metadata().Leader())

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 4, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s3, sc.Metadata().Leader())

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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, 1*time.Second)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s1, sc.Metadata().Leader())

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 3, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s2, sc.Metadata().Leader())

	// The swap dataServer should be free to complete as well
	assert.NoError(t, wg.Wait(context.Background()))

	// Eventually, the shard should get deleted
	rpc.GetNode(s1).ExpectDeleteShardRequest(t, shard, 3)
	assert.Eventually(t, func() bool {
		c := sc.(*controller)
		shardMeta := c.metadata.Load()
		return len(shardMeta.PendingDeleteShardNodes) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Next attempt wlll succeed
	rpc.GetNode(s1).DeleteShardResponse(nil)
	rpc.GetNode(s1).ExpectDeleteShardRequest(t, shard, 3)

	// s1 should be completely removed from list
	assert.Eventually(t, func() bool {
		c := sc.(*controller)
		shardMeta := c.metadata.Load()
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
	sc := NewController(constant.DefaultNamespace, shardId, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, NoOpSupportedFeaturesSupplier, nil, rpc, 1*time.Second)

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

	metaSnap := sc.Metadata().Load()
	assert.EqualValues(t, metaSnap.Ensemble, []*proto.DataServerIdentity{s1, s2, s3})

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

	metaSnap = sc.Metadata().Load()
	assert.EqualValues(t, metaSnap.Ensemble, []*proto.DataServerIdentity{s1, s2, s3})
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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, featureSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, featureSupplier, nil, rpc, DefaultPeriodicTasksInterval)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	sc.Close()
}

// Each UpdateShardStatus persists the full cluster status, so re-writing it
// for every shard on every periodic tick costs O(shards^2) bytes per interval
// on the metadata backend. The periodic task must persist only when the
// status resource diverges from the controller's local view. Every real
// persist replaces the cached status object graph, so pointer identity
// through GetShardStatus observes whether a write happened.
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
		metadata:      NewMetadata(shardMeta),
		metadataStore: metadata,
		logger:        slog.Default(),
	}

	borrowedBefore, exists := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.True(t, exists)

	// Steady state: the status resource matches the local view — no persist
	s.handlePeriodicTasks()
	borrowedAfter, _ := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.Same(t, borrowedBefore.UnsafeBorrow(), borrowedAfter.UnsafeBorrow())

	// Local divergence (what an election leaves behind): must persist
	shardMeta.Term = 2
	s.metadata.Store(shardMeta)
	s.handlePeriodicTasks()
	borrowedChanged, _ := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.NotSame(t, borrowedAfter.UnsafeBorrow(), borrowedChanged.UnsafeBorrow())
	assert.EqualValues(t, 2, borrowedChanged.UnsafeBorrow().Term)

	// Converged again: back to skipping
	s.handlePeriodicTasks()
	borrowedFinal, _ := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.Same(t, borrowedChanged.UnsafeBorrow(), borrowedFinal.UnsafeBorrow())
}

// While handlePeriodicTasks is blocked in the DeleteShard RPCs, the split
// controller can store a term bump into this controller's metadata
// (runtime.SplitComplete syncing the parent after Cutover). Clearing the
// pending-delete nodes must merge with that update, not write the whole
// pre-RPC snapshot back over it.
func TestController_PendingDeleteDoesNotClobberConcurrentMetadataUpdate(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	leader := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	removed := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	shardMeta := &proto.ShardMetadata{
		Status:                  proto.ShardStatusSteadyState,
		Term:                    1,
		Leader:                  leader,
		PendingDeleteShardNodes: []*proto.DataServerIdentity{removed},
	}
	assert.True(t, metadata.CreateNamespaceStatus(constant.DefaultNamespace, &proto.NamespaceStatus{
		Shards: map[int64]*proto.ShardMetadata{shard: shardMeta},
	}))

	// Built by hand so the run loop's own timer cannot race the direct call
	s := &controller{
		namespace:     constant.DefaultNamespace,
		shard:         shard,
		metadata:      NewMetadata(shardMeta),
		metadataStore: metadata,
		rpc:           rpc,
		logger:        slog.Default(),
	}
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
	defer s.ctxCancel()

	done := make(chan struct{})
	go func() {
		s.handlePeriodicTasks()
		close(done)
	}()

	// The periodic task is now blocked inside the DeleteShard RPC
	rpc.GetNode(removed).ExpectDeleteShardRequest(t, shard, 1)

	// A concurrent term bump lands while the RPC is in flight
	s.metadata.Compute(func(m *proto.ShardMetadata) { m.Term = 7 })

	rpc.GetNode(removed).DeleteShardResponse(nil)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handlePeriodicTasks did not complete")
	}

	// The bump survives the pending-delete bookkeeping, and the merged view
	// is what gets persisted to the status resource
	assert.EqualValues(t, 7, s.metadata.Term())
	assert.Empty(t, s.metadata.Load().PendingDeleteShardNodes)
	borrowed, exists := metadata.GetShardStatus(constant.DefaultNamespace, shard)
	assert.True(t, exists)
	assert.EqualValues(t, 7, borrowed.UnsafeBorrow().Term)
	assert.Empty(t, borrowed.UnsafeBorrow().PendingDeleteShardNodes)
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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, supplier.supply, nil, rpc, DefaultPeriodicTasksInterval)

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

	assert.NotEqual(t, proto.ShardStatusSteadyState, sc.Metadata().Status())
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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, supplier.supply, nil, rpc, DefaultPeriodicTasksInterval)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.Equal(t, s1, sc.Metadata().Leader())

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

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, supplier.supply, nil, rpc, DefaultPeriodicTasksInterval)

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
		return sc.Metadata().Status() == proto.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.NoError(t, sc.Close())
}

// A node swap whose target does not support the features enabled on the
// shard must be rejected before fencing, so the current ensemble keeps
// serving instead of getting stuck in a failing election loop.
func TestController_ChangeEnsembleRejectsIncompatibleTarget(t *testing.T) {
	var shard int64 = 5
	rpc := mockutils.NewRpcProvider()

	s1 := &proto.DataServerIdentity{Public: "s1:9091", Internal: "s1:8191"}
	s2 := &proto.DataServerIdentity{Public: "s2:9091", Internal: "s2:8191"}
	s3 := &proto.DataServerIdentity{Public: "s3:9091", Internal: "s3:8191"}
	s4 := &proto.DataServerIdentity{Public: "s4:9091", Internal: "s4:8191"}

	supplier := newTestFeaturesSupplier()
	supplier.set(s1, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s2, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s3, proto.Feature_FEATURE_DB_CHECKSUM)
	supplier.set(s4) // old binary: no features supported

	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""), &proto.ClusterConfiguration{})

	sc := NewController(constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     1,
		Leader:   s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	}, metadata, supplier.supply, nil, rpc, DefaultPeriodicTasksInterval)

	// Initial verification of the steady-state ensemble
	rpc.GetNode(s1).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s1).GetStatusResponse(1, proto.ServingStatus_LEADER, 0, 0)
	rpc.GetNode(s2).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s2).GetStatusResponse(1, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(s3).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s3).GetStatusResponse(1, proto.ServingStatus_FOLLOWER, 0, 0)

	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)

	// The leader reports FEATURE_DB_CHECKSUM enabled: swapping s3 for the
	// old-binary s4 must be rejected, without starting an election.
	rpc.GetNode(s1).GetStatusResponseWithFeatures(1, proto.ServingStatus_LEADER, 0, 0,
		[]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})

	swap := action.NewChangeEnsembleAction(shard, s3, s4)
	sc.ChangeEnsemble(swap)
	_, err := swap.Wait()
	assert.ErrorIs(t, err, constant.ErrUnsupportedFeatures)

	rpc.GetNode(s1).ExpectGetStatusRequest(t, shard)
	rpc.GetNode(s1).ExpectNoMoreNewTermRequest(t)
	rpc.GetNode(s4).ExpectNoMoreNewTermRequest(t)

	assert.NoError(t, sc.Close())
}
