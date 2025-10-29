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

package controllers

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/oxia-db/oxia/coordinator/actions"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/concurrent"

	"github.com/oxia-db/oxia/coordinator/metadata"
	"github.com/oxia-db/oxia/coordinator/resources"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/entity"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/proto"
)

var namespaceConfig = &model.NamespaceConfig{
	Name:                 "my-namespace",
	InitialShardCount:    1,
	ReplicationFactor:    3,
	NotificationsEnabled: entity.OptBooleanDefaultTrue{},
}

func TestLeaderElection_ShouldChooseHighestTerm(t *testing.T) {
	tests := []struct {
		name           string
		candidates     map[model.Server]*proto.EntryId
		expectedLeader model.Server
	}{
		{
			name: "Choose highest term",
			candidates: map[model.Server]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 2480},
				{Public: "2", Internal: "2"}: {Term: 200, Offset: 2500},
				{Public: "3", Internal: "3"}: {Term: 198, Offset: 3000},
			},
			expectedLeader: model.Server{Public: "2", Internal: "2"},
		},
		{
			name: "Same term, different offsets",
			candidates: map[model.Server]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 1000},
				{Public: "2", Internal: "2"}: {Term: 200, Offset: 2000},
				{Public: "3", Internal: "3"}: {Term: 200, Offset: 1500},
			},
			expectedLeader: model.Server{Public: "2", Internal: "2"},
		},
		{
			name: "Different terms, same offsets",
			candidates: map[model.Server]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 1500},
				{Public: "2", Internal: "2"}: {Term: 198, Offset: 1500},
				{Public: "3", Internal: "3"}: {Term: 199, Offset: 1500},
			},
			expectedLeader: model.Server{Public: "1", Internal: "1"},
		},
		{
			name: "Single candidate",
			candidates: map[model.Server]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 1500},
			},
			expectedLeader: model.Server{Public: "1", Internal: "1"},
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

func TestShardController(t *testing.T) {
	var shard int64 = 5
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()
	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{}, nil
	}, nil, nil)
	defer configResource.Close()

	sc := NewShardController(constant.DefaultNamespace, shard, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc, DefaultPeriodicTasksInterval)

	// Shard controller should initiate a leader election
	// and newTerm each server
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader, because it has the highest offset
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shard, 2, 3)

	assert.Eventually(t, func() bool {
		return sc.Metadata().Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.Equal(t, s1, *sc.Metadata().Leader())

	rpc.GetNode(s2).NewTermResponse(2, 0, nil)
	rpc.GetNode(s3).NewTermResponse(2, -1, nil)

	rpc.GetNode(s2).BecomeLeaderResponse(nil)

	// Simulate the failure of the leader
	rpc.FailNode(s1, errors.New("failed to connect"))
	sc.NodeBecameUnavailable(s1)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 3, true)

	// s2 should be selected as new leader, because it has the highest offset
	rpc.GetNode(s2).expectBecomeLeaderRequest(t, shard, 3, 3)

	assert.Eventually(t, func() bool {
		return sc.Metadata().Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 3, sc.Metadata().Term())
	assert.Equal(t, s2, *sc.Metadata().Leader())

	// Simulate the failure of the leader
	sc.NodeBecameUnavailable(s2)

	rpc.FailNode(s2, errors.New("failed to connect"))
	rpc.GetNode(s3).NewTermResponse(2, -1, nil)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 4, true)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 4, true)
	assert.NoError(t, sc.Close())
}

func TestShardController_StartingWithLeaderAlreadyPresent(t *testing.T) {
	var shard int64 = 5
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}
	n1 := rpc.GetNode(s1)
	n2 := rpc.GetNode(s2)
	n3 := rpc.GetNode(s3)

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()
	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{}, nil
	}, nil, nil)

	sc := NewShardController(constant.DefaultNamespace, shard, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusSteadyState,
		Term:     1,
		Leader:   &s1,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc, DefaultPeriodicTasksInterval)

	n1.expectGetStatusRequest(t, shard)
	n1.GetStatusResponse(1, proto.ServingStatus_LEADER, 0, 0)
	n2.expectGetStatusRequest(t, shard)
	n2.GetStatusResponse(1, proto.ServingStatus_FOLLOWER, 0, 0)
	n3.expectGetStatusRequest(t, shard)
	n3.GetStatusResponse(1, proto.ServingStatus_FOLLOWER, 0, 0)

	n1.expectNoMoreNewTermRequest(t)
	n2.expectNoMoreNewTermRequest(t)
	n3.expectNoMoreNewTermRequest(t)

	assert.NoError(t, sc.Close())
}

func TestShardController_NewTermWithNonRespondingServer(t *testing.T) {
	var shard int64 = 5
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()
	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{}, nil
	}, nil, nil)
	defer configResource.Close()

	sc := NewShardController(constant.DefaultNamespace, shard, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc, DefaultPeriodicTasksInterval)

	timeStart := time.Now()

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	// Shard controller should initiate a leader election
	// and newTerm each server
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	// s3 is not responding

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader, without waiting for s3 to timeout
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shard, 2, 3)

	assert.WithinDuration(t, timeStart, time.Now(), 1*time.Second)

	assert.Eventually(t, func() bool {
		return sc.Metadata().Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, model.ShardStatusSteadyState, sc.Metadata().Status())
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.Equal(t, s1, *sc.Metadata().Leader())

	assert.NoError(t, sc.Close())
}

func TestShardController_NewTermFollowerUntilItRecovers(t *testing.T) {
	var shard int64 = 5
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()
	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{}, nil
	}, nil, nil)
	defer configResource.Close()

	sc := NewShardController(constant.DefaultNamespace, shard, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc, DefaultPeriodicTasksInterval)

	// s3 is failing, though we can still elect a leader
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, errors.New("fails"))

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader, without waiting for s3 to timeout
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shard, 2, 3)

	assert.Eventually(t, func() bool {
		return sc.Metadata().Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s1, *sc.Metadata().Leader())

	// One more failure from s1
	rpc.GetNode(s3).NewTermResponse(1, -1, errors.New("fails"))
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 2, true)

	// Now it succeeds
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 2, true)

	// Leader should be notified
	rpc.GetNode(s1).AddFollowerResponse(nil)
	rpc.GetNode(s1).expectAddFollowerRequest(t, shard, 2)

	assert.NoError(t, sc.Close())
}

func TestShardController_VerifyFollowersWereAllFenced(t *testing.T) {
	var shard int64 = 5
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}
	n1 := rpc.GetNode(s1)
	n2 := rpc.GetNode(s2)
	n3 := rpc.GetNode(s3)

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()
	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{}, nil
	}, nil, nil)
	defer configResource.Close()

	sc := NewShardController(constant.DefaultNamespace, shard, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusSteadyState,
		Term:     4,
		Leader:   &s1,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc, DefaultPeriodicTasksInterval)

	n1.expectGetStatusRequest(t, 5)
	n1.GetStatusResponse(4, proto.ServingStatus_LEADER, 0, 0)

	n2.expectGetStatusRequest(t, 5)
	n2.GetStatusResponse(4, proto.ServingStatus_FOLLOWER, 0, 0)

	// The `s3` server was not properly fenced and it's stuck term 3
	// It needs to be fenced again
	n3.expectGetStatusRequest(t, 5)
	n3.GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 0, 0)

	// This should have triggered a new election, since s3 was in the wrong term
	n1.expectNewTermRequest(t, shard, 5, true)
	n2.expectNewTermRequest(t, shard, 5, true)
	n3.expectNewTermRequest(t, shard, 5, true)

	assert.NoError(t, sc.Close())
}

func TestShardController_NotificationsDisabled(t *testing.T) {
	var shard int64 = 5
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()
	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{
				{
					Name:                 "default",
					InitialShardCount:    1,
					ReplicationFactor:    1,
					NotificationsEnabled: entity.Bool(false),
				},
			},
		}, nil
	}, nil, nil)
	defer configResource.Close()

	sc := NewShardController(constant.DefaultNamespace, shard, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc, DefaultPeriodicTasksInterval)

	// Shard controller should initiate a leader election
	// and newTerm each server
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).BecomeLeaderResponse(nil)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 2, false)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 2, false)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 2, false)

	assert.NoError(t, sc.Close())
}

func TestShardController_SwapNodeWithLeaderElectionFailure(t *testing.T) {
	var shard int64 = 5
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}
	s4 := model.Server{Public: "s4:9091", Internal: "s4:8191"}

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()

	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{}, nil
	}, nil, nil)
	defer configResource.Close()

	sc := NewShardController(constant.DefaultNamespace, shard, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc, DefaultPeriodicTasksInterval)

	// Do initial election
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader, without waiting for s3 to timeout
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shard, 2, 3)

	assert.Eventually(t, func() bool {
		return sc.Metadata().Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s1, *sc.Metadata().Leader())

	wg := concurrent.NewWaitGroup(1)

	wg.Go(func() error {
		action := actions.NewChangeEnsembleAction(shard, s1, s4)
		sc.ChangeEnsemble(action)
		_, err := action.Wait()
		return err
	})

	// First leader election before swap will fail
	rpc.GetNode(s1).NewTermResponse(2, 0, nil)
	rpc.GetNode(s2).NewTermResponse(2, -1, errors.New("fails"))
	rpc.GetNode(s3).NewTermResponse(2, -1, errors.New("fails"))
	rpc.GetNode(s4).NewTermResponse(2, 0, nil)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s4).expectNewTermRequest(t, shard, 3, true)

	// Shard controller should retry and eventually succeed
	rpc.GetNode(s1).NewTermResponse(2, 2, nil)
	rpc.GetNode(s2).NewTermResponse(2, -1, errors.New("fails"))
	rpc.GetNode(s3).NewTermResponse(2, 1, nil)
	rpc.GetNode(s4).NewTermResponse(2, 0, nil)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 4, true)
	rpc.GetNode(s4).expectNewTermRequest(t, shard, 4, true)

	// s3 should be selected as new leader
	rpc.GetNode(s3).BecomeLeaderResponse(nil)
	rpc.GetNode(s3).expectBecomeLeaderRequest(t, shard, 4, 3)

	assert.Eventually(t, func() bool {
		return sc.Metadata().Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 4, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s3, *sc.Metadata().Leader())

	assert.NoError(t, sc.Close())
}

func TestShardController_LeaderElectionShouldNotFailIfRemoveFails(t *testing.T) {
	var shard int64 = 5
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}
	s4 := model.Server{Public: "s4:9091", Internal: "s4:8191"}

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()

	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{}, nil
	}, nil, nil)
	defer configResource.Close()

	sc := NewShardController(constant.DefaultNamespace, shard, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc, 1*time.Second)

	// Do initial election
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 2, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 2, true)

	// s1 should be selected as new leader
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shard, 2, 3)

	// Now start the swap node, which will trigger a new election
	wg := concurrent.NewWaitGroup(1)
	wg.Go(func() error {
		action := actions.NewChangeEnsembleAction(shard, s1, s4)
		sc.ChangeEnsemble(action)
		_, err := action.Wait()
		return err
	})

	rpc.GetNode(s1).NewTermResponse(2, 0, nil)
	rpc.GetNode(s2).NewTermResponse(2, -1, nil)
	rpc.GetNode(s3).NewTermResponse(2, -1, nil)
	rpc.GetNode(s4).NewTermResponse(2, 0, nil)

	rpc.GetNode(s1).expectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shard, 3, true)
	rpc.GetNode(s4).expectNewTermRequest(t, shard, 3, true)

	// s4 should be selected as new leader
	rpc.GetNode(s4).BecomeLeaderResponse(nil)
	rpc.GetNode(s4).expectBecomeLeaderRequest(t, shard, 3, 3)

	rpc.GetNode(s2).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(s3).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 0, 0)
	rpc.GetNode(s4).GetStatusResponse(3, proto.ServingStatus_LEADER, 0, 0)

	rpc.GetNode(s2).expectGetStatusRequest(t, shard)
	rpc.GetNode(s3).expectGetStatusRequest(t, shard)

	// s1 fails in removing the shard the first time
	rpc.GetNode(s1).DeleteShardResponse(errors.New("could not delete shard"))

	assert.Eventually(t, func() bool {
		return sc.Metadata().Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 3, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s4, *sc.Metadata().Leader())

	// The swap node should be free to complete as well
	assert.NoError(t, wg.Wait(context.Background()))

	// Eventually, the shard should get deleted
	rpc.GetNode(s1).expectDeleteShardRequest(t, shard, 3)
	assert.Eventually(t, func() bool {
		c := sc.(*shardController)
		shardMeta := c.metadata.Load()
		return len(shardMeta.PendingDeleteShardNodes) == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Next attempt wlll succeed
	rpc.GetNode(s1).DeleteShardResponse(nil)
	rpc.GetNode(s1).expectDeleteShardRequest(t, shard, 3)

	// s1 should be completely removed from list
	assert.Eventually(t, func() bool {
		c := sc.(*shardController)
		shardMeta := c.metadata.Load()
		return len(shardMeta.PendingDeleteShardNodes) == 0
	}, 10*time.Second, 100*time.Millisecond)

	assert.NoError(t, sc.Close())
}

func TestShardController_ShardsDataLost(t *testing.T) {
	var shardId = rand.Int63()
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}
	s4 := model.Server{Public: "s4:9091", Internal: "s4:8191"}
	s5 := model.Server{Public: "s5:9091", Internal: "s5:8191"}

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()
	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{}, nil
	}, nil, nil)
	defer configResource.Close()
	sc := NewShardController(constant.DefaultNamespace, shardId, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusUnknown,
		Term:     1,
		Leader:   nil,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc, 1*time.Second)

	// Do initial election
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, -1, nil)
	rpc.GetNode(s3).NewTermResponse(1, -1, nil)

	rpc.GetNode(s1).expectNewTermRequest(t, shardId, 2, true)
	rpc.GetNode(s2).expectNewTermRequest(t, shardId, 2, true)
	rpc.GetNode(s3).expectNewTermRequest(t, shardId, 2, true)

	// s1 should be selected as new leader
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	rpc.GetNode(s1).expectBecomeLeaderRequest(t, shardId, 2, 3)

	// Now start the change enesmble, which will trigger a new election

	// plus, 30 offset increased
	go func() {
		action := actions.NewChangeEnsembleAction(shardId, s1, s4)
		sc.ChangeEnsemble(action)
		_, err := action.Wait()
		assert.NoError(t, err)
	}()
	rpc.GetNode(s1).NewTermResponse(2, 30, nil)
	rpc.GetNode(s2).NewTermResponse(2, 30, nil)
	rpc.GetNode(s3).NewTermResponse(2, 20, nil)
	// s4 failed

	// s2 should be selected as new leader
	rpc.GetNode(s2).BecomeLeaderResponse(nil)
	rpc.GetNode(s2).expectBecomeLeaderRequest(t, shardId, 3, 3)

	// wait follower timeout
	rpc.GetNode(s2).GetStatusResponse(3, proto.ServingStatus_LEADER, 0, 0)
	rpc.GetNode(s2).GetStatusResponse(3, proto.ServingStatus_LEADER, 0, 0)
	rpc.GetNode(s2).GetStatusResponse(3, proto.ServingStatus_LEADER, 0, 0)

	// deleted the shard-1
	rpc.GetNode(s1).DeleteShardResponse(nil)

	assert.Eventually(t, func() bool {
		return sc.Metadata().Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 3, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s2, *sc.Metadata().Leader())

	// plus, 30 offset increased
	// swap again
	go func() {
		action := actions.NewChangeEnsembleAction(shardId, s2, s5)
		sc.ChangeEnsemble(action)
		_, err := action.Wait()
		assert.NoError(t, err)
	}()
	rpc.GetNode(s2).NewTermResponse(3, 30, nil)
	rpc.GetNode(s3).NewTermResponse(3, 20, nil)
	// s4 failed
	rpc.GetNode(s5).NewTermResponse(3, -1, nil)

	// s3 should be selected as new leader
	rpc.GetNode(s3).BecomeLeaderResponse(nil)
	rpc.GetNode(s3).expectBecomeLeaderRequest(t, shardId, 4, 3)

	// wait follower timeout
	rpc.GetNode(s3).GetStatusResponse(3, proto.ServingStatus_LEADER, 0, 0)

	assert.Eventually(t, func() bool {
		return sc.Metadata().Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 4, sc.Metadata().Term())
	assert.NotNil(t, sc.Metadata().Leader())
	assert.Equal(t, s3, *sc.Metadata().Leader())
	assert.Fail(t, "data lost!")
}
