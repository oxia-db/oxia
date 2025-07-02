// Copyright 2023 StreamNative, Inc.
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
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

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
	}, configResource, statusResource, nil, rpc)

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
		return sc.Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Term())
	assert.Equal(t, s1, *sc.Leader())

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
		return sc.Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 3, sc.Term())
	assert.Equal(t, s2, *sc.Leader())

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

	meta := metadata.NewMetadataProviderMemory()
	defer meta.Close()
	statusResource := resources.NewStatusResource(meta)
	configResource := resources.NewClusterConfigResource(t.Context(), func() (model.ClusterConfig, error) {
		return model.ClusterConfig{}, nil
	}, nil, nil)
	defer configResource.Close()

	sc := NewShardController(constant.DefaultNamespace, shard, namespaceConfig, model.ShardMetadata{
		Status:   model.ShardStatusSteadyState,
		Term:     1,
		Leader:   &s1,
		Ensemble: []model.Server{s1, s2, s3},
	}, configResource, statusResource, nil, rpc)

	select {
	case <-rpc.GetNode(s1).newTermRequests:
		assert.Fail(t, "shouldn't have received any newTerm requests")
	case <-rpc.GetNode(s2).newTermRequests:
		assert.Fail(t, "shouldn't have received any newTerm requests")
	case <-rpc.GetNode(s3).newTermRequests:
		assert.Fail(t, "shouldn't have received any newTerm requests")

	case <-time.After(1 * time.Second):
		// Ok
	}

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
	}, configResource, statusResource, nil, rpc)

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
		return sc.Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, model.ShardStatusSteadyState, sc.Status())
	assert.EqualValues(t, 2, sc.Term())
	assert.Equal(t, s1, *sc.Leader())

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
	}, configResource, statusResource, nil, rpc)

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
		return sc.Status() == model.ShardStatusSteadyState
	}, 10*time.Second, 100*time.Millisecond)
	assert.EqualValues(t, 2, sc.Term())
	assert.NotNil(t, sc.Leader())
	assert.Equal(t, s1, *sc.Leader())

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
	}, configResource, statusResource, nil, rpc)

	r1 := <-n1.getStatusRequests
	assert.EqualValues(t, 5, r1.Shard)
	n1.getStatusResponses <- struct {
		*proto.GetStatusResponse
		error
	}{&proto.GetStatusResponse{
		Term:   4,
		Status: proto.ServingStatus_LEADER,
	}, nil}

	r2 := <-n2.getStatusRequests
	assert.EqualValues(t, 5, r2.Shard)
	n2.getStatusResponses <- struct {
		*proto.GetStatusResponse
		error
	}{&proto.GetStatusResponse{
		Term:   4,
		Status: proto.ServingStatus_FOLLOWER,
	}, nil}

	// The `s3` server was not properly fenced and it's stuck term 3
	// It needs to be fenced again
	r3 := <-n3.getStatusRequests
	assert.EqualValues(t, 5, r3.Shard)
	n3.getStatusResponses <- struct {
		*proto.GetStatusResponse
		error
	}{&proto.GetStatusResponse{
		Term:   3,
		Status: proto.ServingStatus_FOLLOWER,
	}, nil}

	// This should have triggered a new election, since s3 was in the wrong term
	nt1 := <-n1.newTermRequests
	assert.EqualValues(t, 5, nt1.Term)

	nt2 := <-n2.newTermRequests
	assert.EqualValues(t, 5, nt2.Term)

	nt3 := <-n3.newTermRequests
	assert.EqualValues(t, 5, nt3.Term)

	assert.NoError(t, sc.Close())
}

func TestShardController_NotificationsDisabled(t *testing.T) {
	var shard int64 = 5
	rpc := newMockRpcProvider()

	s1 := model.Server{Public: "s1:9091", Internal: "s1:8191"}
	s2 := model.Server{Public: "s2:9091", Internal: "s2:8191"}
	s3 := model.Server{Public: "s3:9091", Internal: "s3:8191"}

	namespaceConfig := &model.NamespaceConfig{
		Name:                 "my-ns-2",
		InitialShardCount:    1,
		ReplicationFactor:    1,
		NotificationsEnabled: entity.Bool(false),
	}

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
	}, configResource, statusResource, nil, rpc)

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
