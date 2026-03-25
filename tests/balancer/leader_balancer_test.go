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

package balancer

import (
	"testing"
	"time"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
	"github.com/oxia-db/oxia/oxiad/coordinator/util"

	"github.com/oxia-db/oxia/tests/mock"
)

func waitForSteadyState(t *testing.T, statusResource resource.StatusResource, expectedNamespaces int) {
	t.Helper()
	ctx := t.Context()
	err := resource.WaitForCondition(ctx, statusResource, nil, func(status *model.ClusterStatus) bool {
		if len(status.Namespaces) != expectedNamespaces {
			return false
		}
		for _, ns := range status.Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	})
	require.NoError(t, err, "timed out waiting for shards to reach steady state")
}

func waitForLeadersBalanced(t *testing.T, statusResource resource.StatusResource, balancer interface{ Trigger() }, candidates *linkedhashset.Set[string], expectedPerNode int) {
	t.Helper()
	ctx := t.Context()
	err := resource.WaitForCondition(ctx, statusResource, balancer.Trigger, func(status *model.ClusterStatus) bool {
		shardLeaders, electedShards, nodeShards := util.NodeShardLeaders(candidates, status)
		if shardLeaders != electedShards {
			return false
		}
		for _, shards := range nodeShards {
			if shards.Size() != expectedPerNode {
				return false
			}
		}
		return true
	})
	require.NoError(t, err, "timed out waiting for leaders to be balanced (%d per node)", expectedPerNode)
}

func TestLeaderBalanced(t *testing.T) {
	s1, s1ad := mock.NewServer(t, "sv-1")
	defer s1.Close()
	s2, s2ad := mock.NewServer(t, "sv-2")
	defer s2.Close()
	s3, s3ad := mock.NewServer(t, "sv-3")
	defer s3.Close()
	candidates := linkedhashset.New(s1ad.GetIdentifier(), s2ad.GetIdentifier(), s3ad.GetIdentifier())

	cc := &model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{
			{
				Name:              "ns-1",
				InitialShardCount: 5,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-2",
				InitialShardCount: 4,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-3",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
		},
		Servers: []model.Server{s1ad, s2ad, s3ad},
		LoadBalancer: &model.LoadBalancer{
			QuarantineTime: 1 * time.Millisecond,
		},
	}

	ch := make(chan any, 1)
	coordinator := mock.NewCoordinator(t, cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()
	waitForSteadyState(t, statusResource, 3)

	balancer := coordinator.LoadBalancer()
	waitForLeadersBalanced(t, statusResource, balancer, candidates, 4)
}

func TestLeaderBalancedNodeCrashAndBack(t *testing.T) {
	s1, s1ad := mock.NewServer(t, "sv-1")
	defer s1.Close()
	s2, s2ad := mock.NewServer(t, "sv-2")
	defer s2.Close()
	s3, s3ad := mock.NewServer(t, "sv-3")
	candidates := linkedhashset.New(s1ad.GetIdentifier(), s2ad.GetIdentifier(), s3ad.GetIdentifier())

	cc := &model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{
			{
				Name:              "ns-1",
				InitialShardCount: 5,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-2",
				InitialShardCount: 4,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-3",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
		},
		Servers: []model.Server{s1ad, s2ad, s3ad},
		LoadBalancer: &model.LoadBalancer{
			QuarantineTime: 1 * time.Millisecond,
		},
	}

	ch := make(chan any, 1)
	coordinator := mock.NewCoordinator(t, cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()
	waitForSteadyState(t, statusResource, 3)

	balancer := coordinator.LoadBalancer()
	waitForLeadersBalanced(t, statusResource, balancer, candidates, 4)

	// close s3
	require.NoError(t, s3.Close())

	// wait for leader moved
	ctx := t.Context()
	err := resource.WaitForCondition(ctx, statusResource, nil, func(status *model.ClusterStatus) bool {
		_, _, nodeShards := util.NodeShardLeaders(candidates, status)
		shards := nodeShards[s3ad.GetIdentifier()]
		return shards.Size() == 0
	})
	require.NoError(t, err, "timed out waiting for leaders to move off crashed node")

	// start s3
	s3, s3ad = mock.NewServerWithAddress(t, "sv-3", s3ad.Public, s3ad.Internal)
	defer s3.Close()

	// wait for leader balanced
	waitForLeadersBalanced(t, statusResource, balancer, candidates, 4)
}

func TestLeaderBalancedNodeAdded(t *testing.T) {
	s1, s1ad := mock.NewServer(t, "sv-1")
	defer s1.Close()
	s2, s2ad := mock.NewServer(t, "sv-2")
	defer s2.Close()
	s3, s3ad := mock.NewServer(t, "sv-3")
	defer s3.Close()
	s4, s4ad := mock.NewServer(t, "sv-4")
	defer s4.Close()
	s5, s5ad := mock.NewServer(t, "sv-5")
	defer s5.Close()
	s6, s6ad := mock.NewServer(t, "sv-6")
	defer s6.Close()

	candidates := linkedhashset.New(s1ad.GetIdentifier(), s2ad.GetIdentifier(), s3ad.GetIdentifier())

	cc := &model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{
			{
				Name:              "ns-1",
				InitialShardCount: 5,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-2",
				InitialShardCount: 4,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-3",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
		},
		Servers: []model.Server{s1ad, s2ad, s3ad},
		LoadBalancer: &model.LoadBalancer{
			QuarantineTime: 1 * time.Millisecond,
		},
	}

	ch := make(chan any, 1)
	coordinator := mock.NewCoordinator(t, cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()
	waitForSteadyState(t, statusResource, 3)

	balancer := coordinator.LoadBalancer()
	waitForLeadersBalanced(t, statusResource, balancer, candidates, 4)

	cc.Servers = append(cc.Servers, s4ad, s5ad, s6ad)
	ch <- nil
	candidates = linkedhashset.New(s1ad.GetIdentifier(), s2ad.GetIdentifier(), s3ad.GetIdentifier(), s4ad.GetIdentifier(), s5ad.GetIdentifier(), s6ad.GetIdentifier())

	// wait for leader balanced
	waitForLeadersBalanced(t, statusResource, balancer, candidates, 2)
}
