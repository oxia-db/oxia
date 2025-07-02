// Copyright 2025 StreamNative, Inc.
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
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/utils"
	"github.com/oxia-db/oxia/tests/mock"
)

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
	}

	ch := make(chan any, 1)
	coordinator := mock.NewCoordinator(t, cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()

	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		if len(status.Namespaces) != 3 {
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
	}, 10*time.Second, 50*time.Millisecond)

	balancer := coordinator.LoadBalancer()
	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		shardLeaders, electedShards, nodeShards := utils.NodeShardLeaders(candidates, status)
		if shardLeaders != electedShards {
			return false
		}
		balancer.Trigger()
		for _, shards := range nodeShards {
			if shards.Size() != 4 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
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
			QuarantineTime: 1 * time.Second,
		},
	}

	ch := make(chan any, 1)
	coordinator := mock.NewCoordinator(t, cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()

	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		if len(status.Namespaces) != 3 {
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
	}, 10*time.Second, 50*time.Millisecond)

	balancer := coordinator.LoadBalancer()
	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		shardLeaders, electedShards, nodeShards := utils.NodeShardLeaders(candidates, status)
		if shardLeaders != electedShards {
			return false
		}
		balancer.Trigger()
		for _, shards := range nodeShards {
			if shards.Size() != 4 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)

	// close s3
	assert.NoError(t, s3.Close())

	// wait for leader moved
	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		_, _, nodeShards := utils.NodeShardLeaders(candidates, status)
		shards := nodeShards[s3ad.GetIdentifier()]
		return shards.Size() == 0
	}, 10*time.Second, 50*time.Millisecond)

	// start s3
	s3, s3ad = mock.NewServerWithAddress(t, "sv-3", s3ad.Public, s3ad.Internal)
	defer s3.Close()

	// wait for leader balanced
	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		shardLeaders, electedShards, nodeShards := utils.NodeShardLeaders(candidates, status)
		if shardLeaders != electedShards {
			return false
		}
		balancer.Trigger()
		for _, shards := range nodeShards {
			if shards.Size() != 4 {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond)
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
			QuarantineTime: 5 * time.Second,
		},
	}

	ch := make(chan any, 1)
	coordinator := mock.NewCoordinator(t, cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()

	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		if len(status.Namespaces) != 3 {
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
	}, 10*time.Second, 50*time.Millisecond)

	balancer := coordinator.LoadBalancer()
	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		shardLeaders, electedShards, nodeShards := utils.NodeShardLeaders(candidates, status)
		if shardLeaders != electedShards {
			return false
		}
		balancer.Trigger()
		for _, shards := range nodeShards {
			if shards.Size() != 4 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)

	cc.Servers = append(cc.Servers, s4ad, s5ad, s6ad)
	ch <- nil
	candidates = linkedhashset.New(s1ad.GetIdentifier(), s2ad.GetIdentifier(), s3ad.GetIdentifier(), s4ad.GetIdentifier(), s5ad.GetIdentifier(), s6ad.GetIdentifier())

	// wait for leader balanced
	assert.Eventually(t, func() bool {
		status := statusResource.Load()
		shardLeaders, electedShards, nodeShards := utils.NodeShardLeaders(candidates, status)
		if shardLeaders != electedShards {
			return false
		}
		balancer.Trigger()
		for _, shards := range nodeShards {
			if shards.Size() != 2 {
				return false
			}
		}
		return true
	}, 1*time.Minute, 1*time.Second)
}
