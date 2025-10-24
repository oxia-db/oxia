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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/oxia-db/oxia/oxia"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/policies"
	"github.com/oxia-db/oxia/tests/mock"
)

func TestNormalShardBalancer(t *testing.T) {
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

	cc := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{
			{
				Name:              "ns-1",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-2",
				InitialShardCount: 3,
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
	coordinator := mock.NewCoordinator(t, &cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()
	configResource := coordinator.ConfigResource()

	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)

	cc.Servers = append(cc.Servers, s4ad, s5ad)
	ch <- struct{}{}

	assert.Eventually(t, func() bool {
		_, exist := configResource.Node(s4ad.GetIdentifier())
		return exist
	}, 10*time.Second, 50*time.Millisecond)

	balancer := coordinator.LoadBalancer()
	assert.Eventually(t, func() bool {
		balancer.Trigger()
		return balancer.IsBalanced()
	}, 30*time.Second, 50*time.Millisecond)
}

func TestPolicyBasedShardBalancer(t *testing.T) {
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
	serverMetadata := map[string]model.ServerMetadata{
		s1ad.GetIdentifier(): {
			Labels: map[string]string{"zone": "us-east-1"},
		},
		s2ad.GetIdentifier(): {
			Labels: map[string]string{"zone": "us-north-1"},
		},
		s3ad.GetIdentifier(): {
			Labels: map[string]string{"zone": "us-west-1"},
		},
		s4ad.GetIdentifier(): {
			Labels: map[string]string{"zone": "us-west-1"},
		},
		s5ad.GetIdentifier(): {
			Labels: map[string]string{"zone": "us-east-1"},
		},
	}

	cc := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{
			{
				Name:              "ns-1",
				InitialShardCount: 3,
				ReplicationFactor: 3,
				Policies: &policies.Policies{
					AntiAffinities: []policies.AntiAffinity{
						{
							Labels: []string{"zone"},
						},
					},
				},
			},
			{
				Name:              "ns-2",
				InitialShardCount: 3,
				ReplicationFactor: 3,
				Policies: &policies.Policies{
					AntiAffinities: []policies.AntiAffinity{
						{
							Labels: []string{"zone"},
						},
					},
				},
			},
			{
				Name:              "ns-3",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
		},
		ServerMetadata: serverMetadata,
		Servers:        []model.Server{s1ad, s2ad, s3ad},
	}

	ch := make(chan any, 1)
	coordinator := mock.NewCoordinator(t, &cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()
	configResource := coordinator.ConfigResource()

	assert.Eventually(t, func() bool {
		for _, ns := range statusResource.Load().Namespaces {
			for _, shard := range ns.Shards {
				if shard.Status != model.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)

	cc.Servers = append(cc.Servers, s4ad, s5ad)
	ch <- struct{}{}

	assert.Eventually(t, func() bool {
		_, exist := configResource.Node(s4ad.GetIdentifier())
		return exist
	}, 10*time.Second, 50*time.Millisecond)

	balancer := coordinator.LoadBalancer()
	assert.Eventually(t, func() bool {
		balancer.Trigger()
		return balancer.IsBalanced()
	}, 30*time.Second, 50*time.Millisecond)

	// check if follow the policies
	for name, ns := range statusResource.Load().Namespaces {
		for _, shard := range ns.Shards {
			nodeIDs := linkedhashset.New[string]()
			nodeZones := linkedhashset.New[string]()
			for _, server := range shard.Ensemble {
				id := server.GetIdentifier()
				nodeIDs.Add(id)
				metadata := serverMetadata[id]
				nodeZones.Add(metadata.Labels["zone"])
			}
			assert.Equal(t, 3, nodeIDs.Size())
			if name != "ns-3" {
				assert.Equal(t, 3, nodeZones.Size())
			}
		}
	}
}

func TestBalanceWithoutDataLost(t *testing.T) {
	// init 3 nodes
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

	// config
	cc := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{
			{
				Name:              "ns-1",
				InitialShardCount: 3,
				ReplicationFactor: 3,
			},
			{
				Name:              "ns-2",
				InitialShardCount: 3,
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
	coordinator := mock.NewCoordinator(t, &cc, ch)
	defer coordinator.Close()

	statusResource := coordinator.StatusResource()
	configResource := coordinator.ConfigResource()
	assert.Eventually(t, func() bool {
		return statusResource.IsReady(configResource.Load())
	}, 60*time.Second, 1*time.Second)

	namespaces := []string{"ns-1", "ns-2", "ns-3"}

	for _, namespace := range namespaces {
		client, err := oxia.NewSyncClient(s1ad.Public, oxia.WithNamespace(namespace))
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			_, _, err := client.Put(context.Background(), fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
			assert.NoError(t, err)
		}
	}

	cc.Servers = append(cc.Servers, s4ad, s5ad, s6ad)
	ch <- struct{}{}

	assert.Eventually(t, func() bool {
		_, exist := configResource.Node(s4ad.GetIdentifier())
		return exist
	}, 60*time.Second, 1*time.Second)

	balancer := coordinator.LoadBalancer()
	assert.Eventually(t, func() bool {
		balancer.Trigger()
		return balancer.IsBalanced()
	}, 60*time.Second, 1*time.Second)
}
