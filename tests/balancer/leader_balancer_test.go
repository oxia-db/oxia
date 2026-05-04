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

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/state"

	"github.com/oxia-db/oxia/tests/mock"
)

func dataServers(servers ...*proto.DataServerIdentity) []*proto.DataServerIdentity {
	return servers
}

func testNamespace(name string, initialShardCount uint32, replicationFactor uint32) *proto.Namespace {
	return &proto.Namespace{
		Name:   name,
		Policy: proto.NewHierarchyPolicies(initialShardCount, replicationFactor, true, "hierarchical"),
	}
}

func waitForSteadyState(t *testing.T, metadata coordmetadata.Metadata, expectedNamespaces int) {
	t.Helper()
	ctx := t.Context()
	err := coordmetadata.WaitForCondition(ctx, metadata, nil, func(status *proto.ClusterStatus) bool {
		if len(status.Namespaces) != expectedNamespaces {
			return false
		}
		for _, ns := range status.Namespaces {
			for _, shard := range ns.Shards {
				if shard.GetStatusOrDefault() != proto.ShardStatusSteadyState {
					return false
				}
			}
		}
		return true
	})
	require.NoError(t, err, "timed out waiting for shards to reach steady state")
}

func waitForLeadersBalanced(t *testing.T, metadata coordmetadata.Metadata, balancer interface{ Trigger() }, candidates *linkedhashset.Set[string], expectedPerNode int) {
	t.Helper()
	ctx := t.Context()
	err := coordmetadata.WaitForCondition(ctx, metadata, balancer.Trigger, func(status *proto.ClusterStatus) bool {
		shardLeaders, electedShards, nodeShards := state.NodeShardLeaders(candidates, status)
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
	candidates := linkedhashset.New(s1ad.GetNameOrDefault(), s2ad.GetNameOrDefault(), s3ad.GetNameOrDefault())

	cc := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{
			testNamespace("ns-1", 5, 3),
			testNamespace("ns-2", 4, 3),
			testNamespace("ns-3", 3, 3),
		},
		Servers: []*proto.DataServerIdentity{s1ad, s2ad, s3ad},
		LoadBalancer: &proto.LoadBalancer{
			QuarantineTime: "1ms",
		},
	}

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   cc,
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	coordinator := mock.NewCoordinator(t, configProvider)
	defer coordinator.Close()

	metadata := coordinator.Metadata()
	waitForSteadyState(t, metadata, 3)

	balancer := coordinator.LoadBalancer()
	waitForLeadersBalanced(t, metadata, balancer, candidates, 4)
}

func TestLeaderBalancedNodeCrashAndBack(t *testing.T) {
	s1, s1ad := mock.NewServer(t, "sv-1")
	defer s1.Close()
	s2, s2ad := mock.NewServer(t, "sv-2")
	defer s2.Close()
	s3, s3ad := mock.NewServer(t, "sv-3")
	candidates := linkedhashset.New(s1ad.GetNameOrDefault(), s2ad.GetNameOrDefault(), s3ad.GetNameOrDefault())

	cc := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{
			testNamespace("ns-1", 5, 3),
			testNamespace("ns-2", 4, 3),
			testNamespace("ns-3", 3, 3),
		},
		Servers: dataServers(s1ad, s2ad, s3ad),
		LoadBalancer: &proto.LoadBalancer{
			QuarantineTime: "1ms",
		},
	}

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   cc,
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	coordinator := mock.NewCoordinator(t, configProvider)
	defer coordinator.Close()

	metadata := coordinator.Metadata()
	waitForSteadyState(t, metadata, 3)

	balancer := coordinator.LoadBalancer()
	waitForLeadersBalanced(t, metadata, balancer, candidates, 4)

	// close s3
	require.NoError(t, s3.Close())

	// wait for leader moved
	ctx := t.Context()
	err = coordmetadata.WaitForCondition(ctx, metadata, nil, func(status *proto.ClusterStatus) bool {
		_, _, nodeShards := state.NodeShardLeaders(candidates, status)
		shards := nodeShards[s3ad.GetNameOrDefault()]
		return shards.Size() == 0
	})
	require.NoError(t, err, "timed out waiting for leaders to move off crashed node")

	// start s3
	s3, s3ad = mock.NewServerWithAddress(t, "sv-3", s3ad.Public, s3ad.Internal)
	defer s3.Close()

	// wait for leader balanced
	waitForLeadersBalanced(t, metadata, balancer, candidates, 4)
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

	candidates := linkedhashset.New(s1ad.GetNameOrDefault(), s2ad.GetNameOrDefault(), s3ad.GetNameOrDefault())

	cc := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{
			testNamespace("ns-1", 5, 3),
			testNamespace("ns-2", 4, 3),
			testNamespace("ns-3", 3, 3),
		},
		Servers: dataServers(s1ad, s2ad, s3ad),
		LoadBalancer: &proto.LoadBalancer{
			QuarantineTime: "1ms",
		},
	}

	configProvider := memory.NewProvider(metadatacommon.ClusterConfigCodec, metadatacommon.WatchEnabled)
	_, err := configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   cc,
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)
	coordinator := mock.NewCoordinator(t, configProvider)
	defer coordinator.Close()

	metadata := coordinator.Metadata()
	waitForSteadyState(t, metadata, 3)

	balancer := coordinator.LoadBalancer()
	waitForLeadersBalanced(t, metadata, balancer, candidates, 4)

	cc.Servers = append(cc.Servers, dataServers(s4ad, s5ad, s6ad)...)
	version := configProvider.Watch().Load().Version
	_, err = configProvider.Store(provider.Versioned[*proto.ClusterConfiguration]{
		Value:   cc,
		Version: version,
	})
	require.NoError(t, err)
	candidates = linkedhashset.New(s1ad.GetNameOrDefault(), s2ad.GetNameOrDefault(), s3ad.GetNameOrDefault(), s4ad.GetNameOrDefault(), s5ad.GetNameOrDefault(), s6ad.GetNameOrDefault())

	// wait for leader balanced
	waitForLeadersBalanced(t, metadata, balancer, candidates, 2)
}
