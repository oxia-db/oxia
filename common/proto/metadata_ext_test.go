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

package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataServerIdentityGetNameOrDefaultNil(t *testing.T) {
	var identity *DataServerIdentity
	assert.Empty(t, identity.GetNameOrDefault())
}

func TestDataServerGetNameOrDefaultNil(t *testing.T) {
	var server *DataServer
	assert.Empty(t, server.GetNameOrDefault())
}

func TestShardMetadataGetStatusOrDefaultNil(t *testing.T) {
	var metadata *ShardMetadata
	assert.Equal(t, ShardStatusUnknown, metadata.GetStatusOrDefault())
}

func TestSplitMetadataGetPhaseOrDefaultNil(t *testing.T) {
	var metadata *SplitMetadata
	assert.Equal(t, SplitPhaseBootstrap, metadata.GetPhaseOrDefault())
}

func TestNamespaceSetKeySortingType(t *testing.T) {
	var nilNamespace *Namespace
	nilNamespace.SetKeySortingType(KeySortingType_NATURAL)

	ns := &Namespace{}
	ns.SetKeySortingType(KeySortingType_NATURAL)
	assert.Equal(t, "natural", ns.GetKeySorting())
	ns.SetKeySortingType(KeySortingType_HIERARCHICAL)
	assert.Equal(t, "hierarchical", ns.GetKeySorting())

	// An unknown key sorting clears the field, so that the default applies
	ns.SetKeySortingType(KeySortingType_UNKNOWN)
	assert.Empty(t, ns.GetKeySorting())
}

func TestAntiAffinityGetModeOrDefault(t *testing.T) {
	var nilAntiAffinity *AntiAffinity
	assert.Equal(t, AntiAffinityModeUnknown, nilAntiAffinity.GetModeOrDefault())
	assert.Equal(t, AntiAffinityModeStrict, (&AntiAffinity{Mode: "STRICT"}).GetModeOrDefault())
	assert.Equal(t, AntiAffinityModeUnknown, (&AntiAffinity{Mode: "other"}).GetModeOrDefault())
}

func TestAntiAffinitySetModeOrDefault(t *testing.T) {
	var nilAntiAffinity *AntiAffinity
	nilAntiAffinity.SetModeOrDefault(AntiAffinityModeStrict)

	antiAffinity := &AntiAffinity{}
	antiAffinity.SetModeOrDefault("Relaxed")
	assert.Equal(t, AntiAffinityModeRelaxed, antiAffinity.GetMode())
	antiAffinity.SetModeOrDefault("other")
	assert.Equal(t, AntiAffinityModeUnknown, antiAffinity.GetMode())
}

func TestClusterConfigurationGetDataServerNil(t *testing.T) {
	var config *ClusterConfiguration
	server, ok := config.GetDataServer("server-0")
	assert.False(t, ok)
	assert.Nil(t, server)
}

func TestClusterConfigurationListDataServers(t *testing.T) {
	name := "server-1"
	config := &ClusterConfiguration{
		Servers: []*DataServerIdentity{
			{Public: "server-0:6648", Internal: "server-0:6649"},
			{Name: &name, Public: "server-1:6648", Internal: "server-1:6649"},
		},
		ServerMetadata: map[string]*DataServerMetadata{
			"server-1": {Labels: map[string]string{"zone": "a"}},
		},
	}

	dataServers := config.ListDataServers()
	require.Len(t, dataServers, 2)
	assert.Equal(t, "server-0:6649", dataServers[0].GetIdentity().GetName())
	assert.Equal(t, "server-0:6648", dataServers[0].GetIdentity().GetPublic())
	assert.Empty(t, dataServers[0].GetMetadata().GetLabels())
	assert.Equal(t, "server-1", dataServers[1].GetIdentity().GetName())
	assert.Equal(t, map[string]string{"zone": "a"}, dataServers[1].GetMetadata().GetLabels())

	var nilConfig *ClusterConfiguration
	assert.Empty(t, nilConfig.ListDataServers())
}

func TestClusterConfigurationGetCoordinator(t *testing.T) {
	config := &ClusterConfiguration{
		Coordinators: []*Coordinator{
			{Name: "coordinator-0", PublicAddress: "coordinator-0.example.com:6651"},
			{Name: "coordinator-1", PublicAddress: "coordinator-1.example.com:6651"},
		},
	}

	coordinator, ok := config.GetCoordinator("coordinator-1")
	require.True(t, ok)
	require.Equal(t, "coordinator-1.example.com:6651", coordinator.GetPublicAddress())

	_, ok = config.GetCoordinator("")
	require.False(t, ok)

	_, ok = config.GetCoordinator("coordinator-2")
	require.False(t, ok)

	var nilConfig *ClusterConfiguration
	_, ok = nilConfig.GetCoordinator("coordinator-0")
	require.False(t, ok)
}

func TestClusterConfigurationValidateRejectsNil(t *testing.T) {
	var config *ClusterConfiguration
	require.ErrorContains(t, config.Validate(), "must not be nil")
}

func TestClusterConfigurationValidateNamespaces(t *testing.T) {
	servers := []*DataServerIdentity{{Internal: "server-0"}}
	for _, tc := range []struct {
		name      string
		namespace *Namespace
		expected  string
	}{
		{"invalid name", &Namespace{Name: "a/b", ReplicationFactor: 1, InitialShardCount: 1},
			"invalid path separator"},
		{"invalid replication factor", &Namespace{Name: "ns", InitialShardCount: 1},
			`namespace "ns" has invalid replicationFactor=0`},
		{"invalid initial shard count", &Namespace{Name: "ns", ReplicationFactor: 1},
			`namespace "ns" has invalid initialShardCount=0`},
		{"invalid key sorting", &Namespace{Name: "ns", ReplicationFactor: 1, InitialShardCount: 1, KeySorting: "x"},
			`namespace "ns" has invalid keySorting`},
		{"invalid anti-affinity", &Namespace{Name: "ns", ReplicationFactor: 1, InitialShardCount: 1,
			AntiAffinities: []*AntiAffinity{{Mode: AntiAffinityModeStrict}}},
			`namespace "ns" has invalid antiAffinities[0]: labels must not be empty`},
		{"too few servers", &Namespace{Name: "ns", ReplicationFactor: 2, InitialShardCount: 1},
			"replicationFactor=2 but only 1 servers are configured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := &ClusterConfiguration{Namespaces: []*Namespace{tc.namespace}, Servers: servers}
			require.ErrorContains(t, config.Validate(), tc.expected)
		})
	}
}

func TestClusterConfigurationValidateLoadBalancerDurations(t *testing.T) {
	require.ErrorContains(t, (&ClusterConfiguration{
		LoadBalancer: &LoadBalancer{ScheduleInterval: "x"},
	}).Validate(), "invalid loadBalancer.scheduleInterval")
	require.ErrorContains(t, (&ClusterConfiguration{
		LoadBalancer: &LoadBalancer{QuarantineTime: "x"},
	}).Validate(), "invalid loadBalancer.quarantineTime")
	require.NoError(t, (&ClusterConfiguration{
		LoadBalancer: &LoadBalancer{ScheduleInterval: "1s", QuarantineTime: "1m"},
	}).Validate())
}

func TestClusterConfigurationValidateCoordinators(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		require.NoError(t, (&ClusterConfiguration{
			Coordinators: []*Coordinator{{
				Name:          "coordinator-0",
				PublicAddress: "coordinator-0.example.com:6651",
			}},
		}).Validate())
	})

	t.Run("empty name", func(t *testing.T) {
		require.ErrorContains(t, (&ClusterConfiguration{
			Coordinators: []*Coordinator{{
				PublicAddress: "coordinator-0.example.com:6651",
			}},
		}).Validate(), "coordinator name must not be empty")
	})

	t.Run("duplicate name", func(t *testing.T) {
		require.ErrorContains(t, (&ClusterConfiguration{
			Coordinators: []*Coordinator{
				{Name: "coordinator-0", PublicAddress: "coordinator-0.example.com:6651"},
				{Name: "coordinator-0", PublicAddress: "coordinator-0-alt.example.com:6651"},
			},
		}).Validate(), `duplicate coordinator "coordinator-0"`)
	})

	t.Run("empty public address", func(t *testing.T) {
		require.ErrorContains(t, (&ClusterConfiguration{
			Coordinators: []*Coordinator{{
				Name: "coordinator-0",
			}},
		}).Validate(), `coordinator "coordinator-0" publicAddress must not be empty`)
	})
}

func TestKeySortingTypeToKeySorting(t *testing.T) {
	require.Equal(t, KeySorting_KEY_SORTING_NATURAL, KeySortingType_NATURAL.ToKeySorting())
	require.Equal(t, KeySorting_KEY_SORTING_HIERARCHICAL, KeySortingType_HIERARCHICAL.ToKeySorting())
	// The data servers keep the keys in hierarchical order when the key sorting is not set
	require.Equal(t, KeySorting_KEY_SORTING_HIERARCHICAL, KeySortingType_UNKNOWN.ToKeySorting())
}

func TestAntiAffinityValidate(t *testing.T) {
	require.ErrorContains(t, (&AntiAffinity{Mode: AntiAffinityModeStrict}).Validate(), "labels must not be empty")
	require.ErrorContains(t, (&AntiAffinity{Labels: []string{"zone", " "}, Mode: AntiAffinityModeStrict}).Validate(),
		"labels must not contain empty values")
	require.ErrorContains(t, (&AntiAffinity{Labels: []string{"zone"}}).Validate(), "mode must be one of")
	require.NoError(t, (&AntiAffinity{Labels: []string{"zone"}, Mode: AntiAffinityModeRelaxed}).Validate())
}

func TestLoadBalancerScheduleIntervalDuration(t *testing.T) {
	var nilLoadBalancer *LoadBalancer
	duration, err := nilLoadBalancer.GetScheduleIntervalDuration()
	require.NoError(t, err)
	assert.Zero(t, duration)
	assert.Equal(t, defaultLoadBalancerScheduleInterval, nilLoadBalancer.GetScheduleIntervalDurationOrDefault())
	nilLoadBalancer.SetScheduleIntervalDuration(time.Second)

	lb := &LoadBalancer{}
	assert.Equal(t, defaultLoadBalancerScheduleInterval, lb.GetScheduleIntervalDurationOrDefault())
	lb.SetScheduleIntervalDuration(2 * time.Second)
	assert.Equal(t, "2s", lb.GetScheduleInterval())
	assert.Equal(t, 2*time.Second, lb.GetScheduleIntervalDurationOrDefault())

	lb.ScheduleInterval = "x"
	_, err = lb.GetScheduleIntervalDuration()
	require.Error(t, err)
	assert.Equal(t, defaultLoadBalancerScheduleInterval, lb.GetScheduleIntervalDurationOrDefault())
}

func TestLoadBalancerQuarantineTimeDuration(t *testing.T) {
	var nilLoadBalancer *LoadBalancer
	duration, err := nilLoadBalancer.GetQuarantineTimeDuration()
	require.NoError(t, err)
	assert.Zero(t, duration)
	assert.Equal(t, defaultLoadBalancerQuarantineTime, nilLoadBalancer.GetQuarantineTimeDurationOrDefault())
	nilLoadBalancer.SetQuarantineTimeDuration(time.Second)

	lb := &LoadBalancer{}
	assert.Equal(t, defaultLoadBalancerQuarantineTime, lb.GetQuarantineTimeDurationOrDefault())
	lb.SetQuarantineTimeDuration(time.Minute)
	assert.Equal(t, "1m0s", lb.GetQuarantineTime())
	assert.Equal(t, time.Minute, lb.GetQuarantineTimeDurationOrDefault())

	lb.QuarantineTime = "x"
	_, err = lb.GetQuarantineTimeDuration()
	require.Error(t, err)
	assert.Equal(t, defaultLoadBalancerQuarantineTime, lb.GetQuarantineTimeDurationOrDefault())
}
