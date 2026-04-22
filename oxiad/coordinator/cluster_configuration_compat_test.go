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

package coordinator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	commonproto "github.com/oxia-db/oxia/common/proto"
)

func TestDecodeYAMLCompatibility(t *testing.T) {
	config, err := decodeClusterConfigurationYAML([]byte(`
namespaces:
  - name: default
    initialShardCount: 1
    replicationFactor: 3
  - name: analytics
    initialShardCount: 4
    replicationFactor: 2
    notificationsEnabled: false
    keySorting: natural
    policy:
      antiAffinities:
        - labels: ["zone"]
          mode: strict
        - labels: ["rack"]
          mode: relaxed
servers:
  - name: node-1
    public: localhost:6648
    internal: localhost:6649
  - public: localhost:7658
    internal: localhost:7659
  - public: localhost:8658
    internal: localhost:8659
allowExtraAuthorities:
  - bootstrap:6648
serverMetadata:
  node-1:
    labels:
      zone: us-east-1
  localhost:7659:
    labels:
      zone: us-west-1
loadBalancer:
  scheduleInterval: 15s
  quarantineTime: 3m
`))
	require.NoError(t, err)

	require.Len(t, config.GetNamespaces(), 2)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
	require.True(t, config.GetNamespaces()[0].NotificationsEnabledOrDefault())
	require.Nil(t, config.GetNamespaces()[0].NotificationsEnabled)

	require.Equal(t, "analytics", config.GetNamespaces()[1].GetName())
	require.False(t, config.GetNamespaces()[1].NotificationsEnabledOrDefault())
	require.Equal(t, commonproto.KeySortingType_NATURAL, config.GetNamespaces()[1].GetKeySorting())
	require.Len(t, config.GetNamespaces()[1].GetHierarchyPolicies().GetAntiAffinities(), 2)
	require.Equal(t, commonproto.AntiAffinityMode_ANTI_AFFINITY_MODE_STRICT,
		config.GetNamespaces()[1].GetHierarchyPolicies().GetAntiAffinities()[0].GetMode())
	require.Equal(t, commonproto.AntiAffinityMode_ANTI_AFFINITY_MODE_RELAXED,
		config.GetNamespaces()[1].GetHierarchyPolicies().GetAntiAffinities()[1].GetMode())

	require.Len(t, config.GetServers(), 3)
	require.Equal(t, "node-1", config.GetServers()[0].GetIdentifier())
	require.Equal(t, "localhost:7659", config.GetServers()[1].GetIdentifier())
	require.Equal(t, map[string]string{"zone": "us-east-1"}, config.GetServerMetadata()["node-1"].GetLabels())
	require.Equal(t, map[string]string{"zone": "us-west-1"}, config.GetServerMetadata()["localhost:7659"].GetLabels())
	require.Equal(t, []string{"bootstrap:6648"}, config.GetAllowExtraAuthorities())
	require.Equal(t, 15*time.Second, config.GetLoadBalancer().GetScheduleInterval().AsDuration())
	require.Equal(t, 3*time.Minute, config.GetLoadBalancer().GetQuarantineTime().AsDuration())
}

func TestDecodeJSONCompatibility(t *testing.T) {
	config, err := decodeClusterConfigurationYAML([]byte(`{
  "namespaces": [
    {
      "name": "default",
      "initialShardCount": 2,
      "replicationFactor": 1,
      "keySorting": "hierarchical"
    }
  ],
  "servers": [
    {
      "name": "node-1",
      "public": "localhost:6648",
      "internal": "localhost:6649"
    }
  ],
  "serverMetadata": {
    "node-1": {
      "labels": {
        "rack": "rack-a"
      }
    }
  }
}`))
	require.NoError(t, err)
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, commonproto.KeySortingType_HIERARCHICAL, config.GetNamespaces()[0].GetKeySorting())
	require.Equal(t, map[string]string{"rack": "rack-a"}, config.GetServerMetadata()["node-1"].GetLabels())
}

func TestEncodeYAMLRoundTrip(t *testing.T) {
	name := "node-1"
	notificationsEnabled := false
	config := &commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{
			{
				Name:                 "default",
				InitialShardCount:    1,
				ReplicationFactor:    3,
				KeySorting:           commonproto.KeySortingType_HIERARCHICAL,
				NotificationsEnabled: &notificationsEnabled,
				HierarchyPolicies: &commonproto.HierarchyPolicies{
					AntiAffinities: []*commonproto.AntiAffinity{
						{
							Labels: []string{"zone"},
							Mode:   commonproto.AntiAffinityMode_ANTI_AFFINITY_MODE_STRICT,
						},
					},
				},
			},
		},
		Servers: []*commonproto.DataServer{
			{
				Name:            &name,
				PublicAddress:   "localhost:6648",
				InternalAddress: "localhost:6649",
			},
			{
				PublicAddress:   "localhost:7658",
				InternalAddress: "localhost:7659",
			},
			{
				PublicAddress:   "localhost:8658",
				InternalAddress: "localhost:8659",
			},
		},
		AllowExtraAuthorities: []string{"bootstrap:6648"},
		ServerMetadata: map[string]*commonproto.DataServerMetadata{
			"node-1": {
				Labels: map[string]string{"zone": "us-east-1"},
			},
		},
		LoadBalancer: &commonproto.LoadBalancer{
			ScheduleInterval: durationpb.New(30 * time.Second),
			QuarantineTime:   durationpb.New(5 * time.Minute),
		},
	}

	data, err := encodeClusterConfigurationYAML(config)
	require.NoError(t, err)

	decoded, err := decodeClusterConfigurationYAML(data)
	require.NoError(t, err)
	require.True(t, gproto.Equal(config, decoded))
}
