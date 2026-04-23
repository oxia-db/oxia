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

	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

func TestDecodeYAMLCompatibility(t *testing.T) {
	var config ClusterConfiguration
	err := yaml.Unmarshal([]byte(`
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
`), &config)
	require.NoError(t, err)
	config.Normalize()
	require.NoError(t, config.Validate())

	require.Len(t, config.GetNamespaces(), 2)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
	require.True(t, config.GetNamespaces()[0].NotificationsEnabledOrDefault())
	require.Nil(t, config.GetNamespaces()[0].NotificationsEnabled)

	require.Equal(t, "analytics", config.GetNamespaces()[1].GetName())
	require.False(t, config.GetNamespaces()[1].NotificationsEnabledOrDefault())
	require.Equal(t, "natural", config.GetNamespaces()[1].GetKeySorting())
	require.Len(t, config.GetNamespaces()[1].GetPolicy().GetAntiAffinities(), 2)
	require.Equal(t, AntiAffinityModeStrict,
		config.GetNamespaces()[1].GetPolicy().GetAntiAffinities()[0].GetMode())
	require.Equal(t, AntiAffinityModeRelaxed,
		config.GetNamespaces()[1].GetPolicy().GetAntiAffinities()[1].GetMode())

	require.Len(t, config.GetServers(), 3)
	require.Equal(t, "node-1", config.GetServers()[0].GetNameOrDefault())
	require.Equal(t, "localhost:7659", config.GetServers()[1].GetNameOrDefault())
	require.Equal(t, map[string]string{"zone": "us-east-1"}, config.GetServerMetadata()["node-1"].GetLabels())
	require.Equal(t, map[string]string{"zone": "us-west-1"}, config.GetServerMetadata()["localhost:7659"].GetLabels())
	require.Equal(t, []string{"bootstrap:6648"}, config.GetAllowExtraAuthorities())
	require.Equal(t, "15s", config.GetLoadBalancer().GetScheduleInterval())
	require.Equal(t, "3m", config.GetLoadBalancer().GetQuarantineTime())
	require.Equal(t, 15*time.Second, config.GetLoadBalancer().ScheduleIntervalDurationOrDefault())
	require.Equal(t, 3*time.Minute, config.GetLoadBalancer().QuarantineTimeDurationOrDefault())
}

func TestDecodeJSONCompatibility(t *testing.T) {
	var config ClusterConfiguration
	err := yaml.Unmarshal([]byte(`{
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
}`), &config)
	require.NoError(t, err)
	config.Normalize()
	require.NoError(t, config.Validate())
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, "hierarchical", config.GetNamespaces()[0].GetKeySorting())
	require.Equal(t, map[string]string{"rack": "rack-a"}, config.GetServerMetadata()["node-1"].GetLabels())
}

func TestDecodeYAMLOmittedKeySortingCompatibility(t *testing.T) {
	var config ClusterConfiguration
	err := yaml.Unmarshal([]byte(`
namespaces:
  - name: default
    initialShardCount: 1
    replicationFactor: 1
servers:
  - public: localhost:6648
    internal: localhost:6649
`), &config)
	require.NoError(t, err)
	config.Normalize()
	require.NoError(t, config.Validate())
	require.Len(t, config.GetNamespaces(), 1)
	require.Empty(t, config.GetNamespaces()[0].GetKeySorting())
	keySorting, err := config.GetNamespaces()[0].KeySortingType()
	require.NoError(t, err)
	require.Equal(t, KeySortingType_UNKNOWN, keySorting)
}

func TestEncodeYAMLRoundTrip(t *testing.T) {
	name := "node-1"
	notificationsEnabled := false
	config := &ClusterConfiguration{
		Namespaces: []*Namespace{
			{
				Name:                 "default",
				InitialShardCount:    1,
				ReplicationFactor:    3,
				KeySorting:           "hierarchical",
				NotificationsEnabled: &notificationsEnabled,
				Policy: &HierarchyPolicies{
					AntiAffinities: []*AntiAffinity{
						{
							Labels: []string{"zone"},
							Mode:   AntiAffinityModeStrict,
						},
					},
				},
			},
		},
		Servers: []*DataServer{
			{
				Name:     &name,
				Public:   "localhost:6648",
				Internal: "localhost:6649",
			},
			{
				Public:   "localhost:7658",
				Internal: "localhost:7659",
			},
			{
				Public:   "localhost:8658",
				Internal: "localhost:8659",
			},
		},
		AllowExtraAuthorities: []string{"bootstrap:6648"},
		ServerMetadata: map[string]*DataServerMetadata{
			"node-1": {
				Labels: map[string]string{"zone": "us-east-1"},
			},
		},
		LoadBalancer: &LoadBalancer{
			ScheduleInterval: "30s",
			QuarantineTime:   "5m",
		},
	}

	data, err := yaml.Marshal(config)
	require.NoError(t, err)

	var decoded ClusterConfiguration
	err = yaml.Unmarshal(data, &decoded)
	require.NoError(t, err)
	decoded.Normalize()
	require.NoError(t, decoded.Validate())
	require.True(t, gproto.Equal(config, &decoded))
}
