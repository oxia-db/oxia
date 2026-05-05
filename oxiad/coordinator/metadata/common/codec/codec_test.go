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

package codec

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
)

func TestDecodeYAMLCompatibility(t *testing.T) {
	config, err := ClusterConfigCodec.UnmarshalYAML([]byte(`
namespaces:
  - name: default
    initialShardCount: 1
    replicationFactor: 3
  - name: analytics
    initialShardCount: 4
    replicationFactor: 2
    notificationsEnabled: false
    keySorting: natural
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
	require.NoError(t, config.Validate())

	require.Len(t, config.GetNamespaces(), 2)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
	require.True(t, config.GetNamespaces()[0].NotificationsEnabledOrDefault())
	require.Nil(t, config.GetNamespaces()[0].NotificationsEnabled)

	require.Equal(t, "analytics", config.GetNamespaces()[1].GetName())
	require.False(t, config.GetNamespaces()[1].NotificationsEnabledOrDefault())
	require.Equal(t, "natural", config.GetNamespaces()[1].GetKeySorting())
	require.Len(t, config.GetNamespaces()[1].GetAntiAffinities(), 2)
	require.Equal(t, commonproto.AntiAffinityModeStrict,
		config.GetNamespaces()[1].GetAntiAffinities()[0].GetMode())
	require.Equal(t, commonproto.AntiAffinityModeRelaxed,
		config.GetNamespaces()[1].GetAntiAffinities()[1].GetMode())

	require.Len(t, config.GetServers(), 3)
	require.Equal(t, "node-1", config.GetServers()[0].GetNameOrDefault())
	require.Equal(t, "localhost:7659", config.GetServers()[1].GetNameOrDefault())
	require.Equal(t, map[string]string{"zone": "us-east-1"}, config.GetServerMetadata()["node-1"].GetLabels())
	require.Equal(t, map[string]string{"zone": "us-west-1"}, config.GetServerMetadata()["localhost:7659"].GetLabels())
	require.Equal(t, []string{"bootstrap:6648"}, config.GetAllowExtraAuthorities())
	require.Equal(t, "15s", config.GetLoadBalancer().GetScheduleInterval())
	require.Equal(t, "3m", config.GetLoadBalancer().GetQuarantineTime())
	require.Equal(t, 15*time.Second, config.GetLoadBalancer().GetScheduleIntervalDurationOrDefault())
	require.Equal(t, 3*time.Minute, config.GetLoadBalancer().GetQuarantineTimeDurationOrDefault())
}

func TestDecodeJSONCompatibility(t *testing.T) {
	config, err := ClusterConfigCodec.UnmarshalYAML([]byte(`{
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
	require.NoError(t, config.Validate())
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, "hierarchical", config.GetNamespaces()[0].GetKeySorting())
	require.Equal(t, map[string]string{"rack": "rack-a"}, config.GetServerMetadata()["node-1"].GetLabels())
}

func TestDecodeClusterConfigurationJSONIgnoresUnknownFields(t *testing.T) {
	config, err := ClusterConfigCodec.UnmarshalJSON([]byte(`{
  "namespaces": [
    {
      "name": "default",
      "initialShardCount": 1,
      "replicationFactor": 1,
      "keySorting": "hierarchical",
      "unknownNamespaceField": "ignored"
    }
  ],
  "servers": [
    {
      "public": "localhost:6648",
      "internal": "localhost:6649",
      "unknownServerField": "ignored"
    }
  ],
  "unknownTopLevelField": {
    "nested": true
  }
}`))
	require.NoError(t, err)
	require.NoError(t, config.Validate())
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
	require.Len(t, config.GetServers(), 1)
	require.Equal(t, "localhost:6649", config.GetServers()[0].GetInternal())
}

func TestDecodeClusterConfigurationYAMLIgnoresUnknownFields(t *testing.T) {
	config, err := ClusterConfigCodec.UnmarshalYAML([]byte(`
namespaces:
  - name: default
    initialShardCount: 1
    replicationFactor: 1
    keySorting: hierarchical
    unknownNamespaceField: ignored
servers:
  - public: localhost:6648
    internal: localhost:6649
    unknownServerField: ignored
unknownTopLevelField:
  nested: true
`))
	require.NoError(t, err)
	require.NoError(t, config.Validate())
	require.Len(t, config.GetNamespaces(), 1)
	require.Equal(t, "default", config.GetNamespaces()[0].GetName())
	require.Len(t, config.GetServers(), 1)
	require.Equal(t, "localhost:6649", config.GetServers()[0].GetInternal())
}

func TestDecodeYAMLOmittedKeySortingCompatibility(t *testing.T) {
	config, err := ClusterConfigCodec.UnmarshalYAML([]byte(`
namespaces:
  - name: default
    initialShardCount: 1
    replicationFactor: 1
servers:
  - public: localhost:6648
    internal: localhost:6649
`))
	require.NoError(t, err)
	require.NoError(t, config.Validate())
	require.Len(t, config.GetNamespaces(), 1)
	require.Empty(t, config.GetNamespaces()[0].GetKeySorting())
	keySorting, err := config.GetNamespaces()[0].GetKeySortingType()
	require.NoError(t, err)
	require.Equal(t, commonproto.KeySortingType_UNKNOWN, keySorting)
}

func TestValidateAllowsEmptyConfig(t *testing.T) {
	config := &commonproto.ClusterConfiguration{}
	require.NoError(t, config.Validate())
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
				KeySorting:           "hierarchical",
				NotificationsEnabled: &notificationsEnabled,
				AntiAffinities: []*commonproto.AntiAffinity{
					{
						Labels: []string{"zone"},
						Mode:   commonproto.AntiAffinityModeStrict,
					},
				},
			},
		},
		Servers: []*commonproto.DataServerIdentity{
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
		ServerMetadata: map[string]*commonproto.DataServerMetadata{
			"node-1": {
				Labels: map[string]string{"zone": "us-east-1"},
			},
		},
		LoadBalancer: &commonproto.LoadBalancer{
			ScheduleInterval: "30s",
			QuarantineTime:   "5m",
		},
	}

	data, err := ClusterConfigCodec.MarshalYAML(config)
	require.NoError(t, err)

	decoded, err := ClusterConfigCodec.UnmarshalYAML(data)
	require.NoError(t, err)
	require.NoError(t, decoded.Validate())
	require.True(t, gproto.Equal(config, decoded))
}

func TestDecodeClusterStatusJSONCompatibility(t *testing.T) {
	status, err := ClusterStatusCodec.UnmarshalJSON([]byte(`{
  "namespaces": {
    "default": {
      "replicationFactor": 3,
      "shards": {
        "1": {
          "status": "SteadyState",
          "term": 12,
          "leader": {
            "name": "node-1",
            "public": "localhost:6648",
            "internal": "localhost:6649"
          },
          "ensemble": [
            {
              "name": "node-1",
              "public": "localhost:6648",
              "internal": "localhost:6649"
            },
            {
              "public": "localhost:7658",
              "internal": "localhost:7659"
            }
          ],
          "removedNodes": [],
          "pendingDeleteShardNodes": [],
          "int32HashRange": {
            "min": 0,
            "max": 4294967295
          },
          "split": {
            "phase": "Bootstrap",
            "parentShardId": 0,
            "childShardIds": [2, 3],
            "splitPoint": 123,
            "snapshotOffset": 456,
            "parentTermAtBootstrap": 789,
            "childLeadersAtBootstrap": {
              "2": "localhost:6649"
            }
          }
        }
      }
    }
  },
  "shardIdGenerator": 4,
  "serverIdx": 2,
  "instanceId": "instance-1"
}`))
	require.NoError(t, err)
	require.Equal(t, uint32(3), status.GetNamespaces()["default"].GetReplicationFactor())
	shard := status.GetNamespaces()["default"].GetShards()[1]
	require.Equal(t, commonproto.ShardStatusSteadyState, shard.GetStatusOrDefault())
	require.Equal(t, int64(12), shard.GetTerm())
	require.Equal(t, "node-1", shard.GetLeader().GetNameOrDefault())
	require.Equal(t, uint32(0), shard.GetInt32HashRange().GetMin())
	require.Equal(t, uint32(4294967295), shard.GetInt32HashRange().GetMax())
	require.NotNil(t, shard.GetSplit())
	require.Equal(t, commonproto.SplitPhaseBootstrap, shard.GetSplit().GetPhaseOrDefault())
	require.Equal(t, []int64{2, 3}, shard.GetSplit().GetChildShardIds())
	require.Equal(t, "instance-1", status.GetInstanceId())
}

func TestDecodeClusterStatusJSONIgnoresUnknownFields(t *testing.T) {
	status, err := ClusterStatusCodec.UnmarshalJSON([]byte(`{
  "namespaces": {
    "default": {
      "replicationFactor": 3,
      "unknownNamespaceField": "ignored",
      "shards": {
        "1": {
          "status": "SteadyState",
          "term": 12,
          "unknownShardField": {
            "nested": true
          },
          "int32HashRange": {
            "min": 0,
            "max": 4294967295,
            "unknownRangeField": 1
          }
        }
      }
    }
  },
  "instanceId": "instance-1",
  "unknownTopLevelField": 123
}`))
	require.NoError(t, err)
	require.Equal(t, "instance-1", status.GetInstanceId())
	require.Equal(t, uint32(3), status.GetNamespaces()["default"].GetReplicationFactor())
	shard := status.GetNamespaces()["default"].GetShards()[1]
	require.Equal(t, commonproto.ShardStatusSteadyState, shard.GetStatusOrDefault())
	require.Equal(t, int64(12), shard.GetTerm())
	require.Equal(t, uint32(0), shard.GetInt32HashRange().GetMin())
	require.Equal(t, uint32(4294967295), shard.GetInt32HashRange().GetMax())
}

func TestDecodeClusterStatusYAMLCompatibility(t *testing.T) {
	status, err := ClusterStatusCodec.UnmarshalYAML([]byte(`
namespaces:
  default:
    replicationFactor: 1
    shards:
      1:
        status: Election
        term: 7
        ensemble:
          - public: localhost:6648
            internal: localhost:6649
        removedNodes:
          - public: localhost:7658
            internal: localhost:7659
        pendingDeleteShardNodes:
          - public: localhost:8658
            internal: localhost:8659
        int32HashRange:
          min: 1
          max: 2
shardIdGenerator: 2
serverIdx: 1
instanceId: instance-yaml
`))
	require.NoError(t, err)
	shard := status.GetNamespaces()["default"].GetShards()[1]
	require.Equal(t, commonproto.ShardStatusElection, shard.GetStatusOrDefault())
	require.Len(t, shard.GetEnsemble(), 1)
	require.Len(t, shard.GetRemovedNodes(), 1)
	require.Len(t, shard.GetPendingDeleteShardNodes(), 1)
	require.Equal(t, uint32(1), shard.GetInt32HashRange().GetMin())
	require.Equal(t, uint32(2), shard.GetInt32HashRange().GetMax())
	require.Equal(t, "instance-yaml", status.GetInstanceId())
}

func TestDecodeClusterStatusYAMLV0163Compatibility(t *testing.T) {
	status, err := ClusterStatusCodec.UnmarshalYAML([]byte(`
namespaces:
  default:
    replicationFactor: 3
    shards:
      1:
        status: 1
        term: 12
        int32HashRange:
          min: 0
          max: 4294967295
        split:
          phase: 1
          parentShardId: 0
          childShardIds:
          - 2
          - 3
          splitPoint: 123
          snapshotOffset: 456
          parentTermAtBootstrap: 789
          childLeadersAtBootstrap:
            2: localhost:7659
shardIdGenerator: 4
serverIdx: 2
`))
	require.NoError(t, err)

	shard := status.GetNamespaces()["default"].GetShards()[1]
	require.Equal(t, commonproto.ShardStatusSteadyState, shard.GetStatusOrDefault())
	require.Equal(t, commonproto.SplitPhaseCatchUp, shard.GetSplit().GetPhaseOrDefault())
	require.Equal(t, "localhost:7659", shard.GetSplit().GetChildLeadersAtBootstrap()[2])
}

func TestEncodeClusterStatusYAMLRoundTrip(t *testing.T) {
	name := "node-1"
	status := &commonproto.ClusterStatus{
		Namespaces: map[string]*commonproto.NamespaceStatus{
			"default": {
				ReplicationFactor: 2,
				Shards: map[int64]*commonproto.ShardMetadata{
					1: {
						Status: commonproto.ShardStatusSteadyState,
						Term:   3,
						Leader: &commonproto.DataServerIdentity{
							Name:     &name,
							Public:   "localhost:6648",
							Internal: "localhost:6649",
						},
						Ensemble: []*commonproto.DataServerIdentity{
							{
								Name:     &name,
								Public:   "localhost:6648",
								Internal: "localhost:6649",
							},
							{
								Public:   "localhost:7658",
								Internal: "localhost:7659",
							},
						},
						Int32HashRange: &commonproto.HashRange{
							Min: 10,
							Max: 20,
						},
						Split: &commonproto.SplitMetadata{
							Phase:         commonproto.SplitPhaseCatchUp,
							ParentShardId: 0,
							ChildShardIds: []int64{2, 3},
							SplitPoint:    15,
						},
					},
				},
			},
		},
		ShardIdGenerator: 4,
		ServerIdx:        1,
		InstanceId:       "instance-round-trip",
	}

	data, err := ClusterStatusCodec.MarshalYAML(status)
	require.NoError(t, err)
	require.Contains(t, string(data), "status: 1")
	require.Contains(t, string(data), "phase: 1")

	decoded, err := ClusterStatusCodec.UnmarshalYAML(data)
	require.NoError(t, err)
	require.True(t, gproto.Equal(status, decoded))
}
