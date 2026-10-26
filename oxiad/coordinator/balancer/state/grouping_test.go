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

//revive:disable-next-line:var-naming
package state

import (
	"testing"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/stretchr/testify/assert"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

func TestGroupingShardsIncludesNonSteadyShards(t *testing.T) {
	server := func(name string) *proto.DataServerIdentity {
		return &proto.DataServerIdentity{Name: &name}
	}
	ensemble := []*proto.DataServerIdentity{server("server1"), server("server2")}

	namespaces := map[string]commonobject.Borrowed[*proto.NamespaceStatus]{
		"ns-1": commonobject.Borrow(&proto.NamespaceStatus{
			Shards: map[int64]*proto.ShardMetadata{
				// Steady shard with an elected leader
				0: {Status: proto.ShardStatusSteadyState, Leader: server("server1"), Ensemble: ensemble},
				// Just created, no election run yet: still occupies its ensemble
				1: {Status: proto.ShardStatusUnknown, Ensemble: ensemble},
				// Mid-election: still occupies its ensemble
				2: {Status: proto.ShardStatusElection, Ensemble: ensemble},
				// Being deleted: must not be counted
				3: {Status: proto.ShardStatusDeleting, Leader: server("server1"), Ensemble: ensemble},
				// Mid-split: managed by the split controller, must not be counted
				4: {Status: proto.ShardStatusSteadyState, Leader: server("server1"), Ensemble: ensemble,
					Split: &proto.SplitMetadata{ChildShardIds: []int64{5, 6}}},
			},
		}),
	}
	candidates := linkedhashset.New("server1", "server2")

	grouped, _ := GroupingShardsNodeByStatus(candidates, namespaces)
	assert.Len(t, grouped["server1"], 3)
	assert.Len(t, grouped["server2"], 3)

	totalShards, electedShards, leaders := NodeShardLeaders(candidates, namespaces)
	assert.Equal(t, 3, totalShards)
	assert.Equal(t, 1, electedShards)
	assert.Equal(t, 1, leaders["server1"].Size())
	assert.Equal(t, 0, leaders["server2"].Size())
}

func TestGroupingCandidatesNormalCase(t *testing.T) {
	candidatesMetadata := map[string]*proto.DataServerMetadata{
		"server1": {
			Labels: map[string]string{
				"region": "us-east",
				"type":   "compute",
			},
		},
		"server2": {
			Labels: map[string]string{
				"region": "us-west",
				"type":   "compute",
			},
		},
	}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.NotNil(t, result)

	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.True(t, regionGroup["us-east"].Contains("server1"))
	assert.True(t, regionGroup["us-west"].Contains("server2"))

	typeGroup, exists := result["type"]
	assert.True(t, exists)
	assert.True(t, typeGroup["compute"].Contains("server1"))
	assert.True(t, typeGroup["compute"].Contains("server2"))
}

func TestGroupingCandidatesNoCandidates(t *testing.T) {
	candidatesMetadata := map[string]*proto.DataServerMetadata{}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New[string](), candidatesMetadata)
	assert.Empty(t, result)
}

func TestGroupingCandidatesNoMetadata(t *testing.T) {
	candidatesMetadata := map[string]*proto.DataServerMetadata{}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.Empty(t, result)
}

func TestGroupingCandidatesPartialMetadataMissing(t *testing.T) {
	candidatesMetadata := map[string]*proto.DataServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
	}

	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.True(t, regionGroup["us-east"].Contains("server1"))
	assert.False(t, regionGroup["us-east"].Contains("server2"))
}

func TestGroupingCandidatesAllSameLabelValue(t *testing.T) {
	candidatesMetadata := map[string]*proto.DataServerMetadata{
		"server1": {Labels: map[string]string{"region": "us-east"}},
		"server2": {Labels: map[string]string{"region": "us-east"}},
	}
	result := GroupingCandidatesWithLabelValue(linkedhashset.New("server1", "server2"), candidatesMetadata)

	assert.NotNil(t, result)
	regionGroup, exists := result["region"]
	assert.True(t, exists)
	assert.True(t, regionGroup["us-east"].Contains("server1"))
	assert.True(t, regionGroup["us-east"].Contains("server2"))
}
