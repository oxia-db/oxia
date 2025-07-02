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

package utils

import (
	"cmp"

	"github.com/emirpasic/gods/v2/lists/arraylist"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"

	"github.com/oxia-db/oxia/coordinator/model"
)

type NamespaceAndShard struct {
	Namespace string
	ShardID   int64
}

func NodeShardLeaders(candidates *linkedhashset.Set[string], status *model.ClusterStatus) (totalShards int, electedShards int, result map[string]*arraylist.List[NamespaceAndShard]) {
	result = make(map[string]*arraylist.List[NamespaceAndShard])
	totalShards = 0
	electedShards = 0
	for na, ns := range status.Namespaces {
		for shardID, shardStatus := range ns.Shards {
			totalShards++
			if leader := shardStatus.Leader; leader != nil {
				electedShards++
				leaderNodeID := leader.GetIdentifier()
				var exist bool
				if _, exist = result[leaderNodeID]; !exist {
					result[leaderNodeID] = arraylist.New[NamespaceAndShard]()
				}
				result[leaderNodeID].Add(NamespaceAndShard{
					Namespace: na,
					ShardID:   shardID,
				})
			}
		}
	}
	for _, shards := range result {
		shards.Sort(func(x, y NamespaceAndShard) int {
			return cmp.Compare(x.ShardID, y.ShardID)
		})
	}
	for iter := candidates.Iterator(); iter.Next(); {
		nodeID := iter.Value()
		_, exist := result[nodeID]
		if !exist {
			result[nodeID] = arraylist.New[NamespaceAndShard]()
		}
	}
	return totalShards, electedShards, result
}

func GroupingShardsNodeByStatus(candidates *linkedhashset.Set[string], status *model.ClusterStatus) (map[string][]model.ShardInfo, map[string]model.Server) {
	groupingShardByNode := make(map[string][]model.ShardInfo)
	historyNodes := make(map[string]model.Server)
	if status != nil {
		for namespace, namespaceStatus := range status.Namespaces {
			for shard, shardStatus := range namespaceStatus.Shards {
				for idx, node := range shardStatus.Ensemble {
					nodeID := node.GetIdentifier()
					var groupedShard []model.ShardInfo
					var exist bool
					if groupedShard, exist = groupingShardByNode[nodeID]; !exist {
						tmp := make([]model.ShardInfo, 0)
						groupedShard = tmp
					}
					groupedShard = append(groupedShard, model.ShardInfo{
						Namespace: namespace,
						ShardID:   shard,
						Ensemble:  shardStatus.Ensemble,
					})
					groupingShardByNode[nodeID] = groupedShard
					historyNodes[nodeID] = shardStatus.Ensemble[idx]
				}
			}
		}
	}
	for iter := candidates.Iterator(); iter.Next(); {
		nodeID := iter.Value()
		_, exist := groupingShardByNode[nodeID]
		if !exist {
			groupingShardByNode[nodeID] = make([]model.ShardInfo, 0)
		}
	}
	return groupingShardByNode, historyNodes
}

func GroupingCandidatesWithLabelValue(candidates *linkedhashset.Set[string], candidatesMetadata map[string]model.ServerMetadata) map[string]map[string]*linkedhashset.Set[string] {
	groupedCandidates := make(map[string]map[string]*linkedhashset.Set[string])
	for iterator := candidates.Iterator(); iterator.Next(); {
		candidate := iterator.Value()
		metadata, exist := candidatesMetadata[candidate]
		if !exist {
			continue
		}
		for label, labelValue := range metadata.Labels {
			labelGroup, exist := groupedCandidates[label]
			if !exist {
				tmp := make(map[string]*linkedhashset.Set[string])
				groupedCandidates[label] = tmp
				labelGroup = tmp
			}
			labelValueGroup, exist := labelGroup[labelValue]
			if !exist {
				tmp := linkedhashset.New[string]()
				labelGroup[labelValue] = tmp
				labelValueGroup = tmp
			}
			labelValueGroup.Add(candidate)
		}
	}
	return groupedCandidates
}

func GroupingValueWithLabel(candidates *linkedhashset.Set[string], candidatesMetadata map[string]model.ServerMetadata) map[string]*linkedhashset.Set[string] {
	selectedLabelValues := make(map[string]*linkedhashset.Set[string])
	for selectedIter := candidates.Iterator(); selectedIter.Next(); {
		nodeID := selectedIter.Value()
		if metadata, exist := candidatesMetadata[nodeID]; exist {
			for label, labelValue := range metadata.Labels {
				collection := selectedLabelValues[label]
				if collection == nil {
					collection = linkedhashset.New(labelValue)
				} else {
					collection.Add(labelValue)
				}
				selectedLabelValues[label] = collection
			}
		}
	}
	return selectedLabelValues
}
