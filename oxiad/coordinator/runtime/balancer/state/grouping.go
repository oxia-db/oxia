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
	"cmp"

	"github.com/emirpasic/gods/v2/lists/arraylist"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"

	commonobject "github.com/oxia-db/oxia/common/object"
	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/model"
)

type NamespaceAndShard struct {
	Namespace string
	ShardID   int64
}

func NodeShardLeaders(candidates *linkedhashset.Set[string], namespaces map[string]commonobject.Borrowed[*commonproto.NamespaceStatus]) (totalShards int, electedShards int, result map[string]*arraylist.List[NamespaceAndShard]) {
	result = make(map[string]*arraylist.List[NamespaceAndShard])
	totalShards = 0
	electedShards = 0
	for namespace, borrowedNamespaceStatus := range namespaces {
		ns := borrowedNamespaceStatus.UnsafeBorrow()
		for shardID, shardStatus := range ns.Shards {
			if !isBalancingCandidate(shardStatus) {
				continue
			}
			totalShards++
			if leader := shardStatus.Leader; leader != nil {
				electedShards++
				leaderNodeID := leader.GetNameOrDefault()
				var exist bool
				if _, exist = result[leaderNodeID]; !exist {
					result[leaderNodeID] = arraylist.New[NamespaceAndShard]()
				}
				result[leaderNodeID].Add(NamespaceAndShard{
					Namespace: namespace,
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

func GroupingShardsNodeByStatus(candidates *linkedhashset.Set[string], namespaces map[string]commonobject.Borrowed[*commonproto.NamespaceStatus]) (map[string][]model.ShardInfo, map[string]*commonproto.DataServerIdentity) {
	groupingShardByNode := make(map[string][]model.ShardInfo)
	historyNodes := make(map[string]*commonproto.DataServerIdentity)
	for namespace, borrowedNamespaceStatus := range namespaces {
		namespaceStatus := borrowedNamespaceStatus.UnsafeBorrow()
		for shard, shardStatus := range namespaceStatus.Shards {
			if !isBalancingCandidate(shardStatus) {
				continue
			}
			for idx, node := range shardStatus.Ensemble {
				nodeID := node.GetNameOrDefault()
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
	for iter := candidates.Iterator(); iter.Next(); {
		nodeID := iter.Value()
		_, exist := groupingShardByNode[nodeID]
		if !exist {
			groupingShardByNode[nodeID] = make([]model.ShardInfo, 0)
		}
	}
	return groupingShardByNode, historyNodes
}

// A shard participates in load/leader balancing unless it is being deleted or
// is part of an in-progress split. Shards in Unknown or Election status must
// be counted: their ensemble placement is already decided, and ignoring them
// would let the balancer (and the initial ensemble selection) act on a partial
// view of the cluster, proposing moves that re-introduce imbalance once the
// hidden shards become visible again.
func isBalancingCandidate(shardStatus *commonproto.ShardMetadata) bool {
	return shardStatus.GetStatusOrDefault() != commonproto.ShardStatusDeleting && shardStatus.Split == nil
}

func GroupingCandidatesWithLabelValue(candidates *linkedhashset.Set[string], candidatesMetadata map[string]*commonproto.DataServerMetadata) map[string]map[string]*linkedhashset.Set[string] {
	groupedCandidates := make(map[string]map[string]*linkedhashset.Set[string])
	for iterator := candidates.Iterator(); iterator.Next(); {
		candidate := iterator.Value()
		metadata, exist := candidatesMetadata[candidate]
		if !exist {
			continue
		}
		for label, labelValue := range metadata.GetLabels() {
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

func GroupingValueWithLabel(candidates *linkedhashset.Set[string], candidatesMetadata map[string]*commonproto.DataServerMetadata) map[string]*linkedhashset.Set[string] {
	selectedLabelValues := make(map[string]*linkedhashset.Set[string])
	for selectedIter := candidates.Iterator(); selectedIter.Next(); {
		nodeID := selectedIter.Value()
		if metadata, exist := candidatesMetadata[nodeID]; exist {
			for label, labelValue := range metadata.GetLabels() {
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
