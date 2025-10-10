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

package model

import (
	"github.com/emirpasic/gods/v2/lists/arraylist"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/pkg/errors"
)

type ShardInfo struct {
	Namespace string
	ShardID   int64
	Ensemble  []Server
}

type RatioParams struct {
	HistoryNodes    map[string]Server
	NodeShardsInfos map[string][]ShardInfo
	QuarantineNodes *linkedhashset.Set[string]
}

type Ratio struct {
	maxNodeLoadRatio  float64
	minNodeLoadRatio  float64
	avgShardLoadRatio float64
	nodeLoadRatios    *arraylist.List[*NodeLoadRatio]
}

func (r *Ratio) IsBalanced() bool {
	return r.RatioGap() <= r.avgShardLoadRatio
}

func (r *Ratio) NodeIterator() *arraylist.Iterator[*NodeLoadRatio] {
	iter := arraylist.New(r.nodeLoadRatios.Values()...).Iterator()
	return iter
}

func (r *Ratio) MaxNodeLoadRatio() float64 {
	return r.maxNodeLoadRatio
}

func (r *Ratio) MinNodeLoadRatio() float64 {
	return r.minNodeLoadRatio
}

func (r *Ratio) AvgShardLoadRatio() float64 {
	return r.avgShardLoadRatio
}

func (r *Ratio) RatioGap() float64 {
	return r.maxNodeLoadRatio - r.minNodeLoadRatio
}

func (r *Ratio) MoveShardToNode(shard *ShardLoadRatio, fromNode string, toNode string) {
	for iter := r.nodeLoadRatios.Iterator(); iter.Next(); {
		node := iter.Value()
		if node.NodeID == fromNode {
			node.RemoveShard(shard)
			continue
		}
		if node.NodeID == toNode {
			node.AddShard(shard)
			continue
		}
	}
}

func (r *Ratio) ReCalculateRatios() {
	iter := r.nodeLoadRatios.Iterator()
	if !iter.First() {
		return
	}
	nodeRatio := iter.Value()
	r.maxNodeLoadRatio = nodeRatio.Ratio
	r.minNodeLoadRatio = nodeRatio.Ratio

	for iter.Next() {
		nodeLoadRatio := iter.Value()
		if nodeLoadRatio.Ratio > r.maxNodeLoadRatio {
			r.maxNodeLoadRatio = nodeLoadRatio.Ratio
		} else if nodeLoadRatio.Ratio < r.minNodeLoadRatio {
			r.minNodeLoadRatio = nodeLoadRatio.Ratio
		}
	}
}

func (r *Ratio) RemoveDeletedNode(nodeID string) error {
	for iter := r.nodeLoadRatios.Iterator(); iter.Next(); {
		ratio := iter.Value()
		if ratio.NodeID == nodeID {
			if ratio.Ratio != 0.0 {
				return errors.New("cannot remove non-empty node")
			}
			r.nodeLoadRatios.Remove(iter.Index())
			return nil
		}
	}
	return nil
}

func NewRatio(maxNodeLoadRatio float64, minNodeLoadRatio float64, avgShardLoadRatio float64, nodeLoadRatios *arraylist.List[*NodeLoadRatio]) *Ratio {
	return &Ratio{
		maxNodeLoadRatio:  maxNodeLoadRatio,
		minNodeLoadRatio:  minNodeLoadRatio,
		avgShardLoadRatio: avgShardLoadRatio,
		nodeLoadRatios:    nodeLoadRatios,
	}
}

type NodeLoadRatio struct {
	NodeID      string
	Node        Server
	Ratio       float64
	ShardRatios *arraylist.List[*ShardLoadRatio]
}

func (n *NodeLoadRatio) AddShard(shard *ShardLoadRatio) {
	n.Ratio += shard.Ratio
	n.ShardRatios.Add(shard)
}

func (n *NodeLoadRatio) RemoveShard(shard *ShardLoadRatio) {
	n.Ratio -= shard.Ratio
	n.ShardRatios.Remove(n.ShardRatios.IndexOf(shard))
}

func (n *NodeLoadRatio) ShardIterator() *arraylist.Iterator[*ShardLoadRatio] {
	iter := arraylist.New(n.ShardRatios.Values()...).Iterator()
	return iter
}

type ShardLoadRatio struct {
	*ShardInfo
	Ratio float64
}
