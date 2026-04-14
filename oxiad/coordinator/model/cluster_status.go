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

import "github.com/google/uuid"

type Int32HashRange struct {
	// The minimum inclusive hash that the shard can contain
	Min uint32 `json:"min"`

	// The maximum inclusive hash that the shard can contain
	Max uint32 `json:"max"`
}

// SplitMetadata tracks an in-progress shard split. This is set on both the
// parent shard (with ChildShardIDs) and the child shards (with ParentShardId).
// The split state is orthogonal to the shard's operational status.
type SplitMetadata struct {
	Phase SplitPhase `json:"phase" yaml:"phase"`

	// ParentShardId is set on child shards, pointing to the parent they
	// were split from.
	ParentShardId int64 `json:"parentShardId" yaml:"parentShardId"`

	// ChildShardIDs is set on the parent shard, pointing to the children.
	ChildShardIDs []int64 `json:"childShardIds,omitempty" yaml:"childShardIds,omitempty"`

	// SplitPoint is the hash boundary. Left child gets [min, splitPoint],
	// right child gets [splitPoint+1, max].
	SplitPoint uint32 `json:"splitPoint" yaml:"splitPoint"`

	// SnapshotOffset is the parent WAL offset at which the snapshot was
	// taken. Children catch up from this point.
	SnapshotOffset int64 `json:"snapshotOffset,omitempty" yaml:"snapshotOffset,omitempty"`

	// ParentTermAtBootstrap is the parent's term when observers were added
	// during the Bootstrap phase. If the parent's term changes (leader
	// election), the CatchUp phase detects this and falls back to Bootstrap
	// to re-add observers.
	ParentTermAtBootstrap int64 `json:"parentTermAtBootstrap,omitempty" yaml:"parentTermAtBootstrap,omitempty"`

	// ChildLeadersAtBootstrap maps child shard ID → leader internal address
	// as set during Bootstrap. If a child leader changes during CatchUp
	// (detected by comparing with current metadata), the CatchUp phase falls
	// back to Bootstrap to re-add observers on the parent for the new leader.
	ChildLeadersAtBootstrap map[int64]string `json:"childLeadersAtBootstrap,omitempty" yaml:"childLeadersAtBootstrap,omitempty"`
}

func (sm *SplitMetadata) Clone() *SplitMetadata {
	if sm == nil {
		return nil
	}
	r := &SplitMetadata{
		Phase:                 sm.Phase,
		ParentShardId:         sm.ParentShardId,
		SplitPoint:            sm.SplitPoint,
		SnapshotOffset:        sm.SnapshotOffset,
		ParentTermAtBootstrap: sm.ParentTermAtBootstrap,
	}
	if sm.ChildShardIDs != nil {
		r.ChildShardIDs = make([]int64, len(sm.ChildShardIDs))
		copy(r.ChildShardIDs, sm.ChildShardIDs)
	}
	if sm.ChildLeadersAtBootstrap != nil {
		r.ChildLeadersAtBootstrap = make(map[int64]string, len(sm.ChildLeadersAtBootstrap))
		for k, v := range sm.ChildLeadersAtBootstrap {
			r.ChildLeadersAtBootstrap[k] = v
		}
	}
	return r
}

type ShardMetadata struct {
	Status   ShardStatus `json:"status" yaml:"status"`
	Term     int64       `json:"term" yaml:"term"`
	Leader   *Server     `json:"leader" yaml:"leader"`
	Ensemble []Server    `json:"ensemble" yaml:"ensemble"`

	// RemovedNodes are nodes that are being swapped out of the ensembles
	// They still participate in the leader election, as they are part of
	// the NewTerm quorum.
	RemovedNodes []Server `json:"removedNodes" yaml:"removedNodes"`

	// PendingDeleteShardNodes are nodes that were already swapped out
	// of the ensemble. We are just tracking them until the shard can
	// be deleted from disk. These are not anymore part of the NewTerm
	// quorum.
	PendingDeleteShardNodes []Server       `json:"pendingDeleteShardNodes" yaml:"pendingDeleteShardNodes"`
	Int32HashRange          Int32HashRange `json:"int32HashRange" yaml:"int32HashRange"`

	// Non-nil only when this shard is involved in an active split
	// (either as parent or as child).
	Split *SplitMetadata `json:"split,omitempty" yaml:"split,omitempty"`
}

type NamespaceStatus struct {
	ReplicationFactor uint32                  `json:"replicationFactor" yaml:"replicationFactor"`
	Shards            map[int64]ShardMetadata `json:"shards" yaml:"shards"`
}

type ClusterStatus struct {
	ClusterId        string                     `json:"clusterId" yaml:"clusterId"`
	Namespaces       map[string]NamespaceStatus `json:"namespaces" yaml:"namespaces"`
	ShardIdGenerator int64                      `json:"shardIdGenerator" yaml:"shardIdGenerator"`
	ServerIdx        uint32                     `json:"serverIdx" yaml:"serverIdx"`
}

func NewClusterStatus() *ClusterStatus {
	return &ClusterStatus{
		ClusterId:        uuid.NewString(),
		Namespaces:       map[string]NamespaceStatus{},
		ShardIdGenerator: 0,
		ServerIdx:        0,
	}
}

func (sm Int32HashRange) Clone() Int32HashRange {
	return Int32HashRange{
		Min: sm.Min,
		Max: sm.Max,
	}
}

func (sm ShardMetadata) Clone() ShardMetadata {
	r := ShardMetadata{
		Status:                  sm.Status,
		Term:                    sm.Term,
		Leader:                  sm.Leader,
		Ensemble:                make([]Server, len(sm.Ensemble)),
		RemovedNodes:            make([]Server, len(sm.RemovedNodes)),
		PendingDeleteShardNodes: make([]Server, len(sm.PendingDeleteShardNodes)),
		Int32HashRange:          sm.Int32HashRange.Clone(),
		Split:                   sm.Split.Clone(),
	}

	copy(r.Ensemble, sm.Ensemble)
	copy(r.RemovedNodes, sm.RemovedNodes)
	copy(r.PendingDeleteShardNodes, sm.PendingDeleteShardNodes)

	return r
}

func (n NamespaceStatus) Clone() NamespaceStatus {
	r := NamespaceStatus{
		Shards:            make(map[int64]ShardMetadata),
		ReplicationFactor: n.ReplicationFactor,
	}

	for shard, sm := range n.Shards {
		r.Shards[shard] = sm.Clone()
	}

	return r
}

func (c ClusterStatus) Clone() *ClusterStatus {
	r := &ClusterStatus{
		ClusterId:        c.ClusterId,
		Namespaces:       make(map[string]NamespaceStatus),
		ShardIdGenerator: c.ShardIdGenerator,
		ServerIdx:        c.ServerIdx,
	}

	for name, n := range c.Namespaces {
		r.Namespaces[name] = n.Clone()
	}

	return r
}
