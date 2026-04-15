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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClusterStatus_Clone(t *testing.T) {
	leaderName := "leader-1"
	followerName1 := "follower-1"
	followerName2 := "follower-2"
	removedName := "removed-1"

	cs1 := &ClusterStatus{
		Namespaces: map[string]NamespaceStatus{
			"test-ns": {
				ReplicationFactor: 3,
				Shards: map[int64]ShardMetadata{
					0: {
						Status: ShardStatusSteadyState,
						Term:   1,
						Leader: &Server{
							Name:     &leaderName,
							Public:   "l1",
							Internal: "l1",
						},
						Ensemble: []Server{{
							Name:     &followerName1,
							Public:   "f1",
							Internal: "f1",
						}, {
							Name:     &followerName2,
							Public:   "f2",
							Internal: "f2",
						}},
						Int32HashRange: Int32HashRange{},
						RemovedNodes: []Server{{
							Name:     &removedName,
							Public:   "r1",
							Internal: "r1",
						}},
						PendingDeleteShardNodes: []Server{{}},
					},
				},
			},
		},
		ShardIdGenerator: 5,
		ServerIdx:        7,
	}

	cs2 := cs1.Clone()

	assert.Equal(t, cs1, cs2)
	assert.NotSame(t, cs1, cs2)
	assert.Equal(t, cs1.Namespaces, cs2.Namespaces)
	assert.NotSame(t, &cs1.Namespaces, &cs2.Namespaces)
	assert.Equal(t, cs1.Namespaces["test-ns"].Shards, cs2.Namespaces["test-ns"].Shards)
	assert.Equal(t, cs1.Namespaces["test-ns"].Shards[0], cs2.Namespaces["test-ns"].Shards[0])
	assert.NotSame(t, cs1.Namespaces["test-ns"].Shards[0].Leader, cs2.Namespaces["test-ns"].Shards[0].Leader)
	assert.NotSame(t, cs1.Namespaces["test-ns"].Shards[0].Leader.Name, cs2.Namespaces["test-ns"].Shards[0].Leader.Name)
	assert.NotSame(t, &cs1.Namespaces["test-ns"].Shards[0].Ensemble[0], &cs2.Namespaces["test-ns"].Shards[0].Ensemble[0])

	assert.Equal(t, cs1.ShardIdGenerator, cs2.ShardIdGenerator)
	assert.Equal(t, cs1.ServerIdx, cs2.ServerIdx)
}

func TestSplitMetadata_Clone(t *testing.T) {
	// Nil clone returns nil
	var nilSplit *SplitMetadata
	assert.Nil(t, nilSplit.Clone())

	// Non-nil clone
	sm := &SplitMetadata{
		Phase:          SplitPhaseCatchUp,
		ParentShardId:  10,
		ChildShardIDs:  []int64{20, 21},
		SplitPoint:     0x80000000,
		SnapshotOffset: 42,
	}

	cloned := sm.Clone()
	assert.Equal(t, sm, cloned)
	assert.NotSame(t, sm, cloned)

	// Verify deep copy of ChildShardIDs slice
	cloned.ChildShardIDs[0] = 99
	assert.EqualValues(t, 20, sm.ChildShardIDs[0])
}

func TestShardMetadata_Clone_WithSplit(t *testing.T) {
	sm := ShardMetadata{
		Status: ShardStatusSteadyState,
		Term:   5,
		Leader: &Server{
			Public:   "l1",
			Internal: "l1",
		},
		Ensemble: []Server{{
			Public:   "f1",
			Internal: "f1",
		}},
		RemovedNodes:            []Server{},
		PendingDeleteShardNodes: []Server{},
		Int32HashRange:          Int32HashRange{Min: 0, Max: 0xFFFFFFFF},
		Split: &SplitMetadata{
			Phase:          SplitPhaseBootstrap,
			ParentShardId:  0,
			ChildShardIDs:  []int64{1, 2},
			SplitPoint:     0x80000000,
			SnapshotOffset: 100,
		},
	}

	cloned := sm.Clone()
	assert.Equal(t, sm, cloned)
	assert.NotSame(t, sm.Split, cloned.Split)

	// Verify deep copy: modifying the clone doesn't affect the original
	cloned.Split.Phase = SplitPhaseCutover
	assert.Equal(t, SplitPhaseBootstrap, sm.Split.Phase)

	cloned.Split.ChildShardIDs[0] = 99
	assert.EqualValues(t, 1, sm.Split.ChildShardIDs[0])
}

func TestShardMetadata_Clone_NilSplit(t *testing.T) {
	sm := ShardMetadata{
		Status: ShardStatusSteadyState,
		Term:   1,
		Ensemble: []Server{{
			Public:   "f1",
			Internal: "f1",
		}},
		RemovedNodes:            []Server{},
		PendingDeleteShardNodes: []Server{},
		Int32HashRange:          Int32HashRange{Min: 0, Max: 0xFFFFFFFF},
		Split:                   nil,
	}

	cloned := sm.Clone()
	assert.Equal(t, sm, cloned)
	assert.Nil(t, cloned.Split)
}
