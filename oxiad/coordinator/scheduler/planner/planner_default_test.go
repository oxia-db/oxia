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

package planner

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

func TestNewGroupsTheTiersInPhases(t *testing.T) {
	p := defaultPlanner()

	require.Len(t, p.phases, 3)
	assert.Equal(t, model.Replace, p.phases[0].kind)
	assert.Len(t, p.phases[0].tiers, 3)
	assert.Equal(t, model.TransferLeader, p.phases[1].kind)
	assert.Len(t, p.phases[1].tiers, 2)
	assert.Equal(t, model.Split, p.phases[2].kind)
	assert.Len(t, p.phases[2].tiers, 1)
}

func TestPlannerPlanDrainsRemovedDataServer(t *testing.T) {
	s := cluster(3)
	removed(s, "r")
	addShard(s, "a", 1, "r", "n0")
	addShard(s, "a", 2, "n1", "n2")

	ops, blocked := defaultPlanner().Plan(s)

	assert.Empty(t, blocked)
	require.Len(t, ops, 1)
	assert.Equal(t, []string{"replace 1 r n1"}, describe(ops))
}

func TestPlannerPlanSettlesBalanced(t *testing.T) {
	for dataServers := 3; dataServers <= 6; dataServers++ {
		t.Run(fmt.Sprintf("dataServers=%d", dataServers), func(t *testing.T) {
			s := cluster(dataServers)
			// Every replica starts on the first data servers, and a removed one
			removed(s, "r")
			for i := range 12 {
				addShard(s, "a", int64(i), "n0", "n1", "r").Leader = "n0"
				addShard(s, "b", int64(100+i), "n0", "n1").Leader = "n0"
			}

			moves := 0
			for {
				ops, _ := defaultPlanner().Plan(s)
				if len(ops) == 0 {
					break
				}
				apply(s, ops[0])
				moves++
				require.Less(t, moves, 1000, "the plan doesn't settle")
			}

			l := tier.NewLoad(s)
			assert.Zero(t, l.GetTotalReplicas()["r"])
			for _, ns := range []string{"a", "b"} {
				assert.LessOrEqual(t, spread(s, l.GetReplicas(ns)), 1, ns)
			}
			assert.LessOrEqual(t, spread(s, l.GetTotalReplicas()), 1)
			for _, ns := range []string{"a", "b"} {
				assert.LessOrEqual(t, spread(s, l.GetLeaders(ns)), 1, ns)
			}
			assert.LessOrEqual(t, spread(s, l.GetTotalLeaders()), 1)
		})
	}
}

func TestPlannerPlanMovesOnAGapOfTwo(t *testing.T) {
	s := cluster(4)
	addShard(s, "a", 1, "n0", "n1")
	addShard(s, "a", 2, "n0", "n2")
	addShard(s, "a", 3, "n0", "n3")
	addShard(s, "a", 4, "n0", "n1")
	addShard(s, "a", 5, "n2", "n3")

	// n0 has 4 replicas, the others 2
	ops, _ := defaultPlanner().Plan(s)
	require.Len(t, ops, 1)
	assert.Equal(t, "n0", ops[0].(*action.ChangeEnsembleAction).From.GetInternal())

	// n0 has 3, a gap of 1
	apply(s, ops[0])
	ops, _ = defaultPlanner().Plan(s)
	assert.Empty(t, ops)
}

func TestPlannerPlanRespectsTheLimits(t *testing.T) {
	newDrain := func() *stats.ClusterStats {
		s := cluster(5)
		removed(s, "r")
		for i := range 4 {
			addShard(s, "a", int64(i), "r", "n0")
			addShard(s, "b", int64(10+i), "r", "n0")
		}
		return s
	}

	t.Run("one replica per data server", func(t *testing.T) {
		ops, _ := defaultPlanner().Plan(newDrain())
		assert.ElementsMatch(t, []string{"n1", "n2", "n3", "n4"}, targets(ops))
	})
	t.Run("one leader per data server", func(t *testing.T) {
		// n0 leads 4 shards, n1 none
		s := cluster(2)
		for i := range 4 {
			addShard(s, "a", int64(i), "n0", "n1").Leader = "n0"
		}
		ops, _ := defaultPlanner().Plan(s)
		assert.Equal(t, []string{"transfer 0 n1"}, describe(ops))
	})
	t.Run("one op per shard", func(t *testing.T) {
		s := cluster(4)
		removed(s, "r0")
		removed(s, "r1")
		addShard(s, "a", 1, "r0", "r1", "n0")
		ops, _ := defaultPlanner().Plan(s)
		assert.Len(t, ops, 1)
	})
}

func TestPlannerPlanTransfersTheLeadersOnceTheReplicasSettle(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"
	addShard(s, "a", 2, "n0", "n2").Leader = "n0"
	addShard(s, "a", 3, "n1", "n2").Leader = "n1"

	ops, _ := defaultPlanner().Plan(s)

	assert.Equal(t, []string{"transfer 2 n2"}, describe(ops))
}

// n0 and n1 share zone a, so no shard has both: n0 can't pass a leader to n1,
// and no transfer improves the leaders alone. n2 passes one to n1, and n0 one
// to n2.
func TestPlannerPlanPassesALeaderThroughAnotherZone(t *testing.T) {
	s := cluster(3)
	for name, zone := range map[string]string{"n0": "a", "n1": "a", "n2": "b"} {
		s.DataServers[name].Metadata = &proto.DataServerMetadata{Labels: map[string]string{"zone": zone}}
	}
	addShard(s, "a", 1, "n0", "n2").Leader = "n0"
	addShard(s, "a", 2, "n0", "n2").Leader = "n0"
	addShard(s, "a", 3, "n1", "n2").Leader = "n2"
	s.Namespaces["a"].AntiAffinities = []*proto.AntiAffinity{{Labels: []string{"zone"}, Mode: "strict"}}

	ops, _ := defaultPlanner().Plan(s)
	assert.Equal(t, []string{"transfer 3 n1", "transfer 1 n2"}, describe(ops))

	for _, op := range ops {
		apply(s, op)
	}
	ops, _ = defaultPlanner().Plan(s)
	assert.Empty(t, ops)
}

func TestPlannerPlanHoldsTheLeadersWhileTheReplicasMove(t *testing.T) {
	newCluster := func() *stats.ClusterStats {
		s := cluster(3)
		addShard(s, "a", 1, "n0", "n1").Leader = "n0"
		addShard(s, "a", 2, "n0", "n2").Leader = "n0"
		addShard(s, "a", 3, "n1", "n2").Leader = "n1"
		return s
	}
	transfers := func(ops []action.Action) int {
		count := 0
		for _, op := range ops {
			if _, ok := op.(*action.ElectionAction); ok {
				count++
			}
		}
		return count
	}

	t.Run("replace left", func(t *testing.T) {
		s := newCluster()
		removed(s, "r")
		addShard(s, "b", 4, "r", "n0")
		ops, _ := defaultPlanner().Plan(s)
		assert.Len(t, ops, 1)
		assert.Zero(t, transfers(ops))
	})
	t.Run("change pending", func(t *testing.T) {
		s := newCluster()
		addShard(s, "b", 4, "n1", "n2").Changing = true
		ops, _ := defaultPlanner().Plan(s)
		assert.Empty(t, ops)
	})
}

func TestPlannerPlanSplitsTheShardTheMostOverOnceTheLeadersSettle(t *testing.T) {
	s := splitCluster()

	ops, _ := defaultPlanner().Plan(s)

	assert.Equal(t, []string{"split a 2"}, describe(ops))
}

func TestPlannerPlanHoldsTheSplits(t *testing.T) {
	t.Run("leaders unbalanced", func(t *testing.T) {
		s := splitCluster()
		s.Namespaces["a"].Shards[3].Leader = "n0"
		ops, _ := defaultPlanner().Plan(s)
		require.Len(t, ops, 1)
		assert.IsType(t, &action.ElectionAction{}, ops[0])
	})
	t.Run("split in progress", func(t *testing.T) {
		s := splitCluster()
		addShard(s, "b", 9, "n0", "n1").Splitting = true
		ops, _ := defaultPlanner().Plan(s)
		assert.Empty(t, ops)
	})
	t.Run("not splittable", func(t *testing.T) {
		s := splitCluster()
		for _, shard := range s.Namespaces["a"].Shards {
			shard.Splittable = false
		}
		ops, _ := defaultPlanner().Plan(s)
		assert.Empty(t, ops)
	})
}

func TestPlannerPlanPrefersMovingAFollower(t *testing.T) {
	s := cluster(3)
	removed(s, "r0")
	removed(s, "r1")
	addShard(s, "a", 1, "r0", "r1", "n0").Leader = "r0"

	ops, _ := defaultPlanner().Plan(s)

	assert.Equal(t, []string{"replace 1 r1 n1"}, describe(ops))
}

func TestPlannerPlacePicksTheLeastLoaded(t *testing.T) {
	s := cluster(4)
	removed(s, "r")
	// n0 has a replica of a, n1 one of b: a goes to n1 before n0
	addShard(s, "a", 1, "n0", "r")
	addShard(s, "b", 2, "n1", "r")

	ensemble, err := defaultPlanner().Place(s, "a", 3, nil, nil)

	require.NoError(t, err)
	assert.Equal(t, []string{"n2", "n3", "n1"}, ensemble)
}

func TestPlannerPlaceKeepsTheAntiAffinities(t *testing.T) {
	s := cluster(4)
	for name, zone := range map[string]string{"n0": "z0", "n1": "z0", "n2": "z1", "n3": "z1"} {
		s.DataServers[name].Metadata = &proto.DataServerMetadata{Labels: map[string]string{"zone": zone}}
	}
	zones := []*proto.AntiAffinity{{Labels: []string{"zone"}}}

	ensemble, err := defaultPlanner().Place(s, "a", 2, zones, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"n0", "n2"}, ensemble)

	_, err = defaultPlanner().Place(s, "a", 3, zones, nil)
	assert.ErrorIs(t, err, errs.ErrNotFound)
}

func TestPlannerPlaceOnlyPicksEligible(t *testing.T) {
	s := cluster(3)

	ensemble, err := defaultPlanner().Place(s, "a", 2, nil, func(node *stats.DataServerStats) bool { return node.Identity.GetInternal() != "n0" })

	require.NoError(t, err)
	assert.Equal(t, []string{"n1", "n2"}, ensemble)
}

// Data servers join each zone: a plan moves a replica onto each of them.
func BenchmarkPlannerPlan(b *testing.B) {
	for _, size := range []struct {
		name                                     string
		zones, perZone, used, namespaces, shards int
	}{
		{"9-servers-99-shards", 3, 3, 2, 1, 99},
		{"51-servers-1000-shards", 3, 17, 12, 1, 1000},
		{"51-servers-10000-shards", 3, 17, 12, 1, 10000},
		{"51-servers-100000-shards", 3, 17, 12, 1, 100000},
		{"51-servers-10-namespaces-10000-shards", 3, 17, 12, 10, 10000},
		{"51-servers-100-namespaces-10000-shards", 3, 17, 12, 100, 10000},
	} {
		b.Run(size.name, func(b *testing.B) {
			s := zonedCluster(size.zones, size.perZone, size.used, size.namespaces, size.shards)
			p := defaultPlanner()
			for b.Loop() {
				p.Plan(s)
			}
		})
	}
}

// cluster returns a snapshot of count data servers, n0 to
// n<count-1>.
func cluster(count int) *stats.ClusterStats {
	s := &stats.ClusterStats{DataServers: make(map[string]*stats.DataServerStats), Namespaces: make(map[string]*stats.NamespaceStats)}
	for i := range count {
		name := fmt.Sprintf("n%d", i)
		s.DataServers[name] = &stats.DataServerStats{DataServer: &proto.DataServer{Identity: &proto.DataServerIdentity{Internal: name}}}
	}
	return s
}

func removed(s *stats.ClusterStats, name string) {
	s.DataServers[name] = &stats.DataServerStats{DataServer: &proto.DataServer{Identity: &proto.DataServerIdentity{Internal: name}},
		Removed: true}
}

func addShard(s *stats.ClusterStats, namespace string, id int64, ensemble ...string) *stats.ShardStats {
	ns, ok := s.Namespaces[namespace]
	if !ok {
		ns = &stats.NamespaceStats{Shards: make(map[int64]*stats.ShardStats)}
		s.Namespaces[namespace] = ns
	}
	shard := &stats.ShardStats{Ensemble: ensemble}
	ns.Shards[id] = shard
	return shard
}

// apply makes op on s, as if the election of a replace kept the leader, or
// moved it to the new member if it replaced the leader.
func apply(s *stats.ClusterStats, op action.Action) {
	shard := s.GetShard(op.GetShard())
	switch op := op.(type) {
	case *action.ElectionAction:
		shard.Leader = op.PreferredLeader
	case *action.ChangeEnsembleAction:
		from, to := op.From.GetInternal(), op.To.GetInternal()
		shard.Ensemble[slices.Index(shard.Ensemble, from)] = to
		if shard.Leader == from {
			shard.Leader = to
		}
	default:
	}
}

// describe names each action by its kind, its shard, and the data servers it
// moves from and to.
func describe(actions []action.Action) []string {
	var names []string
	for _, op := range actions {
		switch op := op.(type) {
		case *action.ElectionAction:
			names = append(names, fmt.Sprintf("transfer %d %s", op.Shard, op.PreferredLeader))
		case *action.SplitAction:
			names = append(names, fmt.Sprintf("split %s %d", op.Namespace, op.Shard))
		case *action.ChangeEnsembleAction:
			names = append(names, fmt.Sprintf("replace %d %s %s", op.Shard, op.From.GetInternal(), op.To.GetInternal()))
		default:
			names = append(names, fmt.Sprintf("%T", op))
		}
	}
	return names
}

func targets(ops []action.Action) []string {
	var to []string
	for _, op := range ops {
		to = append(to, op.(*action.ChangeEnsembleAction).To.GetInternal())
	}
	return to
}

// spread is the gap between the most and the least loaded data servers that
// aren't removed.
func spread(s *stats.ClusterStats, counts map[string]int) int {
	lowest, highest := math.MaxInt, 0
	for name, node := range s.DataServers {
		if !node.Removed {
			lowest = min(lowest, counts[name])
			highest = max(highest, counts[name])
		}
	}
	return highest - lowest
}

// splitCluster has the replicas and the leaders of namespace a balanced on n0
// to n2, and its splittable shards 1 and 2 over their thresholds, 2 the most.
func splitCluster() *stats.ClusterStats {
	s := cluster(3)
	for id, ensemble := range map[int64][]string{1: {"n0", "n1"}, 2: {"n1", "n2"}, 3: {"n2", "n0"}} {
		shard := addShard(s, "a", id, ensemble...)
		shard.Leader = ensemble[0]
		shard.Splittable = true
	}
	s.Namespaces["a"].Shards[1].Overshoot = 1.5
	s.Namespaces["a"].Shards[2].Overshoot = 3
	return s
}

func defaultPlanner() *planner {
	return New(filter.Defaults(), tier.Defaults()).(*planner)
}

// zonedCluster returns zones zones of perZone data servers each, z<zone>-<i>,
// and shards shards spread over namespaces namespaces, each with an
// anti-affinity on the zone: each shard has a replica in each zone, on one of
// its first used data servers, and the member of the first zone leads it. The
// other data servers are empty.
func zonedCluster(zones, perZone, used, namespaces, shards int) *stats.ClusterStats {
	s := &stats.ClusterStats{DataServers: make(map[string]*stats.DataServerStats),
		Namespaces: make(map[string]*stats.NamespaceStats)}
	name := func(zone, i int) string { return fmt.Sprintf("z%d-%02d", zone, i) }
	for zone := range zones {
		for i := range perZone {
			s.DataServers[name(zone, i)] = &stats.DataServerStats{DataServer: &proto.DataServer{
				Identity: &proto.DataServerIdentity{Internal: name(zone, i)},
				Metadata: &proto.DataServerMetadata{Labels: map[string]string{"zone": fmt.Sprint(zone)}}}}
		}
	}
	for n := range namespaces {
		s.Namespaces[fmt.Sprintf("ns-%03d", n)] = &stats.NamespaceStats{Shards: make(map[int64]*stats.ShardStats),
			AntiAffinities: []*proto.AntiAffinity{{Labels: []string{"zone"}, Mode: "strict"}}}
	}
	for id := range shards {
		ensemble := make([]string, 0, zones)
		for zone := range zones {
			ensemble = append(ensemble, name(zone, id%used))
		}
		ns := s.Namespaces[fmt.Sprintf("ns-%03d", id%namespaces)]
		ns.Shards[int64(id)] = &stats.ShardStats{Ensemble: ensemble, Leader: ensemble[0]}
	}
	return s
}
