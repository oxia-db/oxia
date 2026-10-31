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
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/load"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

func TestPlannerPlanDrainsRemovedDataServer(t *testing.T) {
	s := cluster(3)
	removed(s, "r")
	addShard(s, "a", 1, "r", "n0")
	addShard(s, "a", 2, "n1", "n2")

	ops, stuck := defaultPlanner().Plan(s, unlimited(10, 0), nil)

	assert.Empty(t, stuck)
	require.Len(t, ops, 1)
	assert.Equal(t, model.Op{Namespace: "a", Shard: 1, From: "r", To: "n1", Reason: "drain"}, ops[0])
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
				ops, _ := defaultPlanner().Plan(s, unlimited(1, 0), nil)
				if len(ops) == 0 {
					break
				}
				apply(s, ops[0])
				moves++
				require.Less(t, moves, 1000, "the plan doesn't settle")
			}

			l := load.New(s)
			assert.Zero(t, l.TotalReplicas()["r"])
			for _, ns := range []string{"a", "b"} {
				assert.LessOrEqual(t, spread(s, l.Replicas(ns)), 1, ns)
			}
			assert.LessOrEqual(t, spread(s, l.TotalReplicas()), 1)
			for _, ns := range []string{"a", "b"} {
				assert.LessOrEqual(t, spread(s, l.Leaders(ns)), 1, ns)
			}
			assert.LessOrEqual(t, spread(s, l.TotalLeaders()), 1)
		})
	}
}

func TestPlannerPlanStopsWithinTolerance(t *testing.T) {
	s := cluster(4)
	addShard(s, "a", 1, "n0", "n1")
	addShard(s, "a", 2, "n0", "n2")
	addShard(s, "a", 3, "n0", "n3")
	addShard(s, "a", 4, "n0", "n1")
	addShard(s, "a", 5, "n2", "n3")

	// The mean is 2.5: a tolerance of 1 needs a gap of 3, and n0 has 4 to 2
	ops, _ := defaultPlanner().Plan(s, unlimited(10, 1), nil)
	assert.Empty(t, ops)

	ops, _ = defaultPlanner().Plan(s, unlimited(10, 0), nil)
	require.Len(t, ops, 1)
	assert.Equal(t, "n0", ops[0].From)
	assert.Equal(t, "namespace-replicas", ops[0].Reason)
}

func TestPlannerPlanRespectsTheLimits(t *testing.T) {
	newDrain := func() *model.Snapshot {
		s := cluster(5)
		removed(s, "r")
		for i := range 4 {
			addShard(s, "a", int64(i), "r", "n0")
			addShard(s, "b", int64(10+i), "r", "n0")
		}
		return s
	}

	t.Run("moves", func(t *testing.T) {
		ops, _ := defaultPlanner().Plan(newDrain(), unlimited(3, 0), nil)
		assert.Len(t, ops, 3)
		ops, _ = defaultPlanner().Plan(newDrain(), unlimited(3, 0), []model.Op{{Namespace: "c", Shard: 99, To: "x"}})
		assert.Len(t, ops, 2)
	})
	t.Run("per data server", func(t *testing.T) {
		p := unlimited(10, 0)
		p.MaxMovesPerDataServer = 1
		ops, _ := defaultPlanner().Plan(newDrain(), p, []model.Op{{Namespace: "c", Shard: 99, To: "n1"}})
		assert.ElementsMatch(t, []string{"n2", "n3", "n4"}, targets(ops))
	})
	t.Run("per namespace", func(t *testing.T) {
		p := unlimited(10, 0)
		p.MaxMovesPerNamespace = 1
		ops, _ := defaultPlanner().Plan(newDrain(), p, []model.Op{{Namespace: "b", Shard: 99, To: "x"}})
		require.Len(t, ops, 1)
		assert.Equal(t, "a", ops[0].Namespace)
	})
	t.Run("one op per shard", func(t *testing.T) {
		s := cluster(4)
		removed(s, "r0")
		removed(s, "r1")
		addShard(s, "a", 1, "r0", "r1", "n0")
		ops, _ := defaultPlanner().Plan(s, unlimited(10, 0), nil)
		assert.Len(t, ops, 1)
	})
}

// An op in flight counts in the load until the snapshot shows it: the next ops
// don't overshoot, and undo it.
func TestPlannerPlanCountsTheOpsInFlight(t *testing.T) {
	// n0 and n1 have 3 replicas of a each, n2 none; shard 1 moves from n0 to
	// n2, and is frozen meanwhile
	newCluster := func() *model.Snapshot {
		s := cluster(3)
		addShard(s, "a", 1, "n0", "n1").Frozen = true
		addShard(s, "a", 2, "n0", "n1")
		addShard(s, "a", 3, "n0", "n1")
		return s
	}
	move := model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n2"}
	want := []model.Op{{Namespace: "a", Shard: 2, From: "n1", To: "n2", Reason: "namespace-replicas"}}

	// Not shown yet: n0 counts 2 already, so only n1 gives a replica to n2
	ops, _ := defaultPlanner().Plan(newCluster(), unlimited(10, 0), []model.Op{move})
	assert.Equal(t, want, ops)

	// Shown: it isn't counted twice
	s := newCluster()
	apply(s, move)
	ops, _ = defaultPlanner().Plan(s, unlimited(10, 0), []model.Op{move})
	assert.Equal(t, want, ops)
}

func TestPlannerPendingShowsWhetherTheSnapshotHasTheOp(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"

	assert.True(t, pending(s, model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n2"}))
	assert.False(t, pending(s, model.Op{Namespace: "a", Shard: 1, From: "n2", To: "n1"}))
	assert.True(t, pending(s, model.Op{Kind: model.TransferLeader, Namespace: "a", Shard: 1, From: "n0", To: "n1"}))
	assert.False(t, pending(s, model.Op{Kind: model.TransferLeader, Namespace: "a", Shard: 1, From: "n1", To: "n0"}))
	assert.False(t, pending(s, model.Op{Namespace: "a", Shard: 9, From: "n0", To: "n2"}))
}

func TestPlannerPlanTransfersTheLeadersOnceTheReplicasSettle(t *testing.T) {
	s := cluster(3)
	addShard(s, "a", 1, "n0", "n1").Leader = "n0"
	addShard(s, "a", 2, "n0", "n2").Leader = "n0"
	addShard(s, "a", 3, "n1", "n2").Leader = "n1"

	ops, _ := defaultPlanner().Plan(s, unlimited(10, 0), nil)

	assert.Equal(t, []model.Op{{Kind: model.TransferLeader, Namespace: "a", Shard: 2, From: "n0", To: "n2",
		Reason: "namespace-leaders"}}, ops)
}

func TestPlannerPlanHoldsTheLeadersWhileTheReplicasMove(t *testing.T) {
	newCluster := func() *model.Snapshot {
		s := cluster(3)
		addShard(s, "a", 1, "n0", "n1").Leader = "n0"
		addShard(s, "a", 2, "n0", "n2").Leader = "n0"
		addShard(s, "a", 3, "n1", "n2").Leader = "n1"
		return s
	}
	transfers := func(ops []model.Op) int {
		count := 0
		for _, op := range ops {
			if op.Kind == model.TransferLeader {
				count++
			}
		}
		return count
	}

	t.Run("replace left", func(t *testing.T) {
		s := newCluster()
		removed(s, "r")
		addShard(s, "b", 4, "r", "n0")
		ops, _ := defaultPlanner().Plan(s, unlimited(10, 0), nil)
		assert.Len(t, ops, 1)
		assert.Zero(t, transfers(ops))
	})
	t.Run("replace in flight", func(t *testing.T) {
		ops, _ := defaultPlanner().Plan(newCluster(), unlimited(10, 0), []model.Op{{Namespace: "b", Shard: 9, To: "n2"}})
		assert.Empty(t, ops)
	})
	t.Run("change pending", func(t *testing.T) {
		s := newCluster()
		addShard(s, "b", 4, "n1", "n2").Changing = true
		ops, _ := defaultPlanner().Plan(s, unlimited(10, 0), nil)
		assert.Empty(t, ops)
	})
	t.Run("transfer in flight", func(t *testing.T) {
		p := unlimited(10, 0)
		p.MaxMovesPerDataServer = 1
		ops, _ := defaultPlanner().Plan(newCluster(), p, []model.Op{{Kind: model.TransferLeader, Namespace: "b",
			Shard: 9, To: "n2"}})
		assert.Equal(t, 1, transfers(ops), "a transfer copies no replica onto its target")
	})
}

func TestPlannerPlanPrefersMovingAFollower(t *testing.T) {
	s := cluster(3)
	removed(s, "r0")
	removed(s, "r1")
	addShard(s, "a", 1, "r0", "r1", "n0").Leader = "r0"

	ops, _ := defaultPlanner().Plan(s, unlimited(1, 0), nil)

	require.Len(t, ops, 1)
	assert.Equal(t, "r1", ops[0].From)
}

func TestPlannerPlanReportsStuckReplicas(t *testing.T) {
	s := cluster(2)
	removed(s, "r")
	addShard(s, "single", 1, "r")
	addShard(s, "zones", 2, "r", "n0")
	s.Namespaces["zones"].AntiAffinities = []*proto.AntiAffinity{{Labels: []string{"zone"}}}
	addShard(s, "frozen", 3, "r", "n0").Frozen = true

	_, stuck := defaultPlanner().Plan(s, unlimited(10, 0), nil)

	assert.Equal(t, []model.Stuck{
		{Namespace: "single", Shard: 1, DataServer: "r", Reason: "replicated: failed precondition: shard 1 has a single replica"},
		{Namespace: "zones", Shard: 2, DataServer: "r", Reason: `anti-affinity: failed precondition: "n1" breaks the anti-affinity of shard 2`},
	}, stuck)
}

func TestPlannerPlanReportsStuckReplicasWithoutTarget(t *testing.T) {
	s := cluster(1)
	removed(s, "r")
	addShard(s, "a", 1, "r", "n0")

	_, stuck := defaultPlanner().Plan(s, unlimited(10, 0), nil)

	assert.Equal(t, []model.Stuck{{Namespace: "a", Shard: 1, DataServer: "r", Reason: "no data server can take a replica"}}, stuck)
}

func TestPlannerValidateAcceptsAnImprovingMove(t *testing.T) {
	s := validateCluster()

	assert.NoError(t, defaultPlanner().Validate(s, model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n3"}))
}

func TestPlannerValidateRejectsUnsafeOps(t *testing.T) {
	for _, tc := range []struct {
		name   string
		op     model.Op
		modify func(*model.Snapshot)
		err    error
	}{
		{"unknown namespace", model.Op{Namespace: "x", Shard: 1, From: "n0", To: "n3"}, nil, errs.ErrNotFound},
		{"unknown shard", model.Op{Namespace: "a", Shard: 9, From: "n0", To: "n3"}, nil, errs.ErrNotFound},
		{"frozen shard", model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n3"},
			func(s *model.Snapshot) { s.Namespaces["a"].Shards[1].Frozen = true }, errs.ErrFailedPrecondition},
		{"single replica", model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n3"},
			func(s *model.Snapshot) { s.Namespaces["a"].Shards[1].Ensemble = []string{"n0"} }, errs.ErrFailedPrecondition},
		{"source not a member", model.Op{Namespace: "a", Shard: 1, From: "n3", To: "n2"}, nil, errs.ErrInvalidArgument},
		{"target already a member", model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n1"}, nil, errs.ErrInvalidArgument},
		{"unknown target", model.Op{Namespace: "a", Shard: 1, From: "n0", To: "x"}, nil, errs.ErrUnavailable},
		{"removed target", model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n3"},
			func(s *model.Snapshot) { s.DataServers["n3"].Removed = true }, errs.ErrUnavailable},
		{"unstable target", model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n3"},
			func(s *model.Snapshot) { s.DataServers["n3"].Stable = false }, errs.ErrUnavailable},
		{"source down", model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n3"},
			func(s *model.Snapshot) { s.DataServers["n0"].Running = false }, errs.ErrUnavailable},
		{"other member down", model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n3"},
			func(s *model.Snapshot) { s.DataServers["n1"].Running = false }, errs.ErrUnavailable},
		{"anti-affinity", model.Op{Namespace: "a", Shard: 1, From: "n0", To: "n3"},
			func(s *model.Snapshot) {
				s.Namespaces["a"].AntiAffinities = []*proto.AntiAffinity{{Labels: []string{"zone"}}}
				s.DataServers["n3"].Labels = map[string]string{"zone": "z1"}
			}, errs.ErrFailedPrecondition},
		{"no improvement", model.Op{Namespace: "a", Shard: 2, From: "n2", To: "n3"}, nil, errs.ErrFailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := validateCluster()
			if tc.modify != nil {
				tc.modify(s)
			}
			assert.ErrorIs(t, defaultPlanner().Validate(s, tc.op), tc.err)
		})
	}
}

func TestPlannerValidateDrainsADownRemovedDataServer(t *testing.T) {
	s := validateCluster()
	removed(s, "r")
	s.DataServers["r"].Running = false
	addShard(s, "a", 3, "r", "n1")

	assert.NoError(t, defaultPlanner().Validate(s, model.Op{Namespace: "a", Shard: 3, From: "r", To: "n3"}))
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
		s.DataServers[name].Labels = map[string]string{"zone": zone}
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

	ensemble, err := defaultPlanner().Place(s, "a", 2, nil, func(node *model.DataServer) bool { return node.Identity.GetInternal() != "n0" })

	require.NoError(t, err)
	assert.Equal(t, []string{"n1", "n2"}, ensemble)
}

// cluster returns a snapshot of count running, stable data servers, n0 to
// n<count-1>.
func cluster(count int) *model.Snapshot {
	s := &model.Snapshot{DataServers: make(map[string]*model.DataServer), Namespaces: make(map[string]*model.Namespace)}
	for i := range count {
		name := fmt.Sprintf("n%d", i)
		s.DataServers[name] = &model.DataServer{Identity: &proto.DataServerIdentity{Internal: name}, Running: true, Stable: true}
	}
	return s
}

func removed(s *model.Snapshot, name string) {
	s.DataServers[name] = &model.DataServer{Identity: &proto.DataServerIdentity{Internal: name}, Removed: true, Running: true}
}

func addShard(s *model.Snapshot, namespace string, id int64, ensemble ...string) *model.Shard {
	ns, ok := s.Namespaces[namespace]
	if !ok {
		ns = &model.Namespace{Shards: make(map[int64]*model.Shard)}
		s.Namespaces[namespace] = ns
	}
	shard := &model.Shard{Ensemble: ensemble}
	ns.Shards[id] = shard
	return shard
}

// apply makes op on s, as if the election of a replace kept the leader, or
// moved it to the new member if it replaced the leader.
func apply(s *model.Snapshot, op model.Op) {
	shard := s.Namespaces[op.Namespace].Shards[op.Shard]
	if op.Kind == model.Replace {
		shard.Ensemble[slices.Index(shard.Ensemble, op.From)] = op.To
	}
	if shard.Leader == op.From {
		shard.Leader = op.To
	}
}

// unlimited is a policy of moves in flight, without a limit per data server or
// namespace.
func unlimited(moves int, tolerance float64) policy.Policy {
	return policy.Policy{MaxMoves: moves, MaxMovesPerDataServer: math.MaxInt, MaxMovesPerNamespace: math.MaxInt,
		Tolerance: tolerance}
}

func targets(ops []model.Op) []string {
	var to []string
	for _, op := range ops {
		to = append(to, op.To)
	}
	return to
}

// spread is the gap between the most and the least loaded data servers that
// aren't removed.
func spread(s *model.Snapshot, counts map[string]int) int {
	lowest, highest := math.MaxInt, 0
	for name, node := range s.DataServers {
		if !node.Removed {
			lowest = min(lowest, counts[name])
			highest = max(highest, counts[name])
		}
	}
	return highest - lowest
}

// validateCluster has n0 with two replicas of namespace a and n3 with none,
// with every data server labelled with its zone.
func validateCluster() *model.Snapshot {
	s := cluster(4)
	for name, zone := range map[string]string{"n0": "z0", "n1": "z1", "n2": "z2"} {
		s.DataServers[name].Labels = map[string]string{"zone": zone}
	}
	addShard(s, "a", 1, "n0", "n1")
	addShard(s, "a", 2, "n0", "n2")
	return s
}

func defaultPlanner() *planner {
	return New(filter.Defaults(), tier.Defaults()).(*planner)
}
