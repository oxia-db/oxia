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
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/algorithm"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
)

var _ Planner = &planner{}

type planner struct {
	filters []filter.Filter
	// phases plan the ops of each kind, in the order of their first tier.
	phases []phase
}

// phase plans the ops of one kind, scored by the tiers of that kind in
// priority order.
type phase struct {
	kind  model.Kind
	tiers []tier.Tier
}

func New(filters []filter.Filter, tiers []tier.Tier) Planner {
	p := &planner{filters: filters}
	for _, t := range tiers {
		i := slices.IndexFunc(p.phases, func(ph phase) bool { return ph.kind == t.GetKind() })
		if i < 0 {
			p.phases = append(p.phases, phase{kind: t.GetKind()})
			i = len(p.phases) - 1
		}
		p.phases[i].tiers = append(p.phases[i].tiers, t)
	}
	return p
}

func (p *planner) Plan(s *stats.ClusterStats) ([]action.Action, []model.Blocked) {
	pl := newPlan(s)
	for _, ph := range p.phases {
		if pl.allWaiting() {
			break
		}
		p.planPhase(s, pl, ph.kind, ph.tiers)
		pl.endPhase(ph.kind)
	}
	return pl.actions, filter.Blocked(p.filters, s)
}

// planPhase adds the ops of kind to pl, scored by tiers, then for the
// transfers the paths of the leader graph, until none is left.
func (p *planner) planPhase(s *stats.ClusterStats, pl *plan, kind model.Kind, tiers []tier.Tier) {
	greedy := algorithm.NewShardGreedy(s, kind, tiers, p.filters)
	var leaders *algorithm.LeaderGraph
	for {
		if c, found := greedy.FindBest(pl.dataServerLoad, pl.allows); found {
			pl.add(c)
			continue
		}
		if kind != model.TransferLeader {
			return
		}
		if leaders == nil {
			leaders = algorithm.NewLeaderGraph(s, pl.dataServerNames, p.filters)
		}
		path := leaders.FindPath(pl.dataServerLoad, pl.allows)
		if path == nil {
			return
		}
		for _, c := range path {
			pl.add(c)
		}
	}
}

func (*planner) Place(s *stats.ClusterStats, namespace string, replicas int, antiAffinities []*proto.AntiAffinity,
	eligible func(*stats.DataServerStats) bool) ([]string, error) {
	l := tier.NewLoad(s)
	counts := l.GetReplicas(namespace)
	candidates := slices.DeleteFunc(slices.Collect(maps.Keys(s.DataServers)), func(name string) bool {
		dataServer := s.DataServers[name]
		return dataServer.Removed || (eligible != nil && !eligible(dataServer))
	})
	slices.SortFunc(candidates, func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[a], counts[b]), cmp.Compare(l.GetTotalReplicas()[a], l.GetTotalReplicas()[b]), strings.Compare(a, b))
	})
	ensemble := make([]string, 0, replicas)
	for _, name := range candidates {
		if len(ensemble) == replicas {
			break
		}
		if !filter.Conflicts(s, antiAffinities, ensemble, "", name) {
			ensemble = append(ensemble, name)
		}
	}
	if len(ensemble) < replicas {
		return nil, fmt.Errorf("%w: only %d of %d data servers keep the anti-affinities",
			errs.ErrNotFound, len(ensemble), replicas)
	}
	return ensemble, nil
}
