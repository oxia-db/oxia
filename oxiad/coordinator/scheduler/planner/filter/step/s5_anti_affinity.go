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

package step

import (
	"fmt"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/internal/candidate"
)

// AntiAffinity rejects a replace whose target breaks an anti-affinity of the
// namespace: e.g. with an anti-affinity on the label zone, no two members of a
// shard may share a zone, so that losing a zone loses at most one replica. A
// transfer and a split keep the ensemble, so they can't break one. It runs
// late since it reads the labels of every member.
type AntiAffinity struct{}

func (AntiAffinity) GetName() string {
	return "anti-affinity"
}

func (AntiAffinity) Filter(s *stats.ClusterStats, op candidate.Candidate) error {
	if op.Kind != candidate.Replace {
		return nil
	}
	ns, shard := op.GetShard(s)
	if Conflicts(s, ns.AntiAffinities, shard.Ensemble, op.From, op.To) {
		return fmt.Errorf("%w: %q breaks the anti-affinity of shard %d", errs.ErrFailedPrecondition, op.To,
			op.Shard)
	}
	return nil
}

// Conflicts reports whether to lacks a label of the anti-affinities, or shares
// its value with a member other than from. Relaxed anti-affinities are kept as
// strictly as the strict ones until the planner ranks them as a tier.
func Conflicts(s *stats.ClusterStats, antiAffinities []*proto.AntiAffinity, members []string, from string,
	to string) bool {
	labels := s.DataServers[to].GetMetadata().GetLabels()
	for _, antiAffinity := range antiAffinities {
		for _, label := range antiAffinity.GetLabels() {
			value, ok := labels[label]
			if !ok {
				return true
			}
			for _, member := range members {
				if member == from {
					continue
				}
				if v, ok := s.DataServers[member].GetMetadata().GetLabels()[label]; ok && v == value {
					return true
				}
			}
		}
	}
	return false
}
