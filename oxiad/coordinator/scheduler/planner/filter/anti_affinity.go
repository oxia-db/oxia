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

package filter

import (
	"fmt"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

// AntiAffinity rejects a replace whose target breaks an anti-affinity of the
// namespace. A transfer keeps the ensemble, so it can't break one.
type AntiAffinity struct{}

func (AntiAffinity) Name() string {
	return "anti-affinity"
}

func (AntiAffinity) Filter(s *model.Snapshot, op model.Op) error {
	if op.Kind == model.TransferLeader {
		return nil
	}
	ns, shard := s.Shard(op)
	if Conflicts(s, ns.AntiAffinities, shard.Ensemble, op.From, op.To) {
		return fmt.Errorf("%w: %q breaks the anti-affinity of shard %d", errs.ErrFailedPrecondition, op.To,
			op.Shard)
	}
	return nil
}

// Conflicts reports whether to lacks a label of the anti-affinities, or shares
// its value with a member other than from. Relaxed anti-affinities are kept as
// strictly as the strict ones until the planner ranks them as a tier.
func Conflicts(s *model.Snapshot, antiAffinities []*proto.AntiAffinity, members []string, from string,
	to string) bool {
	labels := s.DataServers[to].Labels
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
				if v, ok := s.DataServers[member].Labels[label]; ok && v == value {
					return true
				}
			}
		}
	}
	return false
}
