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

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/internal/candidate"
)

// Splittable rejects a split of a shard that can't be split now: one that
// isn't in steady state, has a hash range too narrow for two children, or
// replicas left to delete; one in a namespace at its maximum of shards, or
// whose leader still filters the keys of a previous split. The collector sets
// ShardStats.Splittable from the metadata and the stats of the leader.
type Splittable struct{}

func (Splittable) GetName() string {
	return "splittable"
}

func (Splittable) Filter(s *stats.ClusterStats, op candidate.Candidate) error {
	if op.Kind != candidate.Split {
		return nil
	}
	if _, shard := op.GetShard(s); !shard.Splittable {
		return fmt.Errorf("%w: shard %d can't be split now", errs.ErrFailedPrecondition, op.Shard)
	}
	return nil
}
