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

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// Frozen rejects every op on a shard that is part of a split, has a change of
// its ensemble pending, or backs off after a failed op. A second op would race
// the one in progress: the shard controller applies one change of the
// ensemble at a time, and a split reads the ensemble of its parent. A failed
// op waits the schedule interval before it is planned again, so that it
// doesn't retry in a busy loop, while the other shards move on. It runs first
// since it is the cheapest check, and it rejects every kind of op.
type Frozen struct{}

func (Frozen) GetName() string {
	return "frozen"
}

func (Frozen) Filter(s *stats.ClusterStats, op model.Candidate) error {
	_, shard := op.GetShard(s)
	if shard.Changing || shard.Splitting {
		return fmt.Errorf("%w: shard %d can't change now", errs.ErrFailedPrecondition, op.Shard)
	}
	if shard.BackingOff {
		return fmt.Errorf("%w: an op of shard %d failed recently", errs.ErrUnavailable, op.Shard)
	}
	return nil
}
