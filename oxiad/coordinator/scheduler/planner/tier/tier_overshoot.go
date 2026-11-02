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

package tier

import (
	"math"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// Overshoot counts how far the shards are over their split thresholds: a
// split of a shard over its threshold lowers it, the most for the shard the
// most over it. The potential is in hundredths of the thresholds.
type Overshoot struct{}

func (Overshoot) GetName() string {
	return "split"
}

func (Overshoot) GetKind() model.Kind {
	return model.Split
}

func (Overshoot) Delta(s *stats.ClusterStats, _ *Load, op model.Candidate) int {
	_, shard := op.GetShard(s)
	if shard.Overshoot <= 1 {
		return 0
	}
	return -int(math.Ceil(shard.Overshoot * 100))
}
