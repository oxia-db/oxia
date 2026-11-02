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
	"slices"

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// Running rejects an op that needs a data server that is down: every member of
// the shard and the target, except the source of a replace off a removed data
// server, which is drained even when it is down. The dispatch would fail it:
// the copy needs the members, and the election a quorum and its target. The
// plan skips it instead, so that the other shards still move, and the
// dispatch checks again, for a data server that went down since the snapshot.
type Running struct{}

func (Running) GetName() string {
	return "running"
}

func (Running) Filter(s *stats.ClusterStats, op model.Candidate) error {
	_, shard := op.GetShard(s)
	needed := shard.Ensemble
	if op.Kind != model.Split {
		needed = append(slices.Clone(needed), op.To)
	}
	for _, name := range needed {
		if op.Kind == model.Replace && name == op.From && s.DataServers[name].Removed {
			continue
		}
		if s.DataServers[name].Down {
			return fmt.Errorf("%w: data server %q is down", errs.ErrUnavailable, name)
		}
	}
	return nil
}
