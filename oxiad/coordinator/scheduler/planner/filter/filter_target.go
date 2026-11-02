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

// Target rejects a target that is unknown or removed: a removed data server is
// being drained, so a replica moved onto it would have to move again. It
// doesn't check that the target is running: the dispatch does, right before it
// sends the action. It also keeps the leadership off a removed member until the
// drain moves it out; a split has no target.
type Target struct{}

func (Target) GetName() string {
	return "target"
}

func (Target) Filter(s *stats.ClusterStats, op model.Candidate) error {
	if op.Kind == model.Split {
		return nil
	}
	if to := s.DataServers[op.To]; to == nil || to.Removed {
		return fmt.Errorf("%w: data server %q can't take a replica", errs.ErrUnavailable, op.To)
	}
	return nil
}
