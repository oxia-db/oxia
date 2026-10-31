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
)

// Members rejects a shard with another member down: the new member copies from
// the others, and without them the new ensemble could be left with a single
// member that has the data.
type Members struct{}

func (Members) Name() string {
	return "members"
}

func (Members) Filter(s *model.Snapshot, op model.Op) error {
	_, shard := s.Shard(op)
	for _, member := range shard.Ensemble {
		if member != op.From && !s.DataServers[member].Running {
			return fmt.Errorf("%w: member %q of shard %d is down", errs.ErrUnavailable, member, op.Shard)
		}
	}
	return nil
}
