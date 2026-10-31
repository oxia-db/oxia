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
)

// Membership rejects an op that doesn't fit the ensemble: a replace must move a
// member to a data server that isn't one, and a transfer must move the
// leadership from the leader to another member.
type Membership struct{}

func (Membership) Name() string {
	return "membership"
}

func (Membership) Filter(s *model.Snapshot, op model.Op) error {
	_, shard := s.Shard(op)
	if op.Kind == model.TransferLeader {
		if op.From != shard.Leader {
			return fmt.Errorf("%w: %q is not the leader of shard %d", errs.ErrInvalidArgument, op.From, op.Shard)
		}
		if op.To == op.From || !slices.Contains(shard.Ensemble, op.To) {
			return fmt.Errorf("%w: %q is not another member of shard %d", errs.ErrInvalidArgument, op.To, op.Shard)
		}
		return nil
	}
	if !slices.Contains(shard.Ensemble, op.From) {
		return fmt.Errorf("%w: %q is not a member of shard %d", errs.ErrInvalidArgument, op.From, op.Shard)
	}
	if slices.Contains(shard.Ensemble, op.To) {
		return fmt.Errorf("%w: %q is already a member of shard %d", errs.ErrInvalidArgument, op.To, op.Shard)
	}
	return nil
}
