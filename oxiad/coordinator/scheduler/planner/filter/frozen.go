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

// Frozen rejects the shards that no op may change now.
type Frozen struct{}

func (Frozen) Name() string {
	return "frozen"
}

func (Frozen) Filter(s *model.Snapshot, op model.Op) error {
	_, shard := s.Shard(op)
	if shard.Frozen {
		return fmt.Errorf("%w: shard %d can't change now", errs.ErrFailedPrecondition, op.Shard)
	}
	return nil
}
