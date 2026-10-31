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

// Target rejects a target that is unknown, removed or not stable yet.
type Target struct{}

func (Target) Name() string {
	return "target"
}

func (Target) Filter(s *model.Snapshot, op model.Op) error {
	if to := s.DataServers[op.To]; to == nil || to.Removed || !to.Stable {
		return fmt.Errorf("%w: data server %q can't take a replica", errs.ErrUnavailable, op.To)
	}
	return nil
}
