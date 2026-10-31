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

// Source rejects a source that is down. A removed data server is drained even
// when it is down, but the replicas of a data server that is down are left to
// the failure handling.
type Source struct{}

func (Source) Name() string {
	return "source"
}

func (Source) Filter(s *model.Snapshot, op model.Op) error {
	if from := s.DataServers[op.From]; !from.Removed && !from.Running {
		return fmt.Errorf("%w: data server %q is down", errs.ErrUnavailable, op.From)
	}
	return nil
}
