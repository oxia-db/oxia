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

// Replicated rejects a replace of a shard with a single replica: no member
// would be left to copy the data from, and the shard would lose its data. Such
// a replica on a removed data server is reported as blocked instead, for an
// operator to handle. A transfer and a split copy no replica.
type Replicated struct{}

func (Replicated) GetName() string {
	return "replicated"
}

func (Replicated) Filter(s *stats.ClusterStats, op model.Candidate) error {
	_, shard := op.GetShard(s)
	if op.Kind == model.Replace && len(shard.Ensemble) < 2 {
		return fmt.Errorf("%w: shard %d has a single replica", errs.ErrFailedPrecondition, op.Shard)
	}
	return nil
}
