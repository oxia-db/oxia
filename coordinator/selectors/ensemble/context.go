// Copyright 2025 StreamNative, Inc.
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

package ensemble

import (
	"github.com/emirpasic/gods/v2/sets/linkedhashset"

	"github.com/oxia-db/oxia/coordinator/model"
	p "github.com/oxia-db/oxia/coordinator/policies"
)

type Context struct {
	Candidates         *linkedhashset.Set[string]
	CandidatesMetadata map[string]model.ServerMetadata
	Policies           *p.Policies
	Status             *model.ClusterStatus
	Replicas           int

	LoadRatioSupplier func() *model.Ratio
}
