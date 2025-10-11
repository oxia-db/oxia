// Copyright 2023-2025 The Oxia Authors
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

package selectors

import (
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/coordinator/model"
)

var (
	ErrUnsatisfiedEnsembleReplicas = errors.New("selector: unsatisfied ensemble replicas")
	ErrUnsatisfiedAntiAffinity     = errors.New("selector: unsatisfied anti-affinity")
	ErrUnsupportedAntiAffinityMode = errors.New("selector: unsupported anti-affinity mode")
	ErrNoFunctioning               = errors.New("selector: no functioning selection")
	ErrMultipleResult              = errors.New("selector: multiple results")
)

type LoadRatioAlgorithm = func(params *model.RatioParams) *model.Ratio

type Selector[O any, R any] interface {
	Select(o O) (R, error)
}
