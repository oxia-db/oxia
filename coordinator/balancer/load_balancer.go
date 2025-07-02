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

package balancer

import (
	"io"

	"golang.org/x/net/context"

	"github.com/oxia-db/oxia/coordinator/actions"

	"github.com/oxia-db/oxia/coordinator/resources"

	"github.com/oxia-db/oxia/coordinator/selectors"
)

type Options struct {
	context.Context

	StatusResource        resources.StatusResource
	ClusterConfigResource resources.ClusterConfigResource

	NodeAvailableJudger func(nodeID string) bool
}

type LoadBalancer interface {
	io.Closer

	Trigger()

	Action() <-chan actions.Action

	IsBalanced() bool

	LoadRatioAlgorithm() selectors.LoadRatioAlgorithm
}
