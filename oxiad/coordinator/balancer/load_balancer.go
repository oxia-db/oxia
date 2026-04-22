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

package balancer

import (
	"context"
	"io"

	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/selector"
)

type Options struct {
	context.Context

	Metadata coordmetadata.Metadata

	NodeAvailableJudger func(nodeID string) bool
}

type LoadBalancer interface {
	io.Closer

	Start()

	Trigger()

	Action() <-chan action.Action

	IsBalanced() bool

	LoadRatioAlgorithm() selector.LoadRatioAlgorithm
}
