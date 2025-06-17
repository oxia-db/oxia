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
	"time"

	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"golang.org/x/net/context"

	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/coordinator/selectors"
)

type Options struct {
	context.Context

	ScheduleInterval time.Duration
	QuarantineTime   time.Duration

	CandidatesSupplier        func() *linkedhashset.Set[string]
	CandidateMetadataSupplier func() map[string]model.ServerMetadata
	NamespaceConfigSupplier   func(namespace string) *model.NamespaceConfig
	StatusSupplier            func() *model.ClusterStatus
}

type LoadBalancer interface {
	io.Closer

	Trigger()

	Action() <-chan Action

	IsBalanced() bool

	LoadRatioAlgorithm() selectors.LoadRatioAlgorithm
}
