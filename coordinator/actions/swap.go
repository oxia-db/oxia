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

package actions

import (
	"sync"

	"github.com/oxia-db/oxia/coordinator/model"
)

var _ Action = &SwapNodeAction{}

type SwapNodeAction struct {
	Shard int64
	From  model.Server
	To    model.Server

	Waiter *sync.WaitGroup
}

func (s *SwapNodeAction) Done(_ any) {
	s.Waiter.Done()
}

func (*SwapNodeAction) Type() Type {
	return SwapNode
}
