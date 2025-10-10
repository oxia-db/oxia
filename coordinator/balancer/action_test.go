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
	"sync"
	"testing"

	"github.com/oxia-db/oxia/coordinator/actions"
	"github.com/oxia-db/oxia/coordinator/model"
)

func TestActionSwapDone(t *testing.T) {
	group := &sync.WaitGroup{}
	group.Add(1)
	swapAction := actions.SwapNodeAction{
		Shard: int64(1),
		From: model.Server{
			Internal: "sv-1",
		},
		To: model.Server{
			Internal: "sv-2",
		},
		Waiter: group,
	}
	swapAction.Done(nil)
	group.Wait()
}
