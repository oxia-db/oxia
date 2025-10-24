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

package actions

import "sync"

type ElectionAction struct {
	Shard int64

	Waiter *sync.WaitGroup

	newLeader string
}

func (e *ElectionAction) Done(leader any) {
	e.newLeader = leader.(string) //nolint:revive
	e.Waiter.Done()
}

func (e *ElectionAction) NewLeader() string {
	return e.newLeader
}

func (*ElectionAction) Type() Type {
	return Election
}

func (e *ElectionAction) Clone() *ElectionAction {
	return &ElectionAction{
		Shard:  e.Shard,
		Waiter: &sync.WaitGroup{},
	}
}
