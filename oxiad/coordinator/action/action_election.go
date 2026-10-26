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

package action

import "sync"

type ElectionAction struct {
	Shard int64

	waiter    *sync.WaitGroup
	newLeader string
	err       error
}

func NewElectionAction(shard int64) *ElectionAction {
	action := &ElectionAction{Shard: shard, waiter: &sync.WaitGroup{}}
	action.waiter.Add(1)
	return action
}

func (e *ElectionAction) Done(leader any) {
	e.newLeader = leader.(string) //nolint:revive
	e.waiter.Done()
}

func (e *ElectionAction) Error(err error) {
	e.err = err
	e.waiter.Done()
}

// Wait waits for the election, and returns the name of the new leader, or the
// error that stopped it.
func (e *ElectionAction) Wait() (string, error) {
	e.waiter.Wait()
	return e.newLeader, e.err
}

func (e *ElectionAction) GetShard() int64 {
	return e.Shard
}
