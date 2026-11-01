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

import (
	"context"

	"github.com/oxia-db/oxia/common/concurrent"
)

type ElectionAction struct {
	Shard int64
	// PreferredLeader is the data server that the election elects if it is
	// among the most up-to-date members, or empty to let the election choose.
	PreferredLeader string

	promise concurrent.Future[any]
}

func NewElectionAction(shard int64) *ElectionAction {
	return NewLeaderTransferAction(shard, "")
}

// NewLeaderTransferAction returns an election of shard that prefers leader.
func NewLeaderTransferAction(shard int64, leader string) *ElectionAction {
	return &ElectionAction{Shard: shard, PreferredLeader: leader, promise: concurrent.NewFuture[any]()}
}

func (e *ElectionAction) Done(leader any) {
	e.promise.Complete(leader)
}

func (e *ElectionAction) Error(err error) {
	e.promise.Fail(err)
}

// Wait waits for the election, and returns the name of the new leader, or the
// error that stopped it.
func (e *ElectionAction) Wait(ctx context.Context) (any, error) {
	return e.promise.Wait(ctx)
}

func (e *ElectionAction) GetShard() int64 {
	return e.Shard
}
