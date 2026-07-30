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
	"sync"
	"sync/atomic"
)

type SplitResult struct {
	LeftChild  int64
	RightChild int64
}

type SplitAction struct {
	Shard      int64
	SplitPoint *uint32

	finished atomic.Bool
	result   SplitResult
	err      error
	waiter   sync.WaitGroup
}

func NewSplitAction(shard int64, splitPoint *uint32) *SplitAction {
	a := &SplitAction{
		Shard:      shard,
		SplitPoint: splitPoint,
	}
	a.waiter.Add(1)
	return a
}

func (a *SplitAction) Done(result any) {
	if !a.finished.CompareAndSwap(false, true) {
		return
	}
	a.result = result.(SplitResult) //nolint:revive
	a.waiter.Done()
}

func (a *SplitAction) Error(err error) {
	if !a.finished.CompareAndSwap(false, true) {
		return
	}
	a.err = err
	a.waiter.Done()
}

func (a *SplitAction) Wait() (SplitResult, error) {
	a.waiter.Wait()
	return a.result, a.err
}

func (*SplitAction) Type() Type {
	return Split
}
