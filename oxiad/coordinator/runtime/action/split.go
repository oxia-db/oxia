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

	"github.com/oxia-db/oxia/common/concurrent"
	commonproto "github.com/oxia-db/oxia/common/proto"
)

var _ Action = &SplitAction{}

// SplitAction asks a shard controller to split its shard in two. It carries
// the parts of the split that only the coordinator can decide: the reserved
// child ids and the ensembles the children are placed on, which are picked
// from the cluster-wide view of the load. Everything else is derived from the
// parent shard when the split is recorded.
type SplitAction struct {
	sync.WaitGroup

	Shard      int64
	Left       int64
	Right      int64
	SplitPoint uint32

	LeftEnsemble  []*commonproto.DataServerIdentity
	RightEnsemble []*commonproto.DataServerIdentity

	finished     atomic.Bool
	executeError error
	callback     concurrent.Callback[any]
}

func (s *SplitAction) Done(_ any) {
	if !s.finished.CompareAndSwap(false, true) { // Ordering::SeqCst
		return
	}
	if s.callback != nil {
		s.callback.OnComplete(nil)
	}
	s.WaitGroup.Done()
}

func (s *SplitAction) Error(err error) {
	if !s.finished.CompareAndSwap(false, true) { // Ordering::SeqCst
		return
	}
	s.executeError = err
	if s.callback != nil {
		s.callback.OnCompleteError(err)
	}
	s.WaitGroup.Done()
}

func (s *SplitAction) Wait() (any, error) {
	s.WaitGroup.Wait()
	return nil, s.executeError
}

func (*SplitAction) Type() Type {
	return Split
}

func NewSplitAction(shard, left, right int64, splitPoint uint32,
	leftEnsemble, rightEnsemble []*commonproto.DataServerIdentity) *SplitAction {
	action := SplitAction{
		WaitGroup:     sync.WaitGroup{},
		Shard:         shard,
		Left:          left,
		Right:         right,
		SplitPoint:    splitPoint,
		LeftEnsemble:  leftEnsemble,
		RightEnsemble: rightEnsemble,
		finished:      atomic.Bool{},
		executeError:  nil,
		callback:      nil,
	}
	action.Add(1)
	return &action
}
