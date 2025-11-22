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

package action

import (
	"sync"
	"sync/atomic"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

var _ Action = &ChangeEnsembleAction{}

type ChangeEnsembleAction struct {
	sync.WaitGroup

	Shard int64
	From  model.Server
	To    model.Server

	finished     atomic.Bool
	executeError error
	callback     concurrent.Callback[any]
}

func (s *ChangeEnsembleAction) Done(_ any) {
	if !s.finished.CompareAndSwap(false, true) { // Ordering::SeqCst
		return
	}
	if s.callback != nil {
		s.callback.OnComplete(nil)
	}
	s.WaitGroup.Done()
}

func (s *ChangeEnsembleAction) Error(err error) {
	if !s.finished.CompareAndSwap(false, true) { // Ordering::SeqCst
		return
	}
	s.executeError = err
	if s.callback != nil {
		s.callback.OnCompleteError(err)
	}
	s.WaitGroup.Done()
}

func (s *ChangeEnsembleAction) Wait() (any, error) {
	s.WaitGroup.Wait()
	return nil, s.executeError
}

func (*ChangeEnsembleAction) Type() Type {
	return SwapNode
}

func NewChangeEnsembleAction(shard int64, from model.Server, to model.Server) *ChangeEnsembleAction {
	return NewChangeEnsembleActionWithCallback(shard, from, to, nil)
}

func NewChangeEnsembleActionWithCallback(shard int64, from model.Server, to model.Server, callback concurrent.Callback[any]) *ChangeEnsembleAction {
	action := ChangeEnsembleAction{
		WaitGroup:    sync.WaitGroup{},
		Shard:        shard,
		From:         from,
		To:           to,
		finished:     atomic.Bool{},
		executeError: nil,
		callback:     callback,
	}
	action.Add(1)
	return &action
}
