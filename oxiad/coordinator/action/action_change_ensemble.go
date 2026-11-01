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
	commonproto "github.com/oxia-db/oxia/common/proto"
)

var _ Action = &ChangeEnsembleAction{}

type ChangeEnsembleAction struct {
	Shard int64
	From  *commonproto.DataServerIdentity
	To    *commonproto.DataServerIdentity

	promise concurrent.Future[any]
}

func (s *ChangeEnsembleAction) Done(_ any) {
	s.promise.Complete(nil)
}

func (s *ChangeEnsembleAction) Error(err error) {
	s.promise.Fail(err)
}

func (s *ChangeEnsembleAction) Wait(ctx context.Context) (any, error) {
	return s.promise.Wait(ctx)
}

func (s *ChangeEnsembleAction) GetShard() int64 {
	return s.Shard
}

func NewChangeEnsembleAction(shard int64, from *commonproto.DataServerIdentity, to *commonproto.DataServerIdentity) *ChangeEnsembleAction {
	return &ChangeEnsembleAction{Shard: shard, From: from, To: to, promise: concurrent.NewFuture[any]()}
}
