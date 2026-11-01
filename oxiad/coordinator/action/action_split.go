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

var _ Action = &SplitAction{}

// SplitAction splits a shard of a namespace in two children at the middle of
// its hash range. The runtime runs it: the split is done once it starts, and
// the metadata shows it until the children take over.
type SplitAction struct {
	Namespace string
	Shard     int64

	promise concurrent.Future[any]
}

// NewSplitAction returns a split of shard, which completes with the IDs of
// its children.
func NewSplitAction(namespace string, shard int64) *SplitAction {
	return &SplitAction{Namespace: namespace, Shard: shard, promise: concurrent.NewFuture[any]()}
}

func (s *SplitAction) GetShard() int64 {
	return s.Shard
}

func (s *SplitAction) Done(children any) {
	s.promise.Complete(children)
}

func (s *SplitAction) Error(err error) {
	s.promise.Fail(err)
}

func (s *SplitAction) Wait(ctx context.Context) (any, error) {
	return s.promise.Wait(ctx)
}
