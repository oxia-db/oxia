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

package controllers

import (
	"sync"

	"github.com/oxia-db/oxia/coordinator/model"
)

type Metadata struct {
	sync.RWMutex
	shardMetadata model.ShardMetadata
}

func (s *Metadata) Load() model.ShardMetadata {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Clone()
}

func (s *Metadata) Compute(fn func(metadata *model.ShardMetadata)) model.ShardMetadata {
	s.Lock()
	defer s.Unlock()
	fn(&s.shardMetadata)
	return s.shardMetadata.Clone()
}

func (s *Metadata) Store(metadata model.ShardMetadata) {
	s.Lock()
	defer s.Unlock()
	s.shardMetadata = metadata
}

func (s *Metadata) Status() model.ShardStatus {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Status
}

func (s *Metadata) Leader() *model.Server {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Leader
}

func (s *Metadata) Term() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Term
}

func NewMetadata(metadata model.ShardMetadata) Metadata {
	return Metadata{
		RWMutex:       sync.RWMutex{},
		shardMetadata: metadata,
	}
}
