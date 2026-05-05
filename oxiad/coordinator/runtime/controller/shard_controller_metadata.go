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

package controller

import (
	"sync"

	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
)

type Metadata struct {
	sync.RWMutex
	shardMetadata *commonproto.ShardMetadata
}

func (s *Metadata) Load() *commonproto.ShardMetadata {
	s.RLock()
	defer s.RUnlock()
	return gproto.Clone(s.shardMetadata).(*commonproto.ShardMetadata) //nolint:revive
}

func (s *Metadata) Compute(fn func(metadata *commonproto.ShardMetadata)) *commonproto.ShardMetadata {
	s.Lock()
	defer s.Unlock()
	fn(s.shardMetadata)
	return gproto.Clone(s.shardMetadata).(*commonproto.ShardMetadata) //nolint:revive
}

func (s *Metadata) Store(metadata *commonproto.ShardMetadata) {
	s.Lock()
	defer s.Unlock()
	s.shardMetadata = gproto.Clone(metadata).(*commonproto.ShardMetadata) //nolint:revive
}

func (s *Metadata) Status() commonproto.ShardStatus {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.GetStatusOrDefault()
}

func (s *Metadata) Leader() *commonproto.DataServerIdentity {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.GetLeader()
}

func (s *Metadata) Term() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Term
}

func NewMetadata(metadata *commonproto.ShardMetadata) Metadata {
	return Metadata{
		RWMutex:       sync.RWMutex{},
		shardMetadata: gproto.Clone(metadata).(*commonproto.ShardMetadata),
	}
}
