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

package internal

import (
	"github.com/oxia-db/oxia/common/hash"
)

type shardStrategyImpl struct {
	hashFunc func(string) uint32
}

func NewShardStrategy() ShardStrategy {
	return &shardStrategyImpl{
		hashFunc: hash.Xxh332,
	}
}

func (s *shardStrategyImpl) Get(key string) func(Shard) bool {
	code := s.hashFunc(key)
	return func(shard Shard) bool {
		hashRange := shard.HashRange
		return hashRange.MinInclusive <= code && code <= hashRange.MaxInclusive
	}
}
