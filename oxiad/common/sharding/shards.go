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

package sharding

import (
	"math"
)

type Shard struct {
	Id  int64
	Min uint32
	Max uint32
}

func GenerateShards(baseId int64, numShards uint32) []Shard {
	shards := make([]Shard, 0)
	bucketSize := (math.MaxUint32 / numShards) + 1
	for i := uint32(0); i < numShards; i++ {
		lowerBound := i * bucketSize
		upperBound := lowerBound + bucketSize - 1
		if i == numShards-1 {
			upperBound = math.MaxUint32
		}
		shards = append(shards, Shard{
			Id:  baseId + int64(i),
			Min: lowerBound,
			Max: upperBound,
		})
	}
	return shards
}
