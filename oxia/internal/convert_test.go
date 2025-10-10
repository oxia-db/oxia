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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/proto"
)

func TestToShard(t *testing.T) {
	for _, item := range []struct {
		assignment *proto.ShardAssignment
		shard      Shard
		err        error
	}{
		{
			&proto.ShardAssignment{
				Shard:  1,
				Leader: "leader:1234",
				ShardBoundaries: &proto.ShardAssignment_Int32HashRange{
					Int32HashRange: &proto.Int32HashRange{
						MinHashInclusive: 1,
						MaxHashInclusive: 2,
					},
				},
			}, Shard{
				Id:        1,
				Leader:    "leader:1234",
				HashRange: hashRange(1, 2),
			}, nil},
	} {
		result := toShard(item.assignment)
		assert.Equal(t, item.shard, result)
	}
}
