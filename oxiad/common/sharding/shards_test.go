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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateShards(t *testing.T) {
	type args struct {
		baseId    int64
		numShards uint32
	}
	tests := []struct {
		name string
		args args
		want []Shard
	}{
		{"1-shard",
			args{0, 1},
			[]Shard{
				{0, 0, math.MaxUint32},
			}},
		{"2-shards",
			args{0, 2},
			[]Shard{
				{0, 0, 2147483647},
				{1, 2147483648, math.MaxUint32},
			}},
		{"3-shards",
			args{0, 3},
			[]Shard{
				{0, 0, 1431655765},
				{1, 1431655766, 2863311531},
				{2, 2863311532, math.MaxUint32},
			}},
		{"4-shards",
			args{0, 4},
			[]Shard{
				{0, 0, 1073741823},
				{1, 1073741824, 2147483647},
				{2, 2147483648, 3221225471},
				{3, 3221225472, math.MaxUint32},
			}},
		{"2-shards-different-base-id",
			args{5, 2},
			[]Shard{
				{5, 0, 2147483647},
				{6, 2147483648, math.MaxUint32},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want,
				GenerateShards(tt.args.baseId, tt.args.numShards),
				"GenerateShards(%v)", tt.args.numShards)
		})
	}
}
