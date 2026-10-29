// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package entry

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestCompareOrdersByTermThenOffset(t *testing.T) {
	for _, test := range []struct {
		name     string
		a        *proto.EntryId
		b        *proto.EntryId
		expected int
	}{
		{"equal", &proto.EntryId{Term: 2, Offset: 5}, &proto.EntryId{Term: 2, Offset: 5}, 0},
		{"lower offset", &proto.EntryId{Term: 2, Offset: 4}, &proto.EntryId{Term: 2, Offset: 5}, -1},
		{"higher offset", &proto.EntryId{Term: 2, Offset: 6}, &proto.EntryId{Term: 2, Offset: 5}, 1},
		{"higher term wins over offset", &proto.EntryId{Term: 3, Offset: 0}, &proto.EntryId{Term: 2, Offset: 5}, 1},
		{"lower term loses over offset", &proto.EntryId{Term: 1, Offset: 9}, &proto.EntryId{Term: 2, Offset: 5}, -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, Compare(test.a, test.b))
		})
	}
}
