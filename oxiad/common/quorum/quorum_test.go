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

package quorum

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMajorityOverlapsAnyOtherMajority(t *testing.T) {
	for n, expected := range map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3} {
		assert.Equal(t, expected, Majority(n), "n=%d", n)
		// Two majorities can't be disjoint
		assert.Greater(t, 2*Majority(n), n, "n=%d", n)
	}
	assert.Equal(t, uint32(2), Majority(uint32(3)))
}
