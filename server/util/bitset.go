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

package util

import (
	"fmt"
	"math/bits"
)

const MaxBitSetSize = 16

// BitSet
// Simplified and compact bitset.
type BitSet struct {
	bits uint16
}

func (bs *BitSet) Count() int {
	return bits.OnesCount16(bs.bits)
}

func (bs *BitSet) Set(idx int) {
	if idx < 0 || idx >= MaxBitSetSize {
		panic(fmt.Sprintf("invalid index: %d", idx))
	}
	bs.bits |= 1 << idx
}
