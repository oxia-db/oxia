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

package crc

import (
	"hash/crc32"
)

const MagicNumber uint32 = 0xa282ead8

var table = crc32.MakeTable(crc32.Castagnoli)

type Checksum uint32

func (c Checksum) Update(b []byte) Checksum {
	return Checksum(crc32.Update(uint32(c), table, b))
}

func (c Checksum) IsZero() bool {
	return c == 0
}

func (c Checksum) Value() uint32 {
	return uint32(c>>15|c<<17) + MagicNumber
}
