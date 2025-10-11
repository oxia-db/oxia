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

package hash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXxh332(t *testing.T) {
	for _, test := range []struct {
		key      string
		expected uint32
	}{
		{"foo", 125730186},
		{"bar", 2687685474},
		{"baz", 862947621},
	} {
		t.Run(test.key, func(t *testing.T) {
			hash := Xxh332(test.key)
			assert.Equal(t, test.expected, hash)
		})
	}
}
