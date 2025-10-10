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

package cache

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemoize(t *testing.T) {
	count := atomic.Int64{}

	f := func() int64 {
		return count.Add(1)
	}

	m := Memoize(f, 1*time.Second)

	for i := 0; i < 10; i++ {
		assert.EqualValues(t, 1, m())
	}

	// Let the cached value expire
	time.Sleep(1100 * time.Millisecond)

	for i := 0; i < 10; i++ {
		assert.EqualValues(t, 2, m())
	}
}
