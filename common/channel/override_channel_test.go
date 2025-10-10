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

package channel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOverrideChannel(t *testing.T) {
	och := NewOverrideChannel[int]()

	och.WriteLast(1)
	och.WriteLast(2)

	n, err := och.Receive(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, n)

	done := atomic.Bool{}
	lastWritten := atomic.Int32{}
	go func() {
		x, _ := och.Receive(context.Background())

		done.Store(true)
		lastWritten.Store(int32(x))
	}()

	time.Sleep(1 * time.Second)
	assert.False(t, done.Load())

	och.WriteLast(5)
	time.Sleep(1 * time.Second)

	assert.True(t, done.Load())
	assert.EqualValues(t, 5, lastWritten.Load())
}
