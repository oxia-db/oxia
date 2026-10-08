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

package metric

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdder_Uncontended(t *testing.T) {
	var a adder
	a.Add(5)
	a.Add(-2)
	assert.EqualValues(t, 3, a.Sum())
	assert.Nil(t, a.stripes.Load())
}

func TestAdder_Contended(t *testing.T) {
	var a adder
	const goroutines, adds = 16, 100_000
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			<-start
			for range adds {
				a.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	assert.EqualValues(t, goroutines*adds, a.Sum())
	// Adds only collide when they run in parallel: with a single P, they can
	// all go through the base atomic.
	if runtime.GOMAXPROCS(0) > 1 {
		assert.NotNil(t, a.stripes.Load())
	}
}
