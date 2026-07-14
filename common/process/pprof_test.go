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

package process

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunProfilingEnablesContentionProfiles(t *testing.T) {
	PprofEnable = true
	PprofBindAddress = "127.0.0.1:0"
	t.Cleanup(func() {
		PprofEnable = false
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	})

	closer := RunProfiling()

	// SetMutexProfileFraction with a negative value only reads the current
	// setting. There is no equivalent getter for the block profile rate.
	assert.Equal(t, mutexProfileFraction, runtime.SetMutexProfileFraction(-1))

	// Closing the profiling server restores the runtime defaults
	assert.NoError(t, closer.Close())
	assert.Equal(t, 0, runtime.SetMutexProfileFraction(-1))
}
