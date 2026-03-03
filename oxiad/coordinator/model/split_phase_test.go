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

package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitPhase_String(t *testing.T) {
	assert.Equal(t, "Init", SplitPhaseInit.String())
	assert.Equal(t, "Bootstrap", SplitPhaseBootstrap.String())
	assert.Equal(t, "CatchUp", SplitPhaseCatchUp.String())
	assert.Equal(t, "Cutover", SplitPhaseCutover.String())
	assert.Equal(t, "Cleanup", SplitPhaseCleanup.String())
}

func TestSplitPhase_JSON(t *testing.T) {
	for _, phase := range []SplitPhase{
		SplitPhaseInit,
		SplitPhaseBootstrap,
		SplitPhaseCatchUp,
		SplitPhaseCutover,
		SplitPhaseCleanup,
	} {
		j, err := phase.MarshalJSON()
		assert.NoError(t, err)

		var decoded SplitPhase
		err = decoded.UnmarshalJSON(j)
		assert.NoError(t, err)
		assert.Equal(t, phase, decoded)
	}

	// Specific value check
	j, err := SplitPhaseCatchUp.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, []byte(`"CatchUp"`), j)

	var s SplitPhase
	err = s.UnmarshalJSON(j)
	assert.NoError(t, err)
	assert.Equal(t, SplitPhaseCatchUp, s)

	// Invalid JSON
	err = s.UnmarshalJSON([]byte("xyz"))
	assert.Error(t, err)
}
