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

func TestShardStatus_String(t *testing.T) {
	assert.Equal(t, "Unknown", ShardStatusUnknown.String())
	assert.Equal(t, "SteadyState", ShardStatusSteadyState.String())
	assert.Equal(t, "Election", ShardStatusElection.String())
}

func TestShardStatus_JSON(t *testing.T) {
	j, err := ShardStatusSteadyState.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, []byte("\"SteadyState\""), j)

	var s ShardStatus
	err = s.UnmarshalJSON(j)
	assert.NoError(t, err)
	assert.Equal(t, ShardStatusSteadyState, s)

	err = s.UnmarshalJSON([]byte("xyz"))
	assert.Error(t, err)
}
