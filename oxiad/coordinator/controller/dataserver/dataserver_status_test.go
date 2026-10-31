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

package dataserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

// The first transition, the data server joining, isn't timed: a data server
// that never failed has no transition time. The later ones are.
func TestStatusSetAvailableRecordsTransitionsAfterTheFirst(t *testing.T) {
	status := NewStatus(proto.DataServerState_DATA_SERVER_STATE_RUNNING)
	assert.False(t, status.IsAvailable())

	status.SetAvailable(true)
	assert.True(t, status.IsAvailable())
	assert.True(t, status.GetLastTransitionTime().IsZero())

	beforeFailure := time.Now()
	status.SetAvailable(false)
	assert.False(t, status.IsAvailable())
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_RUNNING, status.GetState())
	assert.False(t, status.GetLastTransitionTime().Before(beforeFailure))
}

// The availability doesn't change the stage of the lifecycle, and still
// advances the epoch, so that the observations made before it are discarded.
func TestStatusSetAvailableKeepsState(t *testing.T) {
	status := NewStatus(proto.DataServerState_DATA_SERVER_STATE_RUNNING)
	status.Advance(proto.DataServerState_DATA_SERVER_STATE_DRAINING)

	epoch := status.epoch
	status.SetAvailable(true)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINING, status.GetState())
	assert.True(t, status.IsAvailable())
	assert.Equal(t, epoch+1, status.epoch)

	status.SetAvailable(true)
	assert.Equal(t, epoch+2, status.epoch)

	status.Advance(proto.DataServerState_DATA_SERVER_STATE_DRAINED)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINED, status.GetState())
	assert.True(t, status.IsAvailable())
}
