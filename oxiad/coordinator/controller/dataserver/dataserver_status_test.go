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
	status := NewStatus(proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE)
	assert.False(t, status.IsAvailable())

	status.SetAvailable(true)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_RUNNING, status.GetState())
	assert.True(t, status.GetLastTransitionTime().IsZero())

	beforeFailure := time.Now()
	status.SetAvailable(false)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE, status.GetState())
	assert.False(t, status.GetLastTransitionTime().Before(beforeFailure))
}

// A DRAINING data server stays DRAINING whether it is available or not, and
// the availability still advances the epoch, so that the observations made
// before it are discarded.
func TestStatusSetAvailableKeepsLifecycle(t *testing.T) {
	status := NewStatus(proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE)
	status.Advance(proto.DataServerState_DATA_SERVER_STATE_DRAINING)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINING, status.GetState())
	assert.False(t, status.IsAvailable())

	epoch := status.epoch
	status.SetAvailable(true)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINING, status.GetState())
	assert.True(t, status.IsAvailable())
	assert.Equal(t, epoch+1, status.epoch)

	status.SetAvailable(true)
	assert.Equal(t, epoch+2, status.epoch)

	status.Advance(proto.DataServerState_DATA_SERVER_STATE_DRAINED)
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINED, status.GetState())
}

func TestNewStatus(t *testing.T) {
	for _, state := range []proto.DataServerState{
		proto.DataServerState_DATA_SERVER_STATE_RUNNING,
		proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE,
		proto.DataServerState_DATA_SERVER_STATE_DRAINING,
		proto.DataServerState_DATA_SERVER_STATE_DRAINED,
	} {
		assert.Equal(t, state, NewStatus(state).GetState())
	}
}
