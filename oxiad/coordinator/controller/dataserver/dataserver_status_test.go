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

func TestStateToProtoReportsUnspecifiedForUnknownState(t *testing.T) {
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_RUNNING, Running.ToProto())
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE, NotRunning.ToProto())
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_DRAINING, Draining.ToProto())
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_UNSPECIFIED, State(99).ToProto())
}

// The first transition, the data server joining, isn't timed: a data server
// that never failed has no transition time. The later ones are.
func TestStatusAdvanceRecordsTransitionsAfterTheFirst(t *testing.T) {
	status := NewStatus(NotRunning)

	status.Advance(Running)
	assert.Equal(t, Running, status.GetState())
	assert.True(t, status.GetLastTransitionTime().IsZero())

	beforeFailure := time.Now()
	status.Advance(NotRunning)
	assert.Equal(t, NotRunning, status.GetState())
	assert.False(t, status.GetLastTransitionTime().Before(beforeFailure))
}

// A transition to the same state still advances the epoch, so that the
// observations made before it are discarded.
func TestStatusAdvanceToSameStateIncrementsEpoch(t *testing.T) {
	status := NewStatus(Draining)
	epoch := status.epoch

	status.Advance(Draining)
	assert.Equal(t, Draining, status.GetState())
	assert.Equal(t, epoch+1, status.epoch)
}
