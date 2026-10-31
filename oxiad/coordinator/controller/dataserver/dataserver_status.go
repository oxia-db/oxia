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
	"time"

	"github.com/oxia-db/oxia/common/proto"
)

// Status is the state of a data server, and when it last changed. The state
// combines where the data server is in its lifecycle with whether it is
// available: a data server is UNAVAILABLE only while it is RUNNING, and a
// DRAINING one stays DRAINING, so that it is still drained.
type Status struct {
	// lifecycle is RUNNING, DRAINING or DRAINED.
	lifecycle proto.DataServerState
	// available is whether the data server answered its health checks since
	// its last handshake.
	available bool
	// lastTransitionTime is when the state last changed: zero until it changes
	// after the data server joined, as a data server that never failed doesn't
	// need to prove that it's stable.
	lastTransitionTime time.Time
	// epoch increments on every transition, so that a probe observation (a
	// SERVING answer or a failure) obtained before a concurrent transition can
	// be recognized as stale and discarded instead of flapping the state back.
	epoch int64
}

// NewStatus returns the status of a data server that GetState reports in
// state, before any transition: UNAVAILABLE is a RUNNING data server that
// isn't available, and any other state an available one.
func NewStatus(state proto.DataServerState) Status {
	if state == proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE {
		return Status{lifecycle: proto.DataServerState_DATA_SERVER_STATE_RUNNING}
	}
	return Status{lifecycle: state, available: true}
}

// GetState returns the state of the data server.
func (s Status) GetState() proto.DataServerState {
	if s.lifecycle == proto.DataServerState_DATA_SERVER_STATE_RUNNING && !s.available {
		return proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE
	}
	return s.lifecycle
}

// IsAvailable reports whether the data server answered its health checks
// since its last handshake.
func (s Status) IsAvailable() bool {
	return s.available
}

// GetLastTransitionTime returns when the state last changed, or zero if it
// didn't change since the data server joined.
func (s Status) GetLastTransitionTime() time.Time {
	return s.lastTransitionTime
}

// Advance moves the data server to the DRAINING or DRAINED stage of its
// lifecycle.
func (s *Status) Advance(lifecycle proto.DataServerState) {
	s.lifecycle = lifecycle
	s.transition()
}

// SetAvailable records whether the data server is available, even if it
// already was: the epoch still increments.
func (s *Status) SetAvailable(available bool) {
	s.available = available
	s.transition()
}

// transition increments the epoch, and records the transition time, except
// for the first transition, the data server joining.
func (s *Status) transition() {
	if s.epoch > 0 {
		s.lastTransitionTime = time.Now()
	}
	s.epoch++
}
