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

// Status is the state of a data server, whether it is available, and when
// either last changed.
//
// A Status isn't safe for concurrent use: one goroutine at a time changes it.
// The controller changes a copy under its mu, then publishes it, and
// GetStatus returns a copy to each reader.
type Status struct {
	// state is RUNNING, DRAINING or DRAINED: the stage of the lifecycle that
	// the metadata stores.
	state proto.DataServerState
	// available is whether the data server answered its health checks since
	// its last handshake. It is never stored.
	available bool
	// lastTransitionTime is when the status last changed: zero until it
	// changes after the data server joined, as a data server that never
	// failed doesn't need to prove that it's stable.
	lastTransitionTime time.Time
	// epoch increments on every transition, so that a probe observation (a
	// SERVING answer or a failure) obtained before a concurrent transition can
	// be recognized as stale and discarded instead of flapping the state back.
	epoch int64
}

// NewStatus returns the status of a data server in state, before any
// transition: it isn't available until its first handshake.
func NewStatus(state proto.DataServerState) Status {
	return Status{state: state}
}

// GetState returns the stage of the lifecycle of the data server.
func (s Status) GetState() proto.DataServerState {
	return s.state
}

// IsAvailable reports whether the data server answered its health checks
// since its last handshake.
func (s Status) IsAvailable() bool {
	return s.available
}

// GetLastTransitionTime returns when the status last changed, or zero if it
// didn't change since the data server joined.
func (s Status) GetLastTransitionTime() time.Time {
	return s.lastTransitionTime
}

// Advance moves the data server to the DRAINING or DRAINED stage of its
// lifecycle.
func (s *Status) Advance(state proto.DataServerState) {
	if s.epoch > 0 {
		s.lastTransitionTime = time.Now()
	}
	s.state = state
	s.epoch++
}

// SetAvailable records whether the data server is available, even if it
// already was: the epoch still increments. The first transition, the data
// server joining, isn't timed.
func (s *Status) SetAvailable(available bool) {
	if s.epoch > 0 {
		s.lastTransitionTime = time.Now()
	}
	s.available = available
	s.epoch++
}
