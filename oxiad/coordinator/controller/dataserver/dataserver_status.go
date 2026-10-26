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

// State is the state of a data server, as its controller sees it:
//
//	           handshake succeeded
//	NotRunning -------------------> Running
//	    ^                              |
//	    +------------------------------+
//	           health check failed
//
//	removed from the configuration (Drain): Running or NotRunning --> Draining
type State uint32

const (
	// Running is a data server that passes its health checks, and that the
	// handshake bound to this coordinator: it receives the shard assignments.
	Running State = iota
	// NotRunning is a data server whose health checks fail, or that isn't
	// bound to this coordinator yet: the state of a new controller.
	NotRunning
	// Draining is a data server removed from the configuration. It keeps
	// receiving the shard assignments until it becomes unavailable, and the
	// runtime closes its controller then.
	Draining
)

// ToProto returns the state of the data server reported by the management API.
func (s State) ToProto() proto.DataServerState {
	switch s {
	case Running:
		return proto.DataServerState_DATA_SERVER_STATE_RUNNING
	case NotRunning:
		return proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE
	case Draining:
		return proto.DataServerState_DATA_SERVER_STATE_DRAINING
	default:
		return proto.DataServerState_DATA_SERVER_STATE_UNSPECIFIED
	}
}

// Status is the state of a data server, and when it last changed.
type Status struct {
	state State
	// lastTransitionTime is when the state last changed: zero until it changes
	// after the data server joined, as a data server that never failed doesn't
	// need to prove that it's stable.
	lastTransitionTime time.Time
	// epoch increments on every transition, so that a probe observation (a
	// SERVING answer or a failure) obtained before a concurrent transition can
	// be recognized as stale and discarded instead of flapping the state back.
	epoch int64
}

// NewStatus returns the status of a data server in state, before any
// transition.
func NewStatus(state State) Status {
	return Status{state: state}
}

// GetState returns the state of the data server.
func (s Status) GetState() State {
	return s.state
}

// GetLastTransitionTime returns when the state last changed, or zero if it
// didn't change since the data server joined.
func (s Status) GetLastTransitionTime() time.Time {
	return s.lastTransitionTime
}

// Advance moves the status to state, even if it already is in it: the epoch
// increments, and the transition time is recorded, except for the first
// transition, the data server joining.
func (s *Status) Advance(state State) {
	if s.epoch > 0 {
		s.lastTransitionTime = time.Now()
	}
	s.state = state
	s.epoch++
}
