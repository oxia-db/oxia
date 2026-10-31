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

// Package policy resolves the scheduler policy of the cluster configuration,
// with its defaults.
package policy

import (
	"time"

	"github.com/oxia-db/oxia/common/proto"
)

const (
	DefaultInterval              = 30 * time.Second
	DefaultTolerance             = 0.05
	DefaultMovesPerDataServer    = 2
	DefaultMaxMovesPerDataServer = 1
	DefaultMaxMovesPerNamespace  = 2
	DefaultMoveTimeout           = 5 * time.Minute
	DefaultMoveBackoff           = 10 * time.Second
	DefaultStableTime            = 5 * time.Minute
)

// Policy is the scheduler policy that a round applies. See
// proto.SchedulerPolicy for each field.
type Policy struct {
	Interval  time.Duration
	Tolerance float64
	// MaxMoves is 0 by default, for DefaultMovesPerDataServer: see
	// MaxMovesFor.
	MaxMoves              int
	MaxMovesPerDataServer int
	MaxMovesPerNamespace  int
	MoveTimeout           time.Duration
	MoveBackoff           time.Duration
	StableTime            time.Duration
}

// New resolves the policy of the configuration: a field left unset takes its
// default.
func New(config *proto.SchedulerPolicy) Policy {
	return Policy{
		Interval:              durationOrDefault(config.GetInterval(), DefaultInterval),
		Tolerance:             orDefault(config.GetTolerance(), DefaultTolerance),
		MaxMoves:              int(config.GetMaxMoves()),
		MaxMovesPerDataServer: int(orDefault(config.GetMaxMovesPerDataServer(), DefaultMaxMovesPerDataServer)),
		MaxMovesPerNamespace:  int(orDefault(config.GetMaxMovesPerNamespace(), DefaultMaxMovesPerNamespace)),
		MoveTimeout:           durationOrDefault(config.GetMoveTimeout(), DefaultMoveTimeout),
		MoveBackoff:           durationOrDefault(config.GetMoveBackoff(), DefaultMoveBackoff),
		StableTime:            durationOrDefault(config.GetStableTime(), DefaultStableTime),
	}
}

// MaxMovesFor returns the moves in flight for a cluster of dataServers data
// servers.
func (p Policy) MaxMovesFor(dataServers int) int {
	if p.MaxMoves > 0 {
		return p.MaxMoves
	}
	return DefaultMovesPerDataServer * dataServers
}

func orDefault[T uint32 | float64](value T, defaultValue T) T {
	if value > 0 {
		return value
	}
	return defaultValue
}

// The configuration validates the durations; an invalid one takes the
// default, as an unset one does.
func durationOrDefault(value string, defaultValue time.Duration) time.Duration {
	if duration, err := time.ParseDuration(value); err == nil && duration > 0 {
		return duration
	}
	return defaultValue
}
