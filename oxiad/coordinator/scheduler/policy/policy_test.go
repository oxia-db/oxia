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

package policy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestNewTakesTheDefaults(t *testing.T) {
	assert.Equal(t, Policy{
		Interval:              DefaultInterval,
		Tolerance:             DefaultTolerance,
		MaxMovesPerDataServer: DefaultMaxMovesPerDataServer,
		MaxMovesPerNamespace:  DefaultMaxMovesPerNamespace,
		MoveTimeout:           DefaultMoveTimeout,
		MoveBackoff:           DefaultMoveBackoff,
		StableTime:            DefaultStableTime,
	}, New(nil))
	assert.Equal(t, DefaultInterval, New(&proto.SchedulerPolicy{Interval: "x"}).Interval)
}

func TestNewTakesTheConfiguration(t *testing.T) {
	assert.Equal(t, Policy{
		Interval:              time.Second,
		Tolerance:             0.2,
		MaxMoves:              7,
		MaxMovesPerDataServer: 3,
		MaxMovesPerNamespace:  4,
		MoveTimeout:           time.Minute,
		MoveBackoff:           2 * time.Second,
		StableTime:            3 * time.Second,
	}, New(&proto.SchedulerPolicy{Interval: "1s", Tolerance: 0.2, MaxMoves: 7, MaxMovesPerDataServer: 3,
		MaxMovesPerNamespace: 4, MoveTimeout: "1m", MoveBackoff: "2s", StableTime: "3s"}))
}

func TestPolicyMaxMovesFor(t *testing.T) {
	assert.Equal(t, 6, New(nil).MaxMovesFor(3))
	assert.Equal(t, 7, New(&proto.SchedulerPolicy{MaxMoves: 7}).MaxMovesFor(3))
}
