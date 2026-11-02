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

package step

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/internal/candidate"
)

func TestRunningName(t *testing.T) {
	assert.Equal(t, "running", Running{}.GetName())
}

func TestRunningRejectsAnOpThatNeedsADataServerDown(t *testing.T) {
	s := newStats()
	assert.NoError(t, Running{}.Filter(s, move("n0", "n2")))

	s.DataServers["n2"].Down = true
	assert.ErrorIs(t, Running{}.Filter(s, move("n0", "n2")), errs.ErrUnavailable, "the target is down")
	assert.NoError(t, Running{}.Filter(s, move("n0", "n3")))

	s.DataServers["n1"].Down = true
	assert.ErrorIs(t, Running{}.Filter(s, move("n0", "n3")), errs.ErrUnavailable, "another member is down")
	assert.ErrorIs(t, Running{}.Filter(s, split), errs.ErrUnavailable)
	assert.ErrorIs(t, Running{}.Filter(s, candidate.Candidate{Kind: candidate.TransferLeader, Namespace: "a", Shard: 1,
		From: "n0", To: "n1"}), errs.ErrUnavailable)
}

func TestRunningDrainsARemovedDataServerDown(t *testing.T) {
	s := newStats()
	s.DataServers["n0"].Down = true
	assert.ErrorIs(t, Running{}.Filter(s, move("n0", "n2")), errs.ErrUnavailable, "the source must copy")

	s.DataServers["n0"].Removed = true
	assert.NoError(t, Running{}.Filter(s, move("n0", "n2")))
}
