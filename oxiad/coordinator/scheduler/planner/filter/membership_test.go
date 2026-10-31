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

package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

func TestMembershipNeedsAMemberSourceAndANewTarget(t *testing.T) {
	s := snapshot()

	assert.NoError(t, Membership{}.Filter(s, move("n0", "n2")))
	assert.ErrorIs(t, Membership{}.Filter(s, move("n2", "n3")), errs.ErrInvalidArgument)
	assert.ErrorIs(t, Membership{}.Filter(s, move("n0", "n1")), errs.ErrInvalidArgument)
}

func TestMembershipTransfersFromTheLeaderToAnotherMember(t *testing.T) {
	s := snapshot()
	s.Namespaces["a"].Shards[1].Leader = "n0"
	transfer := func(from string, to string) model.Op {
		return model.Op{Kind: model.TransferLeader, Namespace: "a", Shard: 1, From: from, To: to}
	}

	assert.NoError(t, Membership{}.Filter(s, transfer("n0", "n1")))
	assert.ErrorIs(t, Membership{}.Filter(s, transfer("n1", "n0")), errs.ErrInvalidArgument)
	assert.ErrorIs(t, Membership{}.Filter(s, transfer("n0", "n0")), errs.ErrInvalidArgument)
	assert.ErrorIs(t, Membership{}.Filter(s, transfer("n0", "n2")), errs.ErrInvalidArgument)
}
