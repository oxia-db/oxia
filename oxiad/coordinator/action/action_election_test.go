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

package action

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/concurrent"
)

func TestElectionActionWait(t *testing.T) {
	done := NewElectionAction(1)
	assert.Equal(t, int64(1), done.GetShard())
	go done.Done("n0")
	leader, err := done.Wait()
	assert.Equal(t, "n0", leader)
	assert.NoError(t, err)

	failed := NewElectionAction(1)
	go failed.Error(errors.New("unavailable"))
	_, err = failed.Wait()
	assert.Error(t, err)
}

func TestLeaderTransferActionReportsToTheCallback(t *testing.T) {
	var leader any
	var failure error
	callback := func() concurrent.Callback[any] {
		return concurrent.NewOnce(func(l any) { leader = l }, func(err error) { failure = err })
	}

	done := NewLeaderTransferAction(1, "n1", callback())
	assert.Equal(t, "n1", done.PreferredLeader)
	done.Done("n1")
	assert.Equal(t, "n1", leader)

	NewLeaderTransferAction(1, "n1", callback()).Error(errors.New("unavailable"))
	assert.Error(t, failure)
}
