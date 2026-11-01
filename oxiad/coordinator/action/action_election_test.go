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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestElectionActionWait(t *testing.T) {
	done := NewElectionAction(1)
	assert.Equal(t, int64(1), done.GetShard())
	go done.Done("n0")
	leader, err := done.Wait(context.Background())
	assert.Equal(t, "n0", leader)
	assert.NoError(t, err)

	failed := NewElectionAction(1)
	go failed.Error(errors.New("unavailable"))
	_, err = failed.Wait(context.Background())
	assert.Error(t, err)
}

func TestLeaderTransferActionWait(t *testing.T) {
	transfer := NewLeaderTransferAction(1, "n1")
	assert.Equal(t, "n1", transfer.PreferredLeader)
	transfer.Done("n1")
	transfer.Error(errors.New("late"))
	leader, err := transfer.Wait(context.Background())
	assert.Equal(t, "n1", leader)
	assert.NoError(t, err)
}
