// Copyright 2023-2025 The Oxia Authors
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

package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/server/wal"
)

func TestQuorumAckTrackerNoFollower(t *testing.T) {
	at := NewQuorumAckTracker(1, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadOffset())
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	at.AdvanceHeadOffset(5)
	assert.EqualValues(t, 5, at.HeadOffset())
	assert.EqualValues(t, 5, at.CommitOffset())

	at.AdvanceHeadOffset(6)
	assert.EqualValues(t, 6, at.HeadOffset())
	assert.EqualValues(t, 6, at.CommitOffset())

	// Head offset cannot go back in time
	at.AdvanceHeadOffset(2)
	assert.EqualValues(t, 6, at.HeadOffset())
	assert.EqualValues(t, 6, at.CommitOffset())
}

func TestQuorumAckTrackerRF2(t *testing.T) {
	at := NewQuorumAckTracker(2, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadOffset())
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	at.AdvanceHeadOffset(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c1.Ack(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.EqualValues(t, 2, at.CommitOffset())
}

func TestQuorumAckTrackerRF3(t *testing.T) {
	at := NewQuorumAckTracker(3, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadOffset())
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	at.AdvanceHeadOffset(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c2, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c1.Ack(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.EqualValues(t, 2, at.CommitOffset())

	c2.Ack(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.EqualValues(t, 2, at.CommitOffset())
}

func TestQuorumAckTrackerRF5(t *testing.T) {
	at := NewQuorumAckTracker(5, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadOffset())
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	at.AdvanceHeadOffset(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c2, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c3, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c4, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	c1.Ack(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	c2.Ack(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.EqualValues(t, 2, at.CommitOffset())

	c3.Ack(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.EqualValues(t, 2, at.CommitOffset())

	c4.Ack(2)
	assert.EqualValues(t, 2, at.HeadOffset())
	assert.EqualValues(t, 2, at.CommitOffset())
}

func TestQuorumAckTrackerMaxCursors(t *testing.T) {
	at := NewQuorumAckTracker(3, 1, wal.InvalidOffset)

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)
	assert.NotNil(t, c1)

	c2, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)
	assert.NotNil(t, c2)

	c3, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.ErrorIs(t, err, ErrTooManyCursors)
	assert.Nil(t, c3)
}

func TestQuorumAckTracker_WaitForHeadOffset(t *testing.T) {
	at := NewQuorumAckTracker(1, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadOffset())

	ch := make(chan bool)

	go func() {
		assert.NoError(t, at.WaitForHeadOffset(context.Background(), 4))
		ch <- true
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case <-ch:
		assert.Fail(t, "should not be ready")
	default:
		// Expected. There should be nothing in the channel
	}

	at.AdvanceHeadOffset(4)
	assert.Eventually(t, func() bool {
		select {
		case <-ch:
			// Expected, the operation should be already done
			return true
		default:
			// Should have been ready
			return false
		}
	}, 10*time.Second, 100*time.Millisecond)
}

func TestQuorumAckTracker_WaitForCommitOffset(t *testing.T) {
	at := NewQuorumAckTracker(3, 1, wal.InvalidOffset)

	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())
	at.AdvanceHeadOffset(2)
	at.AdvanceHeadOffset(3)
	at.AdvanceHeadOffset(4)
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	ch := make(chan error)

	go func() {
		ch <- at.WaitForCommitOffset(context.Background(), 2)
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case <-ch:
		assert.Fail(t, "should not be ready")
	default:
		// Expected. There should be nothing in the channel
	}

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)
	assert.NotNil(t, c1)
	c1.Ack(2)

	assert.Eventually(t, func() bool {
		select {
		case <-ch:
			// Expected, the operation should be already done
			return true
		default:
			// Should have been ready
			return false
		}
	}, 10*time.Second, 100*time.Millisecond)

	assert.EqualValues(t, 2, at.CommitOffset())
}

func TestQuorumAckTracker_AddingCursors_RF3(t *testing.T) {
	at := NewQuorumAckTracker(3, 10, 5)

	assert.EqualValues(t, 10, at.HeadOffset())
	assert.EqualValues(t, 5, at.CommitOffset())

	c, err := at.NewCursorAcker(11)
	assert.Nil(t, c)
	assert.ErrorIs(t, err, ErrInvalidHeadOffset)

	c1, err := at.NewCursorAcker(7)
	assert.NotNil(t, c1)
	assert.NoError(t, err)

	assert.EqualValues(t, 10, at.HeadOffset())
	assert.EqualValues(t, 7, at.CommitOffset())

	c2, err := at.NewCursorAcker(9)
	assert.NotNil(t, c2)
	assert.NoError(t, err)

	assert.EqualValues(t, 10, at.HeadOffset())
	assert.EqualValues(t, 9, at.CommitOffset())
}

func TestQuorumAckTracker_AddingCursors_RF5(t *testing.T) {
	at := NewQuorumAckTracker(5, 10, 5)

	assert.EqualValues(t, 10, at.HeadOffset())
	assert.EqualValues(t, 5, at.CommitOffset())

	c, err := at.NewCursorAcker(11)
	assert.Nil(t, c)
	assert.ErrorIs(t, err, ErrInvalidHeadOffset)

	c1, err := at.NewCursorAcker(7)
	assert.NotNil(t, c1)
	assert.NoError(t, err)

	assert.EqualValues(t, 10, at.HeadOffset())
	assert.EqualValues(t, 5, at.CommitOffset())

	c2, err := at.NewCursorAcker(9)
	assert.NotNil(t, c2)
	assert.NoError(t, err)

	assert.EqualValues(t, 10, at.HeadOffset())
	assert.EqualValues(t, 7, at.CommitOffset())
}

func TestQuorumAckTracker_ClearPending(t *testing.T) {
	at := NewQuorumAckTracker(5, 10, 5)
	asyncRes := make(chan error, 1)
	go func() {
		asyncRes <- at.WaitForCommitOffset(context.Background(), 6)
	}()

	time.Sleep(100 * time.Millisecond)
	err := at.Close()
	assert.NoError(t, err)

	// Wait for the result from asyncRes
	select {
	case resErr := <-asyncRes:
		// Ensure that we received the expected result (in this case, error should be nil)
		assert.ErrorIs(t, resErr, constant.ErrAlreadyClosed)
	case <-time.After(2 * time.Second): // Adding a timeout for safety
		t.Fatal("Timed out waiting for async result")
	}
}
