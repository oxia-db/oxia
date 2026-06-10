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

package lead

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
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

func TestNoOpCursorAcker_DoesNotAffectQuorum(t *testing.T) {
	at := NewQuorumAckTracker(3, 1, wal.InvalidOffset)

	assert.EqualValues(t, 1, at.HeadOffset())
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	at.AdvanceHeadOffset(2)
	at.AdvanceHeadOffset(3)

	// Create a regular cursor acker
	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	// Create a no-op cursor acker (observer)
	noOp := NewNoOpCursorAcker()

	// No-op acks should not advance the commit offset
	noOp.Ack(2)
	noOp.Ack(3)
	assert.Equal(t, wal.InvalidOffset, at.CommitOffset())

	// Only the real cursor ack should advance the commit offset
	c1.Ack(2)
	assert.EqualValues(t, 2, at.CommitOffset())
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
		assert.ErrorIs(t, resErr, constant.ErrResourceUnavailable)
	case <-time.After(2 * time.Second): // Adding a timeout for safety
		t.Fatal("Timed out waiting for async result")
	}
}

// The waiting-request callbacks perform the database apply and the client response
// on the leader write path: the cursor acks must never block behind them, and they
// must be invoked one at a time, in offset order.
func TestQuorumAckTracker_CallbacksOffTheAckPath(t *testing.T) {
	at := NewQuorumAckTracker(2, 1, wal.InvalidOffset)
	defer at.Close()

	gate := make(chan struct{})
	closeGate := sync.OnceFunc(func() { close(gate) })
	defer closeGate()

	at.AdvanceHeadOffset(2)
	at.AdvanceHeadOffset(3)

	started := make(chan int64, 2)
	completed := make(chan int64, 2)

	at.WaitForCommitOffsetAsync(context.Background(), 2, concurrent.NewOnce(
		func(any) {
			started <- 2
			<-gate
			completed <- 2
		}, func(error) {}))
	at.WaitForCommitOffsetAsync(context.Background(), 3, concurrent.NewOnce(
		func(any) {
			started <- 3
			completed <- 3
		}, func(error) {}))

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)

	// The acks must complete promptly, even with the first callback blocked
	ackDone := make(chan struct{})
	go func() {
		c1.Ack(2)
		c1.Ack(3)
		close(ackDone)
	}()

	select {
	case <-ackDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Ack blocked behind a waiting-request callback")
	}
	assert.EqualValues(t, 3, at.CommitOffset())

	// The first callback is in progress: the second must not start before it completes
	select {
	case offset := <-started:
		assert.EqualValues(t, 2, offset)
	case <-time.After(10 * time.Second):
		t.Fatal("first callback did not start")
	}
	select {
	case <-started:
		t.Fatal("second callback started while the first was still running")
	case <-time.After(100 * time.Millisecond):
	}

	closeGate()

	for _, expected := range []int64{2, 3} {
		select {
		case offset := <-completed:
			assert.EqualValues(t, expected, offset)
		case <-time.After(10 * time.Second):
			t.Fatal("callback did not complete")
		}
	}
}

// A wait registered out of offset order (for an offset that is already
// committed, while an earlier-registered waiter is still pending on a later
// offset) must complete promptly instead of queueing behind the pending one.
func TestQuorumAckTracker_OutOfOrderWait(t *testing.T) {
	at := NewQuorumAckTracker(3, 1, wal.InvalidOffset)
	defer at.Close()

	for offset := int64(2); offset <= 5; offset++ {
		at.AdvanceHeadOffset(offset)
	}

	pending := make(chan error, 1)
	at.WaitForCommitOffsetAsync(context.Background(), 5, concurrent.NewOnce(
		func(any) { pending <- nil }, func(err error) { pending <- err }))

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)
	c1.Ack(2)
	assert.EqualValues(t, 2, at.CommitOffset())

	// Offset 2 is committed: waiting on it must complete even though the
	// waiter for offset 5 was registered first and is still pending
	satisfied := make(chan error, 1)
	at.WaitForCommitOffsetAsync(context.Background(), 2, concurrent.NewOnce(
		func(any) { satisfied <- nil }, func(err error) { satisfied <- err }))

	select {
	case err := <-satisfied:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("already-satisfied wait queued behind a pending one")
	}
	assert.Len(t, pending, 0)

	for offset := int64(3); offset <= 5; offset++ {
		c1.Ack(offset)
	}
	select {
	case err := <-pending:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("pending wait did not complete")
	}
}

// Close must not return while a waiting-request callback is still in flight,
// so that the database is never closed under an in-progress apply.
func TestQuorumAckTracker_CloseWaitsForInFlightCallback(t *testing.T) {
	at := NewQuorumAckTracker(2, 1, wal.InvalidOffset)

	gate := make(chan struct{})
	closeGate := sync.OnceFunc(func() { close(gate) })
	defer closeGate()

	at.AdvanceHeadOffset(2)

	started := make(chan struct{})
	completed := make(chan struct{}, 1)
	at.WaitForCommitOffsetAsync(context.Background(), 2, concurrent.NewOnce(
		func(any) {
			close(started)
			<-gate
			completed <- struct{}{}
		}, func(error) {}))

	c1, err := at.NewCursorAcker(wal.InvalidOffset)
	assert.NoError(t, err)
	c1.Ack(2)

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("callback did not start")
	}

	closeDone := make(chan struct{})
	go func() {
		assert.NoError(t, at.Close())
		close(closeDone)
	}()

	select {
	case <-closeDone:
		t.Fatal("Close returned while a callback was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	closeGate()

	select {
	case <-closeDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the callback completed")
	}
	assert.Len(t, completed, 1)
}
