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
	"errors"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
)

var (
	ErrTooManyCursors    = errors.New("too many cursors")
	ErrInvalidHeadOffset = errors.New("invalid head offset")
)

// QuorumAckTracker
// The QuorumAckTracker is responsible for keeping track of the head offset and commit offset of a shard
//   - Head offset: the last entry written in the local WAL of the leader
//   - Commit offset: the oldest entry that is considered "fully committed", as it has received the requested amount
//     of acks from the followers
//
// The quorum ack tracker is also used to block until the head offset or commit offset are advanced.
type QuorumAckTracker interface {
	io.Closer

	CommitOffset() int64

	// WaitForCommitOffset
	// Waits for the specific entry, identified by its offset, to be fully committed.
	// Once the commit is confirmed, the function will return without error.
	//
	// Parameters:
	// - ctx: The context used for managing cancellation and deadlines for the operation.
	// - offset: The unique identifier (offset) of the entry to wait for.
	//
	// Returns:
	// - error: Returns an error if the operation is unsuccessful, otherwise nil.
	//
	// Note:
	// This method blocks until the commit is confirmed.

	WaitForCommitOffset(ctx context.Context, offset int64) error

	// WaitForCommitOffsetAsync
	// Asynchronously waits for the specific entry, identified by its offset, to be fully committed.
	// Once the commit is confirmed, the provided callback function (cb) is invoked.
	//
	// Parameters:
	// - ctx: The context used for managing cancellation and deadlines for the operation.
	// - offset: The unique identifier (offset) of the entry to wait for.
	// - cb: The callback function to invoke after the commit is confirmed. The callback
	//       will receive the result or error from the operation.
	//
	// Returns:
	// - This method does not return anything immediately. The callback will handle
	//   the result or error asynchronously.
	//
	// Note:
	// This method returns immediately and does not block the caller, allowing other
	// operations to continue while waiting for the commit.
	WaitForCommitOffsetAsync(ctx context.Context, offset int64, cb concurrent.Callback[any]) // NextOffset returns the offset for the next entry to write

	// Note this can go ahead of the head-offset as there can be multiple operations in flight.
	NextOffset() int64

	HeadOffset() int64

	AdvanceHeadOffset(headOffset int64)

	// WaitForHeadOffset
	// Waits until the specified entry is written on the wal
	WaitForHeadOffset(ctx context.Context, offset int64) error

	// WaitForHeadOffsetOrCommitAdvance
	// Waits until the head offset reaches `headOffset` or the commit offset
	// advances beyond `commitOffset`. Used by observer cursors parked at the
	// head of the wal: with no new entries to piggyback it on, a commit
	// advancement must be propagated to the follower on its own.
	WaitForHeadOffsetOrCommitAdvance(ctx context.Context, headOffset int64, commitOffset int64) error

	// NewCursorAcker creates a tracker for a new cursor
	// The `ackOffset` is the previous last-acked position for the cursor
	NewCursorAcker(ackOffset int64) (CursorAcker, error)
}

type quorumAckTracker struct {
	sync.Mutex
	waitingRequests []waitingRequest

	// Signaled whenever the head offset or the commit offset advances,
	// and on close. Waiters re-check their own predicate.
	progressCond concurrent.ConditionContext

	replicationFactor uint32
	requiredAcks      uint32

	nextOffset   atomic.Int64
	headOffset   atomic.Int64
	commitOffset atomic.Int64

	// Keep track of the number of acks that each entry has received
	// The bitset is used to handle duplicate acks from a single follower
	tracker            map[int64]*BitSet
	cursorIdxGenerator int
	closed             bool

	// The callbacks of the waiting requests perform the database apply and the
	// client response on the write path: they must never run under the tracker
	// mutex or on the cursor ack goroutines, where they would stall the whole
	// shard pipeline. They are invoked, in offset order, by a dedicated
	// goroutine that is woken up through commitSignal.
	commitSignal  chan struct{}
	callbacksDone chan struct{}
}

type CursorAcker interface {
	Ack(offset int64)
}

type cursorAcker struct {
	quorumTracker *quorumAckTracker
	cursorIdx     int

	// The highest offset acked by this cursor; guarded by the tracker mutex
	lastAckedOffset int64
}

type waitingRequest struct {
	minOffset int64
	callback  concurrent.Callback[any]
}

func NewQuorumAckTracker(replicationFactor uint32, headOffset int64, commitOffset int64) QuorumAckTracker {
	q := &quorumAckTracker{
		// Ack quorum is number of follower acks that are required to consider the entry fully committed
		// We are using RF/2 (and not RF/2 + 1) because the leader is already storing 1 copy locally
		requiredAcks:      replicationFactor / 2,
		replicationFactor: replicationFactor,
		tracker:           make(map[int64]*BitSet),
		waitingRequests:   make([]waitingRequest, 0),
	}

	q.nextOffset.Store(headOffset)
	q.headOffset.Store(headOffset)
	q.commitOffset.Store(commitOffset)

	// Add entries to track the entries we're not yet sure that are fully committed
	for offset := commitOffset + 1; offset <= headOffset; offset++ {
		q.tracker[offset] = &BitSet{}
	}

	q.progressCond = concurrent.NewConditionContext(q)
	q.commitSignal = make(chan struct{}, 1)
	q.callbacksDone = make(chan struct{})
	go q.runCallbacks()
	return q
}

// runCallbacks is the only place where the waiting requests get completed,
// keeping the callbacks out of the tracker mutex and in offset order.
func (q *quorumAckTracker) runCallbacks() {
	defer close(q.callbacksDone)

	for {
		<-q.commitSignal

		for {
			q.Lock()
			if q.closed {
				pending := q.waitingRequests
				q.waitingRequests = nil
				q.Unlock()

				for _, r := range pending {
					r.callback.OnCompleteError(constant.ErrResourceUnavailable)
				}
				return
			}

			ready := q.dequeueReadyWaiters()
			q.Unlock()

			if len(ready) == 0 {
				break
			}
			for _, r := range ready {
				r.callback.OnComplete(nil)
			}
		}
	}
}

// dequeueReadyWaiters must be called while holding the tracker mutex.
// The waiting requests are kept sorted by minOffset (see insertWaitingRequest),
// so the committed ones are always a prefix of the slice.
func (q *quorumAckTracker) dequeueReadyWaiters() []waitingRequest {
	commitOffset := q.commitOffset.Load()
	n := 0
	for n < len(q.waitingRequests) &&
		(q.requiredAcks == 0 || q.waitingRequests[n].minOffset <= commitOffset) {
		n++
	}
	ready := q.waitingRequests[:n]
	q.waitingRequests = q.waitingRequests[n:]
	return ready
}

func (q *quorumAckTracker) AdvanceHeadOffset(headOffset int64) {
	q.Lock()
	defer q.Unlock()

	if q.closed {
		return
	}

	if headOffset <= q.headOffset.Load() {
		return
	}

	q.headOffset.Store(headOffset)
	q.progressCond.Broadcast()

	if q.requiredAcks == 0 {
		q.notifyCommitOffsetAdvanced(headOffset)
	} else {
		q.tracker[headOffset] = &BitSet{}
	}
}

func (q *quorumAckTracker) NextOffset() int64 {
	return q.nextOffset.Add(1)
}

func (q *quorumAckTracker) CommitOffset() int64 {
	return q.commitOffset.Load()
}

func (q *quorumAckTracker) HeadOffset() int64 {
	return q.headOffset.Load()
}

func (q *quorumAckTracker) WaitForHeadOffset(ctx context.Context, offset int64) error {
	q.Lock()
	defer q.Unlock()

	for !q.closed && q.headOffset.Load() < offset {
		if err := q.progressCond.Wait(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (q *quorumAckTracker) WaitForHeadOffsetOrCommitAdvance(ctx context.Context, headOffset int64, commitOffset int64) error {
	q.Lock()
	defer q.Unlock()

	for !q.closed && q.headOffset.Load() < headOffset && q.commitOffset.Load() <= commitOffset {
		if err := q.progressCond.Wait(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (q *quorumAckTracker) WaitForCommitOffset(ctx context.Context, offset int64) error {
	ch := make(chan error, 1)
	q.WaitForCommitOffsetAsync(ctx, offset, concurrent.NewOnce(
		func(_ any) { ch <- nil },
		func(err error) { ch <- err },
	))

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *quorumAckTracker) WaitForCommitOffsetAsync(_ context.Context, offset int64, cb concurrent.Callback[any]) {
	q.Lock()

	if q.closed {
		q.Unlock()
		cb.OnCompleteError(constant.ErrResourceUnavailable)
		return
	}

	q.insertWaitingRequest(waitingRequest{offset, cb})
	if q.requiredAcks == 0 || q.commitOffset.Load() >= offset {
		// Already satisfied: the callback is still invoked from the callbacks
		// goroutine, never inline, so that the caller (e.g. the WAL sync
		// goroutine) does not block behind the database apply.
		channel.PushNoBlock(q.commitSignal, struct{}{})
	}
	q.Unlock()
}

// insertWaitingRequest keeps the waiting requests sorted by minOffset, so that
// the committed ones always form a prefix of the slice. The requests are
// normally registered in offset order, making this an append in practice; the
// requests for the same offset preserve their registration order.
// It must be called while holding the tracker mutex.
func (q *quorumAckTracker) insertWaitingRequest(r waitingRequest) {
	i := sort.Search(len(q.waitingRequests), func(i int) bool {
		return q.waitingRequests[i].minOffset > r.minOffset
	})
	q.waitingRequests = append(q.waitingRequests, waitingRequest{})
	copy(q.waitingRequests[i+1:], q.waitingRequests[i:])
	q.waitingRequests[i] = r
}

func (q *quorumAckTracker) notifyCommitOffsetAdvanced(commitOffset int64) {
	q.commitOffset.Store(commitOffset)
	q.progressCond.Broadcast()

	if len(q.waitingRequests) > 0 {
		channel.PushNoBlock(q.commitSignal, struct{}{})
	}
}

func (q *quorumAckTracker) Close() error {
	q.Lock()
	alreadyClosed := q.closed
	q.closed = true
	q.progressCond.Broadcast()
	q.Unlock()

	if !alreadyClosed {
		channel.PushNoBlock(q.commitSignal, struct{}{})
	}
	// Wait for the callbacks goroutine to fail the pending requests and drain:
	// once Close returns, no callback is running or will ever run.
	<-q.callbacksDone
	return nil
}

func (q *quorumAckTracker) NewCursorAcker(ackOffset int64) (CursorAcker, error) {
	q.Lock()
	defer q.Unlock()

	if uint32(q.cursorIdxGenerator) >= q.replicationFactor-1 {
		return nil, ErrTooManyCursors
	}

	if ackOffset > q.headOffset.Load() {
		return nil, ErrInvalidHeadOffset
	}

	qa := &cursorAcker{
		quorumTracker:   q,
		cursorIdx:       q.cursorIdxGenerator,
		lastAckedOffset: ackOffset,
	}

	// If the new cursor is already past the current quorum commit offset, we have
	// to mark these entries as acked (by that cursor).
	for offset := q.commitOffset.Load() + 1; offset <= ackOffset; offset++ {
		qa.ack(offset)
	}

	q.cursorIdxGenerator++
	return qa, nil
}

// noOpCursorAcker is a CursorAcker that does not participate in the
// quorum. Used for observer followers (e.g., shard split children).
type noOpCursorAcker struct{}

func NewNoOpCursorAcker() CursorAcker {
	return &noOpCursorAcker{}
}

func (*noOpCursorAcker) Ack(_ int64) {
	// no-op: observer acks don't affect quorum commit offset
}

// Ack acknowledges all the entries up to the given offset: acks are
// cumulative, so a follower can confirm a whole sync round with a single
// message, and the implied range gets accounted under one lock acquisition.
// An offset at or below the previously acked one is a no-op.
func (c *cursorAcker) Ack(offset int64) {
	q := c.quorumTracker
	q.Lock()
	defer q.Unlock()

	if q.closed {
		return
	}

	// Entries at or below the commit offset have already reached the quorum
	start := max(c.lastAckedOffset, q.commitOffset.Load()) + 1
	for o := start; o <= offset; o++ {
		c.ack(o)
	}
	if offset > c.lastAckedOffset {
		c.lastAckedOffset = offset
	}
}

func (c *cursorAcker) ack(offset int64) {
	q := c.quorumTracker

	e, found := q.tracker[offset]
	if !found {
		// The entry has already previously reached the quorum.
		// There's nothing more left to do here.
		return
	}

	// Mark that this follower has acked the entry
	e.Set(c.cursorIdx)
	if uint32(e.Count()) == q.requiredAcks {
		delete(q.tracker, offset)

		// Advance the commit offset
		q.notifyCommitOffsetAdvanced(offset)
	}
}
