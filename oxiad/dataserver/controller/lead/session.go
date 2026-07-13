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
	"container/heap"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/oxia-db/oxia/common/proto"
)

// --- Session

// session tracks one live client session on the shard leader. It is a small
// passive record: expiry is driven by the manager's shared scheduler (see
// expiryLoop) instead of a per-session goroutine and timer, so that holding
// many live sessions stays cheap (https://github.com/oxia-db/oxia/issues/1235).
type session struct {
	id             SessionId
	clientIdentity string
	timeout        time.Duration

	// deadline is the expiry instant, in nanoseconds on the manager's
	// monotonic clock (sessionManager.now). Heartbeats store a new value;
	// the expiry scheduler loads it to decide whether the session is still
	// alive.
	deadline atomic.Int64

	// heapDeadline is the deadline this session was queued with in the
	// manager's expiry heap. It can lag behind deadline: heartbeats only
	// update deadline, and the scheduler re-queues stale entries when they
	// reach the top of the heap. Guarded by the manager's lock.
	heapDeadline int64

	// heapIdx is this session's position in the manager's expiry heap, -1
	// when not queued. Guarded by the manager's lock.
	heapIdx int
}

// heartbeat pushes the expiry deadline out to now+timeout. Lock-free: it is
// called on the keep-alive hot path while holding only the manager's read
// lock.
func (s *session) heartbeat(now int64) {
	s.deadline.Store(now + int64(s.timeout))
}

// startSession registers the session with the manager and queues it on the
// expiry heap. The caller must hold the manager's write lock.
func startSession(sessionId SessionId, sessionMetadata *proto.SessionMetadata, sm *sessionManager) *session {
	s := &session{
		id:             sessionId,
		clientIdentity: sessionMetadata.Identity,
		timeout:        time.Duration(sessionMetadata.TimeoutMs) * time.Millisecond,
		heapIdx:        -1,
	}
	deadline := sm.now() + int64(s.timeout)
	s.deadline.Store(deadline)
	s.heapDeadline = deadline

	sm.sessions[sessionId] = s
	sm.activeSessions.Inc()
	heap.Push(&sm.expiryHeap, s)
	if sm.expiryHeap[0] == s {
		// New earliest deadline: nudge the expiry scheduler to re-arm its
		// timer.
		sm.wake()
	}

	sm.log.Debug(
		"Session started",
		slog.Int64("session-id", int64(sessionId)),
		slog.String("client-identity", s.clientIdentity),
		slog.Duration("session-timeout", s.timeout),
	)
	return s
}

// --- Expiry scheduler

// expiryBatchSize caps how many expired sessions get deleted in a single
// write: deleting a session record cascades to all its ephemeral keys, so the
// batch is kept moderate while still amortizing the replication round-trip
// during mass expiry.
const expiryBatchSize = 128

// idleExpiryWait is how long the scheduler sleeps when no deadline is queued;
// queuing an earlier deadline nudges it awake before that.
const idleExpiryWait = time.Hour

// sessionHeap is a min-heap of sessions ordered by heapDeadline.
type sessionHeap []*session

func (h sessionHeap) Len() int           { return len(h) }
func (h sessionHeap) Less(i, j int) bool { return h[i].heapDeadline < h[j].heapDeadline }

func (h sessionHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}

func (h *sessionHeap) Push(x any) {
	s := x.(*session) //nolint:revive
	s.heapIdx = len(*h)
	*h = append(*h, s)
}

func (h *sessionHeap) Pop() any {
	old := *h
	n := len(old)
	s := old[n-1]
	old[n-1] = nil
	s.heapIdx = -1
	*h = old[:n-1]
	return s
}

// expiryLoop is the shared session-expiry scheduler: a single goroutine per
// session manager that sleeps until the earliest queued deadline and expires
// every session whose deadline has passed.
func (sm *sessionManager) expiryLoop() {
	defer sm.latch.Done()
	timer := time.NewTimer(idleExpiryWait)
	defer timer.Stop()
	for {
		due, next := sm.collectDueSessions()
		if len(due) > 0 {
			sm.expireSessions(due)
			// More sessions may have become due while the deletes were in
			// flight: collect again before going back to sleep.
			continue
		}
		// No Stop+drain dance before Reset: with go >= 1.23 Reset flushes a
		// pending fire, and a spurious wake-up would only cost one empty
		// collect pass anyway.
		timer.Reset(next)
		select {
		case <-sm.ctx.Done():
			return
		case <-sm.wakeCh:
		case <-timer.C:
		}
	}
}

// collectDueSessions pops off the expiry heap every session whose deadline
// has passed. Sessions whose deadline was pushed forward by heartbeats since
// they were queued are re-queued at their current deadline instead. It also
// returns how long the scheduler may sleep until the next queued deadline.
func (sm *sessionManager) collectDueSessions() (due []*session, next time.Duration) {
	sm.Lock()
	defer sm.Unlock()
	now := sm.now()
	for len(sm.expiryHeap) > 0 {
		s := sm.expiryHeap[0]
		if s.heapDeadline > now {
			return due, time.Duration(s.heapDeadline - now)
		}
		if deadline := s.deadline.Load(); deadline > now {
			s.heapDeadline = deadline
			heap.Fix(&sm.expiryHeap, 0)
			continue
		}
		heap.Pop(&sm.expiryHeap)
		due = append(due, s)
	}
	return due, idleExpiryWait
}

// expireSessions deletes the due sessions from the database in batches and
// removes them from the manager, mirroring what the expiry path of the old
// per-session goroutine used to do.
func (sm *sessionManager) expireSessions(due []*session) {
	for start := 0; start < len(due) && sm.ctx.Err() == nil; start += expiryBatchSize {
		batch := due[start:min(start+expiryBatchSize, len(due))]
		expired := sm.requeueRefreshed(batch)
		if len(expired) == 0 {
			continue
		}

		ids := make([]SessionId, len(expired))
		for i, s := range expired {
			ids[i] = s.id
			sm.log.Warn(
				"Session expired",
				slog.Int64("session-id", int64(s.id)),
				slog.String("client-identity", s.clientIdentity),
			)
		}
		if err := sm.deleteSessions(ids); err != nil {
			sm.log.Error(
				"Failed to delete expired sessions",
				slog.Any("error", err),
				slog.Int("count", len(ids)),
			)
		}

		sm.Lock()
		for _, s := range expired {
			sm.removeSession(s.id)
			sm.expiredSessions.Inc()
		}
		sm.Unlock()
	}
}

// requeueRefreshed gives popped sessions a last chance before their delete is
// issued: any session that received a heartbeat while earlier batches were
// being deleted goes back on the heap, the rest are confirmed expired.
func (sm *sessionManager) requeueRefreshed(batch []*session) (expired []*session) {
	sm.Lock()
	defer sm.Unlock()
	now := sm.now()
	expired = make([]*session, 0, len(batch))
	for _, s := range batch {
		deadline := s.deadline.Load()
		if deadline <= now {
			expired = append(expired, s)
			continue
		}
		if _, stillTracked := sm.sessions[s.id]; stillTracked {
			s.heapDeadline = deadline
			heap.Push(&sm.expiryHeap, s)
		}
	}
	return expired
}
