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
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
)

// newBareSessionManager builds a session manager for unit tests that never
// touches the database: the expiry scheduler is not started, so sessions can
// be created and inspected without a leader controller.
func newBareSessionManager() *sessionManager {
	sm := &sessionManager{
		sessions: make(map[SessionId]*session),
		wakeCh:   make(chan struct{}, 1),
		epoch:    time.Now(),
		log:      slog.Default(),
		activeSessions: metric.NewUpDownCounter("oxia_test_bare_active_sessions", "test", "count",
			map[string]any{}),
		expiredSessions: metric.NewCounter("oxia_test_bare_expired_sessions", "test", "count",
			map[string]any{}),
	}
	sm.ctx, sm.cancel = context.WithCancel(context.Background())
	return sm
}

func newTestSession(sm *sessionManager, id SessionId, timeout time.Duration) *session {
	sm.Lock()
	defer sm.Unlock()
	return startSession(id, &proto.SessionMetadata{
		TimeoutMs: uint32(timeout.Milliseconds()),
		Identity:  "test-client",
	}, sm)
}

func TestSession_HeartbeatExtendsDeadline(t *testing.T) {
	sm := newBareSessionManager()
	defer func() { assert.NoError(t, sm.Close()) }()

	timeout := 10 * time.Second
	s := newTestSession(sm, 1, timeout)

	// Pretend the deadline is about to pass, then heartbeat
	s.deadline.Store(sm.now() + 1)
	assert.NoError(t, sm.KeepAlive(1))

	deadline := s.deadline.Load()
	assert.Greater(t, deadline, sm.now())
	assert.LessOrEqual(t, deadline, sm.now()+int64(timeout))
}

func TestSession_KeepAliveAfterRemove(t *testing.T) {
	sm := newBareSessionManager()
	defer func() { assert.NoError(t, sm.Close()) }()

	s := newTestSession(sm, 1, 10*time.Second)

	sm.Lock()
	sm.removeSession(1)
	sm.Unlock()

	assert.ErrorIs(t, sm.KeepAlive(1), constant.ErrSessionNotFound)

	// A raced heartbeat on an already-removed session must be harmless
	assert.NotPanics(t, func() {
		s.heartbeat(sm.now())
	})
}

// startSession must nudge the expiry scheduler only when the new session's
// deadline is earlier than every queued one; otherwise the scheduler's timer
// is already armed to fire soon enough.
func TestSession_StartNudgesSchedulerOnEarlierDeadline(t *testing.T) {
	sm := newBareSessionManager()
	defer func() { assert.NoError(t, sm.Close()) }()

	drainWake := func() bool {
		select {
		case <-sm.wakeCh:
			return true
		default:
			return false
		}
	}

	newTestSession(sm, 1, 10*time.Minute)
	assert.True(t, drainWake(), "first session must wake the scheduler")

	newTestSession(sm, 2, 20*time.Minute)
	assert.False(t, drainWake(), "later deadline must not wake the scheduler")

	newTestSession(sm, 3, 1*time.Minute)
	assert.True(t, drainWake(), "earlier deadline must wake the scheduler")
}

func TestSessionManager_CollectDueSessions(t *testing.T) {
	sm := newBareSessionManager()
	defer func() { assert.NoError(t, sm.Close()) }()

	due := newTestSession(sm, 1, 10*time.Second)
	refreshed := newTestSession(sm, 2, 10*time.Second)
	future := newTestSession(sm, 3, 10*time.Minute)

	// Make sessions 1 and 2 due, then heartbeat session 2
	now := sm.now()
	sm.Lock()
	due.deadline.Store(now - 1)
	due.heapDeadline = now - 1
	refreshed.heapDeadline = now - 1
	heap.Init(&sm.expiryHeap)
	sm.Unlock()
	refreshed.heartbeat(sm.now())

	collected, next := sm.collectDueSessions()
	assert.Equal(t, []*session{due}, collected)

	// The due session is off the heap, the others are still queued; the
	// refreshed session was re-queued at its extended deadline, which makes
	// it the new heap root
	assert.Equal(t, -1, due.heapIdx)
	assert.Len(t, sm.expiryHeap, 2)
	assert.Equal(t, refreshed.deadline.Load(), refreshed.heapDeadline)
	assert.Equal(t, refreshed, sm.expiryHeap[0])
	assert.Equal(t, future, sm.expiryHeap[1])

	// The scheduler sleeps until the earliest queued deadline
	assert.Greater(t, next, time.Duration(0))
	assert.LessOrEqual(t, next, 10*time.Second)
}

// A session popped as due can still be rescued before its delete is issued if
// a heartbeat arrives in between; a session concurrently closed must not be
// re-queued.
func TestSessionManager_RequeueRefreshed(t *testing.T) {
	sm := newBareSessionManager()
	defer func() { assert.NoError(t, sm.Close()) }()

	expired := newTestSession(sm, 1, 10*time.Second)
	rescued := newTestSession(sm, 2, 10*time.Second)
	closed := newTestSession(sm, 3, 10*time.Second)

	// All three are popped as due
	now := sm.now()
	for _, s := range []*session{expired, rescued, closed} {
		s.deadline.Store(now - 1)
		s.heapDeadline = now - 1
	}
	collected, _ := sm.collectDueSessions()
	assert.Len(t, collected, 3)
	assert.Empty(t, sm.expiryHeap)

	// Before the delete goes out: session 2 gets a heartbeat, session 3 gets
	// closed
	rescued.heartbeat(sm.now())
	closed.deadline.Store(sm.now() + int64(10*time.Second))
	sm.Lock()
	sm.removeSession(closed.id)
	sm.Unlock()

	stillExpired := sm.requeueRefreshed(collected)
	assert.Equal(t, []*session{expired}, stillExpired)

	// Only the rescued session is back on the heap
	assert.Len(t, sm.expiryHeap, 1)
	assert.Equal(t, rescued, sm.expiryHeap[0])
	assert.Equal(t, -1, closed.heapIdx)
}

func TestSessionHeap_Ordering(t *testing.T) {
	sm := newBareSessionManager()
	defer func() { assert.NoError(t, sm.Close()) }()

	newTestSession(sm, 1, 30*time.Second)
	newTestSession(sm, 2, 10*time.Second)
	newTestSession(sm, 3, 20*time.Second)

	// Indexes are kept consistent while the heap reorders
	for i, s := range sm.expiryHeap {
		assert.Equal(t, i, s.heapIdx)
	}

	var popped []SessionId
	sm.Lock()
	for len(sm.expiryHeap) > 0 {
		s := heap.Pop(&sm.expiryHeap).(*session)
		assert.Equal(t, -1, s.heapIdx)
		popped = append(popped, s.id)
	}
	sm.Unlock()
	assert.Equal(t, []SessionId{2, 3, 1}, popped)
}

// Heartbeats race against session removal and the scheduler's collection;
// this mainly gives the race detector something to chew on.
func TestSession_ConcurrentKeepAliveAndRemove(t *testing.T) {
	sm := newBareSessionManager()
	defer func() { assert.NoError(t, sm.Close()) }()

	const sessions = 32
	for i := 0; i < sessions; i++ {
		newTestSession(sm, SessionId(i), time.Duration(i)*time.Millisecond)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; i < sessions; i++ {
			_ = sm.KeepAlive(int64(i))
		}
	})
	wg.Go(func() {
		for i := 0; i < sessions; i += 2 {
			sm.Lock()
			sm.removeSession(SessionId(i))
			sm.Unlock()
		}
	})
	wg.Go(func() {
		for i := 0; i < 10; i++ {
			collected, _ := sm.collectDueSessions()
			sm.requeueRefreshed(collected)
		}
	})
	wg.Wait()
}
