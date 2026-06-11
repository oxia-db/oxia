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

package oxia

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// An operation hitting a session that failed to start must fail its callback
// and discard the session — without re-acquiring the sessions lock that the
// caller already holds, which was a self-deadlock wedging every subsequent
// ephemeral operation of the client.
func TestSessions_FailedSessionStartDoesNotDeadlock(t *testing.T) {
	s := &sessions{
		sessionsByShard: map[int64]*clientSession{},
		log:             slog.Default(),
	}

	cs := &clientSession{
		shardId:  1,
		sessions: s,
		started:  make(chan error, 1),
		log:      slog.Default(),
	}
	cs.ctx, cs.cancel = context.WithCancel(context.Background())
	cs.started <- errors.New("session start failed")
	s.sessionsByShard[1] = cs

	done := make(chan struct{})
	go func() {
		s.executeWithSessionId(1, func(sessionId int64, err error) {
			assert.Error(t, err)
			assert.EqualValues(t, -1, sessionId)
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("executeWithSessionId deadlocked on a failed session start")
	}

	// The failed session has been discarded: the next operation will attempt
	// a fresh one
	s.Lock()
	_, found := s.sessionsByShard[1]
	s.Unlock()
	assert.False(t, found)
}
