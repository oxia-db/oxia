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

package lead

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/node/constant"
	"github.com/oxia-db/oxia/proto"
)

// --- Session

type session struct {
	io.Closer
	sync.Mutex

	ctx       context.Context
	ctxCancel context.CancelFunc
	latch     sync.WaitGroup

	id             constant.SessionId
	clientIdentity string
	shardId        int64
	timeout        time.Duration
	sm             *sessionManager
	heartbeatCh    chan bool
	log            *slog.Logger
}

func startSession(sessionId constant.SessionId, sessionMetadata *proto.SessionMetadata, sm *sessionManager) *session {
	s := &session{
		latch: sync.WaitGroup{},

		id:             sessionId,
		clientIdentity: sessionMetadata.Identity,
		timeout:        time.Duration(sessionMetadata.TimeoutMs) * time.Millisecond,
		sm:             sm,
		heartbeatCh:    make(chan bool, 1),

		log: slog.With(
			slog.String("client-identity", sessionMetadata.Identity),
			slog.String("component", "session"),
			slog.Int64("session-id", int64(sessionId)),
			slog.String("namespace", sm.namespace),
			slog.Int64("shard", sm.shardId),
		),
	}
	sm.sessions.Put(sessionId, s)
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	s.latch.Add(1)
	go process.DoWithLabels(s.ctx, map[string]string{
		"oxia":            "session",
		"client-identity": sessionMetadata.Identity,
		"session-id":      fmt.Sprintf("%d", sessionId),
		"namespace":       sm.namespace,
		"shard":           fmt.Sprintf("%d", sm.shardId),
	}, s.waitForHeartbeats)

	s.log.Info("Session started", slog.Duration("session-timeout", s.timeout))
	return s
}

func (s *session) Close() {
	s.close()
	// wait until all the background goroutine gracefully shutdown
	s.latch.Wait()
}

func (s *session) close() {
	s.Lock()
	s.ctxCancel()
	if s.heartbeatCh != nil {
		close(s.heartbeatCh)
		s.heartbeatCh = nil
	}
	s.Unlock()
	s.log.Debug("Session channels closed")
}

func (s *session) delete() error {
	_, err := s.sm.leaderController.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard:        &s.shardId,
		Puts:         nil,
		Deletes:      []*proto.DeleteRequest{{Key: constant.SessionKey(s.id)}},
		DeleteRanges: nil,
	})
	return err
}

func (s *session) heartbeat() {
	s.Lock()
	defer s.Unlock()
	if s.heartbeatCh != nil {
		s.heartbeatCh <- true
	}
}

func (s *session) waitForHeartbeats() {
	s.Lock()
	heartbeatChannel := s.heartbeatCh
	s.Unlock()
	s.log.Debug("Waiting for heartbeats")
	timeoutTimer := time.NewTimer(s.timeout)
	defer func() {
		timeoutTimer.Stop()
		s.latch.Done()
	}()
	for {
		select {
		case <-s.ctx.Done():
			return
		case heartbeat := <-heartbeatChannel:
			if !heartbeat {
				// The channel is closed, so the session must be closing
				return
			}
			timeoutTimer.Reset(s.timeout)
		case <-timeoutTimer.C:
			s.log.Warn("Session expired")

			s.close()
			if err := s.delete(); err != nil {
				s.log.Error("Failed to delete session", slog.Any("error", err))
			}

			s.sm.Lock()
			s.sm.sessions.Remove(s.id)
			s.sm.expiredSessions.Inc()
			s.sm.Unlock()
		}
	}
}
