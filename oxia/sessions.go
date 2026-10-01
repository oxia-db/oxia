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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/internal"
)

func newSessions(ctx context.Context, shardManager internal.ShardManager, rpcProvider internal.RpcProvider, options clientOptions) *sessions {
	s := &sessions{
		clientIdentity:  options.identity,
		ctx:             ctx,
		shardManager:    shardManager,
		rpcProvider:     rpcProvider,
		sessionsByShard: map[int64]*clientSession{},
		clientOpts:      options,
		log: slog.With(
			slog.String("component", "oxia-session-manager"),
			slog.String("client-identity", options.identity),
		),
	}
	s.sessionsCtx, s.cancelSessions = context.WithCancel(ctx)
	changed := shardManager.Changed()
	go process.DoWithLabels(
		s.sessionsCtx,
		map[string]string{
			"oxia": "session-follow-splits",
		},
		func() { s.followSplitsOnShardMapChange(changed) },
	)
	return s
}

type sessions struct {
	sync.Mutex
	clientIdentity string
	ctx            context.Context
	// sessionsCtx is the parent of the contexts of the sessions, cancelled
	// when they are closed
	sessionsCtx     context.Context
	cancelSessions  context.CancelFunc
	shardManager    internal.ShardManager
	rpcProvider     internal.RpcProvider
	sessionsByShard map[int64]*clientSession
	log             *slog.Logger
	clientOpts      clientOptions
}

// executeWithSessionId invokes the callback with the shard that shardForKey
// returns and the id of the session on that shard, once the session has been
// established. The shard is looked up under the lock: a shard looked up before
// could be one whose session followSplits has moved since. It is looked up
// again when the shard is split before the session is established on it.
func (s *sessions) executeWithSessionId(shardForKey func() int64,
	callback func(shardId int64, sessionId int64, err error)) {
	s.Lock()
	defer s.Unlock()
	for {
		shardId := shardForKey()
		session, found := s.sessionsByShard[shardId]
		if !found {
			// The shard may have replaced a shard that was split, and inherited
			// its session
			s.followSplits()
			session, found = s.sessionsByShard[shardId]
		}
		if !found {
			session = s.startSession(shardId)
			s.sessionsByShard[shardId] = session
		}
		err := session.executeWithId(func(sessionId int64, err error) { callback(shardId, sessionId, err) })
		if err == nil {
			return
		}
		// The session failed to start: forget it, so that the next operation
		// attempts a fresh one
		delete(s.sessionsByShard, shardId)
		if !errors.Is(err, constant.ErrShardNotFound) {
			callback(shardId, -1, err)
			return
		}
		// The shard was split before the session was established: the key is
		// now on one of the shards that replaced it
		session.log.Info("The shard was split before the session was established on it")
	}
}

// followSplitsOnShardMapChange moves the sessions of the shards that were split
// whenever the shard map changes, starting with the change that closes changed:
// the shards that replaced them started to count their timeout when they were
// elected, before the client could know them.
func (s *sessions) followSplitsOnShardMapChange(changed <-chan struct{}) {
	for {
		select {
		case <-changed:
		case <-s.sessionsCtx.Done():
			return
		}
		changed = s.shardManager.Changed()
		s.Lock()
		s.followSplits()
		s.Unlock()
	}
}

// followSplits moves the sessions of the shards that were split to the shards
// that replaced them. Each of those inherited the session, with its ephemeral
// records in their hash range, and expires it unless it is kept alive there.
// It must be called with the lock held.
func (s *sessions) followSplits() {
	if s.sessionsCtx.Err() != nil {
		return
	}
	for shardId, cs := range s.sessionsByShard {
		if s.shardManager.Exists(shardId) {
			continue
		}
		successors := s.currentSuccessors(shardId)
		cs.Lock()
		sessionId, established := cs.sessionId, cs.established
		cs.Unlock()
		if len(successors) == 0 || !established {
			continue
		}

		cs.log.Info(
			"Moving the session to the shards that replaced its shard",
			slog.Any("shards", successors),
		)
		for _, successor := range successors {
			if _, found := s.sessionsByShard[successor]; !found {
				s.sessionsByShard[successor] = s.inheritSession(successor, sessionId)
			}
		}
		delete(s.sessionsByShard, shardId)
		cs.cancel()
	}
}

// currentSuccessors returns the shards of the shard map that replaced a shard
// removed from it: the shards that replaced it may have been split in turn.
func (s *sessions) currentSuccessors(shardId int64) []int64 {
	var current []int64
	for _, successor := range s.shardManager.GetSuccessors(shardId) {
		if s.shardManager.Exists(successor) {
			current = append(current, successor)
		} else {
			current = append(current, s.currentSuccessors(successor)...)
		}
	}
	return current
}

// inheritSession returns the session of a shard that replaced a split shard,
// which the shard inherited from it.
func (s *sessions) inheritSession(shardId int64, sessionId int64) *clientSession {
	cs := s.newClientSession(shardId)
	cs.Lock()
	defer cs.Unlock()
	cs.setEstablished(sessionId)
	return cs
}

func (s *sessions) newClientSession(shardId int64) *clientSession {
	cs := &clientSession{
		shardId:  shardId,
		sessions: s,
		// Buffered: the failure notification must not block the creator
		// goroutine until an operation happens to come by
		started: make(chan error, 1),
		log: slog.With(
			slog.String("component", "session"),
			slog.Int64("shard", shardId),
		),
	}

	cs.ctx, cs.cancel = context.WithCancel(s.sessionsCtx)
	return cs
}

func (s *sessions) startSession(shardId int64) *clientSession {
	cs := s.newClientSession(shardId)

	cs.log.Debug("Creating session")
	go process.DoWithLabels(
		cs.ctx,
		map[string]string{
			"oxia":  "session-start",
			"shard": fmt.Sprintf("%d", cs.shardId),
		},
		func() { cs.createSessionWithRetries() },
	)
	return cs
}

func (s *sessions) Close() error {
	// Stop the sessions first: an operation that waits for one to be
	// established holds the lock
	s.cancelSessions()

	s.Lock()
	defer s.Unlock()
	var err error
	for _, cs := range s.sessionsByShard {
		err = multierr.Append(err, cs.Close())
	}

	return err
}

type clientSession struct {
	sync.Mutex
	started     chan error
	shardId     int64
	sessionId   int64
	established bool
	log         *slog.Logger
	sessions    *sessions
	ctx         context.Context
	cancel      context.CancelFunc
}

// executeWithId invokes the callback with the session id, once the session
// has been established. When the session failed to start, it returns the error
// without invoking the callback: the caller — which already holds the sessions
// lock — discards the session, so that re-acquiring that lock here (a
// self-deadlock) is never needed.
func (cs *clientSession) executeWithId(callback func(int64, error)) error {
	select {
	case err := <-cs.started:
		if err != nil {
			return err
		}
		cs.Lock()
		callback(cs.sessionId, nil)
		cs.Unlock()
	case <-cs.ctx.Done():
		if cs.ctx.Err() != nil && !errors.Is(cs.ctx.Err(), context.Canceled) {
			callback(-1, cs.ctx.Err())
		}
	}
	return nil
}

func (cs *clientSession) createSessionWithRetries() {
	backOff := time2.NewBackOff(cs.ctx)
	// A change of the shard map ends the wait for the next attempt: it can be
	// the split of the shard, which ends the creation
	timer := &internal.ShardMapTimer{}
	err := backoff.RetryNotifyWithTimer(func() error {
		timer.Changed = cs.sessions.shardManager.Changed()
		return cs.createSession()
	}, backOff, func(err error, duration time.Duration) {
		if !errors.Is(err, context.Canceled) {
			cs.log.Error(
				"Error while creating session",
				slog.Any("error", err),
				slog.Duration("retry-after", duration),
			)
		}
	}, timer)
	if err != nil && !errors.Is(err, context.Canceled) {
		cs.Lock()
		cs.started <- err
		close(cs.started)
		cs.Unlock()
	}
}

func (cs *clientSession) createSession() error {
	if !cs.sessions.shardManager.Exists(cs.shardId) {
		// The shard was split and deleted: no data server accepts the session
		// on it anymore
		return backoff.Permanent(constant.ErrShardNotFound)
	}
	ctx, cancel := context.WithTimeout(cs.ctx, cs.sessions.clientOpts.requestTimeout)
	defer cancel()
	createSessionResponse, err := cs.sessions.rpcProvider.CreateSession(ctx, cs.leader(), &proto.CreateSessionRequest{
		Shard:            cs.shardId,
		ClientIdentity:   cs.sessions.clientIdentity,
		SessionTimeoutMs: uint32(cs.sessions.clientOpts.sessionTimeout.Milliseconds()),
	})
	if err != nil {
		return err
	}
	cs.Lock()
	defer cs.Unlock()
	cs.setEstablished(createSessionResponse.SessionId)
	cs.log.Debug("Successfully created session")
	return nil
}

// setEstablished records the id of the established session, and starts to
// keep it alive. It must be called with the lock held.
func (cs *clientSession) setEstablished(sessionId int64) {
	cs.sessionId = sessionId
	cs.established = true
	cs.log = cs.log.With(
		slog.Int64("session-id", sessionId),
		slog.String("client-identity", cs.sessions.clientIdentity),
	)
	close(cs.started)

	go process.DoWithLabels(
		cs.ctx,
		map[string]string{
			"oxia":    "session-keep-alive",
			"shard":   fmt.Sprintf("%d", cs.shardId),
			"session": fmt.Sprintf("%x016", cs.sessionId),
		},
		func() {
			backOff := time2.NewBackOff(cs.sessions.ctx)
			err := backoff.RetryNotify(func() error {
				err := cs.keepAlive(backOff)
				if errors.Is(err, constant.ErrSessionNotFound) {
					cs.log.Error(
						"Session is no longer valid",
						slog.Any("error", err),
					)

					cs.sessions.Lock()
					defer cs.sessions.Unlock()
					cs.Lock()
					defer cs.Unlock()
					delete(cs.sessions.sessionsByShard, cs.shardId)
					return backoff.Permanent(err)
				}
				return err
			}, backOff, func(err error, duration time.Duration) {
				slog.Debug(
					"Failed to send session heartbeat, retrying later",
					slog.Any("error", err),
					slog.Duration("retry-after", duration),
				)
			})

			if err != nil && !errors.Is(err, context.Canceled) {
				cs.log.Error(
					"Failed to keep alive session",
					slog.Any("error", err),
				)
			}
		},
	)
}

func (cs *clientSession) leader() string {
	return cs.sessions.shardManager.Leader(cs.shardId)
}

func (cs *clientSession) Close() error {
	cs.cancel()

	ctx, cancel := context.WithTimeout(cs.sessions.ctx, cs.sessions.clientOpts.requestTimeout)
	defer cancel()

	if _, err := cs.sessions.rpcProvider.CloseSession(ctx, cs.leader(), &proto.CloseSessionRequest{
		Shard:     cs.shardId,
		SessionId: cs.sessionId,
	}); err != nil {
		if errors.Is(err, constant.ErrSessionNotFound) {
			return nil
		}
		return err
	}
	return nil
}

func (cs *clientSession) keepAlive(backOff backoff.BackOff) error {
	cs.sessions.Lock()
	cs.Lock()
	ctx := cs.ctx
	shardId := cs.shardId
	sessionId := cs.sessionId
	cs.Unlock()
	cs.sessions.Unlock()

	tickTime := cs.sessions.clientOpts.sessionKeepAliveTicker
	if tickTime < 2*time.Second {
		tickTime = 2 * time.Second
	}

	ticker := time.NewTicker(tickTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, err := cs.sessions.rpcProvider.KeepAlive(ctx, cs.sessions.shardManager.Leader(shardId), &proto.SessionHeartbeat{Shard: shardId, SessionId: sessionId})
			if err != nil {
				return err
			}
			// RetryNotify only resets the backoff when it starts: without this,
			// every failure over the lifetime of the session would make the
			// retry of the next one wait longer
			backOff.Reset()
		case <-ctx.Done():
			return nil
		}
	}
}
