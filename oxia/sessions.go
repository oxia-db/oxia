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

// errSessionMoved is returned by executeWithId when the session was moved to
// the shards that replaced its split shard while the operation waited for it.
var errSessionMoved = errors.New("the session was moved to the shards that replaced its shard")

// executeWithSessionId invokes the callback with the shard that shardForKey
// returns and the id of the session on that shard, once the session has been
// established. The shard and its session are looked up under the lock: a shard
// looked up before could be one whose session followSplits has moved since.
// The wait for the session and the callback run outside the lock, so that a
// shard whose session cannot be established, or whose batcher is backed up,
// does not hold up the operations on the other shards. The shard is looked up
// again when the shard is split before the session is established on it, or
// when its session is moved while the operation waits for it.
func (s *sessions) executeWithSessionId(shardForKey func() int64,
	callback func(shardId int64, sessionId int64, err error)) {
	for {
		shardId, session := s.sessionForShard(shardForKey)
		err := session.executeWithId(func(sessionId int64, err error) { callback(shardId, sessionId, err) })
		if err == nil {
			return
		}
		if errors.Is(err, errSessionMoved) {
			continue
		}
		// The session failed to start: forget it, so that the next operation
		// attempts a fresh one
		s.forgetSession(shardId, session)
		if !errors.Is(err, constant.ErrShardNotFound) {
			callback(shardId, -1, err)
			return
		}
		// The shard was split before the session was established: the key is
		// now on one of the shards that replaced it
		session.log.Info("The shard was split before the session was established on it")
	}
}

// sessionForShard returns the shard that shardForKey returns and its session,
// which it starts when the shard has none.
func (s *sessions) sessionForShard(shardForKey func() int64) (int64, *clientSession) {
	s.Lock()
	defer s.Unlock()
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
	return shardId, session
}

// forgetSession removes the session of a shard, unless the shard has another
// session already: another operation may have replaced the failed one.
func (s *sessions) forgetSession(shardId int64, session *clientSession) {
	s.Lock()
	defer s.Unlock()
	if s.sessionsByShard[shardId] == session {
		delete(s.sessionsByShard, shardId)
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
		successors := currentSuccessors(s.shardManager, shardId)
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
func currentSuccessors(shardManager internal.ShardManager, shardId int64) []int64 {
	var current []int64
	for _, successor := range shardManager.GetSuccessors(shardId) {
		if shardManager.Exists(successor) {
			current = append(current, successor)
		} else {
			current = append(current, currentSuccessors(shardManager, successor)...)
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
		started:  make(chan struct{}),
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
	// Stop the sessions first, so that the operations waiting for one to be
	// established stop waiting
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
	// started is closed once the creation of the session has ended: with
	// startErr set when it failed
	started     chan struct{}
	startErr    error
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
// without invoking the callback, and the caller discards the session. When the
// session was moved to the shards that replaced its shard, it returns
// errSessionMoved, and the caller looks the session up again. Several
// operations can wait for the same session.
func (cs *clientSession) executeWithId(callback func(int64, error)) error {
	select {
	case <-cs.started:
		cs.Lock()
		defer cs.Unlock()
		if cs.startErr != nil {
			return cs.startErr
		}
		callback(cs.sessionId, nil)
	case <-cs.ctx.Done():
		if !errors.Is(cs.ctx.Err(), context.Canceled) {
			callback(-1, cs.ctx.Err())
		} else if cs.sessions.sessionsCtx.Err() == nil {
			// Cancelled while the client is open: followSplits moved the
			// session
			return errSessionMoved
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
		cs.startErr = err
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
