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
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/rpc"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/oxia/internal"
	"github.com/oxia-db/oxia/proto"
)

func newSessions(ctx context.Context, shardManager internal.ShardManager, pool rpc.ClientPool, options clientOptions) *sessions {
	s := &sessions{
		clientIdentity:  options.identity,
		ctx:             ctx,
		shardManager:    shardManager,
		pool:            pool,
		sessionsByShard: map[int64]*clientSession{},
		clientOpts:      options,
		log: slog.With(
			slog.String("component", "oxia-session-manager"),
			slog.String("client-identity", options.identity),
		),
	}
	return s
}

type sessions struct {
	sync.Mutex
	clientIdentity  string
	ctx             context.Context
	shardManager    internal.ShardManager
	pool            rpc.ClientPool
	sessionsByShard map[int64]*clientSession
	log             *slog.Logger
	clientOpts      clientOptions
}

func (s *sessions) executeWithSessionId(shardId int64, callback func(int64, error)) {
	s.Lock()
	defer s.Unlock()
	session, found := s.sessionsByShard[shardId]
	if !found {
		session = s.startSession(shardId)
		s.sessionsByShard[shardId] = session
	}
	session.executeWithId(callback)
}

func (s *sessions) startSession(shardId int64) *clientSession {
	cs := &clientSession{
		shardId:  shardId,
		sessions: s,
		started:  make(chan error),
		log: slog.With(
			slog.String("component", "session"),
			slog.Int64("shard", shardId),
		),
	}

	cs.ctx, cs.cancel = context.WithCancel(s.ctx)

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
	var err error
	for _, cs := range s.sessionsByShard {
		err = multierr.Append(err, cs.Close())
	}

	return err
}

type clientSession struct {
	sync.Mutex
	started   chan error
	shardId   int64
	sessionId int64
	log       *slog.Logger
	sessions  *sessions
	ctx       context.Context
	cancel    context.CancelFunc
}

func (cs *clientSession) executeWithId(callback func(int64, error)) {
	select {
	case err := <-cs.started:
		if err != nil {
			callback(-1, err)
			cs.sessions.Lock()
			defer cs.sessions.Unlock()
			cs.Lock()
			defer cs.Unlock()
			delete(cs.sessions.sessionsByShard, cs.shardId)
		} else {
			cs.Lock()
			callback(cs.sessionId, nil)
			cs.Unlock()
		}
	case <-cs.ctx.Done():
		if cs.ctx.Err() != nil && !errors.Is(cs.ctx.Err(), context.Canceled) {
			callback(-1, cs.ctx.Err())
		}
	}
}

func (cs *clientSession) createSessionWithRetries() {
	backOff := time2.NewBackOff(cs.ctx)
	err := backoff.RetryNotify(cs.createSession,
		backOff, func(err error, duration time.Duration) {
			if !errors.Is(err, context.Canceled) {
				cs.log.Error(
					"Error while creating session",
					slog.Any("error", err),
					slog.Duration("retry-after", duration),
				)
			}
		})
	if err != nil && !errors.Is(err, context.Canceled) {
		cs.Lock()
		cs.started <- err
		close(cs.started)
		cs.Unlock()
	}
}

func (cs *clientSession) createSession() error {
	client, err := cs.getRpc()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(cs.ctx, cs.sessions.clientOpts.requestTimeout)
	defer cancel()
	createSessionResponse, err := client.CreateSession(ctx, &proto.CreateSessionRequest{
		Shard:            cs.shardId,
		ClientIdentity:   cs.sessions.clientIdentity,
		SessionTimeoutMs: uint32(cs.sessions.clientOpts.sessionTimeout.Milliseconds()),
	})
	if err != nil {
		return err
	}
	sessionId := createSessionResponse.SessionId
	cs.Lock()
	defer cs.Unlock()
	cs.sessionId = sessionId
	cs.log = cs.log.With(
		slog.Int64("session-id", sessionId),
		slog.String("client-identity", cs.sessions.clientIdentity),
	)
	close(cs.started)
	cs.log.Debug("Successfully created session")

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
				err := cs.keepAlive()
				if status.Code(err) == constant.CodeSessionNotFound {
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

	return nil
}

func (cs *clientSession) getRpc() (proto.OxiaClientClient, error) {
	leader := cs.sessions.shardManager.Leader(cs.shardId)
	return cs.sessions.pool.GetClientRpc(leader)
}

func (cs *clientSession) Close() error {
	cs.cancel()

	client, err := cs.getRpc()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(cs.sessions.ctx, cs.sessions.clientOpts.requestTimeout)
	defer cancel()

	if _, err = client.CloseSession(ctx, &proto.CloseSessionRequest{
		Shard:     cs.shardId,
		SessionId: cs.sessionId,
	}); err != nil {
		return err
	}
	return nil
}

func (cs *clientSession) keepAlive() error {
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

	client, err := cs.getRpc()
	if err != nil {
		return err
	}

	for {
		select {
		case <-ticker.C:
			_, err = client.KeepAlive(ctx, &proto.SessionHeartbeat{Shard: shardId, SessionId: sessionId})
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}
