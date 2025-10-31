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

	"github.com/oxia-db/oxia/common/collection"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	. "github.com/oxia-db/oxia/node/constant"
	"github.com/oxia-db/oxia/proto"
	"github.com/pkg/errors"
)

// --- SessionManager

type SessionManager interface {
	io.Closer
	CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error)
	KeepAlive(sessionId int64) error
	CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error)
	Initialize() error
}

var _ SessionManager = (*sessionManager)(nil)

type sessionManager struct {
	sync.RWMutex
	leaderController *leaderController
	namespace        string
	shardId          int64
	sessions         collection.Map[SessionId, *session]
	log              *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	createdSessions metric.Counter
	closedSessions  metric.Counter
	expiredSessions metric.Counter
	activeSessions  metric.Gauge
}

func NewSessionManager(ctx context.Context, namespace string, shardId int64, controller *leaderController) SessionManager {
	labels := metric.LabelsForShard(namespace, shardId)
	sm := &sessionManager{
		sessions:         collection.NewVisibleMap[SessionId, *session](),
		namespace:        namespace,
		shardId:          shardId,
		leaderController: controller,
		log: slog.With(
			slog.String("component", "session-manager"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
			slog.Int64("term", controller.term),
		),

		createdSessions: metric.NewCounter("oxia_server_sessions_created",
			"The total number of sessions created", "count", labels),
		closedSessions: metric.NewCounter("oxia_server_sessions_closed",
			"The total number of sessions closed", "count", labels),
		expiredSessions: metric.NewCounter("oxia_server_sessions_expired",
			"The total number of sessions expired", "count", labels),
	}

	sm.ctx, sm.cancel = context.WithCancel(ctx)

	sm.activeSessions = metric.NewGauge("oxia_server_session_active",
		"The number of sessions currently active", "count", labels, func() int64 {
			return int64(sm.sessions.Size())
		})

	return sm
}

func (sm *sessionManager) CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return sm.createSession(request, MinSessionTimeout)
}

func (sm *sessionManager) createSession(request *proto.CreateSessionRequest, minTimeout time.Duration) (*proto.CreateSessionResponse, error) {
	timeout := time.Duration(request.SessionTimeoutMs) * time.Millisecond
	if timeout > MaxSessionTimeout || timeout < minTimeout {
		return nil, errors.Wrap(constant.ErrInvalidSessionTimeout, fmt.Sprintf("timeoutMs=%d", request.SessionTimeoutMs))
	}

	metadata := proto.SessionMetadataFromVTPool()
	metadata.TimeoutMs = uint32(timeout.Milliseconds())
	metadata.Identity = request.ClientIdentity
	defer metadata.ReturnToVTPool()

	marshalledMetadata, err := metadata.MarshalVT()
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal session metadata")
	}
	var sessionId SessionId
	resp, err := sm.leaderController.writeBlock(sm.ctx, func(offset int64) *proto.WriteRequest {
		sessionId = SessionId(offset)
		return &proto.WriteRequest{
			Shard: &request.Shard,
			Puts: []*proto.PutRequest{{
				Key:   SessionKey(sessionId),
				Value: marshalledMetadata,
			}},
		}
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to register session")
	}
	if resp.Puts[0].Status != proto.Status_OK {
		return nil, errors.Errorf("failed to register session. invalid status %#v", resp.Puts[0].Status)
	}

	sm.Lock()
	defer sm.Unlock()
	s := startSession(sessionId, metadata, sm)

	sm.createdSessions.Inc()
	return &proto.CreateSessionResponse{SessionId: int64(s.id)}, nil
}

func (sm *sessionManager) getSession(sessionId int64) (*session, error) {
	s, found := sm.sessions.Get(SessionId(sessionId))
	if !found {
		sm.log.Warn(
			"Session not found",
			slog.Int64("session-id", sessionId),
		)
		return nil, constant.ErrSessionNotFound
	}
	return s, nil
}

func (sm *sessionManager) KeepAlive(sessionId int64) error {
	sm.RLock()
	s, err := sm.getSession(sessionId)
	sm.RUnlock()
	if err != nil {
		return err
	}
	s.heartbeat()
	return nil
}

func (sm *sessionManager) CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	sm.Lock()
	s, err := sm.getSession(request.SessionId)
	if err != nil {
		sm.Unlock()
		return nil, err
	}
	sm.sessions.Remove(s.id)
	sm.Unlock()

	s.log.Info("Session closing")
	s.Close()
	err = s.delete()
	if err != nil {
		return nil, err
	}

	sm.closedSessions.Inc()
	return &proto.CloseSessionResponse{}, nil
}

func (sm *sessionManager) Initialize() error {
	sm.Lock()
	defer sm.Unlock()
	sessions, err := sm.readSessions()
	if err != nil {
		return err
	}
	for sessionId, sessionMetadata := range sessions {
		startSession(sessionId, sessionMetadata, sm)
	}
	return nil
}

func (sm *sessionManager) readSessions() (map[SessionId]*proto.SessionMetadata, error) {
	keys, err := sm.leaderController.ListBlock(context.Background(), &proto.ListRequest{
		Shard:          &sm.shardId,
		StartInclusive: SessionKeyPrefix + "/",
		EndExclusive:   SessionKeyPrefix + "//",
	})
	if err != nil {
		return nil, err
	}

	sm.log.Info("All sessions", slog.Int("count", len(keys)))

	result := map[SessionId]*proto.SessionMetadata{}

	for _, key := range keys {
		metaEntry, err := sm.leaderController.db.Get(&proto.GetRequest{
			Key:          key,
			IncludeValue: true,
		})
		if err != nil {
			return nil, err
		}

		if metaEntry.Status != proto.Status_OK {
			sm.log.Warn(
				"error reading session metadata",
				slog.String("key", key),
				slog.Any("status", metaEntry.Status),
			)
			continue
		}
		sessionId, err := KeyToId(key)
		if err != nil {
			sm.log.Warn(
				"error parsing session key",
				slog.Any("error", err),
				slog.String("key", key),
			)
			continue
		}
		value := metaEntry.Value
		metadata := proto.SessionMetadata{}
		err = metadata.UnmarshalVT(value)
		if err != nil {
			sm.log.Warn(
				"error unmarshalling session metadata",
				slog.Any("error", err),
				slog.Int64("session-id", int64(sessionId)),
				slog.String("key", key),
			)
			continue
		}

		result[sessionId] = &metadata
	}

	return result, nil
}

func (sm *sessionManager) Close() error {
	sm.Lock()
	defer sm.Unlock()
	sm.cancel()
	for _, s := range sm.sessions.Values() {
		sm.sessions.Remove(s.id)
		s.Close()
	}

	sm.activeSessions.Unregister()
	return nil
}
