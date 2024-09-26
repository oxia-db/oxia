// Copyright 2023 StreamNative, Inc.
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

package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/pkg/errors"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
)

const (
	sessionKeyPrefix = common.InternalKeyPrefix + "session"
	sessionKeyFormat = sessionKeyPrefix + "/%016x"
)

type SessionId int64

func SessionKey(sessionId SessionId) string {
	return fmt.Sprintf("%s/%016x", sessionKeyPrefix, sessionId)
}

func ShadowKey(sessionId SessionId, key string) string {
	return fmt.Sprintf("%s/%016x/%s", sessionKeyPrefix, sessionId, url.PathEscape(key))
}

func KeyToId(key string) (SessionId, error) {
	var id int64
	items, err := fmt.Sscanf(key, sessionKeyFormat, &id)
	if err != nil {
		return 0, err
	}

	if items != 1 {
		return 0, errors.New("failed to parse session key: " + key)
	}

	return SessionId(id), nil
}

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
	sessions         map[SessionId]*session
	log              *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	createdSessions metrics.Counter
	closedSessions  metrics.Counter
	expiredSessions metrics.Counter
	activeSessions  metrics.Gauge
}

func NewSessionManager(ctx context.Context, namespace string, shardId int64, controller *leaderController) SessionManager {
	labels := metrics.LabelsForShard(namespace, shardId)
	sm := &sessionManager{
		sessions:         make(map[SessionId]*session),
		namespace:        namespace,
		shardId:          shardId,
		leaderController: controller,
		log: slog.With(
			slog.String("component", "session-manager"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
			slog.Int64("term", controller.term),
		),

		createdSessions: metrics.NewCounter("oxia_server_sessions_created",
			"The total number of sessions created", "count", labels),
		closedSessions: metrics.NewCounter("oxia_server_sessions_closed",
			"The total number of sessions closed", "count", labels),
		expiredSessions: metrics.NewCounter("oxia_server_sessions_expired",
			"The total number of sessions expired", "count", labels),
	}

	sm.ctx, sm.cancel = context.WithCancel(ctx)

	sm.activeSessions = metrics.NewGauge("oxia_server_session_active",
		"The number of sessions currently active", "count", labels, func() int64 {
			sm.RLock()
			defer sm.RUnlock()
			return int64(len(sm.sessions))
		})

	return sm
}

func (sm *sessionManager) CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return sm.createSession(request, common.MinSessionTimeout)
}

func (sm *sessionManager) createSession(request *proto.CreateSessionRequest, minTimeout time.Duration) (*proto.CreateSessionResponse, error) {
	timeout := time.Duration(request.SessionTimeoutMs) * time.Millisecond
	if timeout > common.MaxSessionTimeout || timeout < minTimeout {
		return nil, errors.Wrap(common.ErrorInvalidSessionTimeout, fmt.Sprintf("timeoutMs=%d", request.SessionTimeoutMs))
	}
	sm.Lock()
	defer sm.Unlock()

	metadata := proto.SessionMetadataFromVTPool()
	metadata.TimeoutMs = uint32(timeout.Milliseconds())
	metadata.Identity = request.ClientIdentity
	defer metadata.ReturnToVTPool()

	marshalledMetadata, err := metadata.MarshalVT()
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal session metadata")
	}
	id, resp, err := sm.leaderController.write(sm.ctx, func(id int64) *proto.WriteRequest {
		return &proto.WriteRequest{
			Shard: &request.Shard,
			Puts: []*proto.PutRequest{{
				Key:   SessionKey(SessionId(id)),
				Value: marshalledMetadata,
			}},
		}
	})

	sessionId := SessionId(id)
	if err != nil {
		return nil, errors.Wrap(err, "failed to register session")
	}

	if resp.Puts[0].Status != proto.Status_OK {
		return nil, errors.Errorf("failed to register session. invalid status %#v", resp.Puts[0].Status)
	}

	s := startSession(sessionId, metadata, sm)

	sm.createdSessions.Inc()
	return &proto.CreateSessionResponse{SessionId: int64(s.id)}, nil
}

func (sm *sessionManager) getSession(sessionId int64) (*session, error) {
	s, found := sm.sessions[SessionId(sessionId)]
	if !found {
		sm.log.Warn(
			"Session not found",
			slog.Int64("session-id", sessionId),
		)
		return nil, common.ErrorInvalidSession
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
	delete(sm.sessions, s.id)
	sm.Unlock()
	s.Lock()
	defer s.Unlock()
	s.closeChannels()
	err = s.close()
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
	list, err := sm.leaderController.ListSliceNoMutex(context.Background(), &proto.ListRequest{
		Shard:          &sm.shardId,
		StartInclusive: sessionKeyPrefix + "/",
		EndExclusive:   sessionKeyPrefix + "//",
	})
	sm.log.Debug("All sessions",
		slog.Int("count", len(list)))

	if err != nil {
		return nil, err
	}

	sm.log.Info("All sessions", slog.Int("count", len(list)))

	result := map[SessionId]*proto.SessionMetadata{}

	for _, key := range list {
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
	for _, s := range sm.sessions {
		delete(sm.sessions, s.id)
		s.Lock()
		s.closeChannels()
		s.Unlock()
	}

	sm.activeSessions.Unregister()
	return nil
}

type updateCallback struct{}

var SessionUpdateOperationCallback kv.UpdateOperationCallback = &updateCallback{}

func (*updateCallback) OnPutWithinSession(batch kv.WriteBatch, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	var _, closer, err = batch.Get(SessionKey(SessionId(*request.SessionId)))
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return proto.Status_SESSION_DOES_NOT_EXIST, nil
		}
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}
	if err = closer.Close(); err != nil {
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}
	// delete existing session shadow
	if status, err := deleteShadow(batch, request.Key, existingEntry); err != nil {
		return status, err
	}
	// Create the session shadow entry
	err = batch.Put(ShadowKey(SessionId(*request.SessionId), request.Key), []byte{})
	if err != nil {
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}

	return proto.Status_OK, nil
}

func (c *updateCallback) OnPut(batch kv.WriteBatch, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	switch {
	// override by normal operation
	case request.SessionId == nil:
		if status, err := deleteShadow(batch, request.Key, existingEntry); err != nil {
			return status, err
		}
		// override by session operation
	case request.SessionId != nil:
		return c.OnPutWithinSession(batch, request, existingEntry)
	}
	return proto.Status_OK, nil
}

func deleteShadow(batch kv.WriteBatch, key string, existingEntry *proto.StorageEntry) (proto.Status, error) {
	// We are overwriting an ephemeral value, let's delete its shadow
	if existingEntry != nil && existingEntry.SessionId != nil {
		existingSessionId := SessionId(*existingEntry.SessionId)
		err := batch.Delete(ShadowKey(existingSessionId, key))
		if err != nil && !errors.Is(err, kv.ErrKeyNotFound) {
			return proto.Status_SESSION_DOES_NOT_EXIST, err
		}
	}
	return proto.Status_OK, nil
}

func (*updateCallback) OnDelete(batch kv.WriteBatch, key string) error {
	se, err := kv.GetStorageEntry(batch, key)
	defer se.ReturnToVTPool()

	if errors.Is(err, kv.ErrKeyNotFound) {
		return nil
	}
	if err == nil && se.SessionId != nil {
		_, err = deleteShadow(batch, key, se)
	}
	return err
}

func (*updateCallback) OnDeleteRange(batch kv.WriteBatch, keyStartInclusive string, keyEndExclusive string) error {
	it, err := batch.RangeScan(keyStartInclusive, keyEndExclusive)
	if err != nil {
		return err
	}

	for it.Next() {
		value, err := it.Value()
		if err != nil {
			return errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to delete range")
		}
		se := proto.StorageEntryFromVTPool()

		err = kv.Deserialize(value, se)
		if err == nil && se.SessionId != nil {
			_, err = deleteShadow(batch, it.Key(), se)
		}

		se.ReturnToVTPool()

		if err != nil {
			return errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to delete range")
		}
	}

	if err := it.Close(); err != nil {
		return errors.Wrap(err, "oxia db: failed to delete range")
	}

	return err
}
