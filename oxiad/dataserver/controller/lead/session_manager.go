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
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
)

const (
	sessionKeyPrefix = constant.InternalKeyPrefix + "session"
	sessionKeyFormat = sessionKeyPrefix + "/%016x"
	sessionKeyLength = len(sessionKeyPrefix) + 1 + 16
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

func IsSessionKey(key string) bool {
	if !strings.HasPrefix(key, sessionKeyPrefix) {
		return false
	}
	if len(key) != sessionKeyLength {
		return false
	}
	return true
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

	// expiryHeap orders the live sessions by expiry deadline; it is consumed
	// by the expiry scheduler (expiryLoop) and guarded by the manager's lock.
	expiryHeap sessionHeap
	// wakeCh nudges the expiry scheduler when a deadline earlier than every
	// queued one gets added.
	wakeCh chan struct{}
	// epoch is the base of the manager's monotonic clock: deadlines are
	// nanoseconds since epoch, see now().
	epoch time.Time
	latch sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	createdSessions metric.Counter
	closedSessions  metric.Counter
	expiredSessions metric.Counter
	activeSessions  metric.UpDownCounter
}

func NewSessionManager(ctx context.Context, namespace string, shardId int64, controller *leaderController) SessionManager {
	labels := metric.LabelsForShard(namespace, shardId)
	sm := &sessionManager{
		sessions:         make(map[SessionId]*session),
		wakeCh:           make(chan struct{}, 1),
		epoch:            time.Now(),
		namespace:        namespace,
		shardId:          shardId,
		leaderController: controller,
		log: slog.With(
			slog.String("component", "session-manager"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
			slog.Int64("term", controller.term.Load()),
		),

		createdSessions: metric.NewCounter("oxia_server_sessions_created",
			"The total number of sessions created", "count", labels),
		closedSessions: metric.NewCounter("oxia_server_sessions_closed",
			"The total number of sessions closed", "count", labels),
		expiredSessions: metric.NewCounter("oxia_server_sessions_expired",
			"The total number of sessions expired", "count", labels),
		activeSessions: metric.NewUpDownCounter("oxia_server_session_active",
			"The number of sessions currently active", "count", labels),
	}

	sm.ctx, sm.cancel = context.WithCancel(ctx)

	sm.latch.Add(1)
	go process.DoWithLabels(sm.ctx, map[string]string{
		"oxia":      "session-expiry",
		"namespace": namespace,
		"shard":     fmt.Sprintf("%d", shardId),
	}, sm.expiryLoop)

	return sm
}

// now returns the manager's monotonic clock reading, in nanoseconds since the
// manager was created. Session deadlines are instants on this clock, so they
// are immune to wall-clock jumps, like the per-session timers they replace.
func (sm *sessionManager) now() int64 {
	return int64(time.Since(sm.epoch))
}

// wake nudges the expiry scheduler to re-evaluate the earliest deadline.
func (sm *sessionManager) wake() {
	select {
	case sm.wakeCh <- struct{}{}:
	default:
	}
}

func (sm *sessionManager) CreateSession(request *proto.CreateSessionRequest) (*proto.CreateSessionResponse, error) {
	return sm.createSession(request, constant.MinSessionTimeout)
}

func (sm *sessionManager) createSession(request *proto.CreateSessionRequest, minTimeout time.Duration) (*proto.CreateSessionResponse, error) {
	timeout := time.Duration(request.SessionTimeoutMs) * time.Millisecond
	if timeout > constant.MaxSessionTimeout || timeout < minTimeout {
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

// removeSession takes the session out of the map and the expiry heap, and
// decrements the active-sessions counter. It must be called while holding the
// manager's write lock. Removing an id that is no longer present is a no-op:
// the expiry path and CloseSession can race on the same session, and the
// loser must not decrement the counter a second time.
func (sm *sessionManager) removeSession(id SessionId) {
	s, found := sm.sessions[id]
	if !found {
		return
	}
	delete(sm.sessions, id)
	if s.heapIdx >= 0 {
		// The expiry scheduler pops sessions before expiring them, so a
		// session mid-expiry is already off the heap.
		heap.Remove(&sm.expiryHeap, s.heapIdx)
	}
	sm.activeSessions.Dec()
}

// deleteSessions removes the session records from the database. Deleting a
// session record cascades, through the storage update callback, to the
// session's ephemeral keys and their shadows. Deleting an already-deleted
// session is harmless: expiry and CloseSession may race on the same session.
func (sm *sessionManager) deleteSessions(ids []SessionId) error {
	deletes := make([]*proto.DeleteRequest, len(ids))
	for i, id := range ids {
		deletes[i] = &proto.DeleteRequest{Key: SessionKey(id)}
	}
	_, err := sm.leaderController.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard:   &sm.shardId,
		Deletes: deletes,
	})
	return err
}

func (sm *sessionManager) getSession(sessionId int64) (*session, error) {
	s, found := sm.sessions[SessionId(sessionId)]
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
	defer sm.RUnlock()
	s, err := sm.getSession(sessionId)
	if err != nil {
		return err
	}
	s.heartbeat(sm.now())
	return nil
}

func (sm *sessionManager) CloseSession(request *proto.CloseSessionRequest) (*proto.CloseSessionResponse, error) {
	sm.Lock()
	s, err := sm.getSession(request.SessionId)
	if err != nil {
		sm.Unlock()
		return nil, err
	}
	sm.removeSession(s.id)
	sm.Unlock()

	sm.log.Debug(
		"Session closing",
		slog.Int64("session-id", int64(s.id)),
		slog.String("client-identity", s.clientIdentity),
	)
	if err = sm.deleteSessions([]SessionId{s.id}); err != nil {
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
		Shard:               &sm.shardId,
		StartInclusive:      sessionKeyPrefix + "/",
		EndExclusive:        sessionKeyPrefix + "//",
		IncludeInternalKeys: true,
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
	sm.cancel()
	for id := range sm.sessions {
		sm.removeSession(id)
	}
	sm.Unlock()

	// Wait for the expiry scheduler outside the manager lock: it acquires
	// the lock to finish an in-flight expiry cycle — waiting under the lock
	// would deadlock.
	sm.latch.Wait()

	return nil
}

type sessionManagerUpdateOperationCallbackS struct{}

var sessionManagerUpdateOperationCallback database.UpdateOperationCallback = &sessionManagerUpdateOperationCallbackS{}

func (*sessionManagerUpdateOperationCallbackS) OnPutWithinSession(batch kvstore.WriteBatch, notification *database.Notifications, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	var _, closer, err = batch.Get(SessionKey(SessionId(*request.SessionId)))
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return proto.Status_SESSION_DOES_NOT_EXIST, nil
		}
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}
	if err = closer.Close(); err != nil {
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}
	// delete existing session shadow
	if status, err := deleteShadow(batch, notification, request.Key, existingEntry); err != nil {
		return status, err
	}
	// Create the session shadow entry
	err = batch.Put(ShadowKey(SessionId(*request.SessionId), request.Key), []byte{})
	if err != nil {
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}

	return proto.Status_OK, nil
}

func (s *sessionManagerUpdateOperationCallbackS) OnPut(batch kvstore.WriteBatch, notification *database.Notifications, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	if request.SessionId != nil {
		// override by session operation
		return s.OnPutWithinSession(batch, notification, request, existingEntry)
	}

	// override by normal operation
	if status, err := deleteShadow(batch, notification, request.Key, existingEntry); err != nil {
		return status, err
	}

	return proto.Status_OK, nil
}

func deleteShadow(batch kvstore.WriteBatch, _ *database.Notifications, key string, existingEntry *proto.StorageEntry) (proto.Status, error) {
	// We are overwriting an ephemeral value, let's delete its shadow
	if existingEntry != nil && existingEntry.SessionId != nil {
		existingSessionId := SessionId(*existingEntry.SessionId)
		err := batch.Delete(ShadowKey(existingSessionId, key))
		if err != nil && !errors.Is(err, kvstore.ErrKeyNotFound) {
			return proto.Status_SESSION_DOES_NOT_EXIST, err
		}
	}
	return proto.Status_OK, nil
}

func (s *sessionManagerUpdateOperationCallbackS) OnDelete(batch kvstore.WriteBatch, notification *database.Notifications, key string) error {
	se, err := database.GetStorageEntryMetadata(batch, key)
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	defer se.ReturnToVTPool()
	return s.OnDeleteWithEntry(batch, notification, key, se)
}

func (*sessionManagerUpdateOperationCallbackS) OnDeleteWithEntry(batch kvstore.WriteBatch, notification *database.Notifications, key string, entry *proto.StorageEntry) error {
	if _, err := deleteShadow(batch, notification, key, entry); err != nil {
		return err
	}
	if !IsSessionKey(key) {
		return nil
	}
	sessionKey := key
	// Read "index"
	it, err := batch.KeyRangeScan(sessionKey+"/", sessionKey+"//")
	if err != nil {
		return err
	}
	defer func() {
		if err = it.Close(); err != nil {
			slog.Warn("Failed to close the iterator when delete session ephemeral keys.", slog.Any("error", err))
		}
	}()
	for ; it.Valid(); it.Next() {
		escapedEphemeralKey := it.Key()
		// delete the ephemeral key index
		if err := batch.Delete(escapedEphemeralKey); err != nil {
			return err
		}
		unescapedEphemeralKey, err := url.PathUnescape(escapedEphemeralKey[len(sessionKey)+1:])
		if err != nil {
			return err
		}
		if unescapedEphemeralKey != "" {
			// delete the ephemeral key
			if err := batch.Delete(unescapedEphemeralKey); err != nil {
				return err
			}
			// add ephemeral key to notification
			if notification != nil {
				notification.Deleted(unescapedEphemeralKey)
			}
		}
	}
	return nil
}

func (s *sessionManagerUpdateOperationCallbackS) OnDeleteRange(batch kvstore.WriteBatch, notification *database.Notifications, keyStartInclusive string, keyEndExclusive string) error {
	it, err := batch.RangeScan(keyStartInclusive, keyEndExclusive)
	if err != nil {
		return err
	}
	defer func() {
		if err = it.Close(); err != nil {
			slog.Warn("Failed to close the iterator when deleting the range.", slog.Any("error", err))
		}
	}()

	// introduce the processor here for better defer resource release
	iteratorProcessor := func(batch kvstore.WriteBatch, it kvstore.KeyValueIterator) error {
		value, err := it.Value()
		if err != nil {
			return err
		}
		se := proto.StorageEntryFromVTPool()
		defer se.ReturnToVTPool()
		if err = database.Deserialize(value, se); err != nil {
			return err
		}
		return s.OnDeleteWithEntry(batch, notification, it.Key(), se)
	}

	for ; it.Valid(); it.Next() {
		if err := iteratorProcessor(batch, it); err != nil {
			return errors.Wrap(err, "oxia db: failed to delete range")
		}
	}
	return nil
}
