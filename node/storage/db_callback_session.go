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

package storage

import (
	"log/slog"
	"net/url"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/node/constant"
	"github.com/oxia-db/oxia/node/storage/kvstore"
	"github.com/oxia-db/oxia/proto"
)

type sessionManagerUpdateOperationCallbackS struct{}

var sessionManagerUpdateOperationCallback UpdateOperationCallback = &sessionManagerUpdateOperationCallbackS{}

func (*sessionManagerUpdateOperationCallbackS) OnPutWithinSession(batch kvstore.WriteBatch, notification *Notifications, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
	var _, closer, err = batch.Get(constant.SessionKey(constant.SessionId(*request.SessionId)))
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
	err = batch.Put(constant.ShadowKey(constant.SessionId(*request.SessionId), request.Key), []byte{})
	if err != nil {
		return proto.Status_SESSION_DOES_NOT_EXIST, err
	}

	return proto.Status_OK, nil
}

func (s *sessionManagerUpdateOperationCallbackS) OnPut(batch kvstore.WriteBatch, notification *Notifications, request *proto.PutRequest, existingEntry *proto.StorageEntry) (proto.Status, error) {
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

func deleteShadow(batch kvstore.WriteBatch, _ *Notifications, key string, existingEntry *proto.StorageEntry) (proto.Status, error) {
	// We are overwriting an ephemeral value, let's delete its shadow
	if existingEntry != nil && existingEntry.SessionId != nil {
		existingSessionId := constant.SessionId(*existingEntry.SessionId)
		err := batch.Delete(constant.ShadowKey(existingSessionId, key))
		if err != nil && !errors.Is(err, kvstore.ErrKeyNotFound) {
			return proto.Status_SESSION_DOES_NOT_EXIST, err
		}
	}
	return proto.Status_OK, nil
}

func (s *sessionManagerUpdateOperationCallbackS) OnDelete(batch kvstore.WriteBatch, notification *Notifications, key string) error {
	se, err := GetStorageEntry(batch, key)
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	defer se.ReturnToVTPool()
	return s.OnDeleteWithEntry(batch, notification, key, se)
}

func (*sessionManagerUpdateOperationCallbackS) OnDeleteWithEntry(batch kvstore.WriteBatch, notification *Notifications, key string, entry *proto.StorageEntry) error {
	if _, err := deleteShadow(batch, notification, key, entry); err != nil {
		return err
	}
	if !constant.IsSessionKey(key) {
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

func (s *sessionManagerUpdateOperationCallbackS) OnDeleteRange(batch kvstore.WriteBatch, notification *Notifications, keyStartInclusive string, keyEndExclusive string) error {
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
		if err = Deserialize(value, se); err != nil {
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
