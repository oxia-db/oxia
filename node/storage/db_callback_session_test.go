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
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/node/constant"
	"github.com/oxia-db/oxia/node/storage/kvstore"
	"github.com/oxia-db/oxia/proto"
)

type mockWriteBatch map[string]any

func (m mockWriteBatch) Count() int {
	return 0
}

func (m mockWriteBatch) Size() int {
	return 0
}

var _ kvstore.WriteBatch = (*mockWriteBatch)(nil)

type mockCloser struct{}

var _ io.Closer = (*mockCloser)(nil)

func (m mockCloser) Close() error {
	return nil
}

func (m mockWriteBatch) Close() error {
	return nil
}

func (m mockWriteBatch) Put(key string, value []byte) error {
	val, found := m[key]
	if found {
		if valAsError, wasError := val.(error); wasError {
			return valAsError
		}
	}
	m[key] = value
	return nil
}

func (m mockWriteBatch) Delete(key string) error {
	delete(m, key)
	return nil
}

func (m mockWriteBatch) Get(key string) ([]byte, io.Closer, error) {
	val, found := m[key]
	if !found {
		return nil, nil, kvstore.ErrKeyNotFound
	}
	err, wasError := val.(error)
	if wasError {
		return nil, nil, err
	}
	return val.([]byte), &mockCloser{}, nil
}

func (m mockWriteBatch) FindLower(key string) (string, error) {
	return "", errors.New("not implemented")
}

func (m mockWriteBatch) DeleteRange(_, _ string) error {
	return nil
}

func (m mockWriteBatch) KeyRangeScan(_, _ string) (kvstore.KeyIterator, error) {
	return nil, kvstore.ErrKeyNotFound
}

func (m mockWriteBatch) RangeScan(_, _ string) (kvstore.KeyValueIterator, error) {
	return nil, kvstore.ErrKeyNotFound
}

func (m mockWriteBatch) Commit() error {
	return nil
}

func storageEntry(t *testing.T, sessionId int64) []byte {
	t.Helper()

	entry := &proto.StorageEntry{
		Value:                 nil,
		VersionId:             0,
		CreationTimestamp:     0,
		ModificationTimestamp: 0,
		SessionId:             &sessionId,
	}
	bytes, err := pb.Marshal(entry)
	assert.NoError(t, err)
	return bytes
}

func TestSessionUpdateOperationCallback_OnDelete(t *testing.T) {
	sessionId := int64(12345)

	writeBatch := mockWriteBatch{
		"a/b/c": storageEntry(t, sessionId),
		constant.SessionKey(constant.SessionId(sessionId)) + "/a%2Fb%2Fc": []byte{},
	}

	err := sessionManagerUpdateOperationCallback.OnDelete(writeBatch, nil, "a/b/c")
	assert.NoError(t, err)
	_, found := writeBatch[constant.SessionKey(constant.SessionId(sessionId))+"/a%2Fb%2Fc"]
	assert.False(t, found)
}

func TestSessionUpdateOperationCallback_OnPut(t *testing.T) {
	sessionId := int64(12345)
	versionId := int64(2)

	noSessionPutRequest := &proto.PutRequest{
		Key:   "a/b/c",
		Value: []byte("b"),
	}
	sessionPutRequest := &proto.PutRequest{
		Key:               "a/b/c",
		Value:             []byte("b"),
		ExpectedVersionId: &versionId,
		SessionId:         &sessionId,
	}

	writeBatch := mockWriteBatch{}

	status, err := sessionManagerUpdateOperationCallback.OnPut(writeBatch, nil, noSessionPutRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	assert.Equal(t, len(writeBatch), 0)

	writeBatch = mockWriteBatch{
		"a/b/c": []byte{},
		constant.ShadowKey(constant.SessionId(sessionId-1), "a/b/c"): []byte{},
	}

	se := &proto.StorageEntry{
		Value:                 []byte("value"),
		VersionId:             0,
		CreationTimestamp:     0,
		ModificationTimestamp: 0,
		SessionId:             &sessionId,
	}

	status, err = sessionManagerUpdateOperationCallback.OnPut(writeBatch, nil, noSessionPutRequest, se)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	_, oldKeyFound := writeBatch[constant.SessionKey(constant.SessionId(sessionId))+"a"]
	assert.False(t, oldKeyFound)

	writeBatch = mockWriteBatch{
		"a/b/c": []byte{},
		constant.ShadowKey(constant.SessionId(sessionId-1), "a/b/c"): []byte{},
		constant.SessionKey(constant.SessionId(sessionId - 1)):       []byte{},
		constant.SessionKey(constant.SessionId(sessionId)):           []byte{},
	}

	status, err = sessionManagerUpdateOperationCallback.OnPut(writeBatch, nil, sessionPutRequest, se)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	_, oldKeyFound = writeBatch[constant.SessionKey(constant.SessionId(sessionId-1))+"a"]
	assert.False(t, oldKeyFound)
	_, newKeyFound := writeBatch[constant.SessionKey(constant.SessionId(sessionId))+"a"]
	assert.False(t, newKeyFound)

	writeBatch = mockWriteBatch{}
	status, err = sessionManagerUpdateOperationCallback.OnPut(writeBatch, nil, sessionPutRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_SESSION_DOES_NOT_EXIST, status)

	// session (sessionID -1) entry
	tmpSessionId := sessionId - 1
	se = &proto.StorageEntry{
		Value:                 []byte("value"),
		VersionId:             0,
		CreationTimestamp:     0,
		ModificationTimestamp: 0,
		SessionId:             &tmpSessionId,
	}
	// sessionID has expired
	writeBatch = mockWriteBatch{
		"a/b/c": []byte{}, // real data
		constant.ShadowKey(constant.SessionId(sessionId-1), "a/b/c"): []byte{}, // shadow key
		constant.SessionKey(constant.SessionId(sessionId - 1)):       []byte{}, // session
	}
	// try to use current session override the (sessionID -1)
	sessionPutRequest = &proto.PutRequest{
		Key:       "a/b/c",
		Value:     []byte("b"),
		SessionId: &sessionId,
	}

	status, err = sessionManagerUpdateOperationCallback.OnPut(writeBatch, nil, sessionPutRequest, se)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_SESSION_DOES_NOT_EXIST, status)
	_, closer, err := writeBatch.Get(constant.ShadowKey(constant.SessionId(sessionId-1), "a/b/c"))
	assert.NoError(t, err)
	closer.Close()

	expectedErr := errors.New("error coming from the DB on read")
	writeBatch = mockWriteBatch{
		constant.SessionKey(constant.SessionId(sessionId)): expectedErr,
	}
	_, err = sessionManagerUpdateOperationCallback.OnPut(writeBatch, nil, sessionPutRequest, nil)
	assert.ErrorIs(t, err, expectedErr)

	writeBatch = mockWriteBatch{
		constant.SessionKey(constant.SessionId(sessionId)): []byte{},
	}
	status, err = sessionManagerUpdateOperationCallback.OnPut(writeBatch, nil, sessionPutRequest, nil)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, status)
	sessionShadowKey := constant.ShadowKey(constant.SessionId(sessionId), "a/b/c")
	_, found := writeBatch[sessionShadowKey]
	assert.True(t, found)

	expectedErr = errors.New("error coming from the DB on write")
	writeBatch = mockWriteBatch{
		constant.SessionKey(constant.SessionId(sessionId)): []byte{},
		sessionShadowKey: expectedErr,
	}
	_, err = sessionManagerUpdateOperationCallback.OnPut(writeBatch, nil, sessionPutRequest, nil)
	assert.ErrorIs(t, err, expectedErr)
}

func TestSession_PutWithExpiredSession(t *testing.T) {
	var oldSessionId int64 = 100
	var newSessionId int64 = 101

	se := &proto.StorageEntry{
		Value:                 []byte("value"),
		VersionId:             0,
		CreationTimestamp:     0,
		ModificationTimestamp: 0,
		SessionId:             &oldSessionId,
	}
	// sessionID has expired
	writeBatch := mockWriteBatch{
		"a/b/c": []byte{}, // real data
		constant.ShadowKey(constant.SessionId(oldSessionId), "a/b/c"): []byte{}, // shadow key
		constant.SessionKey(constant.SessionId(oldSessionId)):         []byte{}, // session
	}
	// try to use current session override the (sessionID -1)
	sessionPutRequest := &proto.PutRequest{
		Key:       "a/b/c",
		Value:     []byte("b"),
		SessionId: &newSessionId,
	}

	status, err := sessionManagerUpdateOperationCallback.OnPut(writeBatch, nil, sessionPutRequest, se)
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_SESSION_DOES_NOT_EXIST, status)

	_, closer, err := writeBatch.Get(constant.ShadowKey(constant.SessionId(oldSessionId), "a/b/c"))
	assert.NoError(t, err)
	assert.NoError(t, closer.Close())
}
