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
	"testing"
	"time"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/node/conf"
	. "github.com/oxia-db/oxia/node/constant"
	"github.com/oxia-db/oxia/node/db/kv"
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/concurrent"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/entity"

	"github.com/oxia-db/oxia/node/wal"
	"github.com/oxia-db/oxia/proto"
)

func TestSessionKey(t *testing.T) {
	id := SessionId(0xC0DE)
	sessionKey := SessionKey(id)
	assert.Equal(t, "__oxia/session/000000000000c0de", sessionKey)
	parsed, err := KeyToId(sessionKey)
	assert.NoError(t, err)
	assert.Equal(t, id, parsed)

	for _, key := range []string{"__oxia/session/", "too_short"} {
		_, err = KeyToId(key)
		assert.Error(t, err, key)
	}
}

func TestSessionManager(t *testing.T) {
	shardId := int64(1)
	// Invalid session timeout
	kvf, walf, sManager, lc := createSessionManager(t)
	_, err := sManager.CreateSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32((1 * time.Hour).Milliseconds()),
	})
	assert.ErrorIs(t, err, constant.ErrInvalidSessionTimeout)
	_, err = sManager.CreateSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32((1 * time.Second).Milliseconds()),
	})
	assert.ErrorIs(t, err, constant.ErrInvalidSessionTimeout)

	// Create and close a session, check if its persisted
	createResp, err := sManager.CreateSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: 5 * 1000,
	})
	assert.NoError(t, err)
	sessionId := createResp.SessionId
	meta := getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	assert.Equal(t, uint32(5000), meta.TimeoutMs)

	_, err = sManager.CloseSession(&proto.CloseSessionRequest{
		Shard:     shardId,
		SessionId: sessionId,
	})
	assert.NoError(t, err)
	assert.Nil(t, getSessionMetadata(t, lc, sessionId))

	// Create a session, watch it time out
	createResp, err = sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(50),
	}, 0)
	assert.NoError(t, err)
	newSessionId := createResp.SessionId
	assert.NotEqual(t, sessionId, newSessionId)
	sessionId = newSessionId
	meta = getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)

	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId) == nil
	}, time.Second, 30*time.Millisecond)

	// Create a session, keep it alive
	createResp, err = sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(50),
	}, 0)
	assert.NoError(t, err)
	sessionId = createResp.SessionId
	meta = getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	keepAlive(t, sManager, sessionId, err, 30*time.Millisecond, 6)
	time.Sleep(200 * time.Millisecond)
	assert.NotNil(t, getSessionMetadata(t, lc, sessionId))

	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId) == nil
	}, 10*time.Second, 30*time.Millisecond)

	// Create a session, put an ephemeral value
	createResp, err = sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(50),
	}, 0)
	assert.NoError(t, err)
	sessionId = createResp.SessionId
	meta = getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	keepAlive(t, sManager, sessionId, err, 30*time.Millisecond, 6)

	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:       "a/b",
			Value:     []byte("a/b"),
			SessionId: &sessionId,
		}},
	})
	assert.NoError(t, err)
	assert.Equal(t, "a/b", getData(t, lc, "a/b"))
	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId) == nil &&
			getData(t, lc, "a/b") == ""
	}, 10*time.Second, 30*time.Millisecond)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvf.Close())
	assert.NoError(t, walf.Close())
}

func TestMultipleSessionsExpiry(t *testing.T) {
	shardId := int64(1)
	// Invalid session timeout
	kvf, walf, sManager, lc := createSessionManager(t)

	// Create 2 sessions
	createResp1, err := sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(3000),
		ClientIdentity:   "session-1",
	}, 0)
	assert.NoError(t, err)
	sessionId1 := createResp1.SessionId

	createResp2, err := sManager.createSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: uint32(50),
		ClientIdentity:   "session-2",
	}, 0)
	assert.NoError(t, err)
	sessionId2 := createResp2.SessionId

	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:       "/ephemeral-1",
			Value:     []byte("hello"),
			SessionId: &sessionId1,
		}},
	})
	assert.NoError(t, err)

	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:       "/ephemeral-2",
			Value:     []byte("hello"),
			SessionId: &sessionId2,
		}},
	})
	assert.NoError(t, err)

	// Let session-2 expire and verify its key was deleted
	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId2) == nil
	}, 10*time.Second, 30*time.Millisecond)

	responses := make(chan *entity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shardId,
		Gets: []*proto.GetRequest{{
			Key:          "/ephemeral-1",
			IncludeValue: true,
		}, {
			Key:          "/ephemeral-2",
			IncludeValue: true,
		}},
	}, concurrent.ReadFromStreamCallback(responses))
	results, err := channel.ReadAll[*proto.GetResponse](context.Background(), responses) // Read entry a
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))

	// ephemeral-1
	assert.Equal(t, proto.Status_OK, results[0].Status)

	// ephemeral-2
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, results[1].Status)

	// Now Let session-1 expire and verify its key was deleted
	assert.Eventually(t, func() bool {
		return getSessionMetadata(t, lc, sessionId1) == nil
	}, 10*time.Second, 30*time.Millisecond)

	responses = make(chan *entity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shardId,
		Gets: []*proto.GetRequest{{
			Key:          "/ephemeral-1",
			IncludeValue: true,
		}, {
			Key:          "/ephemeral-2",
			IncludeValue: true,
		}},
	}, concurrent.ReadFromStreamCallback(responses))
	results, err = channel.ReadAll[*proto.GetResponse](context.Background(), responses)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
	// ephemeral-1
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, results[0].Status)

	// ephemeral-2
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, results[1].Status)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvf.Close())
	assert.NoError(t, walf.Close())
}

func TestSessionManagerReopening(t *testing.T) {
	shardId := int64(1)
	// Invalid session timeout
	walf, kvf, sManager, lc := createSessionManager(t)

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:   "/ledgers",
			Value: []byte("a"),
		}, {
			Key:   "/admin",
			Value: []byte("a"),
		}, {
			Key:   "/test",
			Value: []byte("a"),
		}},
	})
	assert.NoError(t, err)

	// Create session and reopen the session manager
	createResp, err := sManager.CreateSession(&proto.CreateSessionRequest{
		Shard:            shardId,
		SessionTimeoutMs: 5 * 1000,
	})
	assert.NoError(t, err)
	sessionId := createResp.SessionId
	meta := getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	assert.Equal(t, uint32(5000), meta.TimeoutMs)

	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shardId,
		Puts: []*proto.PutRequest{{
			Key:       "/a/b",
			Value:     []byte("/a/b"),
			SessionId: &sessionId,
		}},
	})
	assert.NoError(t, err)
	assert.Equal(t, "/a/b", getData(t, lc, "/a/b"))

	lc = reopenLeaderController(t, walf, kvf, lc)

	meta = getSessionMetadata(t, lc, sessionId)
	assert.NotNil(t, meta)
	assert.Equal(t, uint32(5000), meta.TimeoutMs)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvf.Close())
	assert.NoError(t, walf.Close())
}

func getData(t *testing.T, lc *leaderController, key string) string {
	t.Helper()

	resp, err := lc.db.Get(&proto.GetRequest{
		Key:          key,
		IncludeValue: true,
	})
	assert.NoError(t, err)
	if resp.Status != proto.Status_KEY_NOT_FOUND {
		return string(resp.Value)
	}
	return ""
}

func keepAlive(t *testing.T, sManager *sessionManager, sessionId int64, err error, sleepTime time.Duration, heartbeatCount int) {
	t.Helper()

	go func() {
		assert.NoError(t, err)
		for i := 0; i < heartbeatCount; i++ {
			time.Sleep(sleepTime)
			assert.NoError(t, sManager.KeepAlive(sessionId))
		}
	}()
}

func getSessionMetadata(t *testing.T, lc *leaderController, sessionId int64) *proto.SessionMetadata {
	t.Helper()

	resp, err := lc.db.Get(&proto.GetRequest{
		Key:          SessionKey(SessionId(sessionId)),
		IncludeValue: true,
	})
	assert.NoError(t, err)

	found := resp.Status == proto.Status_OK
	if !found {
		return nil
	}
	meta := proto.SessionMetadata{}
	err = pb.Unmarshal(resp.Value, &meta)
	assert.NoError(t, err)
	return &meta
}

func createSessionManager(t *testing.T) (kv.Factory, wal.Factory, *sessionManager, *leaderController) {
	t.Helper()

	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewTestWalFactory(t)
	lc, err := NewLeaderController(conf.Config{NotificationsRetentionTime: 10 * time.Second}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	assert.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	sessionManager := lc.(*leaderController).sessionManager.(*sessionManager)
	assert.NoError(t, sessionManager.ctx.Err())
	return kvFactory, walFactory, sessionManager, lc.(*leaderController)
}

func reopenLeaderController(t *testing.T, kvFactory kv.Factory, walFactory wal.Factory, oldlc *leaderController) *leaderController {
	t.Helper()

	var shard int64 = 1

	assert.NoError(t, oldlc.Close())

	var err error
	lc, err := NewLeaderController(conf.Config{}, constant.DefaultNamespace, shard, rpc.NewMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	assert.NoError(t, err)
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	return lc.(*leaderController)
}

func TestIsSessionKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{
			name: "ShouldReturnTrueForAValidKey",
			key:  fmt.Sprintf("__oxia/session/%016x", 12345),
			want: true,
		},
		{
			name: "ShouldReturnFalseForMultipleSlashes",
			key:  "__oxia/session//00000000000030d4",
			want: false,
		},
		{
			name: "ShouldReturnFalseWhenKeyHasTrailingSlash",
			key:  "__oxia/session/00000000000030d4/",
			want: false,
		},
		{
			name: "ShouldReturnFalseWhenMultipleSlashesInPrefix",
			key:  "__oxia//session/00000000000030d4",
			want: false,
		},
		{
			name: "ShouldReturnFalseForNestedKey",
			key:  "__oxia/session/00000000000030d4/nested/extra",
			want: false,
		},
		{
			name: "ShouldReturnFalseForPartiallyCorrectKey",
			key:  "__oxia/session/",
			want: false,
		},
		{
			name: "ShouldReturnFalseForExtraPrefixSegments",
			key:  "__oxia/session/more-path/00000000000030d4",
			want: false,
		},
		{
			name: "ShouldReturnFalseForIncorrectPrefix",
			key:  "__oxia/sessio/00000000000030d4",
			want: false,
		},
		{
			name: "ShouldReturnFalseIfKeyIsTooLong",
			key:  "__oxia/session/0000000000000001extra",
			want: false,
		},
		{
			name: "ShouldReturnFalseIfKeyIsTooShort",
			key:  "__oxia/session/000000000000001",
			want: false,
		},
		{
			name: "ShouldReturnFalseIfKeyIsEmpty",
			key:  "",
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsSessionKey(tc.key)
			if got != tc.want {
				t.Errorf("IsSessionKey(%q) got = %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}
