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

package dataserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"

	"github.com/oxia-db/oxia/common/proto"
)

func TestStandaloneSecondaryIndexNameValidation(t *testing.T) {
	standaloneServer, err := NewStandalone(NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	defer standaloneServer.Close()

	leader, err := standaloneServer.shardsDirector.GetLeader(0)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return leader.IsFeatureEnabled(proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION)
	}, 10*time.Second, 10*time.Millisecond)

	conn, err := grpc.NewClient(standaloneServer.ServiceAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	response, err := proto.NewOxiaClientClient(conn).Write(t.Context(), &proto.WriteRequest{
		Shard: pb.Int64(0),
		Puts: []*proto.PutRequest{{
			Key:   "key",
			Value: []byte("value"),
			SecondaryIndexes: []*proto.SecondaryIndex{{
				IndexName:    "tenant/users",
				SecondaryKey: "email",
			}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, response.GetPuts(), 1)
	assert.Equal(t, proto.Status_INVALID_ARGUMENT, response.GetPuts()[0].GetStatus())
}

// With the same WAL dir and data dir, the WAL segments and the database of a
// shard share one directory, and both must survive a restart.
func TestStandaloneSharedWalAndDataDir(t *testing.T) {
	config := NewTestConfig(t.TempDir())
	config.DataServerOptions.Storage.WAL.Dir = config.DataServerOptions.Storage.Database.Dir
	createSession := func(leader lead.LeaderController) int64 {
		res, err := leader.CreateSession(&proto.CreateSessionRequest{
			Shard:            0,
			SessionTimeoutMs: uint32(time.Minute.Milliseconds()),
			ClientIdentity:   "client",
		})
		require.NoError(t, err)
		return res.SessionId
	}

	standaloneServer, err := NewStandalone(config)
	require.NoError(t, err)
	leader, err := standaloneServer.shardsDirector.GetLeader(0)
	require.NoError(t, err)
	_, err = leader.WriteBlock(t.Context(), &proto.WriteRequest{
		Shard: pb.Int64(0),
		Puts:  []*proto.PutRequest{{Key: "key", Value: []byte("value")}},
	})
	require.NoError(t, err)
	sessionId := createSession(leader)
	require.NoError(t, standaloneServer.Close())

	standaloneServer, err = NewStandalone(config)
	require.NoError(t, err)
	defer standaloneServer.Close()
	leader, err = standaloneServer.shardsDirector.GetLeader(0)
	require.NoError(t, err)

	// The WAL must still hold the entries that the database applied
	status, err := leader.GetStatus(&proto.GetStatusRequest{Shard: 0})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, status.CommitOffset, sessionId)
	assert.Equal(t, status.CommitOffset, status.HeadOffset)

	// and new entries must not reuse their offsets
	assert.Greater(t, createSession(leader), sessionId)
}
