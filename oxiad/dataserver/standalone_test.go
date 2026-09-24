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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"

	"github.com/oxia-db/oxia/common/constant"
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

func TestStandaloneRejectsSameWalAndDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	config := NewTestConfig(t.TempDir())
	config.DataServerOptions.Storage.WAL.Dir = dir
	config.DataServerOptions.Storage.Database.Dir = dir

	standaloneServer, err := NewStandalone(config)
	assert.ErrorContains(t, err, "are the same directory")
	assert.Nil(t, standaloneServer)

	// Refused before writing anything
	_, err = os.Stat(dir)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

// A standalone server must refuse to start when a WAL entry already applied to
// the database is corrupted: discarding it would make the leader reuse the
// offsets of committed entries.
func TestStandaloneCorruptedCommittedWalEntry(t *testing.T) {
	dir := t.TempDir()
	standaloneServer, err := NewStandalone(NewTestConfig(dir))
	require.NoError(t, err)

	leader, err := standaloneServer.shardsDirector.GetLeader(0)
	require.NoError(t, err)
	_, err = leader.WriteBlock(t.Context(), &proto.WriteRequest{
		Shard: pb.Int64(0),
		Puts:  []*proto.PutRequest{{Key: "key", Value: []byte("value")}},
	})
	require.NoError(t, err)
	require.NoError(t, standaloneServer.Close())

	// Flip a payload byte of the first entry of the WAL
	segment := filepath.Join(dir, "wal", constant.DefaultNamespace, "shard-0", "0")
	c, exists, err := codec.GetOrCreate(segment)
	require.NoError(t, err)
	require.True(t, exists)
	f, err := os.OpenFile(segment+c.GetTxnExtension(), os.O_RDWR, 0)
	require.NoError(t, err)
	b := make([]byte, 1)
	_, err = f.ReadAt(b, int64(c.GetHeaderSize()))
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b, int64(c.GetHeaderSize()))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = NewStandalone(NewTestConfig(dir))
	assert.ErrorIs(t, err, codec.ErrDataCorrupted)
}
