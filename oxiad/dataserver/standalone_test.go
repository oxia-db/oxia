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
	"math"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/common/vfs"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal/codec"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
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

func TestStandaloneSequenceKeyValidation(t *testing.T) {
	standaloneServer, err := NewStandalone(NewTestConfig(t.TempDir()))
	require.NoError(t, err)
	defer standaloneServer.Close()

	leader, err := standaloneServer.shardsDirector.GetLeader(0)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return leader.IsFeatureEnabled(proto.Feature_FEATURE_SEQUENCE_KEY_VALIDATION)
	}, 10*time.Second, 10*time.Millisecond)

	conn, err := grpc.NewClient(standaloneServer.ServiceAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := proto.NewOxiaClientClient(conn)

	_, err = client.Write(t.Context(), &proto.WriteRequest{
		Shard: pb.Int64(0),
		Puts:  []*proto.PutRequest{{Key: "a", PartitionKey: pb.String("x"), SequenceKeyDelta: []uint64{1, 1}}},
	})
	require.NoError(t, err)

	// The sequence has two parts: the missing delta of the second one is 0. A
	// sequential put without a partition key is invalid.
	response, err := client.Write(t.Context(), &proto.WriteRequest{
		Shard: pb.Int64(0),
		Puts: []*proto.PutRequest{
			{Key: "a", PartitionKey: pb.String("x"), SequenceKeyDelta: []uint64{1}},
			{Key: "b", SequenceKeyDelta: []uint64{1}},
			{Key: "c", Value: []byte("c")},
		},
	})
	require.NoError(t, err)
	require.Len(t, response.GetPuts(), 3)
	assert.Equal(t, proto.Status_OK, response.GetPuts()[0].GetStatus())
	assert.Equal(t, "a-00000000000000000002-00000000000000000001", response.GetPuts()[0].GetKey())
	assert.Equal(t, proto.Status_INVALID_ARGUMENT, response.GetPuts()[1].GetStatus())
	assert.Equal(t, proto.Status_OK, response.GetPuts()[2].GetStatus())
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
	c, exists, err := codec.GetOrCreate(vfs.OSFS{}, segment)
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
	assert.ErrorIs(t, err, errs.ErrCorrupted)
}

func TestNewStandaloneReleasesWhatItStartedOnError(t *testing.T) {
	// NewStandalone fails to bind the public port, after it opened the shards
	publicListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer publicListener.Close()
	publicAddress := publicListener.Addr().String()

	config := NewTestConfig(t.TempDir())
	config.DataServerOptions.Server.Public.BindAddress = publicAddress
	standaloneServer, err := NewStandalone(config)
	require.ErrorContains(t, err, publicAddress)
	assert.Nil(t, standaloneServer)

	// A retry opens the same directories
	config.DataServerOptions.Server.Public.BindAddress = "127.0.0.1:0"
	standaloneServer, err = NewStandalone(config)
	require.NoError(t, err)
	assert.NoError(t, standaloneServer.Close())
}

func TestNewStandaloneFailsWhenDataDirIsFile(t *testing.T) {
	config := NewTestConfig(t.TempDir())
	require.NoError(t, os.WriteFile(config.DataServerOptions.Storage.Database.Dir, nil, 0o644))

	standaloneServer, err := NewStandalone(config)
	assert.Error(t, err)
	assert.Nil(t, standaloneServer)
}

// A standalone server doesn't enable the features of a cluster, e.g. the
// database checksum: it refuses to lead a shard that has them enabled.
func TestNewStandaloneFailsWhenShardHasUnsupportedFeature(t *testing.T) {
	config := NewTestConfig(t.TempDir())
	standaloneServer, err := NewStandalone(config)
	require.NoError(t, err)
	require.NoError(t, standaloneServer.Close())

	kvFactory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{
		DataDir: config.DataServerOptions.Storage.Database.Dir,
	})
	require.NoError(t, err)
	db, err := database.NewDB(constant.DefaultNamespace, 0, kvFactory, config.KeySorting, time.Hour,
		time2.SystemClock)
	require.NoError(t, err)
	commitOffset, err := db.ReadCommitOffset()
	require.NoError(t, err)
	_, err = db.ProcessControlRequest(&proto.ControlRequest{Value: &proto.ControlRequest_FeatureEnable{
		FeatureEnable: &proto.FeatureEnableRequest{Features: []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}},
	}}, commitOffset, 0, database.NoOpCallback)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	require.NoError(t, kvFactory.Close())

	standaloneServer, err = NewStandalone(config)
	assert.ErrorIs(t, err, constant.ErrUnsupportedFeatures)
	assert.Nil(t, standaloneServer)
}

func TestNewStandaloneFailsWhenShardIsAtLastTerm(t *testing.T) {
	config := NewTestConfig(t.TempDir())
	kvFactory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{
		DataDir: config.DataServerOptions.Storage.Database.Dir,
	})
	require.NoError(t, err)
	db, err := database.NewDB(constant.DefaultNamespace, 0, kvFactory, config.KeySorting, time.Hour,
		time2.SystemClock)
	require.NoError(t, err)
	require.NoError(t, db.UpdateTerm(math.MaxInt64, database.TermOptions{}))
	require.NoError(t, db.Close())
	require.NoError(t, kvFactory.Close())

	standaloneServer, err := NewStandalone(config)
	assert.ErrorIs(t, err, constant.ErrInvalidTerm)
	assert.Nil(t, standaloneServer)
}

func TestNewStandaloneFailsWhenTLSCertIsMissing(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	for _, test := range []struct {
		name      string
		configure func(config *StandaloneConfig)
	}{
		{"public", func(config *StandaloneConfig) {
			config.DataServerOptions.Server.Public.TLS.CertFile = missing + ".crt"
			config.DataServerOptions.Server.Public.TLS.KeyFile = missing + ".key"
		}},
		{"metrics", func(config *StandaloneConfig) {
			config.DataServerOptions.Observability.Metric.Enabled = &constant.FlagTrue
			config.DataServerOptions.Observability.Metric.BindAddress = "127.0.0.1:0"
			config.DataServerOptions.Observability.Metric.TLS.CertFile = missing + ".crt"
			config.DataServerOptions.Observability.Metric.TLS.KeyFile = missing + ".key"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := NewTestConfig(t.TempDir())
			test.configure(&config)

			standaloneServer, err := NewStandalone(config)
			assert.ErrorIs(t, err, os.ErrNotExist)
			assert.Nil(t, standaloneServer)

			// The failed start released what it opened
			config = NewTestConfig(filepath.Dir(config.DataServerOptions.Storage.Database.Dir))
			standaloneServer, err = NewStandalone(config)
			require.NoError(t, err)
			assert.NoError(t, standaloneServer.Close())
		})
	}
}

func TestNewStandaloneFailsWhenMetricsPortIsInUse(t *testing.T) {
	metricsListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer metricsListener.Close()
	metricsAddress := metricsListener.Addr().String()

	config := NewTestConfig(t.TempDir())
	config.DataServerOptions.Observability.Metric.Enabled = &constant.FlagTrue
	config.DataServerOptions.Observability.Metric.BindAddress = metricsAddress
	standaloneServer, err := NewStandalone(config)
	assert.ErrorContains(t, err, metricsAddress)
	assert.Nil(t, standaloneServer)

	// The failed start released what it opened, the public server too
	config.DataServerOptions.Observability.Metric.Enabled = &constant.FlagFalse
	standaloneServer, err = NewStandalone(config)
	require.NoError(t, err)
	assert.NoError(t, standaloneServer.Close())
}

// A standalone server has no followers to replicate to.
func TestNoOpReplicationRpcProvider(t *testing.T) {
	provider := newNoOpReplicationRpcProvider()

	assert.Panics(t, func() {
		_, _ = provider.GetReplicateStream(t.Context(), "follower", constant.DefaultNamespace, 0, 1)
	})
	assert.Panics(t, func() {
		_, _ = provider.SendSnapshot(t.Context(), "follower", constant.DefaultNamespace, 0, 1)
	})
	assert.Panics(t, func() { _, _ = provider.Truncate("follower", &proto.TruncateRequest{}) })
	assert.NoError(t, provider.Close())
}
