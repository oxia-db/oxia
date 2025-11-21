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

package dataserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/dataserver/kv"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/compare"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/channel"

	oentity "github.com/oxia-db/oxia/common/entity"

	"github.com/oxia-db/oxia/proto"
)

func AssertProtoEqual(t *testing.T, expected, actual pb.Message) {
	t.Helper()

	if !pb.Equal(expected, actual) {
		protoMarshal := protojson.MarshalOptions{
			EmitUnpopulated: true,
		}
		expectedJSON, _ := protoMarshal.Marshal(expected)
		actualJSON, _ := protoMarshal.Marshal(actual)
		assert.Equal(t, string(expectedJSON), string(actualJSON))
	}
}

func TestLeaderController_NotInitialized(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	res, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.Nil(t, res)
	assert.Equal(t, constant.CodeInvalidStatus, status.Code(err))

	responses := make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: "a"}},
	}, concurrent.ReadFromStreamCallback(responses))

	_, err = channel.ReadAll[*proto.GetResponse](context.Background(), responses)
	assert.Equal(t, constant.CodeInvalidStatus, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_Closed(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	assert.NoError(t, lc.Close())

	res, err := lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  2,
	})

	assert.Nil(t, res)
	assert.Equal(t, constant.CodeAlreadyClosed, status.Code(err))

	res2, err := lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:        shard,
		Term:         2,
		FollowerMaps: map[string]*proto.EntryId{},
	})

	assert.Nil(t, res2)
	assert.Equal(t, constant.CodeAlreadyClosed, status.Code(err))

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_NoFencing(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())
	resp, err := lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, constant.CodeInvalidStatus, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_RF1(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	fr, err := lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  1,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, InvalidEntryId, fr.HeadEntryId)

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 1, lc.Term())
	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	// WriteBlock entry
	res, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res.Puts))
	assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
	assert.EqualValues(t, 0, res.Puts[0].Version.VersionId)
	assert.NotEqualValues(t, 0, res.Puts[0].Version.CreatedTimestamp)
	assert.NotEqualValues(t, 0, res.Puts[0].Version.ModifiedTimestamp)
	assert.EqualValues(t, res.Puts[0].Version.CreatedTimestamp, res.Puts[0].Version.ModifiedTimestamp)

	responses := make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: "a", IncludeValue: true}},
	}, concurrent.ReadFromStreamCallback(responses))

	results, err := channel.ReadAll[*proto.GetResponse](context.Background(), responses)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, proto.Status_OK, results[0].Status)
	assert.Equal(t, []byte("value-a"), results[0].Value)
	assert.EqualValues(t, 0, res.Puts[0].Version.VersionId)

	// Set NewTerm to leader

	fr2, err := lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  2,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Term: 1, Offset: 0}, fr2.HeadEntryId)

	assert.EqualValues(t, 2, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Should not accept anymore writes & reads

	res3, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.Nil(t, res3)
	assert.Equal(t, constant.CodeInvalidStatus, status.Code(err))

	responses = make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: "a"}},
	}, concurrent.ReadFromStreamCallback(responses))

	_, err = channel.ReadAll[*proto.GetResponse](context.Background(), responses)
	assert.Equal(t, constant.CodeInvalidStatus, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeader_RF2(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	rpc := newMockRpcClient()

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, rpc, walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	fr, err := lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  1,
	})
	assert.NoError(t, err)
	assert.Equal(t, InvalidEntryId, fr.HeadEntryId)

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 2,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 1, lc.Term())
	assert.Equal(t, proto.ServingStatus_LEADER, lc.Status())

	go func() {
		req := <-rpc.appendReqs

		rpc.ackResps <- &proto.Ack{
			Offset: req.Entry.Offset,
		}
	}()

	// WriteBlock entry
	res, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res.Puts))
	assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
	assert.EqualValues(t, 0, res.Puts[0].Version.VersionId)

	responses := make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: "a", IncludeValue: true}},
	}, concurrent.ReadFromStreamCallback(responses))

	results, err := channel.ReadAll[*proto.GetResponse](context.Background(), responses) // Read entry

	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, proto.Status_OK, results[0].Status)
	assert.Equal(t, []byte("value-a"), results[0].Value)
	assert.EqualValues(t, 0, res.Puts[0].Version.VersionId)

	// Set NewTerm to leader

	fr2, err := lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  2,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Term: 1, Offset: 0}, fr2.HeadEntryId)

	assert.EqualValues(t, 2, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Should not accept anymore writes & reads

	res3, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	assert.Nil(t, res3)
	assert.Equal(t, constant.CodeInvalidStatus, status.Code(err))

	responses = make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: "a"}},
	}, concurrent.ReadFromStreamCallback(responses))

	_, err = channel.ReadAll[*proto.GetResponse](context.Background(), responses)
	assert.Equal(t, constant.CodeInvalidStatus, status.Code(err))

	close(rpc.ackResps)
	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_TermPersistent(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, wal.InvalidTerm, lc.Term())
	assert.Equal(t, proto.ServingStatus_NOT_MEMBER, lc.Status())

	// Set NewTerm to leader

	fr2, err := lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  5,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Term: wal.InvalidTerm, Offset: wal.InvalidOffset}, fr2.HeadEntryId)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	assert.NoError(t, lc.Close())

	// Re-open lead controller
	lc, err = NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())
	assert.NoError(t, lc.Close())

	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_FenceTerm(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	db, err := kv.NewDB(constant.DefaultNamespace, shard, kvFactory, compare.EncoderHierarchical, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)
	// Force a new term in the DB before opening
	assert.NoError(t, db.UpdateTerm(5, kv.TermOptions{}))
	assert.NoError(t, db.Close())

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Smaller term will fail
	fr, err := lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  4,
	})
	assert.Nil(t, fr)
	assert.Equal(t, constant.CodeInvalidTerm, status.Code(err))
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Same term will succeed
	fr, err = lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  5,
	})
	assert.NoError(t, err)
	assert.NotNil(t, fr)
	AssertProtoEqual(t, InvalidEntryId, fr.HeadEntryId)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_BecomeLeaderTerm(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	db, err := kv.NewDB(constant.DefaultNamespace, shard, kvFactory, compare.EncoderHierarchical, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)
	// Force a new term in the DB before opening
	assert.NoError(t, db.UpdateTerm(5, kv.TermOptions{}))
	assert.NoError(t, db.Close())

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	// Smaller term will fail
	resp, err := lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              4,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, constant.CodeInvalidTerm, status.Code(err))

	// Higher term will fail
	resp, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              6,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.Nil(t, resp)
	assert.Equal(t, constant.CodeInvalidTerm, status.Code(err))

	// Same term will succeed
	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              5,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_AddFollower(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.NewTerm(&proto.NewTermRequest{
		Term:  5,
		Shard: shard,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              5,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	// f1 is already connected
	afRes, err := lc.AddFollower(&proto.AddFollowerRequest{
		Shard:               shard,
		Term:                5,
		FollowerName:        "f1",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Equal(t, &proto.AddFollowerResponse{}, afRes)
	assert.Nil(t, err)

	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		Shard:               shard,
		Term:                5,
		FollowerName:        "f2",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.NoError(t, err)

	// We have already 2 followers and with replication-factor=3
	// it's not possible to add any more followers
	afRes, err = lc.AddFollower(&proto.AddFollowerRequest{
		Shard:               shard,
		Term:                5,
		FollowerName:        "f3",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Error(t, err)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_AddFollowerRepeated(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.NewTerm(&proto.NewTermRequest{
		Term:  5,
		Shard: shard,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 5, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              5,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	// f1 is already connected
	afRes, err := lc.AddFollower(&proto.AddFollowerRequest{
		Shard:               shard,
		Term:                5,
		FollowerName:        "f1",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Nil(t, err)
	assert.Equal(t, &proto.AddFollowerResponse{}, afRes)

	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		Shard:               shard,
		Term:                5,
		FollowerName:        "f1",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Nil(t, err)
	assert.Equal(t, &proto.AddFollowerResponse{}, afRes)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// When a follower is added after the initial leader election,
// the leader should use the head-entry at the time of the
// election instead of the current head-entry.
// Also, it should never ask to truncate to an offset higher
// than what the follower has.
func TestLeaderController_AddFollower_Truncate(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{BaseWalDir: t.TempDir()})

	// Prepare some data in the leader log & db
	walObject, err := walFactory.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	db, err := kv.NewDB(constant.DefaultNamespace, shard, kvFactory, compare.EncoderHierarchical, 1*time.Hour, time2.SystemClock)
	assert.NoError(t, err)

	for i := int64(0); i < 10; i++ {
		wr := &proto.WriteRequest{Puts: []*proto.PutRequest{{
			Key:   "my-key",
			Value: []byte(""),
		}}}
		value, err := pb.Marshal(wrapInLogEntryValue(wr))
		assert.NoError(t, err)

		assert.NoError(t, walObject.Append(&proto.LogEntry{
			Term:   5,
			Offset: i,
			Value:  value,
		}))

		_, err = db.ProcessWrite(wr, i-1, 0, kv.NoOpCallback)
		assert.NoError(t, err)
	}

	assert.NoError(t, db.UpdateTerm(5, kv.TermOptions{}))
	assert.NoError(t, db.Close())
	assert.NoError(t, walObject.Close())

	rpcClient := newMockRpcClient()

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, rpcClient, walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.NewTerm(&proto.NewTermRequest{
		Term:  6,
		Shard: shard,
	})
	assert.NoError(t, err)

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              6,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": {Term: 5, Offset: 9},
		},
	})
	assert.NoError(t, err)

	// Add some entries in term 6 that will be only in the
	// leader and f1
	go func() {
		for i := 0; i < 10; i++ {
			req := <-rpcClient.appendReqs

			rpcClient.ackResps <- &proto.Ack{
				Offset: req.Entry.Offset,
			}
		}
	}()

	// WriteBlock entries
	for i := 10; i < 20; i++ {
		res, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
			Shard: &shard,
			Puts: []*proto.PutRequest{{
				Key:   "my-key",
				Value: []byte("")}},
		})

		assert.NoError(t, err)
		assert.EqualValues(t, 1, len(res.Puts))
		assert.Equal(t, proto.Status_OK, res.Puts[0].Status)
	}

	rpcClient.truncateResps <- struct {
		*proto.TruncateResponse
		error
	}{&proto.TruncateResponse{HeadEntryId: &proto.EntryId{Term: 5, Offset: 9}}, nil}

	// Adding a follower that needs to be truncated
	_, err = lc.AddFollower(&proto.AddFollowerRequest{
		Shard:        shard,
		Term:         6,
		FollowerName: "f2",
		FollowerHeadEntryId: &proto.EntryId{
			Term:   5,
			Offset: 12,
		},
	})
	assert.NoError(t, err)

	trReq := <-rpcClient.truncateReqs
	assert.Equal(t, constant.DefaultNamespace, trReq.Namespace)
	assert.EqualValues(t, 6, trReq.Term)
	AssertProtoEqual(t, &proto.EntryId{Term: 5, Offset: 9}, trReq.HeadEntryId)
	assert.Equal(t, shard, trReq.Shard)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_AddFollowerCheckTerm(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	assert.NoError(t, err)

	_, err = lc.NewTerm(&proto.NewTermRequest{
		Term:  5,
		Shard: shard,
	})
	assert.NoError(t, err)

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              5,
		ReplicationFactor: 3,
		FollowerMaps: map[string]*proto.EntryId{
			"f1": InvalidEntryId,
		},
	})
	assert.NoError(t, err)

	afRes, err := lc.AddFollower(&proto.AddFollowerRequest{
		Shard:               shard,
		Term:                4,
		FollowerName:        "f2",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Equal(t, constant.CodeInvalidTerm, status.Code(err))

	afRes, err = lc.AddFollower(&proto.AddFollowerRequest{
		Shard:               shard,
		Term:                6,
		FollowerName:        "f2",
		FollowerHeadEntryId: InvalidEntryId,
	})
	assert.Nil(t, afRes)
	assert.Equal(t, constant.CodeInvalidTerm, status.Code(err))

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

// When a leader starts, before we can start to serve write/read requests, we need to ensure
// that all the entries that are in the leader wal are fully committed and applied into the db.
// Otherwise, we could have the scenario where entries were already acked to a client though
// are not appearing when doing a subsequent read if the leader has changed.
func TestLeaderController_EntryVisibilityAfterBecomingLeader(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir: t.TempDir(),
	})

	walObject, err := walFactory.NewWal(constant.DefaultNamespace, shard, nil)
	assert.NoError(t, err)
	v, err := pb.Marshal(wrapInLogEntryValue(&proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "my-key",
			Value: []byte("my-value"),
		}},
	}))
	assert.NoError(t, err)
	assert.NoError(t, walObject.Append(&proto.LogEntry{
		Term:   0,
		Offset: 0,
		Value:  v,
	}))

	rpc := newMockRpcClient()

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, rpc, walFactory, kvFactory)

	_, _ = lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  1,
	})

	// Respond to replication flow to follower
	go func() {
		req := <-rpc.appendReqs

		rpc.ackResps <- &proto.Ack{
			Offset: req.Entry.Offset,
		}
	}()

	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 2,
		FollowerMaps: map[string]*proto.EntryId{
			// The follower does not have the entry in its wal yet
			"f1": {Term: 0, Offset: -1},
		},
	})

	// We should be able to read the entry, even if it was not fully committed before the leader started
	responses := make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: "my-key", IncludeValue: true}},
	}, concurrent.ReadFromStreamCallback(responses))

	results, err := channel.ReadAll[*proto.GetResponse](context.Background(), responses)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, proto.Status_OK, results[0].Status)
	assert.Equal(t, []byte("my-value"), results[0].Value)
	assert.EqualValues(t, 0, results[0].Version.VersionId)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_Notifications(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	ctx, cancel := context.WithCancel(context.Background())

	adaptor := concurrent.NewStreamCallbackAdaptor[*proto.NotificationBatch]()
	lc.GetNotifications(ctx, &proto.NotificationsRequest{Shard: shard, StartOffsetExclusive: &wal.InvalidOffset}, adaptor)

	// WriteBlock entry
	_, _ = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})

	nb1 := <-adaptor.Ch()
	assert.EqualValues(t, 0, nb1.Offset)
	assert.Equal(t, 1, len(nb1.Notifications))
	n1 := nb1.Notifications["a"]
	assert.Equal(t, proto.NotificationType_KEY_CREATED, n1.Type)
	assert.EqualValues(t, 0, *n1.VersionId)

	// The handler is still running waiting for more notifications
	time.Sleep(1 * time.Second)
	assert.False(t, adaptor.IsCompleted())

	// Cancelling the stream context should close the `GetNotification()` handler
	cancel()

	assert.Eventually(t, func() bool {
		return adaptor.IsCompleted()
	}, 10*time.Second, 100*time.Millisecond)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_NotificationsCloseLeader(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	ctx, cancel := context.WithCancel(context.Background())
	adaptor := concurrent.NewStreamCallbackAdaptor[*proto.NotificationBatch]()
	lc.GetNotifications(ctx, &proto.NotificationsRequest{Shard: shard, StartOffsetExclusive: &wal.InvalidOffset}, adaptor)

	// The handler is still running waiting for more notifications
	time.Sleep(1 * time.Second)

	assert.False(t, adaptor.IsCompleted())

	// Closing the leader should close the `GetNotification()` handler
	assert.NoError(t, lc.Close())

	time.Sleep(1 * time.Second)

	assert.True(t, adaptor.IsCompleted())

	cancel()
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_NotificationsWhenNotReady(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adaptor := concurrent.NewStreamCallbackAdaptor[*proto.NotificationBatch]()
	lc.GetNotifications(ctx, &proto.NotificationsRequest{Shard: shard, StartOffsetExclusive: &wal.InvalidOffset}, adaptor)
	assert.Equal(t, status.Code(adaptor.Error()), constant.CodeInvalidStatus)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_List(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte{0}},
			{Key: "/b", Value: []byte{0}},
			{Key: "/c", Value: []byte{0}},
			{Key: "/d", Value: []byte{0}},
		},
	})
	assert.NoError(t, err)

	list, err := lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:          &shard,
		StartInclusive: "/a",
		EndExclusive:   "/c",
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"/a", "/b"}, list)

	list, err = lc.ListBlock(context.Background(), &proto.ListRequest{
		Shard:          &shard,
		StartInclusive: "/y",
		EndExclusive:   "/z",
	})
	assert.NoError(t, err)
	assert.Len(t, list, 0)
}

func TestLeaderController_RangeScan(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "/a", Value: []byte{0}},
			{Key: "/b", Value: []byte{0}},
			{Key: "/c", Value: []byte{0}},
			{Key: "/d", Value: []byte{0}},
		},
	})
	assert.NoError(t, err)

	ch := make(chan *oentity.TWithError[*proto.GetResponse], 100)
	lc.RangeScan(context.Background(), &proto.RangeScanRequest{
		Shard:          &shard,
		StartInclusive: "/a",
		EndExclusive:   "/c",
	}, concurrent.ReadFromStreamCallback(ch))
	entity, more := <-ch
	assert.Nil(t, entity.Err)
	assert.Equal(t, "/a", *entity.T.Key)
	assert.True(t, more)
	entity, more = <-ch
	assert.Nil(t, entity.Err)
	assert.Equal(t, "/b", *entity.T.Key)
	assert.True(t, more)
	entity, more = <-ch
	assert.Nil(t, entity)
	assert.False(t, more)

	ch = make(chan *oentity.TWithError[*proto.GetResponse], 100)
	lc.RangeScan(context.Background(), &proto.RangeScanRequest{
		Shard:          &shard,
		StartInclusive: "/y",
		EndExclusive:   "/z",
	}, concurrent.ReadFromStreamCallback(ch))
	entity, more = <-ch
	assert.Nil(t, entity)
	assert.False(t, more)
}

func TestLeaderController_DeleteShard(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: "k1", Value: []byte("hello")}},
	})
	assert.NoError(t, err)

	_, err = lc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: constant.DefaultNamespace,
		Shard:     shard,
		Term:      1,
	})
	assert.NoError(t, err)

	assert.NoError(t, lc.Close())

	lc, _ = NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              2,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	responses := make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: "a", IncludeValue: true}},
	}, concurrent.ReadFromStreamCallback(responses))

	results, err := channel.ReadAll[*proto.GetResponse](context.Background(), responses) // Read entry
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, results[0].Status)
	assert.Nil(t, results[0].Value)

	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_DeleteShard_WrongTerm(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts:  []*proto.PutRequest{{Key: "k1", Value: []byte("hello")}},
	})
	assert.NoError(t, err)

	_, err = lc.DeleteShard(&proto.DeleteShardRequest{
		Namespace: constant.DefaultNamespace,
		Shard:     shard,
		Term:      0,
	})
	assert.ErrorIs(t, err, constant.ErrInvalidTerm)

	assert.NoError(t, lc.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_GetStatus(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              2,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	// WriteBlock entry
	_, _ = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})
	_, _ = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "b",
			Value: []byte("value-b")}},
	})

	res, err := lc.GetStatus(&proto.GetStatusRequest{Shard: shard})
	assert.NoError(t, err)
	assert.Equal(t, &proto.GetStatusResponse{
		Term:         2,
		Status:       proto.ServingStatus_LEADER,
		HeadOffset:   1,
		CommitOffset: 1,
	}, res)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_Write(t *testing.T) {
	var shard int64 = 1

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
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

	// Write entry
	res1, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "a",
			Value: []byte("value-a")}},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res1.Puts))
	assert.Equal(t, proto.Status_OK, res1.Puts[0].Status)
	assert.EqualValues(t, 0, res1.Puts[0].Version.VersionId)
	assert.NotEqualValues(t, 0, res1.Puts[0].Version.CreatedTimestamp)
	assert.NotEqualValues(t, 0, res1.Puts[0].Version.ModifiedTimestamp)
	assert.EqualValues(t, res1.Puts[0].Version.CreatedTimestamp, res1.Puts[0].Version.ModifiedTimestamp)

	res2, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:   "b",
			Value: []byte("value-b")}},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(res2.Puts))
	assert.Equal(t, proto.Status_OK, res2.Puts[0].Status)
	assert.EqualValues(t, 1, res2.Puts[0].Version.VersionId)
	assert.NotEqualValues(t, 0, res2.Puts[0].Version.CreatedTimestamp)
	assert.NotEqualValues(t, 0, res2.Puts[0].Version.ModifiedTimestamp)
	assert.EqualValues(t, res2.Puts[0].Version.CreatedTimestamp, res2.Puts[0].Version.ModifiedTimestamp)

	responses := make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: "a", IncludeValue: true}},
	}, concurrent.ReadFromStreamCallback(responses))

	results, err := channel.ReadAll[*proto.GetResponse](context.Background(), responses) // Read entry a
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, proto.Status_OK, results[0].Status)
	assert.Equal(t, []byte("value-a"), results[0].Value)
	assert.EqualValues(t, 0, res1.Puts[0].Version.VersionId)

	responses = make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: "b", IncludeValue: true}},
	}, concurrent.ReadFromStreamCallback(responses))

	results, err = channel.ReadAll[*proto.GetResponse](context.Background(), responses) // Read entry a
	assert.NoError(t, err)
	assert.Equal(t, proto.Status_OK, results[0].Status)
	assert.Equal(t, []byte("value-b"), results[0].Value)
	assert.EqualValues(t, 0, res1.Puts[0].Version.VersionId)

	// Set NewTerm to leader

	fr2, err := lc.NewTerm(&proto.NewTermRequest{
		Shard: shard,
		Term:  2,
	})
	assert.NoError(t, err)
	AssertProtoEqual(t, &proto.EntryId{Term: 1, Offset: 1}, fr2.HeadEntryId)

	assert.EqualValues(t, 2, lc.Term())
	assert.Equal(t, proto.ServingStatus_FENCED, lc.Status())

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_NotificationsDisabled(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1, Options: &proto.NewTermOptions{EnableNotifications: false}})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adaptor := concurrent.NewStreamCallbackAdaptor[*proto.NotificationBatch]()
	lc.GetNotifications(ctx, &proto.NotificationsRequest{Shard: shard, StartOffsetExclusive: &wal.InvalidOffset}, adaptor)
	assert.ErrorIs(t, adaptor.Error(), constant.ErrNotificationsNotEnabled)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_DuplicateNewTerm_WithSession(t *testing.T) {
	var shard int64 = 2

	kvFactory, err := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	assert.NoError(t, err)
	walFactory := newTestWalFactory(t)

	lc, err := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
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

	csResult, err := lc.CreateSession(&proto.CreateSessionRequest{
		Shard:            shard,
		SessionTimeoutMs: 5_000,
		ClientIdentity:   "my-identity",
	})
	assert.NoError(t, err)

	sessionId := csResult.SessionId
	invalidSessionId := int64(5)

	key := "/namespace/sn/system/0xe0000000_0xf0000000"

	// WriteBlock entry
	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{{
			Key:       key,
			Value:     []byte("value-a"),
			SessionId: &invalidSessionId,
		}},
	})
	assert.NoError(t, err)

	// Start a new term on the same leader
	_, err = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 2})
	assert.NoError(t, err)

	_, err = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              2,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})
	assert.NoError(t, err)

	_, err = lc.CloseSession(&proto.CloseSessionRequest{
		Shard:     shard,
		SessionId: sessionId,
	})
	assert.NoError(t, err)

	responses := make(chan *oentity.TWithError[*proto.GetResponse], 1000)
	lc.Read(context.Background(), &proto.ReadRequest{
		Shard: &shard,
		Gets:  []*proto.GetRequest{{Key: key}},
	}, concurrent.ReadFromStreamCallback(responses))

	results, err := channel.ReadAll[*proto.GetResponse](context.Background(), responses) // Read entry
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, proto.Status_KEY_NOT_FOUND, results[0].Status)

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_GetSequenceUpdates(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	walFactory := newTestWalFactory(t)

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "a", Value: []byte{0}, SequenceKeyDelta: []uint64{1}, PartitionKey: pb.String("x")},
			{Key: "a", Value: []byte{0}, SequenceKeyDelta: []uint64{2}, PartitionKey: pb.String("x")},
			{Key: "a", Value: []byte{0}, SequenceKeyDelta: []uint64{3}, PartitionKey: pb.String("x")},
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	waiter, err := lc.GetSequenceUpdates(ctx, &proto.GetSequenceUpdatesRequest{
		Shard: shard,
		Key:   "a",
	})
	assert.NoError(t, err)

	u1 := <-waiter.Ch()
	assert.Equal(t, fmt.Sprintf("a-%020d", 6), u1)

	_, err = lc.WriteBlock(context.Background(), &proto.WriteRequest{
		Shard: &shard,
		Puts: []*proto.PutRequest{
			{Key: "a", Value: []byte{0}, SequenceKeyDelta: []uint64{5}, PartitionKey: pb.String("x")},
		},
	})
	assert.NoError(t, err)

	u2 := <-waiter.Ch()
	assert.Equal(t, fmt.Sprintf("a-%020d", 11), u2)

	assert.Empty(t, waiter.Ch())

	// When the context is canceled the method should return
	cancel()
	assert.NoError(t, waiter.Close())

	assert.NoError(t, lc.Close())
	assert.NoError(t, kvFactory.Close())
	assert.NoError(t, walFactory.Close())
}

func TestLeaderController_ConcurrentWrite(t *testing.T) {
	var shard int64 = 1

	kvFactory, _ := kv.NewPebbleKVFactory(kv.NewFactoryOptionsForTest(t))
	defer kvFactory.Close()
	walFactory := newTestWalFactory(t)
	defer walFactory.Close()

	lc, _ := NewLeaderController(Config{}, constant.DefaultNamespace, shard, newMockRpcClient(), walFactory, kvFactory)
	defer lc.Close()
	_, _ = lc.NewTerm(&proto.NewTermRequest{Shard: shard, Term: 1})
	_, _ = lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
		Shard:             shard,
		Term:              1,
		ReplicationFactor: 1,
		FollowerMaps:      nil,
	})

	errors := make(chan error, 10)

	for i := 0; i < 10; i++ {
		finalIndex := i
		go func() {
			_, err := lc.WriteBlock(context.Background(), &proto.WriteRequest{
				Shard: &shard,
				Puts: []*proto.PutRequest{
					{Key: fmt.Sprintf("%d", finalIndex), Value: []byte{0}},
				},
			})
			errors <- err
		}()
	}

	for i := 0; i < 10; i++ {
		assert.NoError(t, <-errors)
	}
}
