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

package election

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"golang.org/x/exp/maps"
	gproto "google.golang.org/protobuf/proto"

	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/common/sharding"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	leaderselector "github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector/leader"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/controller"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
)

// The election returns the head entries of the candidates and of the data
// server removed by the pending change, and the features enabled on them.
func TestElectionFenceQuorumReturnsHeadEntries(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	removed := testDataServer("removed")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	rpc.GetNode(s1).NewTermResponseWithFeatures(1, 100, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, nil)
	rpc.GetNode(s2).NewTermResponse(1, 95, nil)
	rpc.GetNode(s3).NewTermResponse(0, 0, errors.New("unreachable"))
	rpc.GetNode(removed).NewTermResponse(1, 101, nil)

	candidates, retainedHeads, features, err := e.fenceQuorum(2, &proto.NewTermOptions{},
		[]*proto.DataServerIdentity{s1, s2, s3}, &proto.EnsembleChangeMetadata{From: removed, To: s3})
	require.NoError(t, err)
	assert.Len(t, candidates, 2)
	assert.EqualValues(t, 100, candidates[s1].GetOffset())
	assert.EqualValues(t, 95, candidates[s2].GetOffset())
	assert.Len(t, retainedHeads, 1)
	assert.EqualValues(t, 101, retainedHeads[removed].GetOffset())
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, features)
}

func TestElectionFenceQuorumFailsWithoutQuorum(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(0, 0, errors.New("unreachable"))
	rpc.GetNode(s3).NewTermResponse(0, 0, errors.New("unreachable"))

	_, _, _, err := e.fenceQuorum(2, &proto.NewTermOptions{}, []*proto.DataServerIdentity{s1, s2, s3}, nil)
	assert.ErrorContains(t, err, "quorum not reached")
	assert.ErrorContains(t, err, "unreachable")
}

// A majority of the fenced data servers that holds a single member of the
// ensemble isn't enough: the election waits for another member.
func TestElectionFenceQuorumWaitsForEnsembleMajority(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	removed := testDataServer("removed")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(removed).NewTermResponse(1, 0, nil)
	go func() {
		rpc.GetNode(s2).ExpectNewTermRequest(t, 5, 2, false)
		time.Sleep(2 * quorumFencingGracePeriod)
		rpc.GetNode(s2).NewTermResponse(1, 0, nil)
	}()

	candidates, _, _, err := e.fenceQuorum(2, &proto.NewTermOptions{}, []*proto.DataServerIdentity{s1, s2},
		&proto.EnsembleChangeMetadata{From: removed, To: s2})
	require.NoError(t, err)
	assert.Len(t, candidates, 2)
}

// Once a quorum is fenced, the election waits for the others during a grace
// period, and goes on without the ones that don't respond in time.
func TestElectionFenceQuorumWaitsForGracePeriod(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")
	s5 := testDataServer("s5")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	removed := testDataServer("removed")
	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, 0, nil)
	rpc.GetNode(s3).NewTermResponse(1, 0, nil)
	rpc.GetNode(s4).NewTermResponse(1, 0, nil)
	// The removed data server answers during the grace period, with a feature
	// enabled on the shard
	go func() {
		rpc.GetNode(removed).ExpectNewTermRequest(t, 5, 2, false)
		time.Sleep(quorumFencingGracePeriod / 4)
		rpc.GetNode(removed).NewTermResponseWithFeatures(1, 0, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, nil)
	}()

	start := time.Now()
	candidates, retainedHeads, features, err := e.fenceQuorum(2, &proto.NewTermOptions{},
		[]*proto.DataServerIdentity{s1, s2, s3, s4, s5}, &proto.EnsembleChangeMetadata{From: removed, To: s5})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), quorumFencingGracePeriod)
	assert.Len(t, candidates, 4)
	assert.NotContains(t, candidates, s5)
	assert.Contains(t, retainedHeads, removed)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, features)
}

// A follower that is not added by the leader yet, behind it, or unreachable,
// is asked again until it catches up.
func TestElectionEnsureEnsembleCaughtUpRetriesUntilCaughtUp(t *testing.T) {
	var shard int64 = 5
	rpc := rpctest.NewRpcProvider()
	leader := testDataServer("leader")
	follower := testDataServer("follower")
	e := newTestElection(t, nil, rpc, shard, &proto.ShardMetadata{})

	rpc.GetNode(follower).EnqueueGetStatusError(constant.ErrNodeIsNotMember)
	rpc.GetNode(follower).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 9, 9)
	rpc.GetNode(follower).EnqueueGetStatusError(errors.New("unreachable"))
	// The head entry may be of a diverging log: only the committed entries count
	rpc.GetNode(follower).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 10, 9)
	rpc.GetNode(follower).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 10, 10)

	require.NoError(t, e.ensureEnsembleCaughtUp([]*proto.DataServerIdentity{leader, follower}, leader,
		&proto.EntryId{Term: 2, Offset: 10}))
	assert.True(t, e.IsQuorumCaughtUp())
	assert.True(t, e.IsEnsembleCaughtUp())

	for range 5 {
		rpc.GetNode(follower).ExpectGetStatusRequest(t, shard)
	}
}

// A majority of the ensemble, the leader included, catches up the quorum,
// which a dead follower doesn't block; the wait goes on for the dead follower
// until the election is canceled, and the ensemble is never caught up.
func TestElectionEnsureEnsembleCaughtUpWaitsForEveryFollower(t *testing.T) {
	var shard int64 = 5
	rpc := rpctest.NewRpcProvider()
	leader := testDataServer("leader")
	follower := testDataServer("follower")
	dead := testDataServer("dead")
	e := newTestElection(t, nil, rpc, shard, &proto.ShardMetadata{})

	rpc.GetNode(follower).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 10, 10)
	result := make(chan error, 1)
	go func() {
		result <- e.ensureEnsembleCaughtUp([]*proto.DataServerIdentity{leader, follower, dead}, leader,
			&proto.EntryId{Term: 2, Offset: 10})
	}()

	rpc.GetNode(follower).ExpectGetStatusRequest(t, shard)
	assert.Eventually(t, e.IsQuorumCaughtUp, 10*time.Second, 10*time.Millisecond)
	rpc.GetNode(dead).ExpectGetStatusRequest(t, shard)
	assert.Empty(t, result)
	e.Cancel()
	assert.ErrorIs(t, <-result, context.Canceled)
	assert.False(t, e.IsEnsembleCaughtUp())
}

// The addresses of the ensemble are loaded before persisting a new term: a
// failed load is retried without using up a term.
func TestElectionStartReturnsMetadataLoadErrorBeforeNewTerm(t *testing.T) {
	var shard int64 = 5
	s1 := testDataServer("s1")
	for _, test := range []struct {
		name  string
		hooks func(m *hookedMetadata)
	}{
		{name: "term options", hooks: func(m *hookedMetadata) {
			m.getNamespaceTermOptions = func(string) (*proto.NewTermOptions, error) {
				return nil, errors.New("config unavailable")
			}
		}},
		{name: "data server", hooks: func(m *hookedMetadata) {
			m.getDataServer = func(string) (commonobject.Borrowed[*proto.DataServer], bool, error) {
				return commonobject.Borrowed[*proto.DataServer]{}, false, errors.New("config unavailable")
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
				&proto.ClusterConfiguration{})
			shardMeta := &proto.ShardMetadata{Status: proto.ShardStatusSteadyState, Term: 2, Leader: s1,
				Ensemble: []*proto.DataServerIdentity{s1}}
			storeTestShardMetadata(t, base, constant.DefaultNamespace, shard, namespaceConfig, shardMeta)
			metadata := &hookedMetadata{Metadata: base}
			test.hooks(metadata)
			rpc := rpctest.NewRpcProvider()
			e := newTestElection(t, metadata, rpc, shard, shardMeta)

			_, err := e.Start()
			assert.ErrorContains(t, err, "config unavailable")
			assert.EqualValues(t, 2, shardTerm(base, constant.DefaultNamespace, shard))
			assert.Empty(t, rpc.GetNode(s1).NewTermRequests)
		})
	}
}

// The election fences the ensemble at the addresses the data servers are
// registered with.
func TestElectionRefreshEnsembleInfoUsesRegisteredAddresses(t *testing.T) {
	serverName := "s1"
	oldIdentity := &proto.DataServerIdentity{Name: &serverName, Public: "old-public:9091", Internal: "old-internal:8191"}
	newIdentity := &proto.DataServerIdentity{Name: &serverName, Public: "new-public:9091", Internal: "new-internal:8191"}
	unregistered := testDataServer("s2")
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	require.NoError(t, metadata.CreateDataServer(&proto.DataServer{Identity: newIdentity}))
	e := newTestElection(t, metadata, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{})
	// The ensemble is compared and logged at debug level only
	e.logger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ensemble, change, err := e.refreshEnsembleInfo(&proto.ShardMetadata{
		Ensemble:       []*proto.DataServerIdentity{oldIdentity},
		EnsembleChange: &proto.EnsembleChangeMetadata{From: unregistered, To: oldIdentity},
	})
	require.NoError(t, err)
	assertShardEnsemble(t, ensemble, newIdentity)
	assertEnsembleChange(t, change, unregistered, newIdentity)

	ensemble, change, err = e.refreshEnsembleInfo(&proto.ShardMetadata{
		Ensemble: []*proto.DataServerIdentity{oldIdentity},
	})
	require.NoError(t, err)
	assertShardEnsemble(t, ensemble, newIdentity)
	assert.Nil(t, change)
}

// A pending change whose to isn't a member of the ensemble, e.g. in a status
// edited by hand, stops the election instead of electing a wrong ensemble.
func TestElectionRefreshEnsembleInfoRejectsChangeOutsideEnsemble(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	e := newTestElection(t, metadata, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{})

	_, _, err := e.refreshEnsembleInfo(&proto.ShardMetadata{
		Ensemble:       []*proto.DataServerIdentity{s1},
		EnsembleChange: &proto.EnsembleChangeMetadata{From: s2, To: s3},
	})
	assert.ErrorIs(t, err, errs.ErrShardNotElectable)
}

// The new term fences the data server removed by the pending change at the
// address it's registered with, as the ensemble.
func TestElectionNewTermRefreshesRemovedDataServerAddress(t *testing.T) {
	var shard int64 = 5
	name := "s2"
	oldIdentity := &proto.DataServerIdentity{Name: &name, Public: "old-public:9091", Internal: "old-internal:8191"}
	newIdentity := &proto.DataServerIdentity{Name: &name, Public: "new-public:9091", Internal: "new-internal:8191"}
	s1 := testDataServer("s1")
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	require.NoError(t, metadata.CreateDataServer(&proto.DataServer{Identity: newIdentity}))
	shardMeta := &proto.ShardMetadata{Status: proto.ShardStatusSteadyState, Term: 2, Leader: s1,
		Ensemble:       []*proto.DataServerIdentity{s1},
		EnsembleChange: &proto.EnsembleChangeMetadata{From: oldIdentity, To: s1}}
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, shardMeta)
	e := newTestElection(t, metadata, rpctest.NewRpcProvider(), shard, shardMeta)

	_, err := e.newTerm()
	require.NoError(t, err)
	stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
	assertEnsembleChange(t, stored.GetEnsembleChange(), newIdentity, s1)
	assertEnsembleChange(t, e.meta.GetEnsembleChange(), newIdentity, s1)
}

// When the data server removed by the pending change makes the fencing
// majority, a majority of the new ensemble must still be fenced.
func TestElectionFenceQuorumRequiresEnsembleMajority(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	removed := testDataServer("removed")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(removed).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(0, 0, errors.New("unreachable"))

	_, _, _, err := e.fenceQuorum(2, &proto.NewTermOptions{}, []*proto.DataServerIdentity{s1, s2},
		&proto.EnsembleChangeMetadata{From: removed, To: s2})
	assert.ErrorContains(t, err, "quorum of the ensemble not reached")
}

// With from and to both unreachable, the other members are a majority of the
// ensemble before the change and of the one after it: the fence succeeds.
func TestElectionFenceQuorumSucceedsWithoutFromAndTo(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	removed := testDataServer("removed")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, 0, nil)
	rpc.GetNode(s3).NewTermResponse(0, 0, errors.New("unreachable"))
	rpc.GetNode(removed).NewTermResponse(0, 0, errors.New("unreachable"))

	candidates, retainedHeads, _, err := e.fenceQuorum(2, &proto.NewTermOptions{},
		[]*proto.DataServerIdentity{s1, s2, s3}, &proto.EnsembleChangeMetadata{From: removed, To: s3})
	require.NoError(t, err)
	assert.Len(t, candidates, 2)
	assert.Empty(t, retainedHeads)
}

// With an even replication factor, overlapping every majority of the ensemble
// before the change takes one member less than a majority: with from
// unreachable, the fence succeeds with a majority of the ensemble that holds to.
func TestElectionFenceQuorumSucceedsWithoutFromWithEvenReplicationFactor(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	to := testDataServer("to")
	removed := testDataServer("removed")
	for _, test := range []struct {
		name        string
		ensemble    []*proto.DataServerIdentity
		unreachable []*proto.DataServerIdentity
	}{
		{name: "rf 2", ensemble: []*proto.DataServerIdentity{s1, to}},
		{name: "rf 4", ensemble: []*proto.DataServerIdentity{s1, s2, s3, to},
			unreachable: []*proto.DataServerIdentity{s3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rpc := rpctest.NewRpcProvider()
			e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})
			for _, server := range test.ensemble {
				if slices.Contains(test.unreachable, server) {
					rpc.GetNode(server).NewTermResponse(0, 0, errors.New("unreachable"))
				} else {
					rpc.GetNode(server).NewTermResponse(1, 0, nil)
				}
			}
			rpc.GetNode(removed).NewTermResponse(0, 0, errors.New("unreachable"))

			candidates, _, _, err := e.fenceQuorum(2, &proto.NewTermOptions{}, test.ensemble,
				&proto.EnsembleChangeMetadata{From: removed, To: to})
			require.NoError(t, err)
			assert.Len(t, candidates, len(test.ensemble)-len(test.unreachable))
		})
	}
}

// A majority of the ensemble isn't enough when it holds to and isn't a majority
// of the ensemble before the change: the entries committed before it may be on
// the others only.
func TestElectionFenceQuorumRequiresOldEnsembleMajority(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	removed := testDataServer("removed")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s3).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(0, 0, errors.New("unreachable"))
	rpc.GetNode(removed).NewTermResponse(0, 0, errors.New("unreachable"))

	_, _, _, err := e.fenceQuorum(2, &proto.NewTermOptions{},
		[]*proto.DataServerIdentity{s1, s2, s3}, &proto.EnsembleChangeMetadata{From: removed, To: s3})
	assert.ErrorContains(t, err, "quorum not reached")
}

func TestElectionChooseLeaderFailsWithoutLeader(t *testing.T) {
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	e := newTestElection(t, metadata, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{})

	_, _, err := e.chooseLeader(map[*proto.DataServerIdentity]*proto.EntryId{}, nil)
	assert.ErrorIs(t, err, errs.ErrNotFound)
}

// The leader has the highest head entry among the fenced data servers, a
// candidate when one ties with the from of the pending change. From, ahead of
// every candidate, undoes the change: it swaps back with to, whatever the head
// entry of to, and to is deleted. To with the highest head entry finishes the
// change: from is deleted.
func TestElectionChooseLeaderChoosesHighestHeadEntry(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")
	for _, test := range []struct {
		name          string
		ensembleHeads sharding.HeadEntries
		retainedHeads sharding.HeadEntries
		leader        *proto.DataServerIdentity
		ensemble      []*proto.DataServerIdentity
		changeFrom    *proto.DataServerIdentity
		changeTo      *proto.DataServerIdentity
		pending       []*proto.DataServerIdentity
		followers     []*proto.DataServerIdentity
	}{{
		name:          "higher term wins over a higher offset",
		ensembleHeads: sharding.HeadEntries{s1: {Term: 3, Offset: 1}, s3: {Term: 2, Offset: 9}, s4: {Term: 2, Offset: 4}},
		leader:        s1,
		ensemble:      []*proto.DataServerIdentity{s1, s3, s4},
		changeFrom:    s2,
		changeTo:      s4,
		followers:     []*proto.DataServerIdentity{s3, s4},
	}, {
		name:          "candidate ties with the retained data server",
		ensembleHeads: sharding.HeadEntries{s1: {Term: 2, Offset: 5}, s3: {Term: 2, Offset: 4}, s4: {Term: -1, Offset: -1}},
		retainedHeads: sharding.HeadEntries{s2: {Term: 2, Offset: 5}},
		leader:        s1,
		ensemble:      []*proto.DataServerIdentity{s1, s3, s4},
		changeFrom:    s2,
		changeTo:      s4,
		followers:     []*proto.DataServerIdentity{s3, s4},
	}, {
		name:          "to has the highest head entry",
		ensembleHeads: sharding.HeadEntries{s1: {Term: 2, Offset: 5}, s3: {Term: 2, Offset: 4}, s4: {Term: 2, Offset: 6}},
		retainedHeads: sharding.HeadEntries{s2: {Term: 2, Offset: 5}},
		leader:        s4,
		ensemble:      []*proto.DataServerIdentity{s1, s3, s4},
		pending:       []*proto.DataServerIdentity{s2},
		followers:     []*proto.DataServerIdentity{s1, s3},
	}, {
		name:          "from ahead by offset",
		ensembleHeads: sharding.HeadEntries{s3: {Term: 2, Offset: 4}, s4: {Term: 1, Offset: 9}},
		retainedHeads: sharding.HeadEntries{s2: {Term: 2, Offset: 5}},
		leader:        s2,
		ensemble:      []*proto.DataServerIdentity{s1, s3, s2},
		pending:       []*proto.DataServerIdentity{s4},
		followers:     []*proto.DataServerIdentity{s3},
	}, {
		name:          "from ahead by term, to isn't the lowest",
		ensembleHeads: sharding.HeadEntries{s3: {Term: 2, Offset: 4}, s4: {Term: 2, Offset: 9}},
		retainedHeads: sharding.HeadEntries{s2: {Term: 3, Offset: 5}},
		leader:        s2,
		ensemble:      []*proto.DataServerIdentity{s1, s3, s2},
		pending:       []*proto.DataServerIdentity{s4},
		followers:     []*proto.DataServerIdentity{s3},
	}, {
		name:          "from ahead, to missed the fence",
		ensembleHeads: sharding.HeadEntries{s1: {Term: 2, Offset: 4}, s3: {Term: 2, Offset: 3}},
		retainedHeads: sharding.HeadEntries{s2: {Term: 2, Offset: 5}},
		leader:        s2,
		ensemble:      []*proto.DataServerIdentity{s1, s3, s2},
		pending:       []*proto.DataServerIdentity{s4},
		followers:     []*proto.DataServerIdentity{s1, s3},
	}} {
		t.Run(test.name, func(t *testing.T) {
			metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
				&proto.ClusterConfiguration{})
			e := newTestElection(t, metadata, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{
				Ensemble:       []*proto.DataServerIdentity{s1, s3, s4},
				EnsembleChange: &proto.EnsembleChangeMetadata{From: s2, To: s4},
			})

			leader, followers, err := e.chooseLeader(test.ensembleHeads, test.retainedHeads)
			require.NoError(t, err)
			assert.Equal(t, test.leader, leader)
			assertShardEnsemble(t, e.meta.Ensemble, test.ensemble...)
			assertEnsembleChange(t, e.meta.EnsembleChange, test.changeFrom, test.changeTo)
			assertShardEnsemble(t, e.meta.PendingDeleteShardNodes, test.pending...)
			assert.ElementsMatch(t, test.followers, maps.Keys(followers))
		})
	}
}

// Among the data servers tied at the highest head entry, the leader selector
// chooses the leader.
func TestElectionChooseLeaderChoosesAmongTiedHeadEntries(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	e := newTestElection(t, metadata, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3},
	})

	leader, followers, err := e.chooseLeader(
		sharding.HeadEntries{s1: {Term: 2, Offset: 5}, s2: {Term: 2, Offset: 5}, s3: {Term: 2, Offset: 4}}, nil)
	require.NoError(t, err)
	assert.Contains(t, []*proto.DataServerIdentity{s1, s2}, leader)
	assert.Len(t, followers, 2)
	assert.NotContains(t, maps.Keys(followers), leader)
	assertShardEnsemble(t, e.meta.Ensemble, s1, s2, s3)
}

// The election retries an attempt whose leader can't be selected, or whose
// leader can't be persisted.
func TestElectionStartReturnsLeaderSelectionErrors(t *testing.T) {
	var shard int64 = 5
	s1 := testDataServer("s1")
	for _, test := range []struct {
		name  string
		hooks func(m *hookedMetadata)
		err   string
	}{
		{name: "status list", err: "status unavailable", hooks: func(m *hookedMetadata) {
			m.listNamespaceStatus = func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
				return nil, errors.New("status unavailable")
			}
		}},
		{name: "leader write", err: "failed to persist the new leader", hooks: func(m *hookedMetadata) {
			writes := 0
			m.patchShardStatus = func(namespace string, shard int64, modify func(*proto.ShardMetadata) (bool, error)) error {
				// The new term is written, the new leader is not
				if writes++; writes > 1 {
					return errors.New("store unavailable")
				}
				return m.Metadata.PatchShardStatus(namespace, shard, modify)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
				&proto.ClusterConfiguration{})
			shardMeta := &proto.ShardMetadata{Status: proto.ShardStatusSteadyState, Term: 2, Leader: s1,
				Ensemble: []*proto.DataServerIdentity{s1}}
			storeTestShardMetadata(t, base, constant.DefaultNamespace, shard, namespaceConfig, shardMeta)
			metadata := &hookedMetadata{Metadata: base}
			test.hooks(metadata)
			rpc := rpctest.NewRpcProvider()
			rpc.GetNode(s1).NewTermResponse(2, 0, nil)
			rpc.GetNode(s1).BecomeLeaderResponse(nil)
			e := newTestElection(t, metadata, rpc, shard, shardMeta)

			_, err := e.Start()
			assert.ErrorContains(t, err, test.err)
			stored := requireShardMetadata(t, base, constant.DefaultNamespace, shard)
			assert.Equal(t, proto.ShardStatusElection, stored.GetStatus())
			assert.EqualValues(t, 3, stored.GetTerm())
			assert.Nil(t, stored.GetLeader())
		})
	}
}

func TestElectionFenceNewTermAndAddFollowerReturnsAddFollowerError(t *testing.T) {
	leader := testDataServer("leader")
	follower := testDataServer("follower")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	rpc.GetNode(follower).NewTermResponse(1, 3, nil)
	rpc.GetNode(leader).AddFollowerResponse(errors.New("leader unavailable"))

	err := e.fenceNewTermAndAddFollower(t.Context(), 2, &proto.NewTermOptions{}, leader, follower)
	assert.ErrorContains(t, err, "leader unavailable")
	request := rpc.GetNode(leader).ExpectAddFollowerRequest(t, 5, 2)
	assert.Equal(t, int64(3), request.GetFollowerHeadEntryId().GetOffset())
}

// A follower that rejects the term of the election as invalid is in a newer
// term already: the election stops trying to add it.
func TestElectionFencingFailedFollowersStopsOnInvalidTerm(t *testing.T) {
	leader := testDataServer("leader")
	follower := testDataServer("follower")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	rpc.GetNode(follower).NewTermResponse(0, 0, constant.ErrInvalidTerm)

	done := make(chan struct{})
	go func() {
		e.fencingFailedFollowers(2, &proto.NewTermOptions{}, []*proto.DataServerIdentity{leader, follower}, leader,
			map[*proto.DataServerIdentity]*proto.EntryId{})
		close(done)
	}()
	requireClosed(t, done, "fencingFailedFollowers to stop")

	rpc.GetNode(follower).ExpectNewTermRequest(t, 5, 2, false)
	rpc.GetNode(follower).ExpectNoMoreNewTermRequest(t)
}

// A change of the ensemble records itself as pending, and takes the added data
// server out of the pending deletions.
func TestElectionPrepareIfChangeEnsembleRecordsChange(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")
	e := &election{
		logger:         slog.Default(),
		changeEnsemble: action.NewChangeEnsembleAction(5, s3, s4),
	}
	shardMeta := &proto.ShardMetadata{
		Ensemble:                []*proto.DataServerIdentity{s1, s2, s3},
		PendingDeleteShardNodes: []*proto.DataServerIdentity{s4},
	}

	e.prepareIfChangeEnsemble(shardMeta)

	assertShardEnsemble(t, shardMeta.Ensemble, s1, s2, s4)
	assertEnsembleChange(t, shardMeta.EnsembleChange, s3, s4)
	assert.Empty(t, shardMeta.PendingDeleteShardNodes)
}

// An earlier election persisted the change already: the election keeps the
// shard metadata as it is, and completes the change.
func TestElectionPrepareIfChangeEnsembleKeepsPersistedChange(t *testing.T) {
	s1 := testDataServer("s1")
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")
	e := &election{
		logger:         slog.Default(),
		changeEnsemble: action.NewChangeEnsembleAction(5, s3, s4),
	}
	shardMeta := &proto.ShardMetadata{
		Ensemble:       []*proto.DataServerIdentity{s1, s4},
		EnsembleChange: &proto.EnsembleChangeMetadata{From: s3, To: s4},
	}

	e.prepareIfChangeEnsemble(shardMeta)

	assertShardEnsemble(t, shardMeta.Ensemble, s1, s4)
	assertEnsembleChange(t, shardMeta.EnsembleChange, s3, s4)
}

// The election of a shard that was removed meanwhile fails instead of
// bringing the shard back.
func TestElectionNewTermFailsWhenShardIsRemoved(t *testing.T) {
	var shard int64 = 5
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard+1, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     1,
		Ensemble: []*proto.DataServerIdentity{testDataServer("s1")},
	})
	e := newTestElection(t, metadata, rpctest.NewRpcProvider(), shard, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     2,
		Ensemble: []*proto.DataServerIdentity{testDataServer("s1")},
	})

	_, err := e.newTerm()
	assert.ErrorIs(t, err, errs.ErrNotFound)
	_, exists := testShardMetadata(metadata, constant.DefaultNamespace, shard)
	assert.False(t, exists)
}

// The leader is persisted before it is told: if BecomeLeader fails, the next
// election starts from it.
func TestElectionStartPersistsLeaderBeforeBecomeLeader(t *testing.T) {
	var shard int64 = 5
	s1 := testDataServer("s1")
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	shardMeta := &proto.ShardMetadata{Status: proto.ShardStatusSteadyState, Term: 2, Leader: s1,
		Ensemble: []*proto.DataServerIdentity{s1}}
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, shardMeta)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).NewTermResponse(2, 0, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(errors.New("unreachable"))
	e := newTestElection(t, metadata, rpc, shard, shardMeta)

	_, err := e.Start()
	assert.ErrorContains(t, err, "unreachable")
	stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
	assert.Equal(t, proto.ShardStatusSteadyState, stored.GetStatus())
	assert.EqualValues(t, 3, stored.GetTerm())
	assert.Equal(t, s1.GetPublic(), stored.GetLeader().GetPublic())
}

// The new term resets ensemble_caught_up; the ensemble is caught up once every
// follower is, not at the majority, and Tick persists it, on a signal.
func TestElectionStartResetsEnsembleCaughtUp(t *testing.T) {
	var shard int64 = 5
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	shardMeta := &proto.ShardMetadata{Status: proto.ShardStatusSteadyState, Term: 2, Leader: s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3}, EnsembleCaughtUp: true}
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, shardMeta)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).NewTermResponse(2, 5, nil)
	rpc.GetNode(s2).NewTermResponse(2, 4, nil)
	rpc.GetNode(s3).NewTermResponse(2, 3, nil)
	rpc.GetNode(s1).BecomeLeaderResponse(nil)
	e := newTestElection(t, metadata, rpc, shard, shardMeta)
	tick := make(chan controllerapi.Signal, 1)
	e.tick = tick

	_, err := e.Start()
	require.NoError(t, err)
	assert.False(t, requireShardMetadata(t, metadata, constant.DefaultNamespace, shard).GetEnsembleCaughtUp())

	rpc.GetNode(s2).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 5, 5)
	rpc.GetNode(s3).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 4, 4)
	<-tick
	assert.True(t, e.IsQuorumCaughtUp())
	assert.False(t, e.IsEnsembleCaughtUp())

	rpc.GetNode(s3).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 5, 5)
	<-tick
	assert.True(t, e.IsEnsembleCaughtUp())
	assert.False(t, requireShardMetadata(t, metadata, constant.DefaultNamespace, shard).GetEnsembleCaughtUp())
	require.NoError(t, e.Tick())
	assert.True(t, requireShardMetadata(t, metadata, constant.DefaultNamespace, shard).GetEnsembleCaughtUp())
}

// From, ahead of every candidate, undoes the change: the ensemble is the one
// before it again, and to is deleted.
func TestElectionStartRestoresRemovedDataServerAhead(t *testing.T) {
	var shard int64 = 5
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	shardMeta := &proto.ShardMetadata{Status: proto.ShardStatusSteadyState, Term: 2, Leader: s1,
		Ensemble: []*proto.DataServerIdentity{s1, s2, s3}}
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, shardMeta)
	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(s1).NewTermResponse(0, 0, errors.New("unreachable"))
	rpc.GetNode(s2).NewTermResponse(2, 5, nil)
	rpc.GetNode(s3).NewTermResponse(2, 4, nil)
	rpc.GetNode(s4).NewTermResponse(-1, -1, nil)
	rpc.GetNode(s2).BecomeLeaderResponse(nil)
	e := New(t.Context(), slog.Default(), nil, nil, metadata, feature.NoOpSupplier, leaderselector.NewSelector(),
		rpc, constant.DefaultNamespace, shard, shardMeta, action.NewChangeEnsembleAction(shard, s2, s4), nil,
		metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency", "", nil),
		metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency", "", nil),
		metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency", "", nil))

	leader, err := e.Start()
	require.NoError(t, err)
	assert.Equal(t, s2, leader)
	// Not released: no follower committed the head entry of the leader
	e.Cancel()
	stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
	assertShardEnsemble(t, stored.GetEnsemble(), s1, s3, s2)
	assert.Nil(t, stored.GetEnsembleChange())
	assertShardEnsemble(t, stored.GetPendingDeleteShardNodes(), s4)
}

// The election keeps the change pending until a majority of the ensemble caught
// up, which a dead follower doesn't block, and Tick finishes
// it then. A canceled election never reports a caught-up majority, nor finishes
// the change.
func TestElectionStartKeepsChangePending(t *testing.T) {
	var shard int64 = 5
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")
	for _, test := range []struct {
		name     string
		caughtUp bool
	}{
		{name: "majority caught up, s4 dead", caughtUp: true},
		{name: "canceled"},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
				&proto.ClusterConfiguration{})
			shardMeta := &proto.ShardMetadata{Status: proto.ShardStatusSteadyState, Term: 2, Leader: s1,
				Ensemble: []*proto.DataServerIdentity{s1, s2, s3}}
			storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard, namespaceConfig, shardMeta)
			rpc := rpctest.NewRpcProvider()
			rpc.GetNode(s1).NewTermResponse(2, 5, nil)
			rpc.GetNode(s2).NewTermResponse(2, 5, nil)
			rpc.GetNode(s3).NewTermResponse(2, 4, nil)
			rpc.GetNode(s4).NewTermResponse(-1, -1, nil)
			rpc.GetNode(s1).BecomeLeaderResponse(nil)
			e := New(t.Context(), slog.Default(), nil, nil, metadata, feature.NoOpSupplier, leaderselector.NewSelector(),
				rpc, constant.DefaultNamespace, shard, shardMeta, action.NewChangeEnsembleAction(shard, s2, s4), nil,
				metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency", "", nil),
				metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency", "", nil),
				metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency", "", nil))
			t.Cleanup(e.Cancel)

			leader, err := e.Start()
			require.NoError(t, err)
			// The ensemble wins the tie
			assert.Equal(t, s1.GetPublic(), leader.GetPublic())

			if !test.caughtUp {
				rpc.GetNode(s3).ExpectGetStatusRequest(t, shard)
				e.Cancel()
				assert.False(t, e.IsQuorumCaughtUp())
				require.NoError(t, e.Tick())
				stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
				assertEnsembleChange(t, stored.GetEnsembleChange(), s2, s4)
				return
			}
			rpc.GetNode(s3).GetStatusResponse(3, proto.ServingStatus_FOLLOWER, 5, 5)
			assert.Eventually(t, e.IsQuorumCaughtUp, 10*time.Second, 10*time.Millisecond)
			require.NoError(t, e.Tick())
			stored := requireShardMetadata(t, metadata, constant.DefaultNamespace, shard)
			assertShardEnsemble(t, stored.GetEnsemble(), s1, s3, s4)
			assert.Nil(t, stored.GetEnsembleChange())
			assertShardEnsemble(t, stored.GetPendingDeleteShardNodes(), s2)
		})
	}
}

// Tick finishes the change once a majority caught up, and
// persists ensemble_caught_up once every member did, once, if the shard is still
// in the term of the election; a failed write is returned.
func TestElectionTick(t *testing.T) {
	var shard int64 = 5
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s4 := testDataServer("s4")
	change := &proto.EnsembleChangeMetadata{From: s2, To: s4}
	for _, test := range []struct {
		name         string
		change       *proto.EnsembleChangeMetadata
		caughtUp     bool
		storedChange *proto.EnsembleChangeMetadata
		storedTerm   int64
		writeErr     error
		finished     bool
		// every member caught up, and persisted ensemble_caught_up
		ensembleCaughtUp bool
		persisted        bool
		storedCaughtUp   bool
	}{
		{name: "majority caught up", change: change, caughtUp: true, storedChange: change, storedTerm: 3, finished: true},
		{name: "majority behind", change: change, storedChange: change, storedTerm: 3},
		{name: "no change", caughtUp: true, storedChange: change, storedTerm: 3},
		{name: "later term", change: change, caughtUp: true, storedChange: change, storedTerm: 4},
		{name: "already finished", change: change, caughtUp: true, storedTerm: 3},
		{name: "write failure", change: change, caughtUp: true, storedChange: change, storedTerm: 3,
			writeErr: errors.New("store unavailable")},
		{name: "ensemble caught up", caughtUp: true, ensembleCaughtUp: true, storedTerm: 3, storedCaughtUp: true},
		{name: "ensemble caught up with a change", change: change, caughtUp: true, ensembleCaughtUp: true,
			storedChange: change, storedTerm: 3, finished: true, storedCaughtUp: true},
		{name: "ensemble caught up already persisted", caughtUp: true, ensembleCaughtUp: true, persisted: true,
			storedTerm: 3},
		{name: "ensemble caught up in a later term", caughtUp: true, ensembleCaughtUp: true, storedTerm: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
				&proto.ClusterConfiguration{})
			storeTestShardMetadata(t, base, constant.DefaultNamespace, shard, namespaceConfig, &proto.ShardMetadata{
				Status: proto.ShardStatusSteadyState, Term: test.storedTerm, Leader: s1,
				Ensemble: []*proto.DataServerIdentity{s1, s4}, EnsembleChange: test.storedChange})
			metadata := &hookedMetadata{Metadata: base}
			if test.writeErr != nil {
				metadata.patchShardStatus = func(string, int64, func(*proto.ShardMetadata) (bool, error)) error {
					return test.writeErr
				}
			}
			e := newTestElection(t, metadata, rpctest.NewRpcProvider(), shard, &proto.ShardMetadata{
				Term: 3, Ensemble: []*proto.DataServerIdentity{s1, s4}, EnsembleChange: test.change,
				EnsembleCaughtUp: test.persisted})
			e.quorumCaughtUp.Store(test.caughtUp)
			e.ensembleCaughtUp.Store(test.ensembleCaughtUp)

			err := e.Tick()
			if test.writeErr != nil {
				assert.ErrorIs(t, err, test.writeErr)
			} else {
				require.NoError(t, err)
			}
			stored := requireShardMetadata(t, base, constant.DefaultNamespace, shard)
			assert.Equal(t, test.storedCaughtUp, stored.GetEnsembleCaughtUp())
			if test.finished {
				assert.Nil(t, stored.GetEnsembleChange())
				assertShardEnsemble(t, stored.GetPendingDeleteShardNodes(), s2)
				return
			}
			if test.storedChange != nil {
				assertEnsembleChange(t, stored.GetEnsembleChange(), s2, s4)
			}
			assert.Empty(t, stored.GetPendingDeleteShardNodes())
		})
	}
}

func TestElectionStartFailsWhenStartedTwice(t *testing.T) {
	e := newTestElection(t, nil, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{})
	e.started.Store(true)
	_, err := e.Start()
	assert.ErrorIs(t, err, errs.ErrFailedPrecondition)
}

func testDataServer(id string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{Internal: id, Public: id}
}

// newTestElection creates an election of the shard, as its shard controller
// does, that the test drives step by step.
func newTestElection(t *testing.T, metadata coordmetadata.Metadata, rpc coordrpc.ControllerRPCProvider, shard int64,
	shardMeta *proto.ShardMetadata) *election {
	t.Helper()
	labels := metric.LabelsForShard(constant.DefaultNamespace, shard)
	e := New(t.Context(), slog.Default(), nil, nil, metadata, feature.NoOpSupplier, leaderselector.NewSelector(),
		rpc, constant.DefaultNamespace, shard, shardMeta, nil, nil,
		metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency",
			"The time it takes to elect a leader for the shard", labels),
		metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency",
			"The time it takes to take the ensemble of data servers to a new term", labels),
		metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency",
			"The time it takes for the new elected leader to start", labels)).(*election)
	t.Cleanup(e.Cancel)
	return e
}

// hookedMetadata calls the hook of each method that has one, and the embedded
// Metadata otherwise.
type hookedMetadata struct {
	coordmetadata.Metadata
	getDataServer           func(name string) (commonobject.Borrowed[*proto.DataServer], bool, error)
	getNamespaceStatus      func(namespace string) (commonobject.Borrowed[*proto.NamespaceStatus], bool, error)
	getShardStatus          func(namespace string, shard int64) (commonobject.Borrowed[*proto.ShardMetadata], bool, error)
	listNamespaceStatus     func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error)
	getNamespaceTermOptions func(namespace string) (*proto.NewTermOptions, error)
	patchNamespaceStatus    func(namespace string, modify func(*proto.NamespaceStatus) (bool, error)) error
	patchShardStatus        func(namespace string, shard int64, modify func(*proto.ShardMetadata) (bool, error)) error
}

func (m *hookedMetadata) GetDataServer(name string) (commonobject.Borrowed[*proto.DataServer], bool, error) {
	if m.getDataServer != nil {
		return m.getDataServer(name)
	}
	return m.Metadata.GetDataServer(name)
}

func (m *hookedMetadata) GetNamespaceStatus(namespace string) (
	commonobject.Borrowed[*proto.NamespaceStatus], bool, error) {
	if m.getNamespaceStatus != nil {
		return m.getNamespaceStatus(namespace)
	}
	return m.Metadata.GetNamespaceStatus(namespace)
}

func (m *hookedMetadata) GetShardStatus(namespace string, shard int64) (
	commonobject.Borrowed[*proto.ShardMetadata], bool, error) {
	if m.getShardStatus != nil {
		return m.getShardStatus(namespace, shard)
	}
	return m.Metadata.GetShardStatus(namespace, shard)
}

func (m *hookedMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	if m.listNamespaceStatus != nil {
		return m.listNamespaceStatus()
	}
	return m.Metadata.ListNamespaceStatus()
}

func (m *hookedMetadata) GetNamespaceTermOptions(namespace string) (*proto.NewTermOptions, error) {
	if m.getNamespaceTermOptions != nil {
		return m.getNamespaceTermOptions(namespace)
	}
	return m.Metadata.GetNamespaceTermOptions(namespace)
}

func (m *hookedMetadata) PatchNamespaceStatus(namespace string, modify func(*proto.NamespaceStatus) (bool, error)) error {
	if m.patchNamespaceStatus != nil {
		return m.patchNamespaceStatus(namespace, modify)
	}
	return m.Metadata.PatchNamespaceStatus(namespace, modify)
}

func (m *hookedMetadata) PatchShardStatus(namespace string, shard int64,
	modify func(*proto.ShardMetadata) (bool, error)) error {
	if m.patchShardStatus != nil {
		return m.patchShardStatus(namespace, shard, modify)
	}
	return m.Metadata.PatchShardStatus(namespace, shard, modify)
}

var namespaceConfig = &proto.Namespace{
	Name:              "my-namespace",
	InitialShardCount: 1,
	ReplicationFactor: 3,
}

func newTestMetadata(t *testing.T, metadataProvider provider.Provider[*proto.ClusterStatus], clusterConfig *proto.ClusterConfiguration) coordmetadata.Metadata {
	t.Helper()

	if clusterConfig == nil {
		clusterConfig = &proto.ClusterConfiguration{}
	}
	if len(clusterConfig.Namespaces) == 0 {
		clusterConfig.Namespaces = []*proto.Namespace{namespaceConfig}
	}
	if len(clusterConfig.Servers) == 0 {
		clusterConfig.Servers = []*proto.DataServerIdentity{
			{Public: "seed-public-1:6648", Internal: "seed-internal-1:6649"},
			{Public: "seed-public-2:7648", Internal: "seed-internal-2:7649"},
			{Public: "seed-public-3:8648", Internal: "seed-internal-3:8649"},
		}
	}

	dir := t.TempDir()
	snapshot, err := metadataProvider.Load()
	require.NoError(t, err)
	statusData, err := metadatacodec.ClusterStatusCodec.MarshalYAML(snapshot.Value)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(dir, coordoption.DefaultFileStatusName), statusData, 0o600))
	configData, err := metadatacodec.ClusterConfigCodec.MarshalYAML(clusterConfig)
	assert.NoError(t, err)
	assert.NoError(t, os.WriteFile(filepath.Join(dir, coordoption.DefaultFileConfigName), configData, 0o600))
	metadataFactory, err := coordmetadata.New(t.Context(), &coordoption.Options{
		Metadata: coordoption.MetadataOptions{
			ProviderOptions: coordoption.ProviderOptions{
				ProviderName: metadatacommon.NameFile,
				File: coordoption.FileMetadata{
					Dir: dir,
				},
			},
		},
	})
	assert.NoError(t, err)
	metadata := metadataFactory.CreateMetadata(t.Context())
	_, err = metadata.WaitToBecomeLeader()
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, metadata.Close())
		assert.NoError(t, metadataFactory.Close())
	})
	return metadata
}

func storeTestShardMetadata(
	t *testing.T,
	metadata coordmetadata.Metadata,
	namespace string,
	shard int64,
	namespaceConfig *proto.Namespace,
	shardMetadata *proto.ShardMetadata,
) {
	t.Helper()
	namespaceStatus := &proto.NamespaceStatus{
		ReplicationFactor: namespaceConfig.GetReplicationFactor(),
		Shards: map[int64]*proto.ShardMetadata{
			shard: gproto.CloneOf(shardMetadata),
		},
	}
	require.NoError(t, metadata.CreateNamespaceStatus(namespace, namespaceStatus))
}

func testShardMetadata(metadata coordmetadata.Metadata, namespace string, shard int64) (*proto.ShardMetadata, bool) {
	borrowedMeta, exists, err := metadata.GetShardStatus(namespace, shard)
	if err != nil || !exists {
		return nil, false
	}
	return gproto.CloneOf(borrowedMeta.UnsafeBorrow()), true
}

func requireShardMetadata(t *testing.T, metadata coordmetadata.Metadata, namespace string, shard int64) *proto.ShardMetadata {
	t.Helper()
	shardMetadata, exists := testShardMetadata(metadata, namespace, shard)
	require.True(t, exists)
	return shardMetadata
}

func shardTerm(metadata coordmetadata.Metadata, namespace string, shard int64) int64 {
	shardMetadata, exists := testShardMetadata(metadata, namespace, shard)
	if !exists {
		return 0
	}
	return shardMetadata.Term
}

func assertEnsembleChange(t *testing.T, actual *proto.EnsembleChangeMetadata, from *proto.DataServerIdentity,
	to *proto.DataServerIdentity) {
	t.Helper()
	if from == nil {
		assert.Nil(t, actual)
		return
	}
	require.NotNil(t, actual)
	assert.Equal(t, from.GetNameOrDefault(), actual.GetFrom().GetNameOrDefault())
	assert.Equal(t, to.GetNameOrDefault(), actual.GetTo().GetNameOrDefault())
}

func assertShardEnsemble(
	t *testing.T,
	actual []*proto.DataServerIdentity,
	expected ...*proto.DataServerIdentity,
) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for i, expectedServer := range expected {
		assert.True(t, gproto.Equal(expectedServer, actual[i]))
	}
}

// requireClosed waits for the channel to be closed.
func requireClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timed out waiting for "+what)
	}
}
