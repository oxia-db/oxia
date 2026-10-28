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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
)

func TestChooseCandidatesChoosesHighestHeadEntry(t *testing.T) {
	tests := []struct {
		name           string
		candidates     map[*proto.DataServerIdentity]*proto.EntryId
		expectedLeader *proto.DataServerIdentity
	}{
		{
			name: "Choose highest term",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 2480},
				{Public: "2", Internal: "2"}: {Term: 200, Offset: 2500},
				{Public: "3", Internal: "3"}: {Term: 198, Offset: 3000},
			},
			expectedLeader: &proto.DataServerIdentity{Public: "2", Internal: "2"},
		},
		{
			name: "Same term, different offsets",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 1000},
				{Public: "2", Internal: "2"}: {Term: 200, Offset: 2000},
				{Public: "3", Internal: "3"}: {Term: 200, Offset: 1500},
			},
			expectedLeader: &proto.DataServerIdentity{Public: "2", Internal: "2"},
		},
		{
			name: "Different terms, same offsets",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 1500},
				{Public: "2", Internal: "2"}: {Term: 198, Offset: 1500},
				{Public: "3", Internal: "3"}: {Term: 199, Offset: 1500},
			},
			expectedLeader: &proto.DataServerIdentity{Public: "1", Internal: "1"},
		},
		{
			name: "Single candidate",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{
				{Public: "1", Internal: "1"}: {Term: 200, Offset: 1500},
			},
			expectedLeader: &proto.DataServerIdentity{Public: "1", Internal: "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates := chooseCandidates(tt.candidates)

			// Check candidates
			assert.Equal(t, 1, len(candidates))
			for _, candidate := range candidates {
				assert.Equal(t, tt.expectedLeader, candidate)
			}
		})
	}
}

// The election returns the head entries of the candidates and of the removed
// data servers, and the features enabled on them.
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

	candidates, removedEntries, features, err := e.fenceQuorum(2, &proto.NewTermOptions{},
		[]*proto.DataServerIdentity{s1, s2, s3}, []*proto.DataServerIdentity{removed})
	require.NoError(t, err)
	assert.Len(t, candidates, 2)
	assert.EqualValues(t, 100, candidates[s1].GetOffset())
	assert.EqualValues(t, 95, candidates[s2].GetOffset())
	assert.Len(t, removedEntries, 1)
	assert.EqualValues(t, 101, removedEntries[removed].GetOffset())
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
		[]*proto.DataServerIdentity{removed})
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

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(1, 0, nil)
	rpc.GetNode(s3).NewTermResponse(1, 0, nil)
	go func() {
		rpc.GetNode(s4).ExpectNewTermRequest(t, 5, 2, false)
		time.Sleep(quorumFencingGracePeriod / 4)
		rpc.GetNode(s4).NewTermResponse(1, 0, nil)
	}()

	start := time.Now()
	candidates, _, _, err := e.fenceQuorum(2, &proto.NewTermOptions{},
		[]*proto.DataServerIdentity{s1, s2, s3, s4, s5}, nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), quorumFencingGracePeriod)
	assert.Len(t, candidates, 4)
	assert.NotContains(t, candidates, s5)
}

// A follower that is not added by the leader yet, behind it, or unreachable,
// is asked again until it catches up.
func TestElectionEnsureFollowerCaughtRetriesUntilCaughtUp(t *testing.T) {
	var shard int64 = 5
	rpc := rpctest.NewRpcProvider()
	leader := testDataServer("leader")
	follower := testDataServer("follower")
	e := newTestElection(t, nil, rpc, shard, &proto.ShardMetadata{})

	rpc.GetNode(follower).EnqueueGetStatusError(constant.ErrNodeIsNotMember)
	rpc.GetNode(follower).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 9, 9)
	rpc.GetNode(follower).EnqueueGetStatusError(errors.New("unreachable"))
	rpc.GetNode(follower).GetStatusResponse(2, proto.ServingStatus_FOLLOWER, 10, 10)

	e.ensureFollowerCaught([]*proto.DataServerIdentity{leader, follower}, leader, &proto.EntryId{Term: 2, Offset: 10})

	for range 4 {
		rpc.GetNode(follower).ExpectGetStatusRequest(t, shard)
	}
}

// Enough candidates must have the entries of the removed data servers for any
// later fence quorum of the new ensemble to include one of them.
func TestElectionValidateConsistencyRequiresCandidatesToHaveRemovedEntries(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")
	for _, test := range []struct {
		name       string
		ensemble   []*proto.DataServerIdentity
		candidates sharding.HeadEntries
		removed    sharding.HeadEntries
		fails      bool
	}{{
		name:       "two of three candidates have the entries",
		ensemble:   []*proto.DataServerIdentity{s1, s3, s4},
		candidates: sharding.HeadEntries{s1: {Term: 2, Offset: 5}, s3: {Term: 2, Offset: 5}, s4: {Term: -1, Offset: -1}},
		removed:    sharding.HeadEntries{s2: {Term: 2, Offset: 5}},
	}, {
		name:       "removed data server not fenced",
		ensemble:   []*proto.DataServerIdentity{s1, s3, s4},
		candidates: sharding.HeadEntries{s3: {Term: 2, Offset: 4}, s4: {Term: -1, Offset: -1}},
		removed:    sharding.HeadEntries{},
	}, {
		name:       "only the leader has the entries",
		ensemble:   []*proto.DataServerIdentity{s1, s3, s4},
		candidates: sharding.HeadEntries{s1: {Term: 2, Offset: 5}, s3: {Term: 2, Offset: 4}, s4: {Term: -1, Offset: -1}},
		removed:    sharding.HeadEntries{s2: {Term: 2, Offset: 5}},
		fails:      true,
	}, {
		name:       "removed data server ahead by term",
		ensemble:   []*proto.DataServerIdentity{s1, s3, s4},
		candidates: sharding.HeadEntries{s1: {Term: 2, Offset: 9}, s3: {Term: 2, Offset: 9}, s4: {Term: -1, Offset: -1}},
		removed:    sharding.HeadEntries{s2: {Term: 3, Offset: 5}},
		fails:      true,
	}, {
		// Any fence quorum of two members includes the one that stays
		name:       "replication factor 2",
		ensemble:   []*proto.DataServerIdentity{s1, s4},
		candidates: sharding.HeadEntries{s1: {Term: 2, Offset: 5}, s4: {Term: -1, Offset: -1}},
		removed:    sharding.HeadEntries{s2: {Term: 2, Offset: 5}},
	}} {
		t.Run(test.name, func(t *testing.T) {
			e := &election{meta: &proto.ShardMetadata{Ensemble: test.ensemble}}
			err := e.validateConsistency(test.candidates, test.removed)
			if test.fails {
				assert.ErrorIs(t, err, errs.ErrInvalidArgument)
			} else {
				assert.NoError(t, err)
			}
		})
	}
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

	ensemble, err := e.refreshEnsembleInfo([]*proto.DataServerIdentity{oldIdentity, unregistered})
	require.NoError(t, err)
	assertShardEnsemble(t, ensemble, newIdentity, unregistered)
}

// When the removed data servers make the fencing majority, a majority of the
// new ensemble must still be fenced.
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
		[]*proto.DataServerIdentity{removed})
	assert.ErrorContains(t, err, "quorum of the new ensemble not reached")
}

func TestElectionChooseLeaderFailsWithoutLeader(t *testing.T) {
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	e := newTestElection(t, metadata, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{})

	_, _, err := e.chooseLeader(map[*proto.DataServerIdentity]*proto.EntryId{})
	assert.ErrorIs(t, err, errs.ErrNotFound)
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

// A data server that is swapped out again while it is still among the
// removed data servers is listed once.
func TestElectionPrepareIfChangeEnsembleKeepsRemovedNodesUnique(t *testing.T) {
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
		RemovedNodes:            []*proto.DataServerIdentity{s3},
		PendingDeleteShardNodes: []*proto.DataServerIdentity{s4},
	}

	e.prepareIfChangeEnsemble(shardMeta)

	assertShardEnsemble(t, shardMeta.Ensemble, s1, s2, s4)
	assertShardEnsemble(t, shardMeta.RemovedNodes, s3)
	assert.Empty(t, shardMeta.PendingDeleteShardNodes)
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
	e := New(t.Context(), slog.Default(), nil, metadata, feature.NoOpSupplier, leaderselector.NewSelector(),
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
