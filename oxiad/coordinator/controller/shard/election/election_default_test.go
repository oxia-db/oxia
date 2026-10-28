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
	"testing"
	"time"

	gproto "google.golang.org/protobuf/proto"

	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/common/feature"

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

func TestElectionWaitForMajoritySuccess(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	server3 := testDataServer("server3")
	ensemble := []*proto.DataServerIdentity{server1, server2, server3}

	ch := make(chan electionResponse, 3)
	ch <- electionResponse{DataServer: server1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 100}}}
	ch <- electionResponse{DataServer: server2, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 95}}}

	result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble, make(map[*proto.DataServerIdentity]*proto.EntryId), make(map[proto.Feature]bool))

	assert.NoError(t, err)
	assert.Equal(t, 2, totalResponses)
	assert.Len(t, result, 2)
	assert.Equal(t, int64(100), result[server1].Offset)
	assert.Equal(t, int64(95), result[server2].Offset)
}

func TestElectionWaitForMajorityFailureNoQuorum(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	server3 := testDataServer("server3")
	ensemble := []*proto.DataServerIdentity{server1, server2, server3}

	ch := make(chan electionResponse, 3)
	ch <- electionResponse{DataServer: server1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 100}}}
	ch <- electionResponse{DataServer: server2, Err: errors.New("connection failed")}
	ch <- electionResponse{DataServer: server3, Err: errors.New("timeout")}

	result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble, make(map[*proto.DataServerIdentity]*proto.EntryId), make(map[proto.Feature]bool))

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "election failed: quorum not reached")
	assert.Nil(t, result)
	assert.Equal(t, 3, totalResponses)
}

func TestElectionWaitForMajorityMixedSuccessAndFailure(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	server3 := testDataServer("server3")
	ensemble := []*proto.DataServerIdentity{server1, server2, server3}

	ch := make(chan electionResponse, 3)
	ch <- electionResponse{DataServer: server1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 100}}}
	ch <- electionResponse{DataServer: server2, Err: errors.New("connection failed")}
	ch <- electionResponse{DataServer: server3, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 90}}}

	result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble, make(map[*proto.DataServerIdentity]*proto.EntryId), make(map[proto.Feature]bool))

	assert.NoError(t, err)
	assert.Equal(t, 3, totalResponses)
	assert.Len(t, result, 2)
	assert.Equal(t, int64(100), result[server1].Offset)
	assert.Equal(t, int64(90), result[server3].Offset)
}

func TestElectionWaitForMajorityExcludesRemovedServers(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	server3 := testDataServer("server3")
	removedServer := testDataServer("removed")
	ensemble := []*proto.DataServerIdentity{server1, server2, server3}

	ch := make(chan electionResponse, 4)
	ch <- electionResponse{DataServer: server1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 100}}}
	ch <- electionResponse{DataServer: removedServer, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 110}}}
	ch <- electionResponse{DataServer: server2, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 95}}}

	removed := make(map[*proto.DataServerIdentity]*proto.EntryId)
	result, totalResponses, err := e.waitForMajority(ch, 4, 3, ensemble, removed, make(map[proto.Feature]bool))

	assert.NoError(t, err)
	assert.Equal(t, 3, totalResponses)
	assert.Len(t, result, 2)
	assert.NotContains(t, result, removedServer, "removed server should not be in result")
	assert.Contains(t, result, server1)
	assert.Contains(t, result, server2)
	assert.Len(t, removed, 1)
	assert.Equal(t, int64(110), removed[removedServer].Offset)
}

func TestElectionWaitForMajorityEarlyReturn(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	server3 := testDataServer("server3")
	ensemble := []*proto.DataServerIdentity{server1, server2, server3}

	ch := make(chan electionResponse, 3)
	ch <- electionResponse{DataServer: server1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 100}}}
	ch <- electionResponse{DataServer: server2, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 95}}}

	result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble, make(map[*proto.DataServerIdentity]*proto.EntryId), make(map[proto.Feature]bool))

	assert.NoError(t, err)
	assert.Equal(t, 2, totalResponses, "should return early after reaching majority")
	assert.Len(t, result, 2)
}

func TestElectionWaitForEnsembleMajorityWaitsForEnsembleMember(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	removedServer := testDataServer("removed")
	ensemble := []*proto.DataServerIdentity{server1, server2}

	ch := make(chan electionResponse, 3)
	ch <- electionResponse{DataServer: server1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 2, Offset: 1}}}
	ch <- electionResponse{DataServer: removedServer, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 0}}}
	ch <- electionResponse{DataServer: server2, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: -1, Offset: -1}}}

	// server1 and the removed server are a majority of the fenced servers,
	// but only one member of the ensemble
	removedResponse := make(map[*proto.DataServerIdentity]*proto.EntryId)
	result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble, removedResponse, make(map[proto.Feature]bool))
	assert.NoError(t, err)
	assert.Equal(t, 2, totalResponses)

	totalResponses, err = e.waitForEnsembleMajority(ch, 3, ensemble, totalResponses, result, removedResponse, make(map[proto.Feature]bool))

	assert.NoError(t, err)
	assert.Equal(t, 3, totalResponses)
	assert.Len(t, result, 2)
	assert.Contains(t, result, server1)
	assert.Contains(t, result, server2)
}

func TestElectionWaitForEnsembleMajorityFailureNoQuorum(t *testing.T) {
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	removedServer := testDataServer("removed")
	ensemble := []*proto.DataServerIdentity{server1, server2}

	failed := electionResponse{DataServer: server2, Err: errors.New("connection failed")}
	leader := electionResponse{DataServer: server1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 2, Offset: 1}}}
	removed := electionResponse{DataServer: removedServer, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 0}}}

	for name, responses := range map[string][]electionResponse{
		"failure before the majority": {failed, leader, removed},
		"failure after the majority":  {leader, removed, failed},
	} {
		t.Run(name, func(t *testing.T) {
			e := &election{}
			ch := make(chan electionResponse, 3)
			for _, r := range responses {
				ch <- r
			}

			removedResponse := make(map[*proto.DataServerIdentity]*proto.EntryId)
			result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble, removedResponse, make(map[proto.Feature]bool))
			assert.NoError(t, err)

			totalResponses, err = e.waitForEnsembleMajority(ch, 3, ensemble, totalResponses, result, removedResponse, make(map[proto.Feature]bool))

			assert.ErrorContains(t, err, "election failed: quorum of the new ensemble not reached")
			assert.Equal(t, 3, totalResponses)
		})
	}
}

func TestElectionWaitForEnsembleMajorityAlreadyReached(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	server3 := testDataServer("server3")
	removedServer := testDataServer("removed")
	ensemble := []*proto.DataServerIdentity{server1, server2, server3}

	ch := make(chan electionResponse, 4)
	ch <- electionResponse{DataServer: server1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 100}}}
	ch <- electionResponse{DataServer: removedServer, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 100}}}
	ch <- electionResponse{DataServer: server2, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 95}}}
	ch <- electionResponse{DataServer: server3, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 90}}}

	removedResponse := make(map[*proto.DataServerIdentity]*proto.EntryId)
	result, totalResponses, err := e.waitForMajority(ch, 4, 3, ensemble, removedResponse, make(map[proto.Feature]bool))
	assert.NoError(t, err)
	assert.Equal(t, 3, totalResponses)

	// With an odd ensemble, the fencing majority already includes a majority
	// of the ensemble
	totalResponses, err = e.waitForEnsembleMajority(ch, 4, ensemble, totalResponses, result, removedResponse, make(map[proto.Feature]bool))

	assert.NoError(t, err)
	assert.Equal(t, 3, totalResponses, "should not wait for more responses")
	assert.Len(t, result, 2)
}

func TestElectionWaitForGracePeriodAllResponsesReceived(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	server3 := testDataServer("server3")
	ensemble := []*proto.DataServerIdentity{server1, server2, server3}

	candidatesResponse := map[*proto.DataServerIdentity]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
	}

	ch := make(chan electionResponse, 3)
	ch <- electionResponse{DataServer: server2, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 95}}}
	ch <- electionResponse{DataServer: server3, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 90}}}

	e.waitForGracePeriod(ch, 3, ensemble, 1, candidatesResponse, make(map[*proto.DataServerIdentity]*proto.EntryId), make(map[proto.Feature]bool))

	assert.Len(t, candidatesResponse, 3, "all servers should be in the result")
	assert.Equal(t, int64(100), candidatesResponse[server1].Offset)
	assert.Equal(t, int64(95), candidatesResponse[server2].Offset)
	assert.Equal(t, int64(90), candidatesResponse[server3].Offset)
}

func TestElectionWaitForGracePeriodTimeout(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	ensemble := []*proto.DataServerIdentity{server1, server2}

	candidatesResponse := map[*proto.DataServerIdentity]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
	}

	ch := make(chan electionResponse, 3)

	start := time.Now()
	e.waitForGracePeriod(ch, 3, ensemble, 1, candidatesResponse, make(map[*proto.DataServerIdentity]*proto.EntryId), make(map[proto.Feature]bool))
	elapsed := time.Since(start)

	assert.Len(t, candidatesResponse, 1, "should only have initial server")
	assert.GreaterOrEqual(t, elapsed, quorumFencingGracePeriod, "should wait at least grace period")
	assert.Less(t, elapsed, quorumFencingGracePeriod+50*time.Millisecond, "should not wait much longer")
}

func TestElectionWaitForGracePeriodIgnoresErrors(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	server3 := testDataServer("server3")
	ensemble := []*proto.DataServerIdentity{server1, server2, server3}

	candidatesResponse := map[*proto.DataServerIdentity]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
	}

	ch := make(chan electionResponse, 3)
	ch <- electionResponse{DataServer: server2, Err: errors.New("connection failed")}
	ch <- electionResponse{DataServer: server3, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 90}}}

	e.waitForGracePeriod(ch, 3, ensemble, 1, candidatesResponse, make(map[*proto.DataServerIdentity]*proto.EntryId), make(map[proto.Feature]bool))

	assert.Len(t, candidatesResponse, 2, "should have initial server and successful response")
	assert.Contains(t, candidatesResponse, server1)
	assert.Contains(t, candidatesResponse, server3)
	assert.NotContains(t, candidatesResponse, server2, "failed server should not be added")
}

func TestElectionWaitForGracePeriodExcludesRemovedServers(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	removedServer := testDataServer("removed")
	ensemble := []*proto.DataServerIdentity{server1, server2}

	candidatesResponse := map[*proto.DataServerIdentity]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
	}

	ch := make(chan electionResponse, 3)
	ch <- electionResponse{DataServer: removedServer, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 110}}}
	ch <- electionResponse{DataServer: server2, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 95}}}

	removed := make(map[*proto.DataServerIdentity]*proto.EntryId)
	e.waitForGracePeriod(ch, 3, ensemble, 1, candidatesResponse, removed, make(map[proto.Feature]bool))

	assert.Len(t, candidatesResponse, 2)
	assert.Contains(t, candidatesResponse, server1)
	assert.Contains(t, candidatesResponse, server2)
	assert.NotContains(t, candidatesResponse, removedServer, "removed server should not be in result")
	assert.Len(t, removed, 1)
	assert.Equal(t, int64(110), removed[removedServer].Offset)
}

func TestElectionWaitForGracePeriodAlreadyComplete(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	ensemble := []*proto.DataServerIdentity{server1, server2}

	candidatesResponse := map[*proto.DataServerIdentity]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
		server2: {Term: 1, Offset: 95},
	}

	ch := make(chan electionResponse, 2)

	start := time.Now()
	e.waitForGracePeriod(ch, 2, ensemble, 2, candidatesResponse, make(map[*proto.DataServerIdentity]*proto.EntryId), make(map[proto.Feature]bool))
	elapsed := time.Since(start)

	assert.Len(t, candidatesResponse, 2)
	assert.Less(t, elapsed, 10*time.Millisecond, "should return immediately when all responses received")
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

func TestElectionCheckRemovedEntriesReplicationFactor3(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")

	// RF=3: s2 is swapped for s4, an entry is committed once 2 members have it
	tests := []struct {
		name       string
		candidates map[*proto.DataServerIdentity]*proto.EntryId
		removed    map[*proto.DataServerIdentity]*proto.EntryId
		loses      bool
	}{
		{
			name:       "removed data server not ahead",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{s1: {Term: 2, Offset: 5}, s4: {Term: -1, Offset: -1}},
			removed:    map[*proto.DataServerIdentity]*proto.EntryId{s2: {Term: 2, Offset: 5}},
		},
		{
			name:       "removed data server not fenced",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{s3: {Term: 2, Offset: 4}, s4: {Term: -1, Offset: -1}},
			removed:    map[*proto.DataServerIdentity]*proto.EntryId{},
		},
		{
			name:       "removed data server ahead and an old member not fenced",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{s3: {Term: 2, Offset: 4}, s4: {Term: -1, Offset: -1}},
			removed:    map[*proto.DataServerIdentity]*proto.EntryId{s2: {Term: 2, Offset: 5}},
			loses:      true,
		},
		{
			name:       "removed data server ahead by term and an old member not fenced",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{s3: {Term: 2, Offset: 9}, s4: {Term: -1, Offset: -1}},
			removed:    map[*proto.DataServerIdentity]*proto.EntryId{s2: {Term: 3, Offset: 5}},
			loses:      true,
		},
		{
			// Its entries beyond the candidates were acked by no other member
			name: "removed data server ahead and all members fenced",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{
				s1: {Term: 2, Offset: 4}, s3: {Term: 2, Offset: 4}, s4: {Term: -1, Offset: -1},
			},
			removed: map[*proto.DataServerIdentity]*proto.EntryId{s2: {Term: 2, Offset: 5}},
		},
		{
			// The added data server was not a member when the entries were written
			name:       "removed data server ahead and the added one not fenced",
			candidates: map[*proto.DataServerIdentity]*proto.EntryId{s1: {Term: 2, Offset: 4}, s3: {Term: 2, Offset: 4}},
			removed:    map[*proto.DataServerIdentity]*proto.EntryId{s2: {Term: 2, Offset: 5}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &election{
				logger: slog.Default(),
				mutableShardMetadata: &proto.ShardMetadata{
					Term:         3,
					Ensemble:     []*proto.DataServerIdentity{s1, s3, s4},
					RemovedNodes: []*proto.DataServerIdentity{s2},
				},
				changeEnsembleAction: action.NewChangeEnsembleAction(5, s2, s4),
			}
			err := e.checkRemovedEntries(tt.candidates, tt.removed)
			if tt.loses {
				assert.ErrorIs(t, err, errs.ErrInvalidArgument)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestElectionCheckRemovedEntriesReplicationFactor5(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")
	s5 := testDataServer("s5")
	s6 := testDataServer("s6")
	e := &election{
		logger: slog.Default(),
		mutableShardMetadata: &proto.ShardMetadata{
			Term:         3,
			Ensemble:     []*proto.DataServerIdentity{s1, s2, s3, s4, s6},
			RemovedNodes: []*proto.DataServerIdentity{s5},
		},
		changeEnsembleAction: action.NewChangeEnsembleAction(5, s5, s6),
	}
	removed := map[*proto.DataServerIdentity]*proto.EntryId{s5: {Term: 2, Offset: 5}}

	// s5 and s1 cannot form a write quorum of 3
	assert.NoError(t, e.checkRemovedEntries(map[*proto.DataServerIdentity]*proto.EntryId{
		s2: {Term: 2, Offset: 4}, s3: {Term: 2, Offset: 4}, s4: {Term: 2, Offset: 4}, s6: {Term: -1, Offset: -1},
	}, removed))

	// s5, s1 and s2 can
	assert.ErrorIs(t, e.checkRemovedEntries(map[*proto.DataServerIdentity]*proto.EntryId{
		s3: {Term: 2, Offset: 4}, s4: {Term: 2, Offset: 4}, s6: {Term: -1, Offset: -1},
	}, removed), errs.ErrInvalidArgument)
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

			_, err := e.start()
			assert.ErrorContains(t, err, "config unavailable")
			assert.EqualValues(t, 2, shardTerm(base, constant.DefaultNamespace, shard))
			assert.Empty(t, rpc.GetNode(s1).NewTermRequests)
		})
	}
}

// The election fences the ensemble at the addresses the data servers are
// registered with.
func TestElectionRefreshedEnsembleUsesRegisteredAddresses(t *testing.T) {
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

	ensemble, err := e.refreshedEnsemble([]*proto.DataServerIdentity{oldIdentity, unregistered})
	require.NoError(t, err)
	assertShardEnsemble(t, ensemble, newIdentity, unregistered)
}

// When the removed data servers make the fencing majority, a majority of the
// new ensemble must still be fenced.
func TestElectionFenceNewTermQuorumRequiresEnsembleMajority(t *testing.T) {
	s1 := testDataServer("s1")
	s2 := testDataServer("s2")
	removed := testDataServer("removed")
	rpc := rpctest.NewRpcProvider()
	e := newTestElection(t, nil, rpc, 5, &proto.ShardMetadata{})

	rpc.GetNode(s1).NewTermResponse(1, 0, nil)
	rpc.GetNode(removed).NewTermResponse(1, 0, nil)
	rpc.GetNode(s2).NewTermResponse(0, 0, errors.New("unreachable"))

	_, _, _, err := e.fenceNewTermQuorum(2, &proto.NewTermOptions{}, []*proto.DataServerIdentity{s1, s2},
		[]*proto.DataServerIdentity{removed})
	assert.ErrorContains(t, err, "quorum of the new ensemble not reached")
}

func TestElectionWaitForEnsembleMajorityCollectsRemovedServers(t *testing.T) {
	e := &election{}
	server1 := testDataServer("server1")
	server2 := testDataServer("server2")
	removed1 := testDataServer("removed1")
	removed2 := testDataServer("removed2")
	removed3 := testDataServer("removed3")
	ensemble := []*proto.DataServerIdentity{server1, server2}

	ch := make(chan electionResponse, 5)
	ch <- electionResponse{DataServer: server1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 10}}}
	ch <- electionResponse{DataServer: removed1, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 10}}}
	ch <- electionResponse{DataServer: removed2, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 10}}}
	ch <- electionResponse{DataServer: removed3, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 11}}}
	ch <- electionResponse{DataServer: server2, Response: &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: 1, Offset: 9}}}

	removedResponse := make(map[*proto.DataServerIdentity]*proto.EntryId)
	result, totalResponses, err := e.waitForMajority(ch, 5, 3, ensemble, removedResponse, make(map[proto.Feature]bool))
	require.NoError(t, err)
	assert.Equal(t, 3, totalResponses)

	totalResponses, err = e.waitForEnsembleMajority(ch, 5, ensemble, totalResponses, result, removedResponse,
		make(map[proto.Feature]bool))
	require.NoError(t, err)
	assert.Equal(t, 5, totalResponses)
	assert.Len(t, result, 2)
	assert.Len(t, removedResponse, 3)
	assert.Equal(t, int64(11), removedResponse[removed3].Offset)
}

func TestElectionSelectNewLeaderFailsWithoutLeader(t *testing.T) {
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	e := newTestElection(t, metadata, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{})

	_, _, err := e.selectNewLeader(map[*proto.DataServerIdentity]*proto.EntryId{})
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
			m.patchNamespaceStatus = func(namespace string, modify func(*proto.NamespaceStatus) bool) error {
				// The new term is written, the new leader is not
				if writes++; writes > 1 {
					return errors.New("store unavailable")
				}
				return m.Metadata.PatchNamespaceStatus(namespace, modify)
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

			_, err := e.start()
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
		logger:               slog.Default(),
		changeEnsembleAction: action.NewChangeEnsembleAction(5, s3, s4),
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

// The data server that an aborted change ensemble was adding is listed once
// among the data servers to delete the shard from.
func TestElectionAbortChangeEnsembleKeepsPendingDeletesUnique(t *testing.T) {
	s3 := testDataServer("s3")
	s4 := testDataServer("s4")
	abortErr := errors.New("aborted")
	e := &election{
		logger:               slog.Default(),
		changeEnsembleAction: action.NewChangeEnsembleAction(5, s3, s4),
	}
	retryShardMetadata := &proto.ShardMetadata{PendingDeleteShardNodes: []*proto.DataServerIdentity{s4}}

	e.abortChangeEnsemble(retryShardMetadata, abortErr)

	assertShardEnsemble(t, retryShardMetadata.PendingDeleteShardNodes, s4)
	assert.Nil(t, e.changeEnsembleAction)
	assert.ErrorIs(t, e.changeEnsembleErr, abortErr)
}

// The election of a shard that was removed meanwhile fails instead of
// bringing the shard back.
func TestElectionPersistFailsWhenShardIsRemoved(t *testing.T) {
	var shard int64 = 5
	metadata := newTestMetadata(t, memory.NewProvider(metadatacodec.ClusterStatusCodec, metadatacommon.WatchDisabled, ""),
		&proto.ClusterConfiguration{})
	storeTestShardMetadata(t, metadata, constant.DefaultNamespace, shard+1, namespaceConfig, &proto.ShardMetadata{
		Status:   proto.ShardStatusSteadyState,
		Term:     1,
		Ensemble: []*proto.DataServerIdentity{testDataServer("s1")},
	})
	e := newTestElection(t, metadata, rpctest.NewRpcProvider(), shard, &proto.ShardMetadata{
		Status:   proto.ShardStatusElection,
		Term:     2,
		Ensemble: []*proto.DataServerIdentity{testDataServer("s1")},
	})

	assert.ErrorContains(t, e.persistNewTerm(), "failed to persist the new term: shard 5 not found")
	assert.ErrorContains(t, e.persistNewLeader(), "failed to persist the new leader: shard 5 not found")
	_, exists := testShardMetadata(metadata, constant.DefaultNamespace, shard)
	assert.False(t, exists)
}

func TestElectionPersistNewLeaderReturnsWriteError(t *testing.T) {
	e := newTestElection(t, statusWriteFailingMetadata{}, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{})
	err := e.persistNewLeader()
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorContains(t, err, "failed to persist the new leader")
}

func TestElectionStartPanicsWhenStartedTwice(t *testing.T) {
	e := newTestElection(t, nil, rpctest.NewRpcProvider(), 5, &proto.ShardMetadata{})
	e.started.Store(true)
	assert.PanicsWithValue(t, "bug! the election has been started", func() { _, _ = e.Start() })
}

type electionResponse = fenceResponse

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
		rpc, constant.DefaultNamespace, shard, shardMeta, nil,
		metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency",
			"The time it takes to elect a leader for the shard", labels),
		metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency",
			"The time it takes to take the ensemble of data servers to a new term", labels),
		metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency",
			"The time it takes for the new elected leader to start", labels),
		metric.NewCounter("oxia_coordinator_leader_election_failed",
			"The number of failed leader elections", "count", labels)).(*election)
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
	patchNamespaceStatus    func(namespace string, modify func(*proto.NamespaceStatus) bool) error
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

func (m *hookedMetadata) PatchNamespaceStatus(namespace string, modify func(*proto.NamespaceStatus) bool) error {
	if m.patchNamespaceStatus != nil {
		return m.patchNamespaceStatus(namespace, modify)
	}
	return m.Metadata.PatchNamespaceStatus(namespace, modify)
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

// statusWriteFailingMetadata fails the shard status writes, as the metadata
// does when it gives up retrying them because the coordinator is closing.
type statusWriteFailingMetadata struct {
	coordmetadata.Metadata
}

func (statusWriteFailingMetadata) UpdateShardStatus(string, int64, *proto.ShardMetadata) error {
	return context.Canceled
}

func (statusWriteFailingMetadata) PatchNamespaceStatus(string, func(*proto.NamespaceStatus) bool) error {
	return context.Canceled
}
