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

package shard

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector"
	leaderselector "github.com/oxia-db/oxia/oxiad/coordinator/balancer/selector/leader"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	coordrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
)

func TestNegotiateEmptyInput(t *testing.T) {
	result := negotiate(nil, 0)
	assert.Nil(t, result)

	result = negotiate(map[string][]proto.Feature{}, 0)
	assert.Nil(t, result)
}

func TestNegotiateSingleNode(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 1)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
}

func TestNegotiateAllNodesSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node3": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
}

func TestNegotiatePartialSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node3": {},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result)
}

func TestNegotiateIgnoresUnknownFeature(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 2)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
	assert.NotContains(t, result, proto.Feature_FEATURE_UNKNOWN)
}

func TestNegotiateHandlesDuplicates(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM, proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 2)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
}

func TestNegotiateNoCommonFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {},
		"node3": {},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result)
}

func TestNegotiateOldNodeWithNoFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"new-node-2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"old-node":   nil,
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "should not enable features when old nodes are present")
}

func TestNegotiateNilNodeFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node3": nil,
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "nil feature info must not count as support")
}

func TestNegotiateMissingFeatureInfo(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "missing feature info must not count as support")
}

func TestNegotiateMixedVersionsRollingUpgrade(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"new-node-2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"old-node":   {},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "features should not be enabled until all nodes are upgraded")

	nodeFeatures["old-node"] = []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}

	result = negotiate(nodeFeatures, 3)
	assert.Contains(t, result, proto.Feature_FEATURE_DB_CHECKSUM, "feature should be enabled after all nodes are upgraded")
}

func TestNegotiateSecondaryIndexValidationRequiresFullUpgrade(t *testing.T) {
	currentFeatures := []proto.Feature{
		proto.Feature_FEATURE_DB_CHECKSUM,
		proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION,
	}
	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": currentFeatures,
		"new-node-2": currentFeatures,
		"old-node":   {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Contains(t, result, proto.Feature_FEATURE_DB_CHECKSUM)
	assert.NotContains(t, result, proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION)

	nodeFeatures["old-node"] = currentFeatures
	result = negotiate(nodeFeatures, 3)
	assert.Contains(t, result, proto.Feature_FEATURE_SECONDARY_INDEX_NAME_VALIDATION)
}

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
	e := &Election{}
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
	e := &Election{}
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
	e := &Election{}
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
	e := &Election{}
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
	e := &Election{}
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
	e := &Election{}
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
			e := &Election{}
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
	e := &Election{}
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
	e := &Election{}
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
	e := &Election{}
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
	e := &Election{}
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
	e := &Election{}
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
	e := &Election{}
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
			e := &Election{
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
				assert.ErrorIs(t, err, ErrChangeEnsembleLosesEntries)
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
	e := &Election{
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
	}, removed), ErrChangeEnsembleLosesEntries)
}

// The split controller updates the parent while a leader election of the
// parent is fencing its ensemble: the election must store its outcome on top
// of these updates.
func TestElectionStartKeepsSplitUpdatesWhileFencing(t *testing.T) {
	for _, test := range []struct {
		name   string
		update func(parent *proto.ShardMetadata)
	}{{
		// CatchUp noticed that the parent's term changed
		name:   "back-to-bootstrap",
		update: func(parent *proto.ShardMetadata) { parent.Split.Phase = proto.SplitPhaseBootstrap },
	}, {
		name:   "abort",
		update: func(parent *proto.ShardMetadata) { parent.Split = nil },
	}} {
		t.Run(test.name, func(t *testing.T) {
			rpc, metadata, _ := setupSplitTest(t, proto.SplitPhaseCatchUp)
			elected := startElection(t, metadata, rpc, 0)

			expectParentFenced(t, rpc, 6)
			split := updateSplitParent(t, metadata, test.update)
			electParentLeader(t, rpc, elected)

			parent := requireShardMetadata(t, metadata, constant.DefaultNamespace, 0)
			assert.True(t, gproto.Equal(split, parent.GetSplit()), "split metadata: %v", parent.GetSplit())
			assert.Equal(t, proto.ShardStatusSteadyState, parent.GetStatus())
			assert.EqualValues(t, 6, parent.GetTerm())
			assert.Equal(t, ps2.GetPublic(), parent.GetLeader().GetPublic())
		})
	}
}

// The split is aborted while a leader election of the parent waits to retry,
// e.g. because its ensemble has no quorum: the next attempt must start from
// the current metadata of the parent, or the parent stays marked as being
// split, into children that no longer exist.
func TestElectionStartKeepsSplitUpdatesBetweenAttempts(t *testing.T) {
	rpc, metadata, _ := setupSplitTest(t, proto.SplitPhaseCatchUp)
	elected := startElection(t, metadata, rpc, 0)

	// The first attempt cannot fence a quorum
	expectParentFenced(t, rpc, 6)
	updateSplitParent(t, metadata, func(parent *proto.ShardMetadata) { parent.Split = nil })
	for _, node := range []*proto.DataServerIdentity{ps1, ps2, ps3} {
		rpc.GetNode(node).NewTermResponse(0, 0, errors.New("unreachable"))
	}

	expectParentFenced(t, rpc, 7)
	assert.Nil(t, requireShardMetadata(t, metadata, constant.DefaultNamespace, 0).GetSplit())
	electParentLeader(t, rpc, elected)

	parent := requireShardMetadata(t, metadata, constant.DefaultNamespace, 0)
	assert.Nil(t, parent.GetSplit())
	assert.Equal(t, proto.ShardStatusSteadyState, parent.GetStatus())
	assert.EqualValues(t, 7, parent.GetTerm())
	assert.Equal(t, ps2.GetPublic(), parent.GetLeader().GetPublic())
}

// Deleting the namespace marks the shard for deletion while its leader
// election is fencing the ensemble: the election must not bring the shard
// back, which would publish it again until its shard controller deletes it.
func TestElectionStartKeepsDeletingStatus(t *testing.T) {
	rpc, metadata, _ := setupSplitTest(t, proto.SplitPhaseCatchUp)
	elected := startElection(t, metadata, rpc, 0)

	expectParentFenced(t, rpc, 6)
	metadata.DeleteNamespaceStatus(constant.DefaultNamespace)
	electParentLeader(t, rpc, elected)

	shard := requireShardMetadata(t, metadata, constant.DefaultNamespace, 0)
	assert.Equal(t, proto.ShardStatusDeleting, shard.GetStatus())
	assert.EqualValues(t, 6, shard.GetTerm())
	assert.Equal(t, ps2.GetPublic(), shard.GetLeader().GetPublic())
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
	e := &Election{}
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
	assert.ErrorIs(t, err, selector.ErrNoCandidates)
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
	e := &Election{
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
	e := &Election{
		logger:               slog.Default(),
		changeEnsembleAction: action.NewChangeEnsembleAction(5, s3, s4),
	}
	retryShardMetadata := &proto.ShardMetadata{PendingDeleteShardNodes: []*proto.DataServerIdentity{s4}}

	e.abortChangeEnsemble(retryShardMetadata, abortErr)

	assertShardEnsemble(t, retryShardMetadata.PendingDeleteShardNodes, s4)
	assert.Nil(t, e.changeEnsembleAction)
	assert.ErrorIs(t, e.ChangeEnsembleError(), abortErr)
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
	assert.PanicsWithValue(t, "bug! the election has been started", func() { e.Start() })
}

type electionResponse = fenceResponse

func testDataServer(id string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{Internal: id, Public: id}
}

// startElection starts a leader election of the shard, as its shard
// controller does, and returns the elected leader once the election ends.
func startElection(t *testing.T, metadata coordmetadata.Metadata, rpc coordrpc.ControllerRPCProvider, shard int64) <-chan *proto.DataServerIdentity {
	t.Helper()
	labels := metric.LabelsForShard(constant.DefaultNamespace, shard)
	s := &controller{
		namespace:                           constant.DefaultNamespace,
		shard:                               shard,
		metadataStore:                       metadata,
		dataServerSupportedFeaturesSupplier: NoOpFeaturesSupplier,
		leaderSelector:                      leaderselector.NewSelector(),
		rpc:                                 rpc,
		logger:                              slog.Default(),
		leaderElectionLatencyHistogram: metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency",
			"The time it takes to elect a leader for the shard", labels),
		failedLeaderElectionsCounter: metric.NewCounter("oxia_coordinator_leader_election_failed",
			"The number of failed leader elections", "count", labels),
		newTermQuorumLatencyHistogram: metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency",
			"The time it takes to take the ensemble of data servers to a new term", labels),
		becomeLeaderLatencyHistogram: metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency",
			"The time it takes for the new elected leader to start", labels),
	}
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	shardMeta, _ := testShardMetadata(metadata, constant.DefaultNamespace, shard)
	elected := make(chan *proto.DataServerIdentity, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		elected <- s.electLeader(shardMeta, nil)
	}()
	t.Cleanup(func() {
		s.ctxCancel()
		<-done
		s.currentElection.Load().Stop()
	})
	return elected
}

// requireElectionEnded waits for the election to end, and returns the leader
// it elected.
func requireElectionEnded(t *testing.T, elected <-chan *proto.DataServerIdentity) *proto.DataServerIdentity {
	t.Helper()
	select {
	case leader := <-elected:
		return leader
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the election did not end in time")
		return nil
	}
}

// electParentLeader completes an election of the parent shard of
// setupSplitTest that is fencing its ensemble: ps2 has the highest entry, and
// becomes the leader.
func electParentLeader(t *testing.T, rpc *rpctest.RpcProvider, elected <-chan *proto.DataServerIdentity) {
	t.Helper()
	rpc.GetNode(ps1).NewTermResponse(5, 100, nil)
	rpc.GetNode(ps2).NewTermResponse(5, 105, nil)
	rpc.GetNode(ps3).NewTermResponse(5, 100, nil)
	rpc.GetNode(ps2).BecomeLeaderResponse(nil)
	assert.Equal(t, ps2.GetPublic(), requireElectionEnded(t, elected).GetPublic())
}

// expectParentFenced waits for the election to fence each member of the
// parent's ensemble at the term.
func expectParentFenced(t *testing.T, rpc *rpctest.RpcProvider, term int64) {
	t.Helper()
	for _, node := range []*proto.DataServerIdentity{ps1, ps2, ps3} {
		rpc.GetNode(node).ExpectNewTermRequest(t, 0, term, true)
	}
}

// updateSplitParent applies an update of the split controller to the parent
// shard of setupSplitTest, and returns the updated split metadata.
func updateSplitParent(t *testing.T, metadata coordmetadata.Metadata, update func(parent *proto.ShardMetadata)) *proto.SplitMetadata {
	t.Helper()
	require.NoError(t, metadata.PatchNamespaceStatus(constant.DefaultNamespace, func(status *proto.NamespaceStatus) bool {
		shards := status.Shards
		update(shards[0])
		return true
	}))
	return requireShardMetadata(t, metadata, constant.DefaultNamespace, 0).GetSplit()
}

// newTestElection creates an election of the shard, as its shard controller
// does, that the test drives step by step.
func newTestElection(t *testing.T, metadata coordmetadata.Metadata, rpc coordrpc.ControllerRPCProvider, shard int64,
	shardMeta *proto.ShardMetadata) *Election {
	t.Helper()
	labels := metric.LabelsForShard(constant.DefaultNamespace, shard)
	e := NewElection(t.Context(), slog.Default(), nil, metadata, NoOpFeaturesSupplier, leaderselector.NewSelector(),
		rpc, constant.DefaultNamespace, shard, shardMeta, nil,
		metric.NewLatencyHistogram("oxia_coordinator_leader_election_latency",
			"The time it takes to elect a leader for the shard", labels),
		metric.NewLatencyHistogram("oxia_coordinator_new_term_quorum_latency",
			"The time it takes to take the ensemble of data servers to a new term", labels),
		metric.NewLatencyHistogram("oxia_coordinator_become_leader_latency",
			"The time it takes for the new elected leader to start", labels),
		metric.NewCounter("oxia_coordinator_leader_election_failed",
			"The number of failed leader elections", "count", labels))
	t.Cleanup(e.Stop)
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
