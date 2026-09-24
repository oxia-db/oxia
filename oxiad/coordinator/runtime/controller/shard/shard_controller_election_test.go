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
	"log/slog"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/action"
)

func testDataServer(id string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{Internal: id, Public: id}
}

type electionResponse = fenceResponse

func TestNegotiate_EmptyInput(t *testing.T) {
	result := negotiate(nil, 0)
	assert.Nil(t, result)

	result = negotiate(map[string][]proto.Feature{}, 0)
	assert.Nil(t, result)
}

func TestNegotiate_SingleNode(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 1)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
}

func TestNegotiate_AllNodesSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node3": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
}

func TestNegotiate_PartialSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node3": {},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result)
}

func TestNegotiate_IgnoresUnknownFeature(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 2)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
	assert.NotContains(t, result, proto.Feature_FEATURE_UNKNOWN)
}

func TestNegotiate_HandlesDuplicates(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM, proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 2)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, result)
}

func TestNegotiate_NoCommonFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {},
		"node3": {},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result)
}

func TestNegotiate_OldNodeWithNoFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"new-node-2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"old-node":   nil,
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "should not enable features when old nodes are present")
}

func TestNegotiate_NilNodeFeatures(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node3": nil,
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "nil feature info must not count as support")
}

func TestNegotiate_MissingFeatureInfo(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_DB_CHECKSUM},
		"node2": {proto.Feature_FEATURE_DB_CHECKSUM},
	}

	result := negotiate(nodeFeatures, 3)
	assert.Empty(t, result, "missing feature info must not count as support")
}

func TestNoOpSupportedFeaturesSupplier(t *testing.T) {
	result := NoOpSupportedFeaturesSupplier(nil)
	assert.NotNil(t, result)
	assert.Empty(t, result)

	server := testDataServer("node1")
	result = NoOpSupportedFeaturesSupplier([]*proto.DataServerIdentity{server})
	assert.Contains(t, result, server.GetNameOrDefault())
	assert.Empty(t, result[server.GetNameOrDefault()])
}

func TestNegotiate_MixedVersions_RollingUpgrade(t *testing.T) {
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

func TestNegotiate_SecondaryIndexValidationRequiresFullUpgrade(t *testing.T) {
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

func TestWaitForMajority_Success(t *testing.T) {
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

func TestWaitForMajority_FailureNoQuorum(t *testing.T) {
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

func TestWaitForMajority_MixedSuccessAndFailure(t *testing.T) {
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

func TestWaitForMajority_ExcludesRemovedServers(t *testing.T) {
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

func TestWaitForMajority_EarlyReturn(t *testing.T) {
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

func TestWaitForEnsembleMajority_WaitsForEnsembleMember(t *testing.T) {
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

func TestWaitForEnsembleMajority_FailureNoQuorum(t *testing.T) {
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

func TestWaitForEnsembleMajority_AlreadyReached(t *testing.T) {
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

func TestWaitForGracePeriod_AllResponsesReceived(t *testing.T) {
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

func TestWaitForGracePeriod_Timeout(t *testing.T) {
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

func TestWaitForGracePeriod_IgnoresErrors(t *testing.T) {
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

func TestWaitForGracePeriod_ExcludesRemovedServers(t *testing.T) {
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

func TestWaitForGracePeriod_AlreadyComplete(t *testing.T) {
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

func TestCheckRemovedEntries(t *testing.T) {
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

func TestCheckRemovedEntries_ReplicationFactor5(t *testing.T) {
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
