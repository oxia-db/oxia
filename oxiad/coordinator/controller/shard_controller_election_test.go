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

package controller

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"

	"github.com/oxia-db/oxia/common/proto"
)

func TestNegotiate_EmptyInput(t *testing.T) {
	result := negotiate(nil)
	assert.Nil(t, result)

	result = negotiate(map[string][]proto.Feature{})
	assert.Nil(t, result)
}

func TestNegotiate_SingleNode(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
	}

	result := negotiate(nodeFeatures)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}, result)
}

func TestNegotiate_AllNodesSupport(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_FINGERPRINT},
		"node3": {proto.Feature_FEATURE_FINGERPRINT},
	}

	result := negotiate(nodeFeatures)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}, result)
}

func TestNegotiate_PartialSupport(t *testing.T) {
	// Only node1 and node2 support FINGERPRINT, node3 doesn't
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_FINGERPRINT},
		"node3": {},
	}

	result := negotiate(nodeFeatures)
	assert.Empty(t, result)
}

func TestNegotiate_IgnoresUnknownFeature(t *testing.T) {
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_UNKNOWN, proto.Feature_FEATURE_FINGERPRINT},
	}

	result := negotiate(nodeFeatures)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}, result)
	assert.NotContains(t, result, proto.Feature_FEATURE_UNKNOWN)
}

func TestNegotiate_HandlesDuplicates(t *testing.T) {
	// Node1 has FINGERPRINT listed twice
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT, proto.Feature_FEATURE_FINGERPRINT},
		"node2": {proto.Feature_FEATURE_FINGERPRINT},
	}

	result := negotiate(nodeFeatures)
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}, result)
}

func TestNegotiate_NoCommonFeatures(t *testing.T) {
	// Simulate a scenario where nodes have no features in common
	// (using empty lists to represent old nodes)
	nodeFeatures := map[string][]proto.Feature{
		"node1": {proto.Feature_FEATURE_FINGERPRINT},
		"node2": {},
		"node3": {},
	}

	result := negotiate(nodeFeatures)
	assert.Empty(t, result)
}

func TestNegotiate_OldNodeWithNoFeatures(t *testing.T) {
	// Simulates rolling upgrade scenario where some nodes are old
	// and report no features (or empty features)
	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": {proto.Feature_FEATURE_FINGERPRINT},
		"new-node-2": {proto.Feature_FEATURE_FINGERPRINT},
		"old-node":   nil, // Old node doesn't report any features
	}

	result := negotiate(nodeFeatures)
	assert.Empty(t, result, "should not enable features when old nodes are present")
}

func TestNoOpSupportedFeaturesSupplier(t *testing.T) {
	result := NoOpSupportedFeaturesSupplier(nil)
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

func TestNegotiate_MixedVersions_RollingUpgrade(t *testing.T) {
	// Simulate a rolling upgrade scenario:
	// - 2 new nodes support FINGERPRINT
	// - 1 old node supports nothing

	nodeFeatures := map[string][]proto.Feature{
		"new-node-1": {proto.Feature_FEATURE_FINGERPRINT},
		"new-node-2": {proto.Feature_FEATURE_FINGERPRINT},
		"old-node":   {}, // Old node reports empty features
	}

	result := negotiate(nodeFeatures)
	assert.Empty(t, result, "features should not be enabled until all nodes are upgraded")

	// After upgrading the old node
	nodeFeatures["old-node"] = []proto.Feature{proto.Feature_FEATURE_FINGERPRINT}

	result = negotiate(nodeFeatures)
	assert.Contains(t, result, proto.Feature_FEATURE_FINGERPRINT, "feature should be enabled after all nodes are upgraded")
}

func TestWaitForMajority_Success(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	server3 := model.Server{Internal: "server3", Public: "server3"}
	ensemble := []model.Server{server1, server2, server3}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 3)

	// Send 2 successful responses (majority for 3 servers)
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server1, &proto.EntryId{Term: 1, Offset: 100}, nil}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server2, &proto.EntryId{Term: 1, Offset: 95}, nil}

	result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble)

	assert.NoError(t, err)
	assert.Equal(t, 2, totalResponses)
	assert.Len(t, result, 2)
	assert.Equal(t, int64(100), result[server1].Offset)
	assert.Equal(t, int64(95), result[server2].Offset)
}

func TestWaitForMajority_FailureNoQuorum(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	server3 := model.Server{Internal: "server3", Public: "server3"}
	ensemble := []model.Server{server1, server2, server3}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 3)

	// Send 1 success and 2 errors (no majority)
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server1, &proto.EntryId{Term: 1, Offset: 100}, nil}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server2, nil, errors.New("connection failed")}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server3, nil, errors.New("timeout")}

	result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "election failed: quorum not reached")
	assert.Nil(t, result)
	assert.Equal(t, 3, totalResponses)
}

func TestWaitForMajority_MixedSuccessAndFailure(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	server3 := model.Server{Internal: "server3", Public: "server3"}
	ensemble := []model.Server{server1, server2, server3}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 3)

	// Send 2 successes and 1 error (reaches majority)
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server1, &proto.EntryId{Term: 1, Offset: 100}, nil}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server2, nil, errors.New("connection failed")}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server3, &proto.EntryId{Term: 1, Offset: 90}, nil}

	result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble)

	assert.NoError(t, err)
	assert.Equal(t, 3, totalResponses)
	assert.Len(t, result, 2)
	assert.Equal(t, int64(100), result[server1].Offset)
	assert.Equal(t, int64(90), result[server3].Offset)
}

func TestWaitForMajority_ExcludesRemovedServers(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	server3 := model.Server{Internal: "server3", Public: "server3"}
	removedServer := model.Server{Internal: "removed", Public: "removed"}
	ensemble := []model.Server{server1, server2, server3}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 4)

	// Send 3 responses including one from a removed server
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server1, &proto.EntryId{Term: 1, Offset: 100}, nil}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{removedServer, &proto.EntryId{Term: 1, Offset: 110}, nil}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server2, &proto.EntryId{Term: 1, Offset: 95}, nil}

	result, totalResponses, err := e.waitForMajority(ch, 4, 3, ensemble)

	assert.NoError(t, err)
	assert.Equal(t, 3, totalResponses)
	assert.Len(t, result, 2)
	assert.NotContains(t, result, removedServer, "removed server should not be in result")
	assert.Contains(t, result, server1)
	assert.Contains(t, result, server2)
}

func TestWaitForMajority_EarlyReturn(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	server3 := model.Server{Internal: "server3", Public: "server3"}
	ensemble := []model.Server{server1, server2, server3}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 3)

	// Send only 2 responses to reach majority
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server1, &proto.EntryId{Term: 1, Offset: 100}, nil}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server2, &proto.EntryId{Term: 1, Offset: 95}, nil}

	result, totalResponses, err := e.waitForMajority(ch, 3, 2, ensemble)

	assert.NoError(t, err)
	assert.Equal(t, 2, totalResponses, "should return early after reaching majority")
	assert.Len(t, result, 2)
}

func TestWaitForGracePeriod_AllResponsesReceived(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	server3 := model.Server{Internal: "server3", Public: "server3"}
	ensemble := []model.Server{server1, server2, server3}

	candidatesResponse := map[model.Server]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
	}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 3)

	// Send remaining responses
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server2, &proto.EntryId{Term: 1, Offset: 95}, nil}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server3, &proto.EntryId{Term: 1, Offset: 90}, nil}

	e.waitForGracePeriod(ch, 3, ensemble, 1, candidatesResponse)

	assert.Len(t, candidatesResponse, 3, "all servers should be in the result")
	assert.Equal(t, int64(100), candidatesResponse[server1].Offset)
	assert.Equal(t, int64(95), candidatesResponse[server2].Offset)
	assert.Equal(t, int64(90), candidatesResponse[server3].Offset)
}

func TestWaitForGracePeriod_Timeout(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	ensemble := []model.Server{server1, server2}

	candidatesResponse := map[model.Server]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
	}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 3)

	start := time.Now()
	e.waitForGracePeriod(ch, 3, ensemble, 1, candidatesResponse)
	elapsed := time.Since(start)

	assert.Len(t, candidatesResponse, 1, "should only have initial server")
	assert.GreaterOrEqual(t, elapsed, quorumFencingGracePeriod, "should wait at least grace period")
	assert.Less(t, elapsed, quorumFencingGracePeriod+50*time.Millisecond, "should not wait much longer")
}

func TestWaitForGracePeriod_IgnoresErrors(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	server3 := model.Server{Internal: "server3", Public: "server3"}
	ensemble := []model.Server{server1, server2, server3}

	candidatesResponse := map[model.Server]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
	}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 3)

	// Send one error and one success
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server2, nil, errors.New("connection failed")}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server3, &proto.EntryId{Term: 1, Offset: 90}, nil}

	e.waitForGracePeriod(ch, 3, ensemble, 1, candidatesResponse)

	assert.Len(t, candidatesResponse, 2, "should have initial server and successful response")
	assert.Contains(t, candidatesResponse, server1)
	assert.Contains(t, candidatesResponse, server3)
	assert.NotContains(t, candidatesResponse, server2, "failed server should not be added")
}

func TestWaitForGracePeriod_ExcludesRemovedServers(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	removedServer := model.Server{Internal: "removed", Public: "removed"}
	ensemble := []model.Server{server1, server2}

	candidatesResponse := map[model.Server]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
	}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 3)

	// Send responses including one from removed server
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{removedServer, &proto.EntryId{Term: 1, Offset: 110}, nil}
	ch <- struct {
		model.Server
		*proto.EntryId
		error
	}{server2, &proto.EntryId{Term: 1, Offset: 95}, nil}

	e.waitForGracePeriod(ch, 3, ensemble, 1, candidatesResponse)

	assert.Len(t, candidatesResponse, 2)
	assert.Contains(t, candidatesResponse, server1)
	assert.Contains(t, candidatesResponse, server2)
	assert.NotContains(t, candidatesResponse, removedServer, "removed server should not be in result")
}

func TestWaitForGracePeriod_AlreadyComplete(t *testing.T) {
	e := &ShardElection{}
	server1 := model.Server{Internal: "server1", Public: "server1"}
	server2 := model.Server{Internal: "server2", Public: "server2"}
	ensemble := []model.Server{server1, server2}

	candidatesResponse := map[model.Server]*proto.EntryId{
		server1: {Term: 1, Offset: 100},
		server2: {Term: 1, Offset: 95},
	}

	ch := make(chan struct {
		model.Server
		*proto.EntryId
		error
	}, 2)

	// Already have all responses, should return immediately
	start := time.Now()
	e.waitForGracePeriod(ch, 2, ensemble, 2, candidatesResponse)
	elapsed := time.Since(start)

	assert.Len(t, candidatesResponse, 2)
	assert.Less(t, elapsed, 10*time.Millisecond, "should return immediately when all responses received")
}
