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
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
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

func TestFenceNewTermQuorum_EnsembleServersIncludedInResults(t *testing.T) {
	// Test that servers in the ensemble that respond successfully are included in results
	ctx := context.Background()
	provider := newMockRpcProvider()

	server1 := model.Server{Internal: "server1"}
	server2 := model.Server{Internal: "server2"}
	server3 := model.Server{Internal: "server3"}

	ensemble := []model.Server{server1, server2, server3}
	removedCandidates := []model.Server{}

	election := &ShardElection{
		Logger:               newTestLogger(),
		Context:              ctx,
		provider:             provider,
		namespace:            "test",
		shard:                1,
		newTermQuorumLatency: noOpLatencyHistogram(),
	}

	// Setup responses for all servers
	provider.GetNode(server1).NewTermResponse(1, 100, nil)
	provider.GetNode(server2).NewTermResponse(1, 100, nil)
	provider.GetNode(server3).NewTermResponse(1, 100, nil)

	results, err := election.fenceNewTermQuorum(1, ensemble, removedCandidates)

	assert.NoError(t, err)
	assert.Len(t, results, 3)
	assert.Contains(t, results, server1)
	assert.Contains(t, results, server2)
	assert.Contains(t, results, server3)
}

func TestFenceNewTermQuorum_RemovedServersExcludedFromResults(t *testing.T) {
	// Test that servers in removedCandidates are excluded from results even if they respond successfully
	// This is the key bug fix being tested
	ctx := context.Background()
	provider := newMockRpcProvider()

	server1 := model.Server{Internal: "server1"}
	server2 := model.Server{Internal: "server2"}
	server3 := model.Server{Internal: "server3"}
	removedServer := model.Server{Internal: "removed-server"}

	ensemble := []model.Server{server1, server2, server3}
	removedCandidates := []model.Server{removedServer}

	election := &ShardElection{
		Logger:               newTestLogger(),
		Context:              ctx,
		provider:             provider,
		namespace:            "test",
		shard:                1,
		newTermQuorumLatency: noOpLatencyHistogram(),
	}

	// Setup responses - all servers including removed one respond successfully
	provider.GetNode(server1).NewTermResponse(1, 100, nil)
	provider.GetNode(server2).NewTermResponse(1, 100, nil)
	provider.GetNode(server3).NewTermResponse(1, 100, nil)
	provider.GetNode(removedServer).NewTermResponse(1, 100, nil)

	results, err := election.fenceNewTermQuorum(1, ensemble, removedCandidates)

	assert.NoError(t, err)
	assert.Len(t, results, 3, "only ensemble servers should be in results")
	assert.Contains(t, results, server1)
	assert.Contains(t, results, server2)
	assert.Contains(t, results, server3)
	assert.NotContains(t, results, removedServer, "removed server should not be in results even though it responded successfully")
}

func TestFenceNewTermQuorum_ErrorsAccumulated(t *testing.T) {
	// Test that errors are accumulated and servers with errors are not in results
	ctx := context.Background()
	provider := newMockRpcProvider()

	server1 := model.Server{Internal: "server1"}
	server2 := model.Server{Internal: "server2"}
	server3 := model.Server{Internal: "server3"}

	ensemble := []model.Server{server1, server2, server3}
	removedCandidates := []model.Server{}

	election := &ShardElection{
		Logger:               newTestLogger(),
		Context:              ctx,
		provider:             provider,
		namespace:            "test",
		shard:                1,
		newTermQuorumLatency: noOpLatencyHistogram(),
	}

	// Setup responses - server1 succeeds, server2 and server3 fail
	provider.GetNode(server1).NewTermResponse(1, 100, nil)
	provider.GetNode(server2).NewTermResponse(0, 0, errors.New("server2 error"))
	provider.GetNode(server3).NewTermResponse(0, 0, errors.New("server3 error"))

	results, err := election.fenceNewTermQuorum(1, ensemble, removedCandidates)

	// Should fail because we don't have quorum (only 1 out of 3 succeeded)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server2 error")
	assert.Contains(t, err.Error(), "server3 error")
	assert.Len(t, results, 0, "no results when quorum is not reached")
}

func TestFenceNewTermQuorum_QuorumWithRemovedServers(t *testing.T) {
	// Test that quorum calculation includes removed servers but they are excluded from results
	ctx := context.Background()
	provider := newMockRpcProvider()

	server1 := model.Server{Internal: "server1"}
	server2 := model.Server{Internal: "server2"}
	server3 := model.Server{Internal: "server3"}
	removedServer := model.Server{Internal: "removed-server"}

	ensemble := []model.Server{server1, server2, server3}
	removedCandidates := []model.Server{removedServer}

	election := &ShardElection{
		Logger:               newTestLogger(),
		Context:              ctx,
		provider:             provider,
		namespace:            "test",
		shard:                1,
		newTermQuorumLatency: noOpLatencyHistogram(),
	}

	// Setup responses - majority responds (3 out of 4), including the removed server
	provider.GetNode(server1).NewTermResponse(1, 100, nil)
	provider.GetNode(server2).NewTermResponse(1, 100, nil)
	provider.GetNode(removedServer).NewTermResponse(1, 100, nil)
	provider.GetNode(server3).NewTermResponse(0, 0, errors.New("server3 error"))

	results, err := election.fenceNewTermQuorum(1, ensemble, removedCandidates)

	assert.NoError(t, err)
	assert.Len(t, results, 2, "only successful ensemble servers in results")
	assert.Contains(t, results, server1)
	assert.Contains(t, results, server2)
	assert.NotContains(t, results, removedServer, "removed server should not be in results")
	assert.NotContains(t, results, server3, "failed server should not be in results")
}

func TestFenceNewTermQuorum_GracePeriodExcludesRemovedServers(t *testing.T) {
	// Test that during grace period after quorum, removed servers are still excluded from results
	ctx := context.Background()
	provider := newMockRpcProvider()

	server1 := model.Server{Internal: "server1"}
	server2 := model.Server{Internal: "server2"}
	server3 := model.Server{Internal: "server3"}
	removedServer := model.Server{Internal: "removed-server"}

	ensemble := []model.Server{server1, server2, server3}
	removedCandidates := []model.Server{removedServer}

	election := &ShardElection{
		Logger:               newTestLogger(),
		Context:              ctx,
		provider:             provider,
		namespace:            "test",
		shard:                1,
		newTermQuorumLatency: noOpLatencyHistogram(),
	}

	// First 3 servers respond quickly to establish quorum
	provider.GetNode(server1).NewTermResponse(1, 100, nil)
	provider.GetNode(server2).NewTermResponse(1, 100, nil)
	provider.GetNode(server3).NewTermResponse(1, 100, nil)
	// Removed server responds during grace period
	provider.GetNode(removedServer).NewTermResponse(1, 100, nil)

	results, err := election.fenceNewTermQuorum(1, ensemble, removedCandidates)

	assert.NoError(t, err)
	assert.Len(t, results, 3, "only ensemble servers in results")
	assert.Contains(t, results, server1)
	assert.Contains(t, results, server2)
	assert.Contains(t, results, server3)
	assert.NotContains(t, results, removedServer, "removed server should not be in results even during grace period")
}

func TestFenceNewTermQuorum_FailsWithoutQuorum(t *testing.T) {
	// Test that the function fails when quorum is not reached
	ctx := context.Background()
	provider := newMockRpcProvider()

	server1 := model.Server{Internal: "server1"}
	server2 := model.Server{Internal: "server2"}
	server3 := model.Server{Internal: "server3"}

	ensemble := []model.Server{server1, server2, server3}
	removedCandidates := []model.Server{}

	election := &ShardElection{
		Logger:               newTestLogger(),
		Context:              ctx,
		provider:             provider,
		namespace:            "test",
		shard:                1,
		newTermQuorumLatency: noOpLatencyHistogram(),
	}

	// Setup responses - only 1 out of 3 succeeds (not a majority)
	provider.GetNode(server1).NewTermResponse(1, 100, nil)
	provider.GetNode(server2).NewTermResponse(0, 0, errors.New("connection error"))
	provider.GetNode(server3).NewTermResponse(0, 0, errors.New("timeout error"))

	results, err := election.fenceNewTermQuorum(1, ensemble, removedCandidates)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fenceNewTerm shard")
	assert.Empty(t, results)
}

// newTestLogger creates a logger for tests
func newTestLogger() *slog.Logger {
	return slog.Default()
}

// noOpLatencyHistogram creates a no-op latency histogram for tests
func noOpLatencyHistogram() metric.LatencyHistogram {
	return metric.NewLatencyHistogram("test", "test histogram", nil)
}
