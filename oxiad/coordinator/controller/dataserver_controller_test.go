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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

func TestDataServerController_HealthCheck(t *testing.T) {
	addr := &proto.DataServer{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}

	sap := newMockShardAssignmentsProvider()
	nal := newMockNodeAvailabilityListener()
	rpc := newMockRpcProvider()
	nc := newDataServerController(context.Background(), addr, sap, nal, rpc, "test-instance", 1*time.Second)

	node := rpc.GetNode(addr)

	// Controller starts as NotRunning and transitions to Running on first health check
	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 100*time.Millisecond)

	node.healthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	unavailableNode := <-nal.events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.Status())

	node.healthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)

	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 100*time.Millisecond)

	node.healthClient.SetError(errors.New("failed to connect"))

	unavailableNode = <-nal.events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.Status())

	assert.NoError(t, nc.Close())
}

func TestDataServerController_HandshakeOnlyCalledOnStateTransition(t *testing.T) {
	addr := &proto.DataServer{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}

	sap := newMockShardAssignmentsProvider()
	nal := newMockNodeAvailabilityListener()
	rpc := newMockRpcProvider()
	nc := newDataServerController(context.Background(), addr, sap, nal, rpc, "test-instance", 1*time.Second)

	node := rpc.GetNode(addr)

	// Wait for the initial Handshake call that happens when the controller starts
	// (the controller starts in Running state, transitions through health check)
	assert.Eventually(t, func() bool {
		return node.handshakeCount.Load() >= 1
	}, 10*time.Second, 100*time.Millisecond)

	// Record the count after initial startup
	initialCount := node.handshakeCount.Load()

	// Wait for several health check cycles (health check runs every 2s)
	// If Handshake were called on every health check, we'd see the count increase
	time.Sleep(5 * time.Second)

	// The count should NOT have increased while the server stayed Running
	countAfterWait := node.handshakeCount.Load()
	assert.Equal(t, initialCount, countAfterWait,
		"Handshake should not be called repeatedly while server is already Running")

	// Now simulate the server going down and coming back
	node.healthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	unavailableNode := <-nal.events
	assert.Equal(t, addr, unavailableNode)
	assert.Equal(t, NotRunning, nc.Status())

	// Bring the server back online
	node.healthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)

	// Handshake should have been called again for the NotRunning -> Running transition
	assert.Eventually(t, func() bool {
		return node.handshakeCount.Load() > countAfterWait
	}, 10*time.Second, 100*time.Millisecond,
		"Handshake should be called on state transition from NotRunning to Running")

	assert.Equal(t, Running, nc.Status())
	countAfterRecovery := node.handshakeCount.Load()

	// Wait again to confirm no further redundant calls
	time.Sleep(5 * time.Second)

	countAfterSecondWait := node.handshakeCount.Load()
	assert.Equal(t, countAfterRecovery, countAfterSecondWait,
		"Handshake should not be called repeatedly after recovery")

	assert.NoError(t, nc.Close())
}

func TestDataServerController_ShardsAssignments(t *testing.T) {
	addr := &proto.DataServer{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}

	sap := newMockShardAssignmentsProvider()
	nal := newMockNodeAvailabilityListener()
	rpc := newMockRpcProvider()
	nc := newDataServerController(context.Background(), addr, sap, nal, rpc, "test-instance", 1*time.Second)

	node := rpc.GetNode(addr)

	resp := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{{
					Shard:  0,
					Leader: "leader-0",
				}, {
					Shard:  1,
					Leader: "leader-1",
				}},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}

	sap.set(resp)

	update := <-node.shardAssignmentsStream.updates
	assert.Equal(t, resp, update)

	// Simulate 1 single stream send error
	node.shardAssignmentsStream.SetError(errors.New("failed to send"))

	resp2 := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{{
					Shard:  0,
					Leader: "leader-1",
				}, {
					Shard:  1,
					Leader: "leader-2",
				}},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}

	sap.set(resp2)

	update = <-node.shardAssignmentsStream.updates
	assert.Equal(t, resp2, update)

	assert.NoError(t, nc.Close())
}
