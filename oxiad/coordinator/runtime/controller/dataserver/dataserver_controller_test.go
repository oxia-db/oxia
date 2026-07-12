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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/mockutils"
)

// testHealthPolicy keeps the probing cadence fast so the tests exercising the
// consecutive-failure threshold complete quickly.
var testHealthPolicy = healthCheckPolicy{
	probeInterval:    20 * time.Millisecond,
	probeTimeout:     1 * time.Second,
	failureThreshold: 3,
}

func expectShardAssignmentsUpdate(t *testing.T, updates <-chan *proto.ShardAssignments, expected *proto.ShardAssignments) {
	t.Helper()

	select {
	case update := <-updates:
		assert.True(t, pb.Equal(expected, update), "expected %v, got %v", expected, update)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "did not receive shard assignments update")
	}
}

func TestDataServerController_HealthCheck(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	// Controller starts as NotRunning and transitions to Running on first health check
	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 100*time.Millisecond)

	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	unavailableNode := <-nal.Events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.Status())

	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)

	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 100*time.Millisecond)

	node.HealthClient.SetError(errors.New("failed to connect"))

	unavailableNode = <-nal.Events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.Status())

	assert.NoError(t, nc.Close())
}

func TestDataServerController_ToleratesTransientHealthCheckFailures(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// Fail fewer consecutive probes than the threshold: a saturated node can
	// miss individual probe deadlines while being perfectly healthy.
	node.HealthClient.FailNextChecks(errors.New("context deadline exceeded"), testHealthPolicy.failureThreshold-1)

	// Wait until all scheduled failures have been consumed, plus a few more
	// successful probe rounds.
	assert.Eventually(t, func() bool {
		return node.HealthClient.PendingCheckFailures() == 0
	}, 10*time.Second, 10*time.Millisecond)
	time.Sleep(5 * testHealthPolicy.probeInterval)

	select {
	case <-nal.Events:
		assert.Fail(t, "probe failures below the threshold must not mark the node unavailable")
	default:
	}
	assert.Equal(t, Running, nc.Status())

	assert.NoError(t, nc.Close())
}

func TestDataServerController_WatchFailureAloneDoesNotFence(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// A watch stream reset while the node keeps answering pings (e.g. an
	// idle stream reset under load) must not fence the node.
	node.HealthClient.FailWatches(errors.New("stream reset"))

	time.Sleep(10 * testHealthPolicy.probeInterval)

	select {
	case <-nal.Events:
		assert.Fail(t, "a watch stream failure alone must not mark the node unavailable")
	default:
	}
	assert.Equal(t, Running, nc.Status())

	assert.NoError(t, nc.Close())
}

// slowPingHealthPolicy makes the ping cadence so slow that only the
// watch-failure verification path can possibly fence the node, so the tests
// below prove pod-death detection does not depend on the ping loop.
var slowPingHealthPolicy = healthCheckPolicy{
	probeInterval:    time.Hour,
	probeTimeout:     1 * time.Second,
	failureThreshold: 3,
}

func TestDataServerController_WatchFailureFencesDeadNode(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, slowPingHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// Simulate a killed pod: the watch stream breaks and every probe fails
	// fast with a hard transport error. The node must be fenced right away
	// through the watch-failure verification, without waiting for the ping
	// cadence (one hour in this test).
	node.HealthClient.SetError(grpcstatus.Error(codes.Unavailable, "connection refused"))

	select {
	case unavailableNode := <-nal.Events:
		assert.Equal(t, addr, unavailableNode)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "a dead node must be fenced when its watch stream breaks")
	}
	assert.Equal(t, NotRunning, nc.Status())

	assert.NoError(t, nc.Close())
}

func TestDataServerController_WatchFailureToleratesSlowNode(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, slowPingHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// The watch stream breaks and the verification probe times out: that is
	// the signature of a saturated node, not a dead one, so it must be left
	// to the ping loop's failure threshold instead of being fenced.
	node.HealthClient.FailNextChecks(grpcstatus.Error(codes.DeadlineExceeded, "context deadline exceeded"), 1)
	node.HealthClient.FailWatches(errors.New("stream reset"))

	select {
	case <-nal.Events:
		assert.Fail(t, "a slow verification probe must not fence the node")
	case <-time.After(500 * time.Millisecond):
	}
	assert.Equal(t, Running, nc.Status())

	assert.NoError(t, nc.Close())
}

func TestDataServerController_IsStablyRunning(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// A node that never failed a health check is stable regardless of the
	// window (initial cluster startup must not delay leader balancing).
	assert.True(t, nc.IsStablyRunning(time.Hour))

	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	<-nal.Events
	assert.False(t, nc.IsStablyRunning(0))

	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)
	assert.Eventually(t, func() bool {
		return nc.Status() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// After a recovery the node is Running but not yet stable for a long
	// window; it becomes stable once it stays Running past the window.
	assert.False(t, nc.IsStablyRunning(time.Hour))
	assert.Eventually(t, func() bool {
		return nc.IsStablyRunning(100 * time.Millisecond)
	}, 10*time.Second, 10*time.Millisecond)

	assert.NoError(t, nc.Close())
}

func TestDataServerController_HandshakeOnlyCalledOnStateTransition(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	// Wait for the initial Handshake call that happens when the controller starts
	// (the controller starts in Running state, transitions through health check)
	assert.Eventually(t, func() bool {
		return node.HandshakeCount.Load() >= 1
	}, 10*time.Second, 100*time.Millisecond)

	// Record the count after initial startup
	initialCount := node.HandshakeCount.Load()

	// Wait for several health check cycles (health check runs every 2s)
	// If Handshake were called on every health check, we'd see the count increase
	time.Sleep(5 * time.Second)

	// The count should NOT have increased while the server stayed Running
	countAfterWait := node.HandshakeCount.Load()
	assert.Equal(t, initialCount, countAfterWait,
		"Handshake should not be called repeatedly while server is already Running")

	// Now simulate the server going down and coming back
	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	unavailableNode := <-nal.Events
	assert.Equal(t, addr, unavailableNode)
	assert.Equal(t, NotRunning, nc.Status())

	// Bring the server back online
	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)

	// Handshake should have been called again for the NotRunning -> Running transition
	assert.Eventually(t, func() bool {
		return node.HandshakeCount.Load() > countAfterWait
	}, 10*time.Second, 100*time.Millisecond,
		"Handshake should be called on state transition from NotRunning to Running")

	assert.Equal(t, Running, nc.Status())
	countAfterRecovery := node.HandshakeCount.Load()

	// Wait again to confirm no further redundant calls
	time.Sleep(5 * time.Second)

	countAfterSecondWait := node.HandshakeCount.Load()
	assert.Equal(t, countAfterRecovery, countAfterSecondWait,
		"Handshake should not be called repeatedly after recovery")

	assert.NoError(t, nc.Close())
}

func TestDataServerController_ShardsAssignments(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)
	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, &proto.ShardAssignments{})

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

	sap.Set(resp)
	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, resp)

	// Simulate 1 single stream send error
	node.ShardAssignmentsStream.SetError(errors.New("failed to send"))

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

	sap.Set(resp2)
	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, resp2)

	assert.NoError(t, nc.Close())
}

func TestDataServerController_RetriesLatestAssignmentsAfterSendError(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	node := rpc.GetNode(addr)
	node.ShardAssignmentsStream.SetError(errors.New("failed to send"))
	resp := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{{
					Shard:  0,
					Leader: "leader-0",
				}},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}

	sap.Set(resp)

	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 10*time.Millisecond, testHealthPolicy)
	defer func() {
		assert.NoError(t, nc.Close())
	}()

	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, resp)
}

// TestDataServerController_ReestablishesStreamAfterServerEnd reproduces the bug
// behind the "stuck shard assignment streams" fix: the server ends the assignment
// RPC while the underlying connection stays healthy and the assignment snapshot is
// unchanged. Because the coordinator only ever sends on the stream, this is noticed
// only by the background drain (RecvMsg); the controller must then re-establish the
// stream and re-send the latest (unchanged) snapshot.
func TestDataServerController_ReestablishesStreamAfterServerEnd(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()
	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 10*time.Millisecond, testHealthPolicy)
	defer func() {
		assert.NoError(t, nc.Close())
	}()

	node := rpc.GetNode(addr)
	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, &proto.ShardAssignments{})

	resp := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{{
					Shard:  0,
					Leader: "leader-0",
				}},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}
	sap.Set(resp)
	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, resp)

	// Server ends the RPC without any new assignment being published.
	node.ShardAssignmentsStream.EndStream()

	// The controller must reconnect and re-send the same (unchanged) snapshot.
	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, resp)
}

// TestDataServerController_RecoversFromStreamEstablishmentRejection covers the
// other facet named in the fix's motivation: the stream is rejected at establishment
// time (e.g. before the data server is initialized). The dispatch loop must keep
// retrying and deliver the assignment once establishment succeeds.
func TestDataServerController_RecoversFromStreamEstablishmentRejection(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := mockutils.NewShardAssignmentsProvider()
	nal := mockutils.NewNodeAvailabilityListener()
	rpc := mockutils.NewRpcProvider()

	// Reject stream establishment up front.
	rpc.FailNode(addr, errors.New("data server not initialized"))

	resp := &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {
				Assignments: []*proto.ShardAssignment{{
					Shard:  0,
					Leader: "leader-0",
				}},
				ShardKeyRouter: proto.ShardKeyRouter_XXHASH3,
			},
		},
	}
	sap.Set(resp)

	nc := newController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 10*time.Millisecond, testHealthPolicy)
	defer func() {
		assert.NoError(t, nc.Close())
	}()

	node := rpc.GetNode(addr)

	// While establishment keeps failing, no assignment can be delivered.
	select {
	case <-node.ShardAssignmentsStream.Updates:
		assert.Fail(t, "should not deliver assignments while stream establishment is rejected")
	case <-time.After(1 * time.Second):
	}

	rpc.RecoverNode(addr)

	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, resp)
}
