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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/health"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc/rpctest"
)

// testHealthPolicy keeps the probing cadence fast so the tests exercising the
// consecutive-failure threshold complete quickly.
var testHealthPolicy = health.CheckPolicy{
	ProbeInterval:    20 * time.Millisecond,
	ProbeTimeout:     1 * time.Second,
	FailureThreshold: 3,
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

func TestDataServerControllerHealthPingWithRetriesTracksServingStatus(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	// Controller starts as NotRunning and transitions to Running on first health check
	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 100*time.Millisecond)

	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	unavailableNode := <-nal.Events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.GetStatus().GetState())

	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 100*time.Millisecond)

	node.HealthClient.SetError(errors.New("failed to connect"))

	unavailableNode = <-nal.Events
	assert.Equal(t, addr, unavailableNode)

	assert.Equal(t, NotRunning, nc.GetStatus().GetState())

	assert.NoError(t, nc.Close())
}

func TestDataServerControllerHealthPingWithRetriesToleratesTransientFailures(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// Fail fewer consecutive probes than the threshold: a saturated node can
	// miss individual probe deadlines while being perfectly healthy.
	node.HealthClient.FailNextChecks(errors.New("context deadline exceeded"), testHealthPolicy.FailureThreshold-1)

	// Wait until all scheduled failures have been consumed, plus a few more
	// successful probe rounds.
	assert.Eventually(t, func() bool {
		return node.HealthClient.PendingCheckFailures() == 0
	}, 10*time.Second, 10*time.Millisecond)
	time.Sleep(5 * testHealthPolicy.ProbeInterval)

	select {
	case <-nal.Events:
		assert.Fail(t, "probe failures below the threshold must not mark the node unavailable")
	default:
	}
	assert.Equal(t, Running, nc.GetStatus().GetState())

	assert.NoError(t, nc.Close())
}

// TestDataServerControllerCloseInterruptsHealthCheckBackoff verifies that
// Close does not wait for a health check loop to sleep out its retry backoff,
// which used to hold up the coordinator shutdown for up to a minute per
// unreachable data server.
func TestDataServerControllerCloseInterruptsHealthCheckBackoff(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", time.Hour, testHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// Make the ping loop give up on the node: it fences the node from its
	// retry notification, right before it starts waiting out the backoff
	// interval (an hour, with jitter, in this test).
	node.HealthClient.FailNextChecks(grpcstatus.Error(codes.Unavailable, "connection refused"),
		testHealthPolicy.FailureThreshold)

	select {
	case unavailableNode := <-nal.Events:
		assert.Equal(t, addr, unavailableNode)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the node must be fenced once the ping loop gives up on it")
	}

	closed := make(chan error, 1)
	go func() {
		closed <- nc.Close()
	}()

	select {
	case err := <-closed:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "Close must not wait for the health check backoff to elapse")
	}
}

func TestDataServerControllerVerifyAfterWatchFailureDoesNotFenceAliveNode(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// A watch stream reset while the node keeps answering pings (e.g. an
	// idle stream reset under load) must not fence the node.
	node.HealthClient.FailWatches(errors.New("stream reset"))

	time.Sleep(10 * testHealthPolicy.ProbeInterval)

	select {
	case <-nal.Events:
		assert.Fail(t, "a watch stream failure alone must not mark the node unavailable")
	default:
	}
	assert.Equal(t, Running, nc.GetStatus().GetState())

	assert.NoError(t, nc.Close())
}

// slowPingHealthPolicy makes the ping cadence so slow that only the
// watch-failure verification path can possibly fence the node, so the tests
// below prove pod-death detection does not depend on the ping loop.
var slowPingHealthPolicy = health.CheckPolicy{
	ProbeInterval:    time.Hour,
	ProbeTimeout:     1 * time.Second,
	FailureThreshold: 3,
}

func TestDataServerControllerVerifyAfterWatchFailureFencesDeadNode(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, slowPingHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
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
	assert.Equal(t, NotRunning, nc.GetStatus().GetState())

	assert.NoError(t, nc.Close())
}

func TestDataServerControllerVerifyAfterWatchFailureToleratesSlowNode(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, slowPingHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
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
	assert.Equal(t, Running, nc.GetStatus().GetState())

	assert.NoError(t, nc.Close())
}

func TestDataServerControllerVerifyAfterWatchFailureToleratesTransportClosing(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, slowPingHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)

	// The connection carrying the watch is being torn down: the stream and
	// the verification probe both fail with "transport is closing". That is a
	// local teardown artifact, not evidence the node is dead: no fencing.
	node.HealthClient.FailNextChecks(grpcstatus.Error(codes.Unavailable, "transport is closing"), 1)
	node.HealthClient.FailWatches(grpcstatus.Error(codes.Unavailable, "transport is closing"))

	select {
	case <-nal.Events:
		assert.Fail(t, "a transport teardown must not fence the node")
	case <-time.After(500 * time.Millisecond):
	}
	assert.Equal(t, Running, nc.GetStatus().GetState())

	assert.NoError(t, nc.Close())
}

// The status records when the state last changed, except for the transition
// of the data server joining: a data server that never failed has no
// transition time.
func TestDataServerControllerGetStatusRecordsTransitionsAfterJoining(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)
	assert.True(t, nc.GetStatus().GetLastTransitionTime().IsZero())

	beforeFailure := time.Now()
	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	<-nal.Events
	failed := nc.GetStatus()
	assert.Equal(t, NotRunning, failed.GetState())
	assert.False(t, failed.GetLastTransitionTime().Before(beforeFailure))

	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)
	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)
	assert.True(t, nc.GetStatus().GetLastTransitionTime().After(failed.GetLastTransitionTime()))

	assert.NoError(t, nc.Close())
}

// A drained data server stays Draining when it becomes unavailable, and the
// listener is told, so that the runtime closes its controller.
func TestDataServerControllerDrainKeepsDataServerDrainingUntilUnavailable(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)

	nc.Drain()
	assert.Equal(t, Draining, nc.GetStatus().GetState())

	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	assert.Equal(t, addr, <-nal.Events)
	assert.Equal(t, Draining, nc.GetStatus().GetState())

	assert.NoError(t, nc.Close())
}

func TestDataServerControllerBecomeAvailableHandshakesOnlyOnTransition(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	node := rpc.GetNode(addr)

	// The controller starts as NotRunning and binds the node with a single
	// Handshake on the transition to Running, even though the watch and ping
	// paths both observe SERVING at startup.
	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 100*time.Millisecond)
	assert.Equal(t, int64(1), node.HandshakeCount.Load(),
		"Handshake should be called exactly once on the initial transition to Running")

	// Wait for several health check cycles. If Handshake were called on every
	// health check, we'd see the count increase
	time.Sleep(5 * time.Second)

	// The count should NOT have increased while the server stayed Running
	assert.Equal(t, int64(1), node.HandshakeCount.Load(),
		"Handshake should not be called repeatedly while server is already Running")

	// Now simulate the server going down and coming back
	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	unavailableNode := <-nal.Events
	assert.Equal(t, addr, unavailableNode)
	assert.Equal(t, NotRunning, nc.GetStatus().GetState())

	// Bring the server back online
	node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)

	// Handshake should be called again, once, for the NotRunning -> Running transition
	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 100*time.Millisecond,
		"the server should be Running again after it reports SERVING")
	assert.Equal(t, int64(2), node.HandshakeCount.Load(),
		"Handshake should be called exactly once on state transition from NotRunning to Running")

	// Wait again to confirm no further redundant calls
	time.Sleep(5 * time.Second)

	assert.Equal(t, int64(2), node.HandshakeCount.Load(),
		"Handshake should not be called repeatedly after recovery")

	assert.NoError(t, nc.Close())
}

// TestDataServerControllerBecomeAvailableHandshakesOnceForConcurrentObservations reproduces
// the double handshake at startup: the watch stream delivers SERVING right away
// on subscribe and the ping loop probes immediately too, so both paths observe
// SERVING for the same status epoch while the node is still NotRunning. The
// node must be bound with a single Handshake.
func TestDataServerControllerBecomeAvailableHandshakesOnceForConcurrentObservations(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	node := rpc.GetNode(addr)
	// Hold the first handshake in flight, so that the second SERVING
	// observation is guaranteed to arrive before the node is Running.
	node.BlockHandshakes()

	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	assert.Eventually(t, func() bool {
		return node.HandshakeCount.Load() >= 1
	}, 10*time.Second, 10*time.Millisecond)

	// Let the ping loop observe SERVING a few more times while the handshake
	// is still in flight: none of them may start another handshake.
	time.Sleep(10 * testHealthPolicy.ProbeInterval)
	assert.Equal(t, int64(1), node.HandshakeCount.Load(),
		"concurrent SERVING observations must not handshake the node more than once")
	assert.Equal(t, NotRunning, nc.GetStatus().GetState())

	node.ReleaseHandshakes()

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(1), node.HandshakeCount.Load(),
		"the node must be bound by a single Handshake")

	assert.NoError(t, nc.Close())
}

// A failed handshake leaves the data server NotRunning, and the next SERVING
// probe retries it: the health loops must keep probing instead of retrying the
// handshake in place.
func TestDataServerControllerBecomeAvailableRetriesFailedHandshakeOnNextProbe(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	node := rpc.GetNode(addr)
	node.SetHandshakeError(errors.New("handshake failed"))

	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	// Each probe retries the handshake
	assert.Eventually(t, func() bool {
		return node.HandshakeCount.Load() >= 3
	}, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, NotRunning, nc.GetStatus().GetState())
	assert.Len(t, nal.FeaturesDiscoveredEvents, 0)

	rpc.Lock()
	node.SetHandshakeError(nil)
	rpc.Unlock()

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)
	select {
	case discovered := <-nal.FeaturesDiscoveredEvents:
		assert.Equal(t, addr, discovered)
	case <-time.After(10 * time.Second):
		assert.Fail(t, "the features of the data server were not reported")
	}

	assert.NoError(t, nc.Close())
}

// The features a data server supports are unknown until its first handshake,
// which reports them to the listener. The later handshakes report them only
// if they changed: e.g. the data server restarted with a newer binary in a
// rolling upgrade.
func TestDataServerControllerBecomeAvailableReportsChangedFeatures(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	node := rpc.GetNode(addr)
	node.SetNodeFeatures([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

	expectReport := func() {
		t.Helper()
		select {
		case discovered := <-nal.FeaturesDiscoveredEvents:
			assert.Equal(t, addr, discovered)
		case <-time.After(10 * time.Second):
			assert.Fail(t, "the features of the data server were not reported")
		}
	}
	restart := func(features []proto.Feature) {
		t.Helper()
		node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		assert.Equal(t, addr, <-nal.Events)
		node.SetNodeFeatures(features)
		node.HealthClient.SetStatus(grpc_health_v1.HealthCheckResponse_SERVING)
		assert.Eventually(t, func() bool {
			return nc.GetStatus().GetState() == Running
		}, 10*time.Second, 10*time.Millisecond)
	}

	expectReport()
	assert.Equal(t, Running, nc.GetStatus().GetState())
	assert.Equal(t, []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}, nc.GetFeatures())

	// The data server restarts with the same binary
	restart([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})
	assert.Equal(t, int64(2), node.HandshakeCount.Load())

	// The data server restarts with a binary that supports more features
	upgraded := []proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM, proto.Feature_FEATURE_ORDERED_WRITES}
	restart(upgraded)
	expectReport()
	assert.Equal(t, int64(3), node.HandshakeCount.Load())
	assert.Equal(t, upgraded, nc.GetFeatures())

	// Close waits for the health checks, so a report would be in the channel
	assert.NoError(t, nc.Close())
	assert.Len(t, nal.FeaturesDiscoveredEvents, 0, "a handshake must not report the features it already knew")
}

func TestDataServerControllerSendAssignmentsDispatchWithRetriesSendsUpdates(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 1*time.Second, testHealthPolicy)

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

func TestDataServerControllerSendAssignmentsDispatchWithRetriesRetriesLatestAfterSendError(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
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

	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 10*time.Millisecond, testHealthPolicy)
	defer func() {
		assert.NoError(t, nc.Close())
	}()

	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, resp)
}

// TestDataServerControllerSendAssignmentsDispatchWithRetriesReestablishesStreamAfterServerEnd reproduces the bug
// behind the "stuck shard assignment streams" fix: the server ends the assignment
// RPC while the underlying connection stays healthy and the assignment snapshot is
// unchanged. Because the coordinator only ever sends on the stream, this is noticed
// only by the background drain (RecvMsg); the controller must then re-establish the
// stream and re-send the latest (unchanged) snapshot.
func TestDataServerControllerSendAssignmentsDispatchWithRetriesReestablishesStreamAfterServerEnd(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 10*time.Millisecond, testHealthPolicy)
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

// TestDataServerControllerSendAssignmentsDispatchWithRetriesRecoversFromStreamRejection covers the
// other facet named in the fix's motivation: the stream is rejected at establishment
// time (e.g. before the data server is initialized). The dispatch loop must keep
// retrying and deliver the assignment once establishment succeeds.
func TestDataServerControllerSendAssignmentsDispatchWithRetriesRecoversFromStreamRejection(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()

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

	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 10*time.Millisecond, testHealthPolicy)
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

// TestDataServerControllerSendAssignmentsDispatchWithRetriesSendsRightAfterHandshake reproduces the
// late first assignments of a data server that is not bound to the coordinator
// yet: it rejects the assignments stream until the handshake binds it, and a
// rejected stream is only retried after the full retry backoff. The dispatcher
// must wait for the handshake instead, and send the assignments right after it.
func TestDataServerControllerSendAssignmentsDispatchWithRetriesSendsRightAfterHandshake(t *testing.T) {
	addr := &proto.DataServerIdentity{
		Public:   "my-server:9190",
		Internal: "my-server:8190",
	}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	node := rpc.GetNode(addr)

	// The data server rejects the stream until the handshake, held in flight
	// here, binds it.
	rpc.FailNode(addr, constant.ErrNotInitialized)
	node.BlockHandshakes()

	// A rejected stream would only be retried after 30s to 90s
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", time.Minute, testHealthPolicy)
	defer func() {
		assert.NoError(t, nc.Close())
	}()

	assert.Eventually(t, func() bool {
		return node.HandshakeCount.Load() >= 1
	}, 10*time.Second, 10*time.Millisecond)
	// Leave the dispatcher the time to open the stream before the handshake
	// completes, if it would.
	time.Sleep(10 * testHealthPolicy.ProbeInterval)

	// The handshake binds the data server
	rpc.RecoverNode(addr)
	node.ReleaseHandshakes()

	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, &proto.ShardAssignments{})
	assert.Equal(t, int64(1), node.PushShardAssignmentsCount.Load(),
		"the stream must be opened once, after the handshake")
}

// A data server that is not initialized yet rejects the assignments stream:
// the dispatcher keeps retrying until it accepts it.
func TestDataServerControllerSendAssignmentsDispatchWithRetriesWaitsForInitialization(t *testing.T) {
	addr := &proto.DataServerIdentity{Public: "my-server:9190", Internal: "my-server:8190"}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	rpc := rpctest.NewRpcProvider()
	node := rpc.GetNode(addr)
	rpc.FailNode(addr, constant.ErrNotInitialized)

	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 10*time.Millisecond, testHealthPolicy)
	defer func() {
		assert.NoError(t, nc.Close())
	}()

	assert.Eventually(t, func() bool {
		return node.PushShardAssignmentsCount.Load() >= 2
	}, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, Running, nc.GetStatus().GetState())

	rpc.RecoverNode(addr)
	expectShardAssignmentsUpdate(t, node.ShardAssignmentsStream.Updates, &proto.ShardAssignments{})
}

// Both health paths retry when they can't get a health client, and the data
// server becomes Running once they can.
func TestDataServerControllerHealthPingWithRetriesRetriesHealthClientErrors(t *testing.T) {
	addr := &proto.DataServerIdentity{Public: "my-server:9190", Internal: "my-server:8190"}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	var calls atomic.Int64
	rpc := &fakeRpcProvider{RpcProvider: rpctest.NewRpcProvider()}
	rpc.getHealthClient = func(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error) {
		if calls.Add(1) <= 4 {
			return nil, errors.New("no connection")
		}
		return rpc.RpcProvider.GetHealthClient(node)
	}

	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 10*time.Millisecond, testHealthPolicy)
	defer func() {
		assert.NoError(t, nc.Close())
	}()

	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running
	}, 10*time.Second, 10*time.Millisecond)
	assert.Greater(t, calls.Load(), int64(4))
	assert.Empty(t, nal.Events)
}

// When the health watch breaks and no health client can be obtained to verify
// the data server, the verification leaves it to the ping loop: the data
// server is not fenced.
func TestDataServerControllerVerifyAfterWatchFailureLeavesHealthClientErrorToPingLoop(t *testing.T) {
	addr := &proto.DataServerIdentity{Public: "my-server:9190", Internal: "my-server:8190"}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	sap := rpctest.NewShardAssignmentsProvider()
	nal := rpctest.NewNodeAvailabilityListener()
	var failing atomic.Bool
	var clients, failedCalls atomic.Int64
	rpc := &fakeRpcProvider{RpcProvider: rpctest.NewRpcProvider()}
	rpc.getHealthClient = func(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error) {
		if failing.Load() {
			failedCalls.Add(1)
			return nil, errors.New("no connection")
		}
		clients.Add(1)
		return rpc.RpcProvider.GetHealthClient(node)
	}
	nc := NewController(context.Background(), dataServer, sap, nal, rpc, "test-instance", 10*time.Millisecond, slowPingHealthPolicy)
	defer func() {
		assert.NoError(t, nc.Close())
	}()
	// The ping loop and the health watch each hold their client
	assert.Eventually(t, func() bool {
		return nc.GetStatus().GetState() == Running && clients.Load() >= 2
	}, 10*time.Second, 10*time.Millisecond)

	failing.Store(true)
	rpc.GetNode(addr).HealthClient.FailWatches(errors.New("stream reset"))

	// The verification gets the first client, the watch retry the second one
	assert.Eventually(t, func() bool {
		return failedCalls.Load() >= 2
	}, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, Running, nc.GetStatus().GetState())
	assert.Empty(t, nal.Events)
}

func TestDataServerControllerDrainIsIdempotent(t *testing.T) {
	addr := &proto.DataServerIdentity{Public: "my-server:9190", Internal: "my-server:8190"}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	rpc := rpctest.NewRpcProvider()
	rpc.GetNode(addr).BlockHandshakes()
	nc := NewController(context.Background(), dataServer, rpctest.NewShardAssignmentsProvider(),
		rpctest.NewNodeAvailabilityListener(), rpc, "test-instance", time.Second, testHealthPolicy)
	defer func() {
		rpc.GetNode(addr).ReleaseHandshakes()
		assert.NoError(t, nc.Close())
	}()

	nc.Drain()
	drained := nc.GetStatus()
	assert.Equal(t, Draining, drained.GetState())

	nc.Drain()
	assert.Equal(t, drained, nc.GetStatus(), "draining again must not advance the status")
}

func TestDataServerControllerCloseIsIdempotent(t *testing.T) {
	addr := &proto.DataServerIdentity{Public: "my-server:9190", Internal: "my-server:8190"}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

	nc := NewController(context.Background(), dataServer, rpctest.NewShardAssignmentsProvider(),
		rpctest.NewNodeAvailabilityListener(), rpctest.NewRpcProvider(), "test-instance", time.Second, testHealthPolicy)

	assert.NoError(t, nc.Close())
	assert.NoError(t, nc.Close())
}

// A handshake that doesn't bind the data server leaves it NotRunning, and the
// next probe retries it.
func TestDataServerControllerBecomeAvailableRejectsUnboundHandshake(t *testing.T) {
	for _, status := range []proto.HandshakeStatus{
		proto.HandshakeStatus_HANDSHAKE_STATUS_MISMATCH,
		proto.HandshakeStatus_HANDSHAKE_STATUS_UNKNOWN,
	} {
		t.Run(status.String(), func(t *testing.T) {
			addr := &proto.DataServerIdentity{Public: "my-server:9190", Internal: "my-server:8190"}
			dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}

			nal := rpctest.NewNodeAvailabilityListener()
			var handshakes atomic.Int64
			rpc := &fakeRpcProvider{RpcProvider: rpctest.NewRpcProvider()}
			rpc.handshake = func(context.Context, *proto.DataServerIdentity, *proto.HandshakeRequest) (
				*proto.HandshakeResponse, error) {
				handshakes.Add(1)
				return &proto.HandshakeResponse{Status: status}, nil
			}
			nc := NewController(context.Background(), dataServer, rpctest.NewShardAssignmentsProvider(),
				nal, rpc, "test-instance", time.Second, testHealthPolicy)

			assert.Eventually(t, func() bool {
				return handshakes.Load() >= 2
			}, 10*time.Second, 10*time.Millisecond)
			assert.Equal(t, NotRunning, nc.GetStatus().GetState())
			assert.Empty(t, nal.FeaturesDiscoveredEvents)

			assert.NoError(t, nc.Close())
		})
	}
}

func TestDataServerControllerNewControllerReportsRunningGauge(t *testing.T) {
	previous := metric.GetMeter()
	reader := sdkmetric.NewManualReader()
	metric.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	defer metric.SetMeter(previous)
	readRunning := func() int64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		var total int64
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != "oxia_coordinator_node_running" {
					continue
				}
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok, "unexpected data: %#v", m.Data)
				for _, dp := range gauge.DataPoints {
					total += dp.Value
				}
			}
		}
		return total
	}

	addr := &proto.DataServerIdentity{Public: "my-server:9190", Internal: "my-server:8190"}
	dataServer := &proto.DataServer{Identity: addr, Metadata: &proto.DataServerMetadata{}}
	rpc := rpctest.NewRpcProvider()
	node := rpc.GetNode(addr)
	node.BlockHandshakes()
	nc := NewController(context.Background(), dataServer, rpctest.NewShardAssignmentsProvider(),
		rpctest.NewNodeAvailabilityListener(), rpc, "test-instance", time.Second, testHealthPolicy)
	defer func() {
		assert.NoError(t, nc.Close())
	}()

	assert.Eventually(t, func() bool {
		return node.HandshakeCount.Load() >= 1
	}, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(0), readRunning())

	node.ReleaseHandshakes()
	assert.Eventually(t, func() bool {
		return readRunning() == 1
	}, 10*time.Second, 10*time.Millisecond)
}

// fakeRpcProvider calls the hook of each method that has one, and the
// embedded RpcProvider otherwise.
type fakeRpcProvider struct {
	*rpctest.RpcProvider
	getHealthClient func(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error)
	handshake       func(ctx context.Context, node *proto.DataServerIdentity, req *proto.HandshakeRequest) (
		*proto.HandshakeResponse, error)
}

func (p *fakeRpcProvider) GetHealthClient(node *proto.DataServerIdentity) (grpc_health_v1.HealthClient, error) {
	if p.getHealthClient != nil {
		return p.getHealthClient(node)
	}
	return p.RpcProvider.GetHealthClient(node)
}

func (p *fakeRpcProvider) Handshake(ctx context.Context, node *proto.DataServerIdentity,
	req *proto.HandshakeRequest) (*proto.HandshakeResponse, error) {
	if p.handshake != nil {
		return p.handshake(ctx, node, req)
	}
	return p.RpcProvider.Handshake(ctx, node, req)
}
