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
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/failpoint"
	commondataserver "github.com/oxia-db/oxia/oxiad/common/dataserver"
	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/common/health"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/controller"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
)

// errNotServing marks a health response where the data server deliberately
// reported NOT_SERVING (e.g. it is shutting down), as opposed to a probe that
// failed at the transport level.
var errNotServing = errors.New("data server is not actively serving")

type controller struct {
	wg                  sync.WaitGroup
	closed              atomic.Bool
	logger              *slog.Logger
	ctx                 context.Context
	ctxCancel           context.CancelFunc
	metadata            coordmetadata.Metadata
	rpc                 rpc.ControllerRPCProvider
	insID               string
	assignmentsProvider controllerapi.ShardAssignmentsProvider
	listener            controllerapi.DataServerEventListener
	healthPolicy        health.CheckPolicy
	initialRetryBackoff time.Duration

	runningGauge              metric.Gauge
	failedHealthChecksCounter metric.Counter

	mu sync.RWMutex
	// status is written under mu, and read without it.
	status *commonwatch.Watch[Status]
	// supportedFeatures are the features that the last handshake reported.
	supportedFeatures []proto.Feature
	// handshaking is set while a handshake is in flight: the health paths keep
	// observing SERVING while the node is unavailable, and at most one of them
	// handshakes it at a time.
	handshaking bool
	// dataServer is the data server as configured: UpdateDataServer replaces
	// it, under the same name.
	dataServer *proto.DataServer
	// endpoint is the internal address that the health checks and the
	// assignments stream go to. UpdateDataServer replaces it when the address
	// changes, and cancels the RPCs to the old one.
	endpoint *endpoint
}

// endpoint is a data server at one internal address. The RPCs to it run
// under ctx, cancelled when the data server moves to another address, and
// its health checks share one backoff, which its handshake resets.
type endpoint struct {
	identity           *proto.DataServerIdentity
	ctx                context.Context
	cancel             context.CancelFunc
	healthCheckBackoff *commontime.ConcurrentBackOff
}

func (n *controller) GetDataServer() commonobject.Borrowed[*proto.DataServer] {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return commonobject.Borrow(n.dataServer)
}

// identity returns the identity of the data server, as configured.
func (n *controller) identity() *proto.DataServerIdentity {
	return n.GetDataServer().UnsafeBorrow().GetIdentity()
}

// currentEndpoint returns the endpoint of the RPCs, until the data server
// moves.
func (n *controller) currentEndpoint() *endpoint {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.endpoint
}

// newEndpoint returns an endpoint whose RPCs stop with ctx, the controller's.
func (n *controller) newEndpoint(ctx context.Context, identity *proto.DataServerIdentity) *endpoint {
	ctx, cancel := context.WithCancel(ctx)
	return &endpoint{
		identity: identity,
		ctx:      ctx,
		cancel:   cancel,
		healthCheckBackoff: commontime.NewConcurrentBackOff(
			commontime.NewBackOffWithInitialInterval(ctx, n.initialRetryBackoff)),
	}
}

func (n *controller) UpdateDataServer(dataServer *proto.DataServer) {
	n.mu.Lock()
	n.dataServer = dataServer
	replaced := n.endpoint
	if replaced.identity.GetInternal() == dataServer.GetIdentity().GetInternal() {
		n.mu.Unlock()
		return
	}
	n.endpoint = n.newEndpoint(n.ctx, dataServer.GetIdentity())
	// The handshake bound the data server at its old address: it is
	// unavailable until it handshakes at the new one, and the observations
	// of the old one still in flight are stale
	status := n.status.Load()
	status.SetAvailable(false)
	n.status.Publish(status)
	n.mu.Unlock()

	replaced.cancel()
	n.logger.Info("Data server moved to another internal address",
		slog.String("from", replaced.identity.GetInternal()),
		slog.String("to", dataServer.GetIdentity().GetInternal()))
}

func (n *controller) GetStatus() Status {
	return n.status.Load()
}

func (n *controller) GetFeatures() []proto.Feature {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.supportedFeatures
}

func (n *controller) Drain() {
	n.mu.Lock()
	defer n.mu.Unlock()
	status := n.status.Load()
	previous := status.GetState()
	if commondataserver.IsRemoved(previous) {
		return
	}
	status.Advance(proto.DataServerState_DATA_SERVER_STATE_DRAINING)
	n.status.Publish(status)
	n.logger.Info("Changed status", slog.Any("from", previous),
		slog.Any("to", proto.DataServerState_DATA_SERVER_STATE_DRAINING))
}

func (n *controller) currentStatusEpoch() int64 {
	return n.status.Load().epoch
}

func (n *controller) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil
	}
	n.runningGauge.Unregister()
	n.ctxCancel()
	n.wg.Wait()

	n.logger.Info("Closed data server controller")
	return nil
}

// openAssignmentsStream opens the assignments stream to ep once the data
// server is RUNNING or DRAINING, and available there. The data server rejects
// the stream until the handshake binds it to this coordinator, and a rejected
// stream is only retried after the full backoff: waiting for the handshake
// instead delivers the first assignments right after it. A data server that
// moved is unavailable until it handshakes at its new address, where its next
// endpoint opens the stream.
func (n *controller) openAssignmentsStream(ep *endpoint) (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	receiver := n.status.Subscribe()
	for {
		if n.isEndpointAvailable(ep, receiver) {
			n.logger.Debug("Ready to send assignments")
			return n.rpc.PushShardAssignments(ep.ctx, ep.identity)
		}

		select {
		case <-ep.ctx.Done():
			return nil, ep.ctx.Err()
		case <-receiver.Changed():
		}
	}
}

// isEndpointAvailable loads the status from receiver and reports whether it is
// the status of ep, where the data server handshook, and it isn't DRAINED.
// Read under mu, under which a move replaces the endpoint.
func (n *controller) isEndpointAvailable(ep *endpoint, receiver *commonwatch.Receiver[Status]) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	status := receiver.Load()
	return n.endpoint == ep && status.IsAvailable() &&
		status.GetState() != proto.DataServerState_DATA_SERVER_STATE_DRAINED
}

// sendAssignmentsDispatchWithRetries pushes the assignments to the data server
// at its current address, and again at the next one when it moves.
func (n *controller) sendAssignmentsDispatchWithRetries() {
	for n.ctx.Err() == nil {
		n.dispatchAssignmentsToEndpointWithRetries(n.currentEndpoint())
	}
}

// assignmentsRetryBackOff returns the backoff of the retries of the
// assignments stream to ep. Not the slow initial interval of the health
// checks: openAssignmentsStream waits until the data server is available
// again, so the retry doesn't have to wait for it. After a restart, the data
// server rejects every client until it has its assignments, so each second
// counts.
func assignmentsRetryBackOff(ep *endpoint) backoff.BackOff {
	initialInterval := commontime.DefaultInitialInterval
	if interval, ok := failpoint.Eval[time.Duration]("dataServerController.assignmentsRetryInterval"); ok {
		// A retry interval that a test reaches otherwise only after a long
		// series of rejected streams
		initialInterval = interval
	}
	return commontime.NewBackOffWithInitialInterval(ep.ctx, initialInterval)
}

// dispatchAssignmentsToEndpointWithRetries pushes the assignments to ep until
// the controller closes or the data server moves. Its retries run on the
// backoff of the endpoint, so a move interrupts a retry that is waiting one
// out, e.g. because the data server kept rejecting the stream at the address
// it left, instead of leaving the new address without its assignments until
// that interval has elapsed.
func (n *controller) dispatchAssignmentsToEndpointWithRetries(ep *endpoint) {
	receiver := n.assignmentsProvider.SubscribeShardAssignments()
	bo := assignmentsRetryBackOff(ep)

	_ = backoff.RetryNotify(func() error {
		stream, err := n.openAssignmentsStream(ep)
		if err != nil {
			n.logger.Debug("Failed to create shard assignments stream", slog.Any("error", err))
			return err
		}
		streamCtx := stream.Context()

		// The coordinator only ever sends on this stream and never reads it, so a
		// server-side termination that leaves the underlying connection healthy
		// (e.g. an initialization rejection or an idle-stream reset from an L7 hop)
		// would otherwise go unnoticed until the next assignment change, leaving a
		// freshly-started data server without its initial assignment. Drain the
		// stream in the background: RecvMsg returns as soon as the server ends the
		// RPC, which cancels streamCtx and wakes the loop below to re-establish it.
		var drainWg sync.WaitGroup
		drainWg.Add(1)
		go func() {
			defer drainWg.Done()
			var resp proto.CoordinationShardAssignmentsResponse
			for {
				if err := stream.RecvMsg(&resp); err != nil {
					return
				}
			}
		}()
		defer drainWg.Wait()

		var assignments *proto.ShardAssignments
		for {
			latest := receiver.Load()
			if latest != nil && !pb.Equal(assignments, latest) {
				n.logger.Debug("Sending assignments", slog.Any("assignments", latest))
				if err := stream.Send(latest); err != nil {
					n.logger.Debug("Failed to send assignments", slog.Any("error", err))
					return err
				}
				n.logger.Debug("Send assignments completed successfully")
				bo.Reset()
				assignments = latest
			}

			// A move of the data server cancels ep, which ends the stream
			// too: the next endpoint waits for its handshake at the new
			// address
			select {
			case <-ep.ctx.Done():
				return nil
			case <-streamCtx.Done():
				return streamCtx.Err()
			case <-receiver.Changed():
			}
		}
	}, bo, func(err error, duration time.Duration) {
		switch {
		case errors.Is(err, context.Canceled):
		case errors.Is(err, constant.ErrNotInitialized):
			n.logger.Debug(
				"Waiting for data server initialization before sending assignments",
				slog.Duration("retry-after", duration),
			)
		default:
			n.logger.Warn(
				"Failed to send assignments updates to storage data server",
				slog.Duration("retry-after", duration),
				slog.Any("error", err),
			)
		}
	})
}

// monitorDraining waits for the data server to be DRAINING, then checks on
// every probe interval whether it is drained, until it stores it DRAINED. A
// failed check is left to the next interval.
func (n *controller) monitorDraining() {
	receiver := n.status.Subscribe()
	for receiver.Load().GetState() != proto.DataServerState_DATA_SERVER_STATE_DRAINING {
		select {
		case <-n.ctx.Done():
			return
		case <-receiver.Changed():
		}
	}

	ticker := time.NewTicker(n.healthPolicy.ProbeInterval)
	defer ticker.Stop()
	for {
		drained, err := n.drain()
		if err != nil {
			n.logger.Warn("Failed to check whether the data server is drained", slog.Any("error", err))
		}
		if drained {
			n.mu.Lock()
			status := n.status.Load()
			status.Advance(proto.DataServerState_DATA_SERVER_STATE_DRAINED)
			n.status.Publish(status)
			n.mu.Unlock()
			n.logger.Info("Data server is drained")
			n.listener.BecameDrained(n.identity())
			return
		}

		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// drain moves the stored status of the data server one step towards DRAINED,
// and reports whether it is DRAINED: it stores it DRAINING first, then
// DRAINED once it is drained. DRAINED is final, as a data server that left
// can't be added again until the configuration confirms its drain.
func (n *controller) drain() (bool, error) {
	name := n.identity().GetNameOrDefault()
	stored, _, err := n.metadata.GetDataServerStatus(name)
	if err != nil {
		return false, err
	}
	state := stored.UnsafeBorrow().GetState()
	next := proto.DataServerState_DATA_SERVER_STATE_DRAINING
	switch state {
	case proto.DataServerState_DATA_SERVER_STATE_DRAINED:
		return true, nil
	case proto.DataServerState_DATA_SERVER_STATE_DRAINING:
		namespaces, err := n.metadata.ListNamespaceStatus()
		if err != nil {
			return false, err
		}
		if !commondataserver.IsDrained(name, namespaces) {
			return false, nil
		}
		next = proto.DataServerState_DATA_SERVER_STATE_DRAINED
	default:
	}

	written := false
	if err := n.metadata.PatchDataServerStatus(name, func(status *proto.DataServerStatus) (bool, error) {
		// Changed since it was read: left to the next check
		if status.GetState() != state {
			return false, nil
		}
		status.State = next
		written = true
		return true, nil
	}); err != nil {
		return false, err
	}
	return written && next == proto.DataServerState_DATA_SERVER_STATE_DRAINED, nil
}

func (n *controller) doHealthPing(ep *endpoint, observedEpoch int64, healthClient grpc_health_v1.HealthClient) error {
	pingCtx, pingCancel := context.WithTimeout(ep.ctx, n.healthPolicy.ProbeTimeout)
	response, err := healthClient.Check(pingCtx, &grpc_health_v1.HealthCheckRequest{Service: ""})
	pingCancel()
	return n.handleHealthCheck(ep, observedEpoch, response, err)
}

// healthPingWithRetries probes the data server at its current address, and
// again at the next one when it moves.
func (n *controller) healthPingWithRetries() {
	for n.ctx.Err() == nil {
		n.pingEndpointWithRetries(n.currentEndpoint())
	}
}

// pingEndpointWithRetries probes ep until the controller closes or the data
// server moves.
func (n *controller) pingEndpointWithRetries(ep *endpoint) {
	// Consecutive probe failures, preserved across the retry cycles so that a
	// data server that keeps failing is re-declared unavailable on every
	// backoff round instead of getting a fresh threshold each time.
	consecutiveFailures := 0
	lastObservedEpoch := int64(-1)
	// The status epoch of the observation that crossed the failure threshold.
	// The notify callback runs on this same goroutine right after the retry
	// function returns.
	var failureEpoch int64

	_ = backoff.RetryNotify(func() error {
		healthClient, err := n.rpc.GetHealthClient(ep.identity)
		if err != nil {
			failureEpoch = n.currentStatusEpoch()
			return err
		}

		ticker := time.NewTicker(n.healthPolicy.ProbeInterval)
		defer ticker.Stop()
		for {
			// Capture the status epoch before probing: an answer produced
			// before a concurrent status transition is stale and must not
			// flap the status back.
			epoch := n.currentStatusEpoch()
			if epoch != lastObservedEpoch {
				// The status changed (e.g. the watch path recovered the
				// node): failures observed against the previous status must
				// not count against the new one.
				consecutiveFailures = 0
				lastObservedEpoch = epoch
			}

			// Immediate check on startup instead of waiting for first tick
			switch err := n.doHealthPing(ep, epoch, healthClient); {
			case err == nil:
				consecutiveFailures = 0
			case errors.Is(err, errNotServing):
				// The data server deliberately reported NOT_SERVING (e.g. it
				// is shutting down): fail it right away.
				failureEpoch = epoch
				return err
			default:
				consecutiveFailures++
				if consecutiveFailures >= n.healthPolicy.FailureThreshold {
					n.logger.Warn("Data server stopped responding to ping",
						slog.Int("consecutive-failures", consecutiveFailures))
					failureEpoch = epoch
					return err
				}
				// A slow probe on a saturated node is not yet a failure:
				// keep probing at the regular cadence
				n.logger.Warn("Data server health ping failed, tolerating below failure threshold",
					slog.Int("consecutive-failures", consecutiveFailures),
					slog.Int("failure-threshold", n.healthPolicy.FailureThreshold),
					slog.Any("error", err))
			}

			select {
			case <-ep.ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}, ep.healthCheckBackoff, func(err error, duration time.Duration) {
		n.logger.Warn(
			"Failed to check storage data server health by ping-pong",
			slog.Duration("retry-after", duration),
			slog.Any("error", err),
		)
		n.becomeUnavailable(ep, failureEpoch)
	})
}

// healthWatchWithRetries watches the health of the data server at its current
// address, and again at the next one when it moves.
func (n *controller) healthWatchWithRetries() {
	for n.ctx.Err() == nil {
		n.watchEndpointWithRetries(n.currentEndpoint())
	}
}

// watchEndpointWithRetries watches ep until the controller closes or the data
// server moves.
func (n *controller) watchEndpointWithRetries(ep *endpoint) {
	_ = backoff.RetryNotify(func() error {
		n.logger.Debug("Start new health watch cycle")
		healthClient, err := n.rpc.GetHealthClient(ep.identity)
		if err != nil {
			return err
		}

		watchStream, err := healthClient.Watch(ep.ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
		if err != nil {
			return err
		}
		for {
			select {
			case <-ep.ctx.Done():
				return nil
			default:
				epoch := n.currentStatusEpoch()
				response, recvErr := watchStream.Recv()
				err := n.handleHealthCheck(ep, epoch, response, recvErr)
				switch {
				case err == nil:
				case errors.Is(err, errNotServing):
					// The data server deliberately reported NOT_SERVING: fail
					// it right away, and keep watching for it to come back.
					n.logger.Warn("Data server reported it is not serving")
					n.becomeUnavailable(ep, epoch)
				default:
					// A broken watch stream is an ambiguous signal: it breaks
					// right away when the process dies (the fast-detection
					// path when a pod is deleted or restarted), but it can
					// also get reset under load while the node is healthy.
					// Disambiguate with an immediate probe instead of waiting
					// for the ping cadence, then re-establish the watch.
					n.verifyAfterWatchFailure()
					return err
				}
			}
		}
	}, ep.healthCheckBackoff, func(err error, duration time.Duration) {
		n.logger.Warn("Failed to check storage data server health by watch",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

// verifyAfterWatchFailure probes the data server once, right after its health
// watch stream broke, so that a dead node is fenced immediately instead of
// waiting for the ping loop's cadence and failure threshold. A dead peer
// fails the probe right away with a hard transport error (e.g. connection
// refused after the pod was deleted) and is fenced on the spot. A probe that
// merely times out is the saturation signature: the node stays up and the
// ping loop's consecutive-failure threshold remains the authority.
func (n *controller) verifyAfterWatchFailure() {
	// Fetch the client anew: the watch usually breaks together with the
	// underlying pooled connection, and a replacement connection fails fast
	// while the peer is unreachable. The watch also breaks when the data
	// server moves: the probe then goes to its new address.
	ep := n.currentEndpoint()
	healthClient, err := n.rpc.GetHealthClient(ep.identity)
	if err != nil {
		// Leave it to the ping loop, which fences on this same error
		return
	}

	epoch := n.currentStatusEpoch()
	switch err := n.doHealthPing(ep, epoch, healthClient); {
	case err == nil:
		// The node is alive: the watch stream reset was transient
	case errors.Is(err, errNotServing):
		n.logger.Warn("Data server reported it is not serving")
		n.becomeUnavailable(ep, epoch)
	case errors.Is(err, context.Canceled) || grpcstatus.Code(err) == codes.Canceled || isTransportClosing(err):
		// The controller is shutting down, or the probe rode a connection
		// that was itself being torn down: neither proves anything about the
		// node's health
	case grpcstatus.Code(err) == codes.DeadlineExceeded:
		// The node is slow, not provably dead: tolerate, the ping loop's
		// failure threshold decides
		n.logger.Warn("Data server is slow to verify after its health watch failed")
	default:
		n.logger.Warn("Data server is gone after its health watch failed",
			slog.Any("error", err))
		n.becomeUnavailable(ep, epoch)
	}
}

// isTransportClosing matches the gRPC client error surfaced when the
// underlying connection is being torn down (e.g. the client pool closing it,
// or a server draining on shutdown): the RPC failed locally, proving nothing
// about the peer's health.
func isTransportClosing(err error) bool {
	return err != nil && strings.Contains(grpcstatus.Convert(err).Message(), "transport is closing")
}

// isStaleObservation reports whether the result of a health check made at ep,
// when the status was at observedEpoch, no longer applies: the data server
// moved away from ep, and what it answers at an address it left says nothing
// about its new one, or its status changed since the check, e.g. another
// health path fenced it or recovered it in the meantime. It must be called
// with mu held, under which both change.
func (n *controller) isStaleObservation(ep *endpoint, observedEpoch int64) bool {
	return n.endpoint != ep || n.status.Load().epoch != observedEpoch
}

// becomeUnavailable fences the data server after a health check failed at ep,
// unless the check is stale: the next one evaluates the fresh state.
func (n *controller) becomeUnavailable(ep *endpoint, observedEpoch int64) {
	n.mu.Lock()
	status := n.status.Load()
	if n.isStaleObservation(ep, observedEpoch) {
		n.mu.Unlock()
		return
	}
	if !status.IsAvailable() {
		n.mu.Unlock()
		return
	}
	status.SetAvailable(false)
	n.status.Publish(status)
	n.mu.Unlock()

	n.failedHealthChecksCounter.Inc()
	n.listener.BecameUnavailable(ep.identity)
}

// becomeAvailable handshakes the data server at ep, where a health check
// found it SERVING, and makes it available. A stale check doesn't handshake
// it, and the result of a handshake of an address the data server left is
// dropped: the features it reports there aren't the ones it supports at its
// new address.
func (n *controller) becomeAvailable(ep *endpoint, observedEpoch int64) {
	n.mu.Lock()
	status := n.status.Load()
	if n.isStaleObservation(ep, observedEpoch) {
		n.mu.Unlock()
		return
	}
	if status.IsAvailable() || n.handshaking {
		// Nothing to do: the node is available, or a handshake is already in
		// flight.
		n.mu.Unlock()
		return
	}
	n.handshaking = true
	n.mu.Unlock()

	ep.healthCheckBackoff.Reset()

	// Bind the node before it can receive other internal traffic.
	handshakeCtx, handshakeCancel := context.WithTimeout(ep.ctx, n.healthPolicy.ProbeTimeout)
	handshake, err := n.rpc.Handshake(handshakeCtx, ep.identity, &proto.HandshakeRequest{
		InstanceId: n.insID,
	})
	handshakeCancel()
	if err == nil {
		switch handshake.Status {
		case proto.HandshakeStatus_HANDSHAKE_STATUS_BOUND, proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND:
		case proto.HandshakeStatus_HANDSHAKE_STATUS_MISMATCH:
			err = errors.New("data server instance id mismatch")
		default:
			err = errors.Errorf("unexpected handshake status: %s", handshake.Status.String())
		}
	}

	if resume, ok := failpoint.Eval[chan struct{}]("dataServerController.handshakeCompleted"); ok {
		// The result of the handshake is only committed under the lock, which
		// a move can take first: pause here to run that race
		<-resume
	}

	n.mu.Lock()
	n.handshaking = false
	if err != nil {
		// The node stays unavailable: the next SERVING observation retries
		n.mu.Unlock()
		n.logger.Warn("Failed to handshake with data server", slog.Any("error", err))
		return
	}
	if n.endpoint != ep {
		// The data server moved while the handshake was in flight: it bound
		// the address it left, and the features it answered with are that
		// one's. Its new address handshakes on its own.
		n.mu.Unlock()
		return
	}
	features := handshake.FeaturesSupported
	featuresChanged := len(feature.Missing(features, n.supportedFeatures)) > 0 ||
		len(feature.Missing(n.supportedFeatures, features)) > 0
	n.supportedFeatures = features
	if status = n.status.Load(); !status.IsAvailable() && status.epoch == observedEpoch {
		status.SetAvailable(true)
		n.status.Publish(status)
		n.logger.Info("Storage data server is back online")
	}
	n.mu.Unlock()

	if featuresChanged {
		// The features the node supports were not known before this handshake:
		// it is the first one, or the node restarted with another binary
		n.listener.FeaturesDiscovered(ep.identity)
	}
}

func (n *controller) handleHealthCheck(ep *endpoint, observedEpoch int64,
	response *grpc_health_v1.HealthCheckResponse, err error) error {
	if err != nil {
		if !errors.Is(err, context.Canceled) && grpcstatus.Code(err) != codes.Canceled && !isTransportClosing(err) {
			n.logger.Warn("Data server health check failed", slog.Any("error", err))
		}
		return err
	}
	if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return errNotServing
	}
	n.becomeAvailable(ep, observedEpoch)
	return nil
}

//nolint:revive // one argument per dependency, as the shard controller
func NewController(ctx context.Context, dataServer *proto.DataServer,
	metadata coordmetadata.Metadata,
	shardAssignmentsProvider controllerapi.ShardAssignmentsProvider,
	dataServerEventListener controllerapi.DataServerEventListener,
	rpcProvider rpc.ControllerRPCProvider,
	insID string,
	initialRetryBackoff time.Duration,
	healthPolicy health.CheckPolicy) Controller {
	dataServerCtx, cancel := context.WithCancel(ctx)
	dataServerID := dataServer.GetIdentity().GetNameOrDefault()
	labels := map[string]any{"data-server": dataServerID}
	logger := slog.With(
		slog.String("component", "data-server-controller"),
		slog.Any("data-server", dataServerID),
	)

	nc := &controller{
		ctx:                 dataServerCtx,
		ctxCancel:           cancel,
		dataServer:          dataServer,
		metadata:            metadata,
		assignmentsProvider: shardAssignmentsProvider,
		listener:            dataServerEventListener,
		rpc:                 rpcProvider,
		insID:               insID,
		status:              commonwatch.New(NewStatus(proto.DataServerState_DATA_SERVER_STATE_RUNNING)),
		supportedFeatures:   make([]proto.Feature, 0),
		logger:              logger,
		healthPolicy:        healthPolicy,
		initialRetryBackoff: initialRetryBackoff,
		failedHealthChecksCounter: metric.NewCounter("oxia_coordinator_node_health_checks_failed",
			"The number of failed health checks to a dataServer", "count", labels),
	}
	nc.endpoint = nc.newEndpoint(dataServerCtx, dataServer.GetIdentity())
	nc.runningGauge = metric.NewGauge("oxia_coordinator_node_running",
		"Whether the dataServer is considered to be running by the coordinator", "count", labels, func() int64 {
			if status := nc.GetStatus(); status.IsAvailable() && status.GetState() == proto.DataServerState_DATA_SERVER_STATE_RUNNING {
				return 1
			}
			return 0
		})

	nc.wg.Go(func() {
		process.DoWithLabels(
			nc.ctx,
			map[string]string{
				"component":  "data-server-controller-health-watch",
				"dataServer": dataServerID,
			},
			nc.healthWatchWithRetries,
		)
	})

	nc.wg.Go(func() {
		process.DoWithLabels(
			nc.ctx,
			map[string]string{
				"component":  "data-server-controller-health-ping",
				"dataServer": dataServerID,
			},
			nc.healthPingWithRetries,
		)
	})

	nc.wg.Go(func() {
		process.DoWithLabels(
			nc.ctx,
			map[string]string{
				"component":  "data-server-controller-assignment-dispatcher",
				"dataServer": dataServerID,
			},
			nc.sendAssignmentsDispatchWithRetries,
		)
	})

	nc.wg.Go(func() {
		process.DoWithLabels(
			nc.ctx,
			map[string]string{
				"component":  "data-server-controller-drain-monitor",
				"dataServer": dataServerID,
			},
			nc.monitorDraining,
		)
	})

	nc.logger.Info("Started data server controller")
	return nc
}
