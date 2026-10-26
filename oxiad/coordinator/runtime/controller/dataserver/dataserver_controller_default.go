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

	"github.com/oxia-db/oxia/oxiad/common/feature"
	"github.com/oxia-db/oxia/oxiad/common/health"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"

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
	dataServer          *proto.DataServer
	rpc                 rpc.ControllerRPCProvider
	insID               string
	assignmentsProvider controllerapi.ShardAssignmentsProvider
	listener            controllerapi.DataServerEventListener
	healthPolicy        health.CheckPolicy
	initialRetryBackoff time.Duration
	healthCheckBackoff  *commontime.ConcurrentBackOff

	runningGauge              metric.Gauge
	failedHealthChecksCounter metric.Counter

	mu sync.RWMutex
	// status is written under mu, and read without it.
	status *commonwatch.Watch[Status]
	// supportedFeatures are the features that the last handshake reported.
	supportedFeatures []proto.Feature
	// handshaking is set while a handshake is in flight: the health paths keep
	// observing SERVING while the node is NotRunning, and at most one of them
	// handshakes it at a time.
	handshaking bool
}

func (n *controller) GetDataServer() commonobject.Borrowed[*proto.DataServer] {
	return commonobject.Borrow(n.dataServer)
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
	if previous == Draining {
		return
	}
	status.Advance(Draining)
	n.status.Publish(status)
	n.logger.Info("Changed status", slog.Any("from", previous), slog.Any("to", Draining))
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

// openAssignmentsStream opens the assignments stream once the data server is
// Running or Draining. The data server rejects the stream until the handshake
// binds it to this coordinator, and a rejected stream is only retried after
// the full backoff: waiting for the handshake instead delivers the first
// assignments right after it.
func (n *controller) openAssignmentsStream() (proto.OxiaCoordination_PushShardAssignmentsClient, error) {
	receiver := n.status.Subscribe()
	for {
		if state := receiver.Load().GetState(); state == Running || state == Draining {
			n.logger.Debug("Ready to send assignments")
			return n.rpc.PushShardAssignments(n.ctx, n.dataServer.GetIdentity())
		}

		select {
		case <-n.ctx.Done():
			return nil, n.ctx.Err()
		case <-receiver.Changed():
		}
	}
}

func (n *controller) sendAssignmentsDispatchWithRetries() {
	receiver := n.assignmentsProvider.SubscribeShardAssignments()
	bo := commontime.NewBackOffWithInitialInterval(n.ctx, n.initialRetryBackoff)

	_ = backoff.RetryNotify(func() error {
		stream, err := n.openAssignmentsStream()
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

			select {
			case <-n.ctx.Done():
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

func (n *controller) doHealthPing(observedEpoch int64, healthClient grpc_health_v1.HealthClient) error {
	pingCtx, pingCancel := context.WithTimeout(n.ctx, n.healthPolicy.ProbeTimeout)
	response, err := healthClient.Check(pingCtx, &grpc_health_v1.HealthCheckRequest{Service: ""})
	pingCancel()
	return n.handleHealthCheck(observedEpoch, response, err)
}

func (n *controller) healthPingWithRetries() {
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
		healthClient, err := n.rpc.GetHealthClient(n.dataServer.GetIdentity())
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
			switch err := n.doHealthPing(epoch, healthClient); {
			case err == nil:
				consecutiveFailures = 0
			case errors.Is(err, errNotServing):
				// The data server deliberately reported NOT_SERVING (e.g. it
				// is shutting down): fail it right away.
				failureEpoch = epoch
				return err
			case errors.Is(err, context.Canceled) || grpcstatus.Code(err) == codes.Canceled:
				// The controller is shutting down
				return nil
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
			case <-n.ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}, n.healthCheckBackoff, func(err error, duration time.Duration) {
		n.logger.Warn(
			"Failed to check storage data server health by ping-pong",
			slog.Duration("retry-after", duration),
			slog.Any("error", err),
		)
		n.becomeUnavailable(failureEpoch)
	})
}

func (n *controller) healthWatchWithRetries() {
	_ = backoff.RetryNotify(func() error {
		n.logger.Debug("Start new health watch cycle")
		healthClient, err := n.rpc.GetHealthClient(n.dataServer.GetIdentity())
		if err != nil {
			return err
		}

		watchStream, err := healthClient.Watch(n.ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
		if err != nil {
			return err
		}
		for {
			select {
			case <-n.ctx.Done():
				return nil
			default:
				epoch := n.currentStatusEpoch()
				response, recvErr := watchStream.Recv()
				err := n.handleHealthCheck(epoch, response, recvErr)
				switch {
				case err == nil:
				case errors.Is(err, errNotServing):
					// The data server deliberately reported NOT_SERVING: fail
					// it right away, and keep watching for it to come back.
					n.logger.Warn("Data server reported it is not serving")
					n.becomeUnavailable(epoch)
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
	}, n.healthCheckBackoff, func(err error, duration time.Duration) {
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
	// while the peer is unreachable.
	healthClient, err := n.rpc.GetHealthClient(n.dataServer.GetIdentity())
	if err != nil {
		// Leave it to the ping loop, which fences on this same error
		return
	}

	epoch := n.currentStatusEpoch()
	switch err := n.doHealthPing(epoch, healthClient); {
	case err == nil:
		// The node is alive: the watch stream reset was transient
	case errors.Is(err, errNotServing):
		n.logger.Warn("Data server reported it is not serving")
		n.becomeUnavailable(epoch)
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
		n.becomeUnavailable(epoch)
	}
}

// isTransportClosing matches the gRPC client error surfaced when the
// underlying connection is being torn down (e.g. the client pool closing it,
// or a server draining on shutdown): the RPC failed locally, proving nothing
// about the peer's health.
func isTransportClosing(err error) bool {
	return err != nil && strings.Contains(grpcstatus.Convert(err).Message(), "transport is closing")
}

func (n *controller) becomeUnavailable(observedEpoch int64) {
	n.mu.Lock()
	status := n.status.Load()
	if status.epoch != observedEpoch {
		// The status changed since the failing observation was made (e.g.
		// the node already recovered through the watch path): the failure is
		// stale and must not fence the node again. The next probe evaluates
		// the fresh state.
		n.mu.Unlock()
		return
	}
	if status.GetState() != Running && status.GetState() != Draining {
		n.mu.Unlock()
		return
	}
	// A Draining data server stays Draining: its epoch still advances, to
	// discard the observations in flight
	next := status.GetState()
	if next == Running {
		next = NotRunning
	}
	status.Advance(next)
	n.status.Publish(status)
	n.mu.Unlock()

	n.failedHealthChecksCounter.Inc()
	n.listener.BecameUnavailable(n.dataServer.GetIdentity())
}

func (n *controller) becomeAvailable(observedEpoch int64) {
	n.mu.Lock()
	status := n.status.Load()
	if status.epoch != observedEpoch {
		// The node was fenced while the probe was in flight: the SERVING
		// answer is stale. The next probe will re-evaluate.
		n.mu.Unlock()
		return
	}
	if status.GetState() != NotRunning || n.handshaking {
		// Nothing to do: the node is Running or Draining, or a handshake is
		// already in flight.
		n.mu.Unlock()
		return
	}
	n.handshaking = true
	n.mu.Unlock()

	n.healthCheckBackoff.Reset()

	// Bind the node before it can receive other internal traffic.
	handshakeCtx, handshakeCancel := context.WithTimeout(n.ctx, n.healthPolicy.ProbeTimeout)
	handshake, err := n.rpc.Handshake(handshakeCtx, n.dataServer.GetIdentity(), &proto.HandshakeRequest{
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

	n.mu.Lock()
	n.handshaking = false
	if err != nil {
		// The node stays NotRunning: the next SERVING observation retries
		n.mu.Unlock()
		n.logger.Warn("Failed to handshake with data server", slog.Any("error", err))
		return
	}
	features := handshake.FeaturesSupported
	featuresChanged := len(feature.Missing(features, n.supportedFeatures)) > 0 ||
		len(feature.Missing(n.supportedFeatures, features)) > 0
	n.supportedFeatures = features
	if status = n.status.Load(); status.GetState() == NotRunning && status.epoch == observedEpoch {
		status.Advance(Running)
		n.status.Publish(status)
		n.logger.Info("Storage data server is back online")
	}
	n.mu.Unlock()

	if featuresChanged {
		// The features the node supports were not known before this handshake:
		// it is the first one, or the node restarted with another binary
		n.listener.FeaturesDiscovered(n.dataServer.GetIdentity())
	}
}

func (n *controller) handleHealthCheck(observedEpoch int64, response *grpc_health_v1.HealthCheckResponse, err error) error {
	if err != nil {
		if !errors.Is(err, context.Canceled) && grpcstatus.Code(err) != codes.Canceled && !isTransportClosing(err) {
			n.logger.Warn("Data server health check failed", slog.Any("error", err))
		}
		return err
	}
	if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return errNotServing
	}
	n.becomeAvailable(observedEpoch)
	return nil
}

func NewController(ctx context.Context, dataServer *proto.DataServer,
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
		assignmentsProvider: shardAssignmentsProvider,
		listener:            dataServerEventListener,
		rpc:                 rpcProvider,
		insID:               insID,
		status:              commonwatch.New(NewStatus(NotRunning)),
		supportedFeatures:   make([]proto.Feature, 0),
		logger:              logger,
		healthPolicy:        healthPolicy,
		initialRetryBackoff: initialRetryBackoff,
		healthCheckBackoff:  commontime.NewConcurrentBackOff(commontime.NewBackOffWithInitialInterval(dataServerCtx, initialRetryBackoff)),
		failedHealthChecksCounter: metric.NewCounter("oxia_coordinator_node_health_checks_failed",
			"The number of failed health checks to a dataServer", "count", labels),
	}
	nc.runningGauge = metric.NewGauge("oxia_coordinator_node_running",
		"Whether the dataServer is considered to be running by the coordinator", "count", labels, func() int64 {
			if nc.GetStatus().GetState() == Running {
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

	nc.logger.Info("Started data server controller")
	return nc
}
