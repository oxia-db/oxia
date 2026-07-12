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
	"io"
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

	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	controllerapi "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"

	"github.com/oxia-db/oxia/common/constant"
	commonobject "github.com/oxia-db/oxia/common/object"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
)

type Status uint32

const (
	Running Status = iota
	NotRunning
	Draining //
)

func (s Status) ToProto() proto.DataServerState {
	switch s {
	case Running:
		return proto.DataServerState_DATA_SERVER_STATE_RUNNING
	case NotRunning:
		return proto.DataServerState_DATA_SERVER_STATE_UNAVAILABLE
	case Draining:
		return proto.DataServerState_DATA_SERVER_STATE_DRAINING
	default:
		return proto.DataServerState_DATA_SERVER_STATE_UNSPECIFIED
	}
}

const defaultInitialRetryBackoff = 10 * time.Second

// errNotServing marks a health response where the data server deliberately
// reported NOT_SERVING (e.g. it is shutting down), as opposed to a probe that
// failed at the transport level.
var errNotServing = errors.New("data server is not actively serving")

// healthCheckPolicy controls how the controller distinguishes a dead data
// server from one that is merely slow. A node that is saturated but healthy
// can miss individual probe deadlines; declaring it unavailable fences its
// shard leaders, moves the load elsewhere and makes the next node miss its
// probes in turn. Requiring several consecutive probe failures keeps that
// feedback loop from starting, while a deliberate NOT_SERVING report still
// fails the node immediately.
type healthCheckPolicy struct {
	probeInterval time.Duration
	probeTimeout  time.Duration
	// failureThreshold is the number of consecutive failed probes after which
	// the data server is declared unavailable.
	failureThreshold int
}

var defaultHealthCheckPolicy = healthCheckPolicy{
	probeInterval:    2 * time.Second,
	probeTimeout:     5 * time.Second,
	failureThreshold: 3,
}

// The Controller takes care of checking the health-status of each dataServer
// and to push all the service discovery updates.
type Controller interface {
	io.Closer

	GetDataServer() commonobject.Borrowed[*proto.DataServer]

	Status() Status

	// IsStablyRunning reports whether the data server is Running and, if it
	// previously failed a health check, whether it has now been Running
	// continuously for at least window. The load balancer uses it to keep a
	// freshly recovered node out of leader rebalancing until it proves
	// stable, so that a flapping node does not get leaders moved back onto
	// it just to lose them again.
	IsStablyRunning(window time.Duration) bool

	SupportedFeatures() []proto.Feature

	SetStatus(status Status)
}

type controller struct {
	logger *slog.Logger
	wg     sync.WaitGroup
	controllerapi.ShardAssignmentsProvider
	controllerapi.DataServerEventListener

	ctx        context.Context
	ctxCancel  context.CancelFunc
	dataServer *proto.DataServer
	rpc        rpc.Provider
	insID      string
	closed     atomic.Bool

	statusLock        sync.RWMutex
	status            Status
	supportedFeatures atomic.Value
	// runningSince is when the node last transitioned to Running;
	// everUnavailable records whether it ever failed a health check;
	// statusEpoch increments on every status transition, so that a probe
	// observation (a SERVING answer or a failure) obtained before a
	// concurrent transition can be recognized as stale and discarded instead
	// of flapping the status back. All are guarded by statusLock.
	runningSince    time.Time
	everUnavailable bool
	statusEpoch     int64

	healthPolicy               healthCheckPolicy
	healthCheckBackoff         *commontime.ConcurrentBackOff
	dispatchAssignmentsBackoff backoff.BackOff

	dataServerRunningGauge metric.Gauge
	failedHealthChecks     metric.Counter
}

func (n *controller) GetDataServer() commonobject.Borrowed[*proto.DataServer] {
	return commonobject.Borrow(n.dataServer)
}

func (n *controller) Status() Status {
	n.statusLock.RLock()
	defer n.statusLock.RUnlock()
	return n.status
}

func (n *controller) SupportedFeatures() []proto.Feature {
	return n.supportedFeatures.Load().([]proto.Feature) //nolint: revive
}

func (n *controller) SetStatus(status Status) {
	n.statusLock.Lock()
	defer n.statusLock.Unlock()
	previous := n.status
	n.status = status
	if status != previous {
		n.statusEpoch++
	}
	if status == Running && previous != Running {
		n.runningSince = time.Now()
	}
	n.logger.Info("Changed status", slog.Any("from", previous), slog.Any("to", status))
}

func (n *controller) IsStablyRunning(window time.Duration) bool {
	n.statusLock.RLock()
	defer n.statusLock.RUnlock()
	if n.status != Running {
		return false
	}
	// A node that never failed a health check (e.g. it just joined, or the
	// whole cluster just started) is considered stable right away.
	return !n.everUnavailable || time.Since(n.runningSince) >= window
}

func (n *controller) currentStatusEpoch() int64 {
	n.statusLock.RLock()
	defer n.statusLock.RUnlock()
	return n.statusEpoch
}

func (n *controller) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil
	}
	n.dataServerRunningGauge.Unregister()
	n.ctxCancel()
	n.wg.Wait()

	n.logger.Info("Closed data server controller")
	return nil
}

func (n *controller) sendAssignmentsDispatchWithRetries() {
	receiver := n.SubscribeShardAssignments()

	_ = backoff.RetryNotify(func() error {
		n.logger.Debug("Ready to send assignments")

		stream, err := n.rpc.PushShardAssignments(n.ctx, n.dataServer.GetIdentity())
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
				n.dispatchAssignmentsBackoff.Reset()
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
	}, n.dispatchAssignmentsBackoff, func(err error, duration time.Duration) {
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
	pingCtx, pingCancel := context.WithTimeout(n.ctx, n.healthPolicy.probeTimeout)
	response, err := healthClient.Check(pingCtx, &grpc_health_v1.HealthCheckRequest{Service: ""})
	pingCancel()
	return n.healthCheckHandler(observedEpoch, response, err)
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

		ticker := time.NewTicker(n.healthPolicy.probeInterval)
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
				if consecutiveFailures >= n.healthPolicy.failureThreshold {
					n.logger.Warn("Data server stopped responding to ping",
						slog.Int("consecutive-failures", consecutiveFailures))
					failureEpoch = epoch
					return err
				}
				// A slow probe on a saturated node is not yet a failure:
				// keep probing at the regular cadence
				n.logger.Warn("Data server health ping failed, tolerating below failure threshold",
					slog.Int("consecutive-failures", consecutiveFailures),
					slog.Int("failure-threshold", n.healthPolicy.failureThreshold),
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
				err := n.healthCheckHandler(epoch, response, recvErr)
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
	n.statusLock.Lock()
	if n.statusEpoch != observedEpoch {
		// The status changed since the failing observation was made (e.g.
		// the node already recovered through the watch path): the failure is
		// stale and must not fence the node again. The next probe evaluates
		// the fresh state.
		n.statusLock.Unlock()
		return
	}
	if n.status != Running && n.status != Draining {
		n.statusLock.Unlock()
		return
	}
	if n.status == Running {
		n.status = NotRunning
	}
	n.everUnavailable = true
	n.statusEpoch++
	n.statusLock.Unlock()

	n.failedHealthChecks.Inc()
	n.BecameUnavailable(n.dataServer.GetIdentity())
}

func (n *controller) becomeAvailable(observedEpoch int64) {
	if n.currentStatusEpoch() != observedEpoch {
		// The node was fenced while the probe was in flight: the SERVING
		// answer is stale. The next probe will re-evaluate.
		return
	}
	if n.Status() != NotRunning {
		return
	}

	n.logger.Info("Storage data server is back online")

	n.healthCheckBackoff.Reset()

	// Bind the node before it can receive other internal traffic.
	bo := commontime.NewBackOffWithInitialInterval(n.ctx, defaultInitialRetryBackoff)
	if err := backoff.RetryNotify(func() error {
		handshake, err := n.rpc.Handshake(n.ctx, n.dataServer.GetIdentity(), &proto.HandshakeRequest{
			InstanceId: n.insID,
		})
		if err != nil {
			return err
		}
		switch handshake.Status {
		case proto.HandshakeStatus_HANDSHAKE_STATUS_BOUND, proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND:
			n.supportedFeatures.Store(handshake.FeaturesSupported)
			return nil
		case proto.HandshakeStatus_HANDSHAKE_STATUS_MISMATCH:
			return errors.New("data server instance id mismatch")
		default:
			return errors.Errorf("unexpected handshake status: %s", handshake.Status.String())
		}
	}, bo, func(err error, duration time.Duration) {
		n.logger.Warn("Failed to handshake with data server",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	}); err != nil {
		return
	}

	n.statusLock.Lock()
	if n.status == NotRunning && n.statusEpoch == observedEpoch {
		n.status = Running
		n.runningSince = time.Now()
		n.statusEpoch++
	}
	n.statusLock.Unlock()
}

func (n *controller) healthCheckHandler(observedEpoch int64, response *grpc_health_v1.HealthCheckResponse, err error) error {
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
	rpcProvider rpc.Provider,
	insID string) Controller {
	return newController(ctx, dataServer, shardAssignmentsProvider, dataServerEventListener, rpcProvider, insID,
		defaultInitialRetryBackoff, defaultHealthCheckPolicy)
}

func newController(ctx context.Context, dataServer *proto.DataServer,
	shardAssignmentsProvider controllerapi.ShardAssignmentsProvider,
	dataServerEventListener controllerapi.DataServerEventListener,
	rpcProvider rpc.Provider,
	insID string,
	initialRetryBackoff time.Duration,
	healthPolicy healthCheckPolicy) Controller {
	dataServerCtx, cancel := context.WithCancel(ctx)
	dataServerID := dataServer.GetIdentity().GetNameOrDefault()
	labels := map[string]any{"data-server": dataServerID}
	logger := slog.With(
		slog.String("component", "data-server-controller"),
		slog.Any("data-server", dataServerID),
	)

	supportedFeatures := atomic.Value{}
	supportedFeatures.Store(make([]proto.Feature, 0))

	nc := &controller{
		ctx:                        dataServerCtx,
		ctxCancel:                  cancel,
		dataServer:                 dataServer,
		ShardAssignmentsProvider:   shardAssignmentsProvider,
		DataServerEventListener:    dataServerEventListener,
		rpc:                        rpcProvider,
		insID:                      insID,
		statusLock:                 sync.RWMutex{},
		status:                     NotRunning,
		supportedFeatures:          supportedFeatures,
		logger:                     logger,
		healthPolicy:               healthPolicy,
		healthCheckBackoff:         commontime.NewConcurrentBackOff(commontime.NewBackOffWithInitialInterval(dataServerCtx, initialRetryBackoff)),
		dispatchAssignmentsBackoff: commontime.NewBackOffWithInitialInterval(dataServerCtx, initialRetryBackoff),
		failedHealthChecks: metric.NewCounter("oxia_coordinator_node_health_checks_failed",
			"The number of failed health checks to a dataServer", "count", labels),
	}
	nc.dataServerRunningGauge = metric.NewGauge("oxia_coordinator_node_running",
		"Whether the dataServer is considered to be running by the coordinator", "count", labels, func() int64 {
			if nc.Status() == Running {
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
