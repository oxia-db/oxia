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
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/commonio"
	commonobject "github.com/oxia-db/oxia/common/object"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	commontime "github.com/oxia-db/oxia/common/time"
)

type DataServerStatus uint32

const (
	Running DataServerStatus = iota
	NotRunning
	Draining //
)

const (
	healthCheckProbeInterval   = 2 * time.Second
	healthCheckProbeTimeout    = 2 * time.Second
	defaultInitialRetryBackoff = 10 * time.Second
)

type ShardAssignmentsProvider interface {
	WaitForNextUpdate(ctx context.Context, currentValue *proto.ShardAssignments) (*proto.ShardAssignments, error)
}

// The DataServerController takes care of checking the health-status of each dataServer
// and to push all the service discovery updates.
type DataServerController interface {
	io.Closer

	GetDataServer() commonobject.Borrowed[*proto.DataServer]

	Status() DataServerStatus

	SupportedFeatures() []proto.Feature

	SetStatus(status DataServerStatus)
}

type dataServerController struct {
	logger *slog.Logger
	wg     sync.WaitGroup
	ShardAssignmentsProvider
	DataServerEventListener

	ctx        context.Context
	ctxCancel  context.CancelFunc
	dataServer *proto.DataServer
	rpc        rpc.Provider
	insID      string
	closed     atomic.Bool

	statusLock        sync.RWMutex
	status            DataServerStatus
	supportedFeatures atomic.Value

	healthClientOnce   sync.Once
	healthClient       grpc_health_v1.HealthClient
	healthClientCloser io.Closer

	healthCheckBackoff         *commontime.ConcurrentBackOff
	dispatchAssignmentsBackoff backoff.BackOff

	dataServerRunningGauge metric.Gauge
	failedHealthChecks     metric.Counter
}

func (n *dataServerController) GetDataServer() commonobject.Borrowed[*proto.DataServer] {
	return commonobject.Borrow(n.dataServer)
}

func (n *dataServerController) Status() DataServerStatus {
	n.statusLock.RLock()
	defer n.statusLock.RUnlock()
	return n.status
}

func (n *dataServerController) SupportedFeatures() []proto.Feature {
	return n.supportedFeatures.Load().([]proto.Feature) //nolint: revive
}

func (n *dataServerController) SetStatus(status DataServerStatus) {
	n.statusLock.Lock()
	defer n.statusLock.Unlock()
	previous := n.status
	n.status = status
	n.logger.Info("Changed status", slog.Any("from", previous), slog.Any("to", status))
}

func (n *dataServerController) maybeInitHealthClient() {
	n.healthClientOnce.Do(func() {
		_ = backoff.RetryNotify(func() error {
			health, closer, err := n.rpc.GetHealthClient(n.dataServer.GetIdentity())
			if err != nil {
				return err
			}
			n.healthClient = health
			n.healthClientCloser = closer
			return nil
		}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
			n.logger.Warn(
				"Failed to create health client to storage data server",
				slog.Duration("retry-after", duration),
				slog.Any("error", err),
			)
		})
	})
}

func (n *dataServerController) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil
	}
	n.dataServerRunningGauge.Unregister()
	n.ctxCancel()
	n.wg.Wait()

	var err error
	if err = n.healthClientCloser.Close(); err != nil {
		n.logger.Warn("close data server controller health client failed", slog.Any("error", err))
	}
	n.logger.Info("Closed data server controller")
	return err
}

func (n *dataServerController) sendAssignmentsDispatchWithRetries() {
	_ = backoff.RetryNotify(func() error {
		n.logger.Debug("Ready to send assignments")

		stream, err := n.rpc.PushShardAssignments(n.ctx, n.dataServer.GetIdentity())
		if err != nil {
			n.logger.Debug("Failed to create shard assignments stream", slog.Any("error", err))
			return err
		}
		streamCtx := stream.Context()
		var assignments *proto.ShardAssignments
		for {
			select {
			case <-n.ctx.Done():
				return nil
			case <-streamCtx.Done():
				return streamCtx.Err()
			default:
				n.logger.Debug(
					"Waiting for next assignments update",
					slog.Any("current-assignments", assignments),
				)
				if assignments, err = n.WaitForNextUpdate(streamCtx, assignments); err != nil {
					n.logger.Debug("Failed to send assignments", slog.Any("error", err))
					return err
				}
				if assignments == nil {
					continue
				}

				n.logger.Debug("Sending assignments", slog.Any("assignments", assignments))
				if err := stream.Send(assignments); err != nil {
					n.logger.Debug("Failed to send assignments", slog.Any("error", err))
					return err
				}
				n.logger.Debug("Send assignments completed successfully")
				n.dispatchAssignmentsBackoff.Reset()
			}
		}
	}, n.dispatchAssignmentsBackoff, func(err error, duration time.Duration) {
		if !errors.Is(err, context.Canceled) {
			n.logger.Warn(
				"Failed to send assignments updates to storage data server",
				slog.Duration("retry-after", duration),
				slog.Any("error", err),
			)
		}
	})
}

func (n *dataServerController) doHealthPing() error {
	pingCtx, pingCancel := context.WithTimeout(n.ctx, healthCheckProbeTimeout)
	response, err := n.healthClient.Check(pingCtx, &grpc_health_v1.HealthCheckRequest{Service: ""})
	pingCancel()
	return n.healthCheckHandler(response, err)
}

func (n *dataServerController) healthPingWithRetries() {
	_ = backoff.RetryNotify(func() error {
		n.maybeInitHealthClient()
		// Immediate check on startup instead of waiting for first tick
		if err := n.doHealthPing(); err != nil {
			return err
		}
		ticker := time.NewTicker(healthCheckProbeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-n.ctx.Done():
				return nil
			case <-ticker.C:
				if err := n.doHealthPing(); err != nil {
					n.logger.Warn("Data server stopped responding to ping")
					return err
				}
			}
		}
	}, n.healthCheckBackoff, func(err error, duration time.Duration) {
		n.logger.Warn(
			"Failed to check storage data server health by ping-pong",
			slog.Duration("retry-after", duration),
			slog.Any("error", err),
		)
		n.becomeUnavailable()
	})
}

func (n *dataServerController) healthWatchWithRetries() {
	_ = backoff.RetryNotify(func() error {
		n.logger.Debug("Start new health watch cycle")
		n.maybeInitHealthClient()

		watchStream, err := n.healthClient.Watch(n.ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
		if err != nil {
			return err
		}
		for {
			select {
			case <-n.ctx.Done():
				return nil
			default:
				if err := n.healthCheckHandler(watchStream.Recv()); err != nil {
					return err
				}
			}
		}
	}, n.healthCheckBackoff, func(err error, duration time.Duration) {
		n.logger.Warn("Failed to check storage data server health by watch",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
		n.becomeUnavailable()
	})
}

func (n *dataServerController) becomeUnavailable() {
	currentStatus := n.Status()
	if currentStatus != Running && currentStatus != Draining {
		return
	}
	n.statusLock.Lock()
	if n.status != Running && n.status != Draining { // double check
		n.statusLock.Unlock()
		return
	}
	if n.status == Running {
		n.status = NotRunning
	}
	n.statusLock.Unlock()

	n.failedHealthChecks.Inc()
	n.BecameUnavailable(n.dataServer.GetIdentity())
}

func (n *dataServerController) becomeAvailable() {
	if n.Status() != NotRunning {
		return
	}

	n.logger.Info("Storage data server is back online")

	// To avoid the send assignments stream to miss the notification about the current
	// dataServer went down, we interrupt the current stream when the ping on the dataServer fails
	n.rpc.ClearPooledConnections(n.dataServer.GetIdentity())
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
	if n.status == NotRunning {
		n.status = Running
	}
	n.statusLock.Unlock()
}

func (n *dataServerController) healthCheckHandler(response *grpc_health_v1.HealthCheckResponse, err error) error {
	if err != nil {
		if !errors.Is(err, context.Canceled) && grpcstatus.Code(err) != codes.Canceled {
			n.logger.Warn("Data server health check failed", slog.Any("error", err))
		}
		return err
	}
	if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return errors.New("Data server is not actively serving")
	}
	n.becomeAvailable()
	return nil
}

func NewDataServerController(ctx context.Context, dataServer *proto.DataServer,
	shardAssignmentsProvider ShardAssignmentsProvider,
	dataServerEventListener DataServerEventListener,
	rpcProvider rpc.Provider,
	insID string) DataServerController {
	return newDataServerController(ctx, dataServer, shardAssignmentsProvider, dataServerEventListener, rpcProvider, insID, defaultInitialRetryBackoff)
}

func newDataServerController(ctx context.Context, dataServer *proto.DataServer,
	shardAssignmentsProvider ShardAssignmentsProvider,
	dataServerEventListener DataServerEventListener,
	rpcProvider rpc.Provider,
	insID string,
	initialRetryBackoff time.Duration) DataServerController {
	dataServerCtx, cancel := context.WithCancel(ctx)
	dataServerID := dataServer.GetIdentity().GetNameOrDefault()
	labels := map[string]any{"data-server": dataServerID}

	supportedFeatures := atomic.Value{}
	supportedFeatures.Store(make([]proto.Feature, 0))

	nc := &dataServerController{
		ctx:                      dataServerCtx,
		ctxCancel:                cancel,
		dataServer:               dataServer,
		ShardAssignmentsProvider: shardAssignmentsProvider,
		DataServerEventListener:  dataServerEventListener,
		rpc:                      rpcProvider,
		insID:                    insID,
		statusLock:               sync.RWMutex{},
		status:                   NotRunning,
		supportedFeatures:        supportedFeatures,
		logger: slog.With(
			slog.String("component", "data-server-controller"),
			slog.Any("data-server", dataServerID),
		),
		healthClientOnce:           sync.Once{},
		healthClient:               nil,
		healthClientCloser:         &commonio.NopCloser{},
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
