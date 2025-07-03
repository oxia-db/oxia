// Copyright 2023 StreamNative, Inc.
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

package controllers

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"google.golang.org/grpc/health/grpc_health_v1"

	commonio "github.com/oxia-db/oxia/common/io"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	commontime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/rpc"
	"github.com/oxia-db/oxia/proto"
)

type NodeStatus uint32

const (
	Running NodeStatus = iota
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

// The NodeController takes care of checking the health-status of each node
// and to push all the service discovery updates.
type NodeController interface {
	io.Closer

	Status() NodeStatus

	SetStatus(status NodeStatus)
}

type nodeController struct {
	*slog.Logger
	sync.WaitGroup
	ShardAssignmentsProvider
	NodeEventListener

	ctx    context.Context
	cancel context.CancelFunc
	node   model.Server
	rpc    rpc.Provider
	closed atomic.Bool

	statusLock sync.RWMutex
	status     NodeStatus

	healthClientOnce   sync.Once
	healthClient       grpc_health_v1.HealthClient
	healthClientCloser io.Closer

	healthCheckBackoff         backoff.BackOff
	dispatchAssignmentsBackoff backoff.BackOff

	nodeIsRunningGauge metric.Gauge
	failedHealthChecks metric.Counter
}

func (n *nodeController) Status() NodeStatus {
	n.statusLock.RLock()
	defer n.statusLock.RUnlock()
	return n.status
}

func (n *nodeController) SetStatus(status NodeStatus) {
	n.statusLock.Lock()
	defer n.statusLock.Unlock()
	previous := n.status
	n.status = status
	n.Info("Changed status", slog.Any("from", previous), slog.Any("to", status))
}

func (n *nodeController) maybeInitHealthClient() {
	n.healthClientOnce.Do(func() {
		_ = backoff.RetryNotify(func() error {
			health, closer, err := n.rpc.GetHealthClient(n.node)
			if err != nil {
				return err
			}
			n.healthClient = health
			n.healthClientCloser = closer
			return nil
		}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
			n.Warn(
				"Failed to create health client to storage node",
				slog.Duration("retry-after", duration),
				slog.Any("error", err),
			)
		})
	})
}

func (n *nodeController) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil
	}
	n.nodeIsRunningGauge.Unregister()
	n.cancel()
	n.Wait()

	err := n.healthClientCloser.Close()
	n.Info("Closed node controller")
	return err
}

func (n *nodeController) sendAssignmentsDispatchWithRetries() {
	defer n.Done()
	_ = backoff.RetryNotify(func() error {
		n.Debug("Ready to send assignments")

		stream, err := n.rpc.PushShardAssignments(n.ctx, n.node)
		if err != nil {
			n.Debug("Failed to create shard assignments stream", slog.Any("error", err))
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
				n.Debug(
					"Waiting for next assignments update",
					slog.Any("current-assignments", assignments),
				)
				if assignments, err = n.WaitForNextUpdate(streamCtx, assignments); err != nil {
					n.Debug("Failed to send assignments", slog.Any("error", err))
					return err
				}
				if assignments == nil {
					continue
				}

				n.Debug("Sending assignments", slog.Any("assignments", assignments))
				if err := stream.Send(assignments); err != nil {
					n.Debug("Failed to send assignments", slog.Any("error", err))
					return err
				}
				n.Debug("Send assignments completed successfully")
				n.dispatchAssignmentsBackoff.Reset()
			}
		}
	}, n.dispatchAssignmentsBackoff, func(err error, duration time.Duration) {
		n.Warn(
			"Failed to send assignments updates to storage node",
			slog.Duration("retry-after", duration),
			slog.Any("error", err),
		)
	})
}

func (n *nodeController) healthPingWithRetries() {
	defer n.Done()
	_ = backoff.RetryNotify(func() error {
		n.maybeInitHealthClient()
		ticker := time.NewTicker(healthCheckProbeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-n.ctx.Done():
				return nil
			case <-ticker.C:
				pingCtx, pingCancel := context.WithTimeout(n.ctx, healthCheckProbeTimeout)
				response, err := n.healthClient.Check(pingCtx, &grpc_health_v1.HealthCheckRequest{Service: ""})
				pingCancel()
				if err := n.healthCheckHandler(response, err); err != nil {
					n.Warn("Node stopped responding to ping")
					return err
				}
			}
		}
	}, n.dispatchAssignmentsBackoff, func(err error, duration time.Duration) {
		n.Warn(
			"Failed to send ping to storage node",
			slog.Duration("retry-after", duration),
			slog.Any("error", err),
		)
		n.becomeUnavailable()
	})
}

func (n *nodeController) healthWatchWithRetries() {
	defer n.Done()
	_ = backoff.RetryNotify(func() error {
		n.Debug("Start new health watch cycle")
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
		n.Warn("Storage node health watch failed",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
		n.becomeUnavailable()
	})
}

func (n *nodeController) becomeUnavailable() {
	n.statusLock.Lock()
	if n.status != Running && n.status != Draining {
		return
	}
	if n.status == Running {
		n.status = NotRunning
	}
	n.statusLock.Unlock()

	n.failedHealthChecks.Inc()
	n.NodeBecameUnavailable(n.node)
}

func (n *nodeController) healthCheckHandler(response *grpc_health_v1.HealthCheckResponse, err error) error {
	if err != nil {
		n.Warn("Node watcher check failed", slog.Any("error", err))
		return err
	}
	if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return errors.New("node is not actively serving")
	}

	n.statusLock.Lock()
	if n.status == NotRunning {
		n.Info("Storage node is back online")

		// To avoid the send assignments stream to miss the notification about the current
		// node went down, we interrupt the current stream when the ping on the node fails
		n.rpc.ClearPooledConnections(n.node)
		n.healthCheckBackoff.Reset()
	}
	n.status = Running
	n.statusLock.Unlock()
	return nil
}

func NewNodeController(ctx context.Context, node model.Server,
	shardAssignmentsProvider ShardAssignmentsProvider,
	nodeEventListener NodeEventListener,
	rpcProvider rpc.Provider) NodeController {
	return newNodeController(ctx, node, shardAssignmentsProvider, nodeEventListener, rpcProvider, defaultInitialRetryBackoff)
}

func newNodeController(parentCtx context.Context, node model.Server,
	shardAssignmentsProvider ShardAssignmentsProvider,
	nodeEventListener NodeEventListener,
	rpcProvider rpc.Provider,
	initialRetryBackoff time.Duration) NodeController {
	nodeCtx, cancel := context.WithCancel(parentCtx)
	nodeID := node.GetIdentifier()
	labels := map[string]any{"node": nodeID}
	nc := &nodeController{
		ctx:                      nodeCtx,
		cancel:                   cancel,
		node:                     node,
		ShardAssignmentsProvider: shardAssignmentsProvider,
		NodeEventListener:        nodeEventListener,
		rpc:                      rpcProvider,
		statusLock:               sync.RWMutex{},
		status:                   Running,
		Logger: slog.With(
			slog.String("component", "node-controller"),
			slog.Any("node", nodeID),
		),
		healthClientOnce:           sync.Once{},
		healthClient:               nil,
		healthClientCloser:         &commonio.NopCloser{},
		healthCheckBackoff:         commontime.NewBackOffWithInitialInterval(nodeCtx, initialRetryBackoff),
		dispatchAssignmentsBackoff: commontime.NewBackOffWithInitialInterval(nodeCtx, initialRetryBackoff),
		failedHealthChecks: metric.NewCounter("oxia_coordinator_node_health_checks_failed",
			"The number of failed health checks to a node", "count", labels),
	}
	nc.nodeIsRunningGauge = metric.NewGauge("oxia_coordinator_node_running",
		"Whether the node is considered to be running by the coordinator", "count", labels, func() int64 {
			if nc.Status() == Running {
				return 1
			}
			return 0
		})

	nc.Add(1)
	go process.DoWithLabels(
		nc.ctx,
		map[string]string{
			"component": "node-controller-health-watch",
			"node":      nodeID,
		},
		nc.healthWatchWithRetries,
	)

	nc.Add(1)
	go process.DoWithLabels(
		nc.ctx,
		map[string]string{
			"component": "node-controller-health-ping",
			"node":      nodeID,
		},
		nc.healthPingWithRetries,
	)

	nc.Add(1)
	go process.DoWithLabels(
		nc.ctx,
		map[string]string{
			"component": "node-controller-assignment-dispatcher",
			"node":      nodeID,
		},
		nc.sendAssignmentsDispatchWithRetries,
	)

	nc.Info("Started node controller")
	return nc
}
