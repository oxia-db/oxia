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

package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/commonio"
	"github.com/oxia-db/oxia/common/failpoint"
	commonmetric "github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	coordreconciler "github.com/oxia-db/oxia/oxiad/coordinator/reconciler"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"

	"github.com/oxia-db/oxia/oxiad/common/metric"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	commonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
)

type coordinator struct {
	mu     sync.Mutex
	closed bool

	ctx          context.Context
	ctxCancel    context.CancelFunc
	wg           sync.WaitGroup
	logger       *slog.Logger
	optionsWatch *commonwatch.Watch[*option.Options]

	grpcServer       commonrpc.GrpcServer
	managementServer commonrpc.GrpcServer
	healthServer     *health.Server
	reconciler       coordreconciler.Reconciler
	runtime          coordruntime.Runtime
	metadata         coordmetadata.Metadata
	metadataFactory  *coordmetadata.Factory
	metrics          *metric.PrometheusMetrics
	leaderGauge      commonmetric.Gauge

	// lost is closed once the coordinator lost the metadata leadership and
	// stopped coordinating, and stopErr is the error of stopping it. Owner: the
	// leadership watcher, which writes stopErr before closing lost. Borrowers:
	// LeadershipLost and the leader gauge, which only receive from lost, and
	// Close, which reads stopErr once lost is closed, after waiting for the watcher.
	lost    chan struct{}
	stopErr error
}

func New(ctx context.Context, optionsWatch *commonwatch.Watch[*option.Options]) (_ Coordinator, err error) {
	options := optionsWatch.Load()
	options.WithDefault()
	if err := options.Validate(); err != nil {
		return nil, err
	}
	slog.Info("Starting Oxia coordinator", slog.Any("options", options))

	ctx, cancel := context.WithCancel(ctx)
	healthServer := health.NewServer()
	var (
		grpcServer           commonrpc.GrpcServer
		managementGrpcServer commonrpc.GrpcServer
		reconciler           coordreconciler.Reconciler
		runtime              coordruntime.Runtime
		metadata             coordmetadata.Metadata
		metadataFactory      *coordmetadata.Factory
		metricsServer        *metric.PrometheusMetrics
	)
	leader := &atomic.Bool{}
	lost := make(chan struct{})
	leaderGauge := commonmetric.NewGauge("oxia_coordinator_is_leader",
		"Whether the coordinator holds the leadership of the cluster metadata", "", nil, leaderGaugeValue(leader, lost))
	defer func() {
		if err == nil {
			return
		}

		cancel()
		leaderGauge.Unregister()
		if metricsServer != nil {
			err = multierr.Append(err, metricsServer.Close())
		}
		err = multierr.Append(err, commonio.CloseIfNotNil(managementGrpcServer))
		err = multierr.Append(err, commonio.CloseIfNotNil(reconciler))
		err = multierr.Append(err, commonio.CloseIfNotNil(runtime))
		err = multierr.Append(err, commonio.CloseIfNotNil(metadata))
		if metadataFactory != nil {
			err = multierr.Append(err, metadataFactory.Close())
		}
		err = multierr.Append(err, commonio.CloseIfNotNil(grpcServer))
	}()

	internalServer := options.Server.Internal
	internalServerTLS, err := internalServer.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	grpcServer, err = commonrpc.Default.StartGrpcServer("coordinator", internalServer.BindAddress, func(registrar grpc.ServiceRegistrar) { //nolint:contextcheck
		grpc_health_v1.RegisterHealthServer(registrar, healthServer)
	}, internalServerTLS, &internalServer.Auth, nil)
	if err != nil {
		return nil, err
	}
	metricsServer, err = startMetricsServer(&options.Observability.Metric) //nolint:contextcheck
	if err != nil {
		return nil, err
	}
	controller := &options.Controller
	controllerTLS, err := controller.TLS.TryIntoClientTLSConf()
	if err != nil {
		return nil, err
	}

	metadataFactory, err = coordmetadata.New(ctx, options)
	if err != nil {
		return nil, err
	}
	metadata = metadataFactory.CreateMetadata(ctx)

	managementSv := options.Server.Public
	managementSvTLS, err := managementSv.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	management := NewManagementServer(metadata)
	managementGrpcServer, err = commonrpc.Default.StartGrpcServer("public", managementSv.BindAddress, func(registrar grpc.ServiceRegistrar) { //nolint:contextcheck
		proto.RegisterOxiaAdminServer(registrar, management)
		grpc_health_v1.RegisterHealthServer(registrar, healthServer)
	}, managementSvTLS, &managementSv.Auth, nil)
	if err != nil {
		return nil, err
	}

	leadershipLost, err := metadata.WaitToBecomeLeader()
	if err != nil {
		return nil, err
	}
	if failLost, ok := failpoint.Eval[<-chan struct{}]("coordinator.leadershipLost"); ok {
		leadershipLost = failLost
	}
	leader.Store(true)
	runtime, err = coordruntime.New(ctx, metadata, rpc.NewControllerRPCProviderFactory(controllerTLS))
	if failErr, ok := failpoint.Eval[error]("coordinator.newRuntime"); ok {
		err = failErr
	}
	if err != nil {
		return nil, err
	}
	management.setRuntime(runtime)
	reconciler, err = coordreconciler.New(ctx, runtime)
	if err != nil {
		return nil, err
	}

	c := &coordinator{
		ctx:              ctx,
		ctxCancel:        cancel,
		wg:               sync.WaitGroup{},
		logger:           slog.With(slog.String("component", "grpc-server")),
		optionsWatch:     optionsWatch,
		grpcServer:       grpcServer,
		managementServer: managementGrpcServer,
		healthServer:     healthServer,
		reconciler:       reconciler,
		runtime:          runtime,
		metadata:         metadata,
		metadataFactory:  metadataFactory,
		metrics:          metricsServer,
		leaderGauge:      leaderGauge,
		lost:             lost,
	}
	c.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "option-watcher",
		}, c.bgOptionWatcher)
	})
	// A provider that can't lose the leadership returns a nil channel, which
	// never fires: the watcher then only ends on Close.
	c.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "leadership-watcher",
		}, func() {
			c.bgLeadershipWatcher(leadershipLost)
		})
	})

	return c, nil
}

// leaderGaugeValue reports 1 while the coordinator leads: from its win until it
// stopped coordinating on a loss, and 0 otherwise.
func leaderGaugeValue(leader *atomic.Bool, lost <-chan struct{}) func() int64 {
	return func() int64 {
		if !leader.Load() {
			return 0
		}
		select {
		case <-lost:
			return 0
		default:
			return 1
		}
	}
}

// startMetricsServer returns a nil server when the metrics are disabled.
func startMetricsServer(options *commonoption.MetricOptions) (*metric.PrometheusMetrics, error) {
	if !options.IsEnabled() {
		return nil, nil //nolint:nilnil
	}
	metricTLS, err := options.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	return metric.Start(options.BindAddress, metricTLS)
}

func (s *coordinator) LeadershipLost() <-chan struct{} {
	return s.lost
}

// bgLeadershipWatcher stops the coordination once the leadership is lost, so
// that this coordinator can't act alongside the new leader, and then reports
// the loss. It ends without stopping anything when the coordinator closes.
func (s *coordinator) bgLeadershipWatcher(leadershipLost <-chan struct{}) {
	select {
	case <-leadershipLost:
	case <-s.ctx.Done():
		return
	}
	s.logger.Error("Coordination leadership lost: stopping to avoid a split brain")
	s.ctxCancel()
	s.healthServer.Shutdown()
	s.stopErr = s.stopCoordination()
	close(s.lost)
}

// stopCoordination stops reconciling and coordinating the cluster, and
// releases the metadata. The coordinator's context must be canceled first: it
// ends the retries of the metadata writes, which would otherwise keep the
// runtime from closing once the leadership is lost.
func (s *coordinator) stopCoordination() error {
	return multierr.Combine(
		s.reconciler.Close(),
		s.runtime.Close(),
		s.metadata.Close(),
	)
}

func (s *coordinator) bgOptionWatcher() {
	receiver := s.optionsWatch.Subscribe()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-receiver.Changed():
		}

		coordinatorOptions := receiver.Load()

		s.logger.Info("configuration options has changed. processing the dynamic updates.")
		logOptions := &coordinatorOptions.Observability.Log
		if logging.ReconfigureLogger(logOptions) {
			s.logger.Info("reconfigured log options", slog.Any("options", logOptions))
		}
	}
}

func (s *coordinator) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	s.ctxCancel()
	s.wg.Wait()

	s.healthServer.Shutdown()
	err := multierr.Combine(
		s.grpcServer.Close(),
		s.managementServer.Close(),
	)
	select {
	case <-s.lost:
		// The leadership watcher stopped the coordination on the loss
		err = multierr.Append(err, s.stopErr)
	default:
		err = multierr.Append(err, s.stopCoordination())
	}
	err = multierr.Append(err, s.metadataFactory.Close())
	s.leaderGauge.Unregister()
	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}
	return err
}
