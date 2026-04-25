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

package coordinator

import (
	"context"
	"log/slog"
	"sync"

	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"

	"github.com/oxia-db/oxia/oxiad/common/metric"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/common/proto"
)

type GrpcServer struct {
	// concurrent control
	ctx              context.Context
	ctxCancel        context.CancelFunc
	wg               sync.WaitGroup
	logger           *slog.Logger
	watchableOptions *commonoption.Watch[*option.Options]

	grpcServer   rpc2.GrpcServer
	adminServer  rpc2.GrpcServer
	healthServer *health.Server
	coordinator  Coordinator
	metadata     coordmetadata.Metadata
	metrics      *metric.PrometheusMetrics
}

func NewGrpcServer(parent context.Context, watchableOptions *commonoption.Watch[*option.Options]) (*GrpcServer, error) {
	options, _ := watchableOptions.Load()
	slog.Info("Starting Oxia coordinator", slog.Any("options", options))

	ctx, cancel := context.WithCancel(parent)
	succeeded := false
	defer cancelContextOnFailure(cancel, &succeeded)

	healthServer := health.NewServer()

	var grpcServer rpc2.GrpcServer
	var adminGrpcServer rpc2.GrpcServer
	var metadata coordmetadata.Metadata
	var coordinatorInstance Coordinator
	var metricsServer *metric.PrometheusMetrics
	defer func() {
		if succeeded {
			return
		}
		if grpcServer != nil {
			_ = grpcServer.Close()
		}
		if adminGrpcServer != nil {
			_ = adminGrpcServer.Close()
		}
		if coordinatorInstance != nil {
			_ = coordinatorInstance.Close()
		}
		if metadata != nil {
			_ = metadata.Close()
		}
		if metricsServer != nil {
			_ = metricsServer.Close()
		}
	}()

	internalServer := options.Server.Internal
	internalServerTLS, err := internalServer.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	grpcServer, err = rpc2.Default.StartGrpcServer("coordinator", internalServer.BindAddress, func(registrar grpc.ServiceRegistrar) { //nolint:contextcheck
		grpc_health_v1.RegisterHealthServer(registrar, healthServer)
	}, internalServerTLS, &internalServer.Auth, nil)
	if err != nil {
		return nil, err
	}
	controller := &options.Controller
	controllerTLS, err := controller.TLS.TryIntoClientTLSConf()
	if err != nil {
		return nil, err
	}

	slog.Info("Waiting to become leader", slog.String("component", "coordinator"))
	metadata, err = coordmetadata.NewFromOptions(ctx, &options.Cluster, &options.Metadata)
	if err != nil {
		return nil, err
	}
	slog.Info("This coordinator is now leader", slog.String("component", "coordinator"))

	coordinatorInstance, err = NewCoordinator(metadata, rpc.NewRpcProviderFactory(controllerTLS)) //nolint:contextcheck
	if err != nil {
		return nil, err
	}

	adminSv := options.Server.Admin
	adminSvTLS, err := adminSv.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	admin := newAdminServer(
		coordinatorInstance.Metadata(),
		coordinatorInstance,
	)
	adminGrpcServer, err = rpc2.Default.StartGrpcServer("admin", adminSv.BindAddress, func(registrar grpc.ServiceRegistrar) { //nolint:contextcheck
		proto.RegisterOxiaAdminServer(registrar, admin)
	}, adminSvTLS, &adminSv.Auth, nil)
	if err != nil {
		return nil, err
	}

	metricsServer, err = startMetricsServer(options.Observability.Metric) //nolint:contextcheck
	if err != nil {
		return nil, err
	}
	server := GrpcServer{
		ctx:              ctx,
		ctxCancel:        cancel,
		wg:               sync.WaitGroup{},
		logger:           slog.With(slog.String("component", "grpc-server")),
		watchableOptions: watchableOptions,
		grpcServer:       grpcServer,
		adminServer:      adminGrpcServer,
		healthServer:     healthServer,
		coordinator:      coordinatorInstance,
		metadata:         metadata,
		metrics:          metricsServer,
	}
	server.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "configuration-watcher",
		}, server.backgroundHandleConfChange)
	})

	succeeded = true
	return &server, nil
}

func cancelContextOnFailure(cancel context.CancelFunc, succeeded *bool) {
	if !*succeeded {
		cancel()
	}
}

func startMetricsServer(metrics commonoption.MetricOptions) (*metric.PrometheusMetrics, error) {
	if !metrics.IsEnabled() {
		return nil, nil //nolint:nilnil
	}
	metricTLS, err := metrics.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	return metric.Start(metrics.BindAddress, metricTLS)
}

func (s *GrpcServer) backgroundHandleConfChange() {
	var coordinatorOptions *option.Options
	var ver uint64
	var err error
	for {
		coordinatorOptions, ver, err = s.watchableOptions.Wait(s.ctx, ver)
		if err != nil {
			s.logger.Warn("exit background configuration watch goroutine due to an error", slog.Any("error", err))
			return
		}

		s.logger.Info("configuration options has changed. processing the dynamic updates.")
		logOptions := &coordinatorOptions.Observability.Log
		if logging.ReconfigureLogger(logOptions) {
			s.logger.Info("reconfigured log options", slog.Any("options", logOptions))
		}
	}
}

func (s *GrpcServer) Close() error {
	// sync close the background task first
	s.ctxCancel()
	s.wg.Wait()

	var err error
	s.healthServer.Shutdown()
	err = multierr.Combine(
		s.grpcServer.Close(),
		s.adminServer.Close(),
		s.coordinator.Close(),
		s.metadata.Close(),
	)
	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}
	return err
}
