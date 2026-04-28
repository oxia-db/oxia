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

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"

	"github.com/oxia-db/oxia/oxiad/common/metric"
	commonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"
)

type GrpcServer struct {
	// concurrent control
	ctx          context.Context
	ctxCancel    context.CancelFunc
	wg           sync.WaitGroup
	logger       *slog.Logger
	optionsWatch *commonwatch.Watch[*option.Options]

	grpcServer       commonrpc.GrpcServer
	managementServer commonrpc.GrpcServer
	healthServer     *health.Server
	runtime          coordruntime.Runtime
	metadata         coordmetadata.Metadata
	metrics          *metric.PrometheusMetrics
}

func NewGrpcServer(parent context.Context, optionsWatch *commonwatch.Watch[*option.Options]) (*GrpcServer, error) {
	options := optionsWatch.Load()
	slog.Info("Starting Oxia coordinator", slog.Any("options", options))

	healthServer := health.NewServer()

	internalServer := options.Server.Internal
	internalServerTLS, err := internalServer.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	grpcServer, err := commonrpc.Default.StartGrpcServer("coordinator", internalServer.BindAddress, func(registrar grpc.ServiceRegistrar) { //nolint:contextcheck
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

	metadata, err := coordmetadata.NewFromOptions(parent, options)
	if err != nil {
		return nil, err
	}
	runtime, err := coordruntime.New(metadata, rpc.NewRpcProviderFactory(controllerTLS)) //nolint:contextcheck
	if err != nil {
		_ = metadata.Close()
		return nil, err
	}

	managementSv := options.Server.Admin
	managementSvTLS, err := managementSv.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	management := newManagementServer(
		runtime.Metadata(),
		runtime,
	)
	managementGrpcServer, err := commonrpc.Default.StartGrpcServer("admin", managementSv.BindAddress, func(registrar grpc.ServiceRegistrar) { //nolint:contextcheck
		proto.RegisterOxiaAdminServer(registrar, management)
	}, managementSvTLS, &managementSv.Auth, nil)
	if err != nil {
		return nil, err
	}

	metricsServer, err := startMetricsServer(options.Observability.Metric) //nolint:contextcheck
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	server := GrpcServer{
		ctx:              ctx,
		ctxCancel:        cancel,
		wg:               sync.WaitGroup{},
		logger:           slog.With(slog.String("component", "grpc-server")),
		optionsWatch:     optionsWatch,
		grpcServer:       grpcServer,
		managementServer: managementGrpcServer,
		healthServer:     healthServer,
		runtime:          runtime,
		metadata:         metadata,
		metrics:          metricsServer,
	}
	server.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "configuration-watcher",
		}, server.backgroundHandleConfChange)
	})

	return &server, nil
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

func (s *GrpcServer) Close() error {
	// sync close the background task first
	s.ctxCancel()
	s.wg.Wait()

	var err error
	s.healthServer.Shutdown()
	err = multierr.Combine(
		s.grpcServer.Close(),
		s.managementServer.Close(),
		s.runtime.Close(),
		s.metadata.Close(),
	)
	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}
	return err
}
