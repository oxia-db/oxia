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
	"errors"
	"log/slog"
	"os"
	"sync"

	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/commonio"
	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	coordreconciler "github.com/oxia-db/oxia/oxiad/coordinator/reconciler"
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

// ServerOption configures optional behavior of the coordinator server.
type ServerOption func(*serverOptions)

type serverOptions struct {
	onLeadershipLost     func()
	initialClusterConfig *proto.ClusterConfiguration
}

func newServerOptions(serverOpts []ServerOption) serverOptions {
	so := serverOptions{onLeadershipLost: onLeadershipLost}
	for _, opt := range serverOpts {
		if opt != nil {
			opt(&so)
		}
	}
	return so
}

func (so *serverOptions) seedClusterConfig(metadataFactory *coordmetadata.Factory) error {
	if so.initialClusterConfig == nil {
		return nil
	}
	return metadataFactory.SeedClusterConfig(so.initialClusterConfig)
}

// WithOnLeadershipLost overrides what happens when this coordinator loses the
// metadata leadership. The default handler terminates the process, which is
// the right behavior for a dedicated coordinator process but not when the
// coordinator is embedded in a larger application.
//
// A coordinator that lost the leadership must stop coordinating: the handler
// is expected to arrange for the server to be closed. It is invoked from an
// internal goroutine that Close waits on, so the handler must not call Close
// synchronously; spawn a goroutine instead.
func WithOnLeadershipLost(handler func()) ServerOption {
	return func(so *serverOptions) {
		if handler == nil {
			// A nil handler would panic at the worst moment; keep the
			// fail-safe default instead.
			handler = onLeadershipLost
		}
		so.onLeadershipLost = handler
	}
}

// WithInitialClusterConfiguration seeds the metadata store with the given
// cluster configuration when none exists yet. It allows bootstrapping a
// cluster programmatically, without a cluster configuration file or a
// separate admin call. If a configuration is already present it is left
// untouched.
func WithInitialClusterConfiguration(config *proto.ClusterConfiguration) ServerOption {
	return func(so *serverOptions) {
		so.initialClusterConfig = config
	}
}

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
	reconciler       coordreconciler.Reconciler
	runtime          coordruntime.Runtime
	metadata         coordmetadata.Metadata
	metadataFactory  *coordmetadata.Factory
	metrics          *metric.PrometheusMetrics
}

// New starts a coordinator with the given options. Unset option values are
// filled with their defaults, and the options are validated before the
// coordinator starts. If options is nil, the default options are used.
//
// The coordinator keeps a reference to options: the caller must not mutate
// them after this call and should use UpdateOptions instead.
//
// With a multi-coordinator metadata provider (file, configmap or raft), this
// call blocks until the coordinator acquires the metadata leadership.
func New(parent context.Context, options *option.Options, serverOpts ...ServerOption) (*GrpcServer, error) {
	if options == nil {
		options = option.NewDefaultOptions()
	}
	options.WithDefault()
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return NewGrpcServer(parent, commonwatch.New(options), serverOpts...)
}

// NewGrpcServer starts a coordinator whose options are supplied through a
// watch, allowing the caller to publish configuration updates at runtime
// (e.g. from a configuration file watcher). Most callers should use New
// instead.
func NewGrpcServer(parent context.Context, optionsWatch *commonwatch.Watch[*option.Options], serverOpts ...ServerOption) (_ *GrpcServer, err error) {
	so := newServerOptions(serverOpts)
	options := optionsWatch.Load()
	slog.Info("Starting Oxia coordinator", slog.Any("options", options))

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
	defer func() {
		if err == nil {
			return
		}

		err = multierr.Append(err, commonio.CloseIfNotNil(metricsServer))
		err = multierr.Append(err, commonio.CloseIfNotNil(managementGrpcServer))
		err = multierr.Append(err, commonio.CloseIfNotNil(reconciler))
		err = multierr.Append(err, commonio.CloseIfNotNil(runtime))
		err = multierr.Append(err, commonio.CloseIfNotNil(metadata))
		err = multierr.Append(err, commonio.CloseIfNotNil(metadataFactory))
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
	controller := &options.Controller
	controllerTLS, err := controller.TLS.TryIntoClientTLSConf()
	if err != nil {
		return nil, err
	}

	metadataFactory, err = coordmetadata.New(parent, options)
	if err != nil {
		return nil, err
	}
	metadata, err = metadataFactory.CreateMetadata(parent)
	if err != nil {
		return nil, err
	}

	managementSv := options.Server.Public
	managementSvTLS, err := managementSv.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	management := newManagementServer(metadata)
	managementGrpcServer, err = commonrpc.Default.StartGrpcServer("public", managementSv.BindAddress, func(registrar grpc.ServiceRegistrar) { //nolint:contextcheck
		proto.RegisterOxiaAdminServer(registrar, management)
		grpc_health_v1.RegisterHealthServer(registrar, healthServer)
	}, managementSvTLS, &managementSv.Auth, nil)
	if err != nil {
		return nil, err
	}

	var leadershipLost <-chan struct{}
	leadershipLost, err = metadata.WaitToBecomeLeader()
	if err != nil {
		return nil, err
	}
	if err = so.seedClusterConfig(metadataFactory); err != nil {
		return nil, err
	}
	runtime, err = coordruntime.New(metadata, rpc.NewRpcProviderFactory(controllerTLS)) //nolint:contextcheck
	if err != nil {
		return nil, err
	}
	management.setRuntime(runtime)
	reconciler = coordreconciler.New(parent, runtime)

	metricsServer, err = startMetricsServer(options.Observability.Metric) //nolint:contextcheck
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
		reconciler:       reconciler,
		runtime:          runtime,
		metadata:         metadata,
		metadataFactory:  metadataFactory,
		metrics:          metricsServer,
	}
	server.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "configuration-watcher",
		}, server.backgroundHandleConfChange)
	})
	if leadershipLost != nil {
		server.wg.Go(func() {
			process.DoWithLabels(ctx, map[string]string{
				"component": "leadership-watcher",
			}, func() {
				select {
				case <-leadershipLost:
					server.logger.Error("Coordination leadership lost: terminating to avoid a split brain")
					so.onLeadershipLost()
				case <-ctx.Done():
				}
			})
		})
	}

	return &server, nil
}

// onLeadershipLost is the default WithOnLeadershipLost handler. It terminates
// the process: a coordinator that lost the leadership must stop coordinating
// immediately, before it can run elections or move ensembles alongside the
// new leader, and a restart rejoins the election from a clean state.
// Overridable in tests.
var onLeadershipLost = func() {
	os.Exit(1)
}

// InternalPort returns the port of the internal gRPC server used for
// coordinator-to-coordinator communication.
func (s *GrpcServer) InternalPort() int {
	return s.grpcServer.Port()
}

// PublicPort returns the port of the public gRPC server exposing the
// management (admin) API.
func (s *GrpcServer) PublicPort() int {
	return s.managementServer.Port()
}

// UpdateOptions publishes a new configuration to the running coordinator.
// Only the dynamic settings (currently the log options) take effect at
// runtime; the remaining settings require a restart.
func (s *GrpcServer) UpdateOptions(options *option.Options) error {
	if options == nil {
		return errors.New("options must not be nil")
	}
	options.WithDefault()
	if err := options.Validate(); err != nil {
		return err
	}
	s.optionsWatch.Publish(options)
	return nil
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
		s.reconciler.Close(),
		s.runtime.Close(),
		s.metadata.Close(),
		s.metadataFactory.Close(),
	)
	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}
	return err
}
