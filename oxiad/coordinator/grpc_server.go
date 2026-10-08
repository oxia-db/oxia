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
	"fmt"
	"io"
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
	// onLeadershipLost is the embedder's handler; nil selects the default,
	// which terminates the process.
	onLeadershipLost     func()
	initialClusterConfig *proto.ClusterConfiguration
}

func newServerOptions(serverOpts []ServerOption) serverOptions {
	so := serverOptions{}
	for _, opt := range serverOpts {
		if opt != nil {
			opt(&so)
		}
	}
	return so
}

func (so *serverOptions) validate() error {
	if so.initialClusterConfig == nil {
		return nil
	}
	if err := so.initialClusterConfig.Validate(); err != nil {
		return fmt.Errorf("invalid initial cluster configuration: %w", err)
	}
	return nil
}

func (so *serverOptions) seedClusterConfig(metadataFactory *coordmetadata.Factory) error {
	if so.initialClusterConfig == nil {
		return nil
	}
	return metadataFactory.SeedClusterConfig(so.initialClusterConfig)
}

// WithOnLeadershipLost replaces what happens when this coordinator loses the
// metadata leadership. By default the process terminates, which is the right
// behavior for a dedicated coordinator process but not when the coordinator
// is embedded in a larger application.
//
// With a handler set, the coordinator first stops coordinating by itself: its
// health turns to NOT_SERVING, its admin API answers as a coordinator that is
// not the leader, and it stops its reconciler, its runtime (the elections and
// the balancing) and its metadata writes. Only then is the handler called, as
// a notification. The coordinator does not regain the leadership: to take
// part in the next election, the application closes it and starts a new one
// with [New].
//
// The handler is called from an internal goroutine that Close waits for, so
// it must not call Close synchronously. A nil handler keeps the default.
//
// Only the configmap and raft metadata providers can lose the leadership; the
// memory and file providers hold it for the lifetime of the coordinator.
func WithOnLeadershipLost(handler func()) ServerOption {
	return func(so *serverOptions) {
		so.onLeadershipLost = handler
	}
}

// WithInitialClusterConfiguration seeds the metadata store with the given
// cluster configuration when none exists yet. It allows bootstrapping a
// cluster programmatically, without a cluster configuration file or a
// separate admin call. If a configuration is already present it is left
// untouched.
//
// The configuration is validated before the coordinator starts: an invalid
// one fails [New] and is never stored.
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
	management       *managementServer
	healthServer     *health.Server
	reconciler       coordreconciler.Reconciler
	runtime          coordruntime.Runtime
	metadata         coordmetadata.Metadata
	metadataFactory  *coordmetadata.Factory
	metrics          *metric.PrometheusMetrics

	stopOnce sync.Once
	stopErr  error
}

// New starts a coordinator with the given options. Unset option values are
// filled with their defaults, and the options are validated before the
// coordinator starts. The metadata provider (Metadata.ProviderName) has no
// default and must be set.
//
// The coordinator keeps a reference to options: the caller must not mutate
// them after this call.
//
// With the file, configmap or raft metadata provider, this call blocks until
// the coordinator acquires the metadata leadership, which lasts as long as
// another coordinator holds it. Its listeners are already bound while it
// waits. Cancelling parent ends the wait: New then releases what it started
// and returns the context's error.
func New(parent context.Context, options *option.Options, serverOpts ...ServerOption) (*GrpcServer, error) {
	if options == nil {
		return nil, errors.New("options must not be nil")
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
	// Fail fast, before binding the ports and waiting for the leadership.
	if err := so.validate(); err != nil {
		return nil, err
	}
	options := optionsWatch.Load()
	slog.Info("Starting Oxia coordinator", slog.Any("options", options))

	ctx, cancel := context.WithCancel(parent)
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

		cancel()
		err = multierr.Append(err, closePointer(metricsServer))
		err = multierr.Append(err, commonio.CloseIfNotNil(managementGrpcServer))
		err = multierr.Append(err, commonio.CloseIfNotNil(reconciler))
		err = multierr.Append(err, commonio.CloseIfNotNil(runtime))
		err = multierr.Append(err, commonio.CloseIfNotNil(metadata))
		err = multierr.Append(err, closePointer(metadataFactory))
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

	// The providers retry their loads until they succeed or their context is
	// canceled: deriving them from the server context lets Close stop those
	// retries.
	metadataFactory, err = coordmetadata.New(ctx, options)
	if err != nil {
		return nil, err
	}
	// The metadata retries its status writes until they succeed or its
	// context is canceled: deriving it from the server context lets Close stop
	// the retries before closing the runtime, which waits for its controllers.
	metadata, err = metadataFactory.CreateMetadata(ctx)
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

	// Waiting for the leadership lasts as long as another coordinator leads.
	// Closing the metadata factory is what ends the wait of every provider,
	// so a cancelled context closes it.
	stopCancelWatch := context.AfterFunc(ctx, func() { _ = metadataFactory.Close() })
	var leadershipLost <-chan struct{}
	leadershipLost, err = metadata.WaitToBecomeLeader()
	if !stopCancelWatch() {
		return nil, fmt.Errorf("coordinator start cancelled while waiting for the leadership: %w", context.Cause(ctx))
	}
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
	server := GrpcServer{
		ctx:              ctx,
		ctxCancel:        cancel,
		wg:               sync.WaitGroup{},
		logger:           slog.With(slog.String("component", "grpc-server")),
		optionsWatch:     optionsWatch,
		grpcServer:       grpcServer,
		managementServer: managementGrpcServer,
		management:       management,
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
				server.watchLeadership(leadershipLost, so.onLeadershipLost)
			})
		})
	}

	return &server, nil
}

// watchLeadership waits for the leadership loss, or for the server to close.
// On a loss, a nil handler terminates the process; otherwise the coordinator
// stops coordinating before the handler is called.
func (s *GrpcServer) watchLeadership(leadershipLost <-chan struct{}, handler func()) {
	select {
	case <-leadershipLost:
	case <-s.ctx.Done():
		return
	}
	if handler == nil {
		s.logger.Error("Coordination leadership lost: terminating to avoid a split brain")
		onLeadershipLost()
		return
	}
	s.logger.Error("Coordination leadership lost: stopping coordination to avoid a split brain")
	if err := s.stopCoordinating(); err != nil {
		s.logger.Warn("Failed to stop coordinating cleanly", slog.Any("error", err))
	}
	handler()
}

// onLeadershipLost is what happens on a leadership loss when no
// WithOnLeadershipLost handler is set. It terminates the process: a
// coordinator that lost the leadership must stop coordinating immediately,
// before it can run elections or move ensembles alongside the new leader, and
// a restart rejoins the election from a clean state. Overridable in tests.
var onLeadershipLost = func() {
	os.Exit(1)
}

// InternalPort returns the port of the internal gRPC server, which serves the
// health checks.
func (s *GrpcServer) InternalPort() int {
	return s.grpcServer.Port()
}

// PublicPort returns the port of the public gRPC server exposing the
// management (admin) API.
func (s *GrpcServer) PublicPort() int {
	return s.managementServer.Port()
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

// stopCoordinating turns the health to NOT_SERVING, turns the admin API away
// to the leader, and stops everything that acts on the cluster: the
// reconciler, the runtime (elections, balancing, splits) and the metadata
// writes. The listeners and the metadata providers stay open until Close. It
// runs once, on a leadership loss or on Close.
func (s *GrpcServer) stopCoordinating() error {
	s.stopOnce.Do(func() {
		// Canceling the context first stops the metadata status write
		// retries, which could otherwise block closing the runtime forever
		// (e.g. after losing the leadership).
		s.ctxCancel()
		s.healthServer.Shutdown()
		s.management.stop()
		s.stopErr = multierr.Combine(
			s.reconciler.Close(),
			s.runtime.Close(),
			s.metadata.Close(),
		)
	})
	return s.stopErr
}

func (s *GrpcServer) Close() error {
	// Canceling the context ends the background tasks: wait for them before
	// closing what they use.
	s.ctxCancel()
	s.wg.Wait()

	s.healthServer.Shutdown()
	err := multierr.Combine(
		s.grpcServer.Close(),
		s.managementServer.Close(),
		s.stopCoordinating(),
		s.metadataFactory.Close(),
	)
	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}
	return err
}

// closePointer closes p unless it is nil. A nil pointer wrapped in an
// interface passes CloseIfNotNil's nil check and is then dereferenced: the
// parts NewGrpcServer holds as pointers are closed through this instead.
func closePointer[T any, P interface {
	*T
	io.Closer
}](p P) error {
	if p == nil {
		return nil
	}
	return p.Close()
}
