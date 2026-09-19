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
	"errors"
	"log/slog"
	"sync"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/oxiad/common/metric"
	commonrpc "github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	manifestpkg "github.com/oxia-db/oxia/oxiad/dataserver/manifest"
	dataserverrpc "github.com/oxia-db/oxia/oxiad/dataserver/rpc"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

type Server struct {
	*internalRpcServer
	*publicRpcServer

	// concurrent control
	ctx          context.Context
	ctxCancel    context.CancelFunc
	wg           sync.WaitGroup
	logger       *slog.Logger
	optionsWatch *commonwatch.Watch[*option.Options]

	replicationRpcProvider    dataserverrpc.ReplicationRpcProvider
	shardAssignmentDispatcher assignment.ShardAssignmentsDispatcher
	shardsDirector            controller.ShardsDirector
	metrics                   *metric.PrometheusMetrics
	walFactory                wal.Factory
	kvFactory                 kvstore.Factory

	healthServer commonrpc.HealthServer
}

// New starts a data server with the given options. Unset option values are
// filled with their defaults, and the options are validated before the server
// starts. If options is nil, the default options are used.
//
// The server keeps a reference to options: the caller must not mutate them
// after this call and should use UpdateOptions instead.
func New(parent context.Context, options *option.Options) (*Server, error) {
	if options == nil {
		options = option.NewDefaultOptions()
	}
	options.WithDefault()
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return NewWithOptionsWatch(parent, commonwatch.New(options))
}

// NewWithOptionsWatch starts a data server whose options are supplied through
// a watch, allowing the caller to publish configuration updates at runtime
// (e.g. from a configuration file watcher). Most callers should use New
// instead.
func NewWithOptionsWatch(parent context.Context, optionsWatch *commonwatch.Watch[*option.Options]) (*Server, error) {
	options := optionsWatch.Load()
	manifest, err := manifestpkg.NewManifest(options.Storage.Database.Dir)
	if err != nil {
		return nil, err
	}
	provider, err := dataserverrpc.NewReplicationRpcProvider(&options.Replication.TLS, manifest)
	if err != nil {
		return nil, err
	}
	grpcProvider, err := NewWithGrpcProvider(parent, optionsWatch, commonrpc.Default, provider, manifest)
	return grpcProvider, err
}

func NewWithGrpcProvider(parent context.Context, optionsWatch *commonwatch.Watch[*option.Options], provider commonrpc.GrpcProvider,
	replicationRpcProvider dataserverrpc.ReplicationRpcProvider, manifest *manifestpkg.Manifest) (*Server, error) {
	options := optionsWatch.Load()
	slog.Info("Starting Oxia dataServer", slog.Any("options", options))

	storage := &options.Storage
	kvFactory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{
		DataDir:     storage.Database.Dir,
		CacheSizeMB: storage.Database.ReadCacheSizeMB,
		UseWAL:      false, // WAL is kept outside the KV store
		SyncData:    false, // WAL is kept outside the KV store
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(parent)
	s := &Server{
		ctx:                    ctx,
		ctxCancel:              cancel,
		wg:                     sync.WaitGroup{},
		logger:                 slog.With(slog.String("component", "grpc-server")),
		optionsWatch:           optionsWatch,
		replicationRpcProvider: replicationRpcProvider,
		walFactory: wal.NewWalFactory(&wal.FactoryOptions{
			BaseWalDir:  storage.WAL.Dir,
			Retention:   storage.WAL.Retention.ToDuration(),
			SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
			SyncData:    storage.WAL.IsSyncEnabled(),
		}),
		kvFactory:    kvFactory,
		healthServer: commonrpc.NewClosableHealthServer(context.Background()),
	}

	s.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "configuration-watcher",
		}, s.backgroundHandleConfChange)
	})

	s.shardsDirector = controller.NewShardsDirector(storage, s.walFactory, s.kvFactory, replicationRpcProvider)
	if checksumInterval := options.Scheduler.Checksum.Interval.ToDuration(); checksumInterval > 0 {
		csScheduler := newChecksumScheduler(ctx, checksumInterval, s.shardsDirector)
		s.wg.Go(csScheduler.run)
	}
	s.shardAssignmentDispatcher = assignment.NewShardAssignmentDispatcher(s.healthServer)

	internalServer := options.Server.Internal
	internalServerTLS, err := internalServer.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	//nolint:contextcheck
	s.internalRpcServer, err = newInternalRpcServer(provider, internalServer.BindAddress,
		s.shardsDirector, s.shardAssignmentDispatcher, s.healthServer, internalServerTLS, &internalServer.Auth, manifest)
	if err != nil {
		return nil, err
	}

	publicServer := options.Server.Public
	publicServerTLS, err := publicServer.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	s.publicRpcServer, err = newPublicRpcServer(provider, publicServer.BindAddress, s.shardsDirector,
		s.shardAssignmentDispatcher, options.FeatureFlags.IsAuthorityValidationEnabled(), s.healthServer, publicServerTLS, &publicServer.Auth)
	if err != nil {
		return nil, err
	}

	observability := options.Observability
	if observability.Metric.IsEnabled() {
		metricTLS, err := observability.Metric.TLS.TryIntoServerTLSConf()
		if err != nil {
			return nil, err
		}
		s.metrics, err = metric.Start(observability.Metric.BindAddress, metricTLS) //nolint:contextcheck
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) GetShardDirector() controller.ShardsDirector {
	return s.shardsDirector
}

func (s *Server) PublicPort() int {
	return s.publicRpcServer.grpcServer.Port()
}

func (s *Server) InternalPort() int {
	return s.internalRpcServer.grpcServer.Port()
}

// UpdateOptions publishes a new configuration to the running server. Only the
// dynamic settings (currently the log options) take effect at runtime; the
// remaining settings require a restart.
func (s *Server) UpdateOptions(options *option.Options) error {
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

func (s *Server) backgroundHandleConfChange() {
	receiver := s.optionsWatch.Subscribe()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-receiver.Changed():
		}

		dataServerOptions := receiver.Load()

		s.logger.Info("configuration options has changed. processing the dynamic updates.")
		logOptions := &dataServerOptions.Observability.Log
		if logging.ReconfigureLogger(logOptions) {
			s.logger.Info("reconfigured log options", slog.Any("options", logOptions))
		}
	}
}

func (s *Server) Close() error {
	// sync close the background task first
	s.ctxCancel()
	s.wg.Wait()

	err := multierr.Combine(
		s.healthServer.Close(),
		s.shardAssignmentDispatcher.Close(),
		s.shardsDirector.Close(),
		s.publicRpcServer.Close(),
		s.internalRpcServer.Close(),
		s.kvFactory.Close(),
		s.walFactory.Close(),
		s.replicationRpcProvider.Close(),
	)

	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}

	return err
}
