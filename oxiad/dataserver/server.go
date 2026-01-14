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

package dataserver

import (
	"context"
	"log/slog"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/oxiad/common/metric"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/rpc"
)

type Server struct {
	*internalRpcServer
	*publicRpcServer

	replicationRpcProvider    rpc.ReplicationRpcProvider
	shardAssignmentDispatcher assignment.ShardAssignmentsDispatcher
	shardsDirector            controller.ShardsDirector
	metrics                   *metric.PrometheusMetrics
	walFactory                wal.Factory
	kvFactory                 kvstore.Factory

	healthServer rpc2.HealthServer
}

func New(options *option.Options) (*Server, error) {
	provider, err := rpc.NewReplicationRpcProvider(&options.Replication)
	if err != nil {
		return nil, err
	}
	return NewWithGrpcProvider(options, rpc2.Default, provider)
}

func NewWithGrpcProvider(options *option.Options, provider rpc2.GrpcProvider, replicationRpcProvider rpc.ReplicationRpcProvider) (*Server, error) {
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
	s := &Server{
		replicationRpcProvider: replicationRpcProvider,
		walFactory: wal.NewWalFactory(&wal.FactoryOptions{
			BaseWalDir:  storage.WAL.Dir,
			Retention:   storage.WAL.Retention,
			SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
			SyncData:    storage.WAL.IsSyncEnabled(),
		}),
		kvFactory:    kvFactory,
		healthServer: rpc2.NewClosableHealthServer(context.Background()),
	}

	s.shardsDirector = controller.NewShardsDirector(storage, s.walFactory, s.kvFactory, replicationRpcProvider)
	s.shardAssignmentDispatcher = assignment.NewShardAssignmentDispatcher(s.healthServer)

	internalServer := options.Server.Internal
	internalServerTLS, err := internalServer.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	s.internalRpcServer, err = newInternalRpcServer(provider, internalServer.BindAddress,
		s.shardsDirector, s.shardAssignmentDispatcher, s.healthServer, internalServerTLS)
	if err != nil {
		return nil, err
	}

	publicServer := options.Server.Public
	publicServerTLS, err := publicServer.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	s.publicRpcServer, err = newPublicRpcServer(provider, publicServer.BindAddress, s.shardsDirector,
		s.shardAssignmentDispatcher, publicServerTLS, &publicServer.Auth)
	if err != nil {
		return nil, err
	}

	observability := options.Observability
	if observability.Metric.IsEnabled() {
		s.metrics, err = metric.Start(observability.Metric.BindAddress)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Server) PublicPort() int {
	return s.publicRpcServer.grpcServer.Port()
}

func (s *Server) InternalPort() int {
	return s.internalRpcServer.grpcServer.Port()
}

func (s *Server) Close() error {
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
