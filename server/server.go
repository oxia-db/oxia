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

package server

import (
	"log/slog"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/grpc/health"

	"github.com/streamnative/oxia/common/container"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
)

type Config struct {
	PublicServiceAddr   string
	InternalServiceAddr string
	MetricsServiceAddr  string
	DataDir             string
	WalDir              string

	WalRetentionTime           time.Duration
	WalSyncData                bool
	NotificationsRetentionTime time.Duration

	DbBlockCacheMB int64
}

type Server struct {
	*internalRpcServer
	*publicRpcServer

	replicationRpcProvider    ReplicationRpcProvider
	shardAssignmentDispatcher ShardAssignmentsDispatcher
	shardsDirector            ShardsDirector
	metrics                   *metrics.PrometheusMetrics
	walFactory                wal.Factory
	kvFactory                 kv.Factory

	healthServer *health.Server
}

func New(config Config) (*Server, error) {
	return NewWithGrpcProvider(config, container.Default, NewReplicationRpcProvider())
}

func NewWithGrpcProvider(config Config, provider container.GrpcProvider, replicationRpcProvider ReplicationRpcProvider) (*Server, error) {
	slog.Info(
		"Starting Oxia server",
		slog.Any("config", config),
	)

	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir:     config.DataDir,
		CacheSizeMB: config.DbBlockCacheMB,
	})
	if err != nil {
		return nil, err
	}

	s := &Server{
		replicationRpcProvider: replicationRpcProvider,
		walFactory: wal.NewWalFactory(&wal.FactoryOptions{
			BaseWalDir:  config.WalDir,
			Retention:   config.WalRetentionTime,
			SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
			SyncData:    true,
		}),
		kvFactory:    kvFactory,
		healthServer: health.NewServer(),
	}

	s.shardsDirector = NewShardsDirector(config, s.walFactory, s.kvFactory, replicationRpcProvider)
	s.shardAssignmentDispatcher = NewShardAssignmentDispatcher(s.healthServer)

	s.internalRpcServer, err = newInternalRpcServer(provider, config.InternalServiceAddr,
		s.shardsDirector, s.shardAssignmentDispatcher, s.healthServer)
	if err != nil {
		return nil, err
	}

	s.publicRpcServer, err = newPublicRpcServer(provider, config.PublicServiceAddr, s.shardsDirector,
		s.shardAssignmentDispatcher)
	if err != nil {
		return nil, err
	}

	if config.MetricsServiceAddr != "" {
		s.metrics, err = metrics.Start(config.MetricsServiceAddr)
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
	s.healthServer.Shutdown()

	err := multierr.Combine(
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
