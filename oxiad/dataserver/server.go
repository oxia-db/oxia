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
	"crypto/tls"
	"log/slog"
	"time"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/rpc/auth"

	"github.com/oxia-db/oxia/oxiad/dataserver/kv"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/common/metric"
)

type Config struct {
	PublicServiceAddr   string
	InternalServiceAddr string
	PeerTLS             *tls.Config
	ServerTLS           *tls.Config
	InternalServerTLS   *tls.Config
	MetricsServiceAddr  string

	AuthOptions auth.Options

	DataDir string
	WalDir  string

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
	metrics                   *metric.PrometheusMetrics
	walFactory                wal.Factory
	kvFactory                 kv.Factory

	healthServer rpc.HealthServer
}

func New(config Config) (*Server, error) {
	return NewWithGrpcProvider(config, rpc.Default, NewReplicationRpcProvider(config.PeerTLS))
}

func NewWithGrpcProvider(config Config, provider rpc.GrpcProvider, replicationRpcProvider ReplicationRpcProvider) (*Server, error) {
	slog.Info(
		"Starting Oxia dataserver",
		slog.Any("config", config),
	)

	kvFactory, err := kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir:     config.DataDir,
		CacheSizeMB: config.DbBlockCacheMB,
		UseWAL:      false, // WAL is kept outside the KV store
		SyncData:    false, // WAL is kept outside the KV store
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
		healthServer: rpc.NewClosableHealthServer(context.Background()),
	}

	s.shardsDirector = NewShardsDirector(config, s.walFactory, s.kvFactory, replicationRpcProvider)
	s.shardAssignmentDispatcher = NewShardAssignmentDispatcher(s.healthServer)

	s.internalRpcServer, err = newInternalRpcServer(provider, config.InternalServiceAddr,
		s.shardsDirector, s.shardAssignmentDispatcher, s.healthServer, config.InternalServerTLS)
	if err != nil {
		return nil, err
	}

	s.publicRpcServer, err = newPublicRpcServer(provider, config.PublicServiceAddr, s.shardsDirector,
		s.shardAssignmentDispatcher, config.ServerTLS, &config.AuthOptions)
	if err != nil {
		return nil, err
	}

	if config.MetricsServiceAddr != "" {
		s.metrics, err = metric.Start(config.MetricsServiceAddr)
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
