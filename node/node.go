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

package node

import (
	"context"
	"log/slog"

	"github.com/oxia-db/oxia/node/assignment"
	"github.com/oxia-db/oxia/node/conf"
	"github.com/oxia-db/oxia/node/controller"
	"github.com/oxia-db/oxia/node/db/kv"
	"github.com/oxia-db/oxia/node/server"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/node/wal"
)

type Node struct {
	*server.InternalRpcServer
	*server.PublicRpcServer

	replicationRpcProvider    rpc.ReplicationRpcProvider
	shardAssignmentDispatcher assignment.ShardAssignmentsDispatcher
	shardsDirector            controller.ShardsDirector
	metrics                   *metric.PrometheusMetrics
	walFactory                wal.Factory
	kvFactory                 kv.Factory

	healthServer rpc.HealthServer
}

func New(config conf.Config) (*Node, error) {
	return NewWithGrpcProvider(config, rpc.Default, rpc.NewReplicationRpcProvider(config.PeerTLS))
}

func NewWithGrpcProvider(config conf.Config, provider rpc.GrpcProvider, replicationRpcProvider rpc.ReplicationRpcProvider) (*Node, error) {
	slog.Info(
		"Starting Oxia Node",
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

	s := &Node{
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

	s.shardsDirector = controller.NewShardsDirector(config, s.walFactory, s.kvFactory, replicationRpcProvider)
	s.shardAssignmentDispatcher = assignment.NewShardAssignmentDispatcher(s.healthServer)

	s.InternalRpcServer, err = server.NewInternalRpcServer(provider, config.InternalServiceAddr,
		s.shardsDirector, s.shardAssignmentDispatcher, s.healthServer, config.InternalServerTLS)
	if err != nil {
		return nil, err
	}

	s.PublicRpcServer, err = server.NewPublicRpcServer(provider, config.PublicServiceAddr, s.shardsDirector,
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

func (s *Node) PublicPort() int {
	return s.PublicRpcServer.Port()
}

func (s *Node) InternalPort() int {
	return s.InternalRpcServer.Port()
}

func (s *Node) Close() error {
	err := multierr.Combine(
		s.healthServer.Close(),
		s.shardAssignmentDispatcher.Close(),
		s.shardsDirector.Close(),
		s.PublicRpcServer.Close(),
		s.InternalRpcServer.Close(),
		s.kvFactory.Close(),
		s.walFactory.Close(),
		s.replicationRpcProvider.Close(),
	)

	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}

	return err
}
