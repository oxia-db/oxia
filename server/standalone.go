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
	"context"
	"log/slog"
	"path/filepath"

	"github.com/streamnative/oxia/server/auth"

	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/container"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/kv"
	"github.com/streamnative/oxia/server/wal"
)

type StandaloneConfig struct {
	Config

	NumShards            uint32
	NotificationsEnabled bool
}

type Standalone struct {
	config                    StandaloneConfig
	rpc                       *publicRpcServer
	kvFactory                 kv.Factory
	walFactory                wal.Factory
	shardsDirector            ShardsDirector
	shardAssignmentDispatcher ShardAssignmentsDispatcher

	metrics *metrics.PrometheusMetrics
}

func NewTestConfig(dir string) StandaloneConfig {
	return StandaloneConfig{
		Config: Config{
			DataDir:             filepath.Join(dir, "db"),
			WalDir:              filepath.Join(dir, "wal"),
			InternalServiceAddr: "localhost:0",
			PublicServiceAddr:   "localhost:0",
			MetricsServiceAddr:  "",
		},
		NumShards:            1,
		NotificationsEnabled: true,
	}
}

func NewStandalone(config StandaloneConfig) (*Standalone, error) {
	slog.Info(
		"Starting Oxia standalone",
		slog.Any("config", config),
	)

	s := &Standalone{config: config}

	kvOptions := kv.FactoryOptions{DataDir: config.DataDir}
	s.walFactory = wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  config.WalDir,
		Retention:   config.WalRetentionTime,
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
		SyncData:    config.WalSyncData,
	})
	var err error
	if s.kvFactory, err = kv.NewPebbleKVFactory(&kvOptions); err != nil {
		return nil, err
	}

	s.shardsDirector = NewShardsDirector(config.Config, s.walFactory, s.kvFactory, newNoOpReplicationRpcProvider())

	if err := s.initializeShards(config.NumShards); err != nil {
		return nil, err
	}

	s.rpc, err = newPublicRpcServer(container.Default, config.PublicServiceAddr, s.shardsDirector,
		nil, config.ServerTLS, &auth.Disabled)
	if err != nil {
		return nil, err
	}

	s.shardAssignmentDispatcher = NewStandaloneShardAssignmentDispatcher(config.NumShards)

	s.rpc.assignmentDispatcher = s.shardAssignmentDispatcher

	if config.MetricsServiceAddr != "" {
		s.metrics, err = metrics.Start(config.MetricsServiceAddr)
	}
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Standalone) initializeShards(numShards uint32) error {
	var err error
	for i := int64(0); i < int64(numShards); i++ {
		var lc LeaderController
		if lc, err = s.shardsDirector.GetOrCreateLeader(common.DefaultNamespace, i); err != nil {
			return err
		}

		newTerm := lc.Term() + 1

		if _, err := lc.NewTerm(&proto.NewTermRequest{
			Shard: i,
			Term:  newTerm,
			Options: &proto.NewTermOptions{
				EnableNotifications: s.config.NotificationsEnabled,
			},
		}); err != nil {
			return err
		}

		if _, err := lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
			Shard:             i,
			Term:              newTerm,
			ReplicationFactor: 1,
			FollowerMaps:      make(map[string]*proto.EntryId),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *Standalone) RpcPort() int {
	return s.rpc.Port()
}

func (s *Standalone) Close() error {
	var err error
	if s.metrics != nil {
		err = s.metrics.Close()
	}

	return multierr.Combine(
		err,
		s.shardsDirector.Close(),
		s.shardAssignmentDispatcher.Close(),
		s.rpc.Close(),
		s.kvFactory.Close(),
	)
}

type noOpReplicationRpcProvider struct {
}

func (noOpReplicationRpcProvider) Close() error {
	return nil
}

func (noOpReplicationRpcProvider) GetReplicateStream(context.Context, string, string, int64, int64) (proto.OxiaLogReplication_ReplicateClient, error) {
	panic("not implemented")
}

func (noOpReplicationRpcProvider) SendSnapshot(context.Context, string, string, int64, int64) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	panic("not implemented")
}

func (noOpReplicationRpcProvider) Truncate(string, *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	panic("not implemented")
}

func newNoOpReplicationRpcProvider() ReplicationRpcProvider {
	return &noOpReplicationRpcProvider{}
}
