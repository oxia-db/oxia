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
	"github.com/rs/zerolog/log"
	"go.uber.org/multierr"
	"os"
	"oxia/common"
	"oxia/common/container"
	"oxia/common/metrics"
	"oxia/proto"
	"oxia/server/kv"
	"oxia/server/wal"
	"path/filepath"
	"testing"
)

type StandaloneConfig struct {
	Config

	NumShards uint32
}

type Standalone struct {
	rpc                       *publicRpcServer
	kvFactory                 kv.KVFactory
	walFactory                wal.WalFactory
	shardsDirector            ShardsDirector
	shardAssignmentDispatcher ShardAssignmentsDispatcher

	metrics *metrics.PrometheusMetrics
}

func NewTestConfig(t *testing.T) StandaloneConfig {
	var dir string
	if t == nil {
		dir, _ = os.MkdirTemp(os.TempDir(), "oxia-test-*")
	} else {
		dir = t.TempDir()
	}

	return StandaloneConfig{
		Config: Config{
			DataDir:             filepath.Join(dir, "db"),
			WalDir:              filepath.Join(dir, "wal"),
			InternalServiceAddr: "localhost:0",
			PublicServiceAddr:   "localhost:0",
			MetricsServiceAddr:  "",
		},
		NumShards: 1,
	}
}

func NewStandalone(config StandaloneConfig) (*Standalone, error) {
	log.Info().
		Interface("config", config).
		Msg("Starting Oxia standalone")

	s := &Standalone{}

	kvOptions := kv.KVFactoryOptions{DataDir: config.DataDir}
	s.walFactory = wal.NewWalFactory(&wal.WalFactoryOptions{
		BaseWalDir: config.WalDir,
		Retention:  config.WalRetentionTime,
	})
	var err error
	if s.kvFactory, err = kv.NewPebbleKVFactory(&kvOptions); err != nil {
		return nil, err
	}

	s.shardsDirector = NewShardsDirector(config.Config, s.walFactory, s.kvFactory, newNoOpReplicationRpcProvider())

	if err := s.initializeShards(config.NumShards); err != nil {
		return nil, err
	}

	s.rpc, err = newPublicRpcServer(container.Default, config.PublicServiceAddr, s.shardsDirector, nil)
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
			ShardId: i,
			Term:    newTerm,
		}); err != nil {
			return err
		}

		if _, err := lc.BecomeLeader(context.Background(), &proto.BecomeLeaderRequest{
			ShardId:           i,
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

///////////////////////////////////

type noOpReplicationRpcProvider struct {
}

func (n noOpReplicationRpcProvider) Close() error {
	return nil
}

func (n noOpReplicationRpcProvider) GetReplicateStream(ctx context.Context, follower string, namespace string, shard int64) (proto.OxiaLogReplication_ReplicateClient, error) {
	panic("not implemented")
}

func (n noOpReplicationRpcProvider) SendSnapshot(ctx context.Context, follower string, namespace string, shard int64) (proto.OxiaLogReplication_SendSnapshotClient, error) {
	panic("not implemented")
}

func (n noOpReplicationRpcProvider) Truncate(follower string, req *proto.TruncateRequest) (*proto.TruncateResponse, error) {
	panic("not implemented")
}

func newNoOpReplicationRpcProvider() ReplicationRpcProvider {
	return &noOpReplicationRpcProvider{}
}
