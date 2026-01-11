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
	"fmt"
	"log/slog"
	"path/filepath"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/oxiad/common/metric"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/oxiad/dataserver/assignment"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/common/proto"
)

type StandaloneConfig struct {
	DataServerOptions dataserveroption.Options

	NumShards            uint32
	NotificationsEnabled bool
	KeySorting           model.KeySorting
}

type Standalone struct {
	config                    StandaloneConfig
	rpc                       *publicRpcServer
	kvFactory                 kvstore.Factory
	walFactory                wal.Factory
	shardsDirector            controller.ShardsDirector
	shardAssignmentDispatcher assignment.ShardAssignmentsDispatcher

	metrics *metric.PrometheusMetrics
}

func NewTestConfig(dir string) StandaloneConfig {
	dataServerOption := dataserveroption.NewDefaultOptions()
	dataServerOption.Server.Public.BindAddress = "localhost:0"
	dataServerOption.Server.Internal.BindAddress = "localhost:0"
	dataServerOption.Observability.Metric.Enabled = &constant.FlagFalse
	dataServerOption.Storage.Database.Dir = filepath.Join(dir, "db")
	dataServerOption.Storage.WAL.Dir = filepath.Join(dir, "wal")
	return StandaloneConfig{
		DataServerOptions:    *dataServerOption,
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

	storageOptions := config.DataServerOptions.Storage
	kvOptions := kvstore.FactoryOptions{
		DataDir:     storageOptions.Database.Dir,
		UseWAL:      false, // WAL is kept outside the KV store
		SyncData:    false, // WAL is kept outside the KV store
		CacheSizeMB: storageOptions.Database.ReadCacheSizeMB,
	}
	s.walFactory = wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  storageOptions.WAL.Dir,
		Retention:   storageOptions.WAL.Retention,
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
		SyncData:    storageOptions.WAL.IsSyncEnabled(),
	})
	var err error
	if s.kvFactory, err = kvstore.NewPebbleKVFactory(&kvOptions); err != nil {
		return nil, err
	}

	s.shardsDirector = controller.NewShardsDirector(&storageOptions, s.walFactory, s.kvFactory, newNoOpReplicationRpcProvider())

	if err := s.initializeShards(config.NumShards); err != nil {
		return nil, err
	}

	publicServer := config.DataServerOptions.Server.Public
	serverTLS, err := publicServer.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	s.rpc, err = newPublicRpcServer(rpc2.Default, publicServer.BindAddress, s.shardsDirector,
		nil, serverTLS, &auth.Disabled)
	if err != nil {
		return nil, err
	}
	s.shardAssignmentDispatcher = assignment.NewStandaloneShardAssignmentDispatcher(config.NumShards)
	s.rpc.assignmentDispatcher = s.shardAssignmentDispatcher

	metricOptions := config.DataServerOptions.Observability.Metric
	if metricOptions.IsEnabled() {
		s.metrics, err = metric.Start(metricOptions.BindAddress)
	}
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Standalone) initializeShards(numShards uint32) error {
	var err error

	newTermOptions := &proto.NewTermOptions{
		EnableNotifications: s.config.NotificationsEnabled,
		KeySorting:          s.config.KeySorting.ToProto(),
	}

	for i := int64(0); i < int64(numShards); i++ {
		var lc lead.LeaderController
		if lc, err = s.shardsDirector.GetOrCreateLeader(constant.DefaultNamespace, i, newTermOptions); err != nil {
			return err
		}

		newTerm := lc.Term() + 1

		if _, err := lc.NewTerm(&proto.NewTermRequest{
			Shard:   i,
			Term:    newTerm,
			Options: newTermOptions,
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

func (s *Standalone) ServiceAddr() string {
	return fmt.Sprintf("localhost:%d", s.rpc.Port())
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

func newNoOpReplicationRpcProvider() rpc.ReplicationRpcProvider {
	return &noOpReplicationRpcProvider{}
}
