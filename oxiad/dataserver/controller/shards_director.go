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

package controller

import (
	"io"
	"log/slog"
	"sync"

	"go.uber.org/multierr"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver/conf"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/follow"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/proto"
)

type ShardsDirector interface {
	io.Closer

	GetLeader(shardId int64) (lead.LeaderController, error)
	GetFollower(shardId int64) (follow.FollowerController, error)

	GetOrCreateLeader(namespace string, shardId int64, newTermOptions *proto.NewTermOptions) (lead.LeaderController, error)
	GetOrCreateFollower(namespace string, shardId int64, term int64, newTermOptions *proto.NewTermOptions) (follow.FollowerController, error)

	DeleteShard(req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)
}

type shardsDirector struct {
	sync.RWMutex

	config    conf.Config
	leaders   map[int64]lead.LeaderController
	followers map[int64]follow.FollowerController

	kvFactory              kvstore.Factory
	walFactory             wal.Factory
	replicationRpcProvider rpc.ReplicationRpcProvider
	closed                 bool
	log                    *slog.Logger

	leadersCounter   metric.UpDownCounter
	followersCounter metric.UpDownCounter
}

func NewShardsDirector(config conf.Config, walFactory wal.Factory, kvFactory kvstore.Factory, provider rpc.ReplicationRpcProvider) ShardsDirector {
	sd := &shardsDirector{
		config:                 config,
		walFactory:             walFactory,
		kvFactory:              kvFactory,
		leaders:                make(map[int64]lead.LeaderController),
		followers:              make(map[int64]follow.FollowerController),
		replicationRpcProvider: provider,
		log: slog.With(
			slog.String("component", "shards-director"),
		),

		leadersCounter: metric.NewUpDownCounter("oxia_server_leaders_count",
			"The number of leader controllers in a dataserver", "count", map[string]any{}),
		followersCounter: metric.NewUpDownCounter("oxia_server_followers_count",
			"The number of follower controllers in a dataserver", "count", map[string]any{}),
	}

	return sd
}

func (s *shardsDirector) GetLeader(shardId int64) (lead.LeaderController, error) {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, constant.ErrAlreadyClosed
	}

	if leader, ok := s.leaders[shardId]; ok {
		// There is already a leader controller for this shard
		return leader, nil
	}

	s.log.Debug(
		"This node is not hosting shard",
		slog.Int64("shard", shardId),
	)
	return nil, status.Errorf(constant.CodeNodeIsNotLeader, "node is not leader for shard %d", shardId)
}

func (s *shardsDirector) GetFollower(shardId int64) (follow.FollowerController, error) {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, constant.ErrAlreadyClosed
	}

	if follower, ok := s.followers[shardId]; ok {
		// There is already a follower controller for this shard
		return follower, nil
	}

	s.log.Debug(
		"This node is not hosting shard",
		slog.Int64("shard", shardId),
	)
	return nil, status.Errorf(constant.CodeNodeIsNotFollower, "node is not follower for shard %d", shardId)
}

func (s *shardsDirector) GetOrCreateLeader(namespace string, shardId int64, newTermOptions *proto.NewTermOptions) (lead.LeaderController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, constant.ErrAlreadyClosed
	}

	if leader, ok := s.leaders[shardId]; ok {
		// There is already a leader controller for this shard
		return leader, nil
	} else if follower, ok := s.followers[shardId]; ok {
		// There is an existing follower controller
		// Let's close it and before creating the leader controller

		if err := follower.Close(); err != nil {
			return nil, err
		}

		if _, ok := s.followers[shardId]; ok {
			s.followersCounter.Dec()
			delete(s.followers, shardId)
		}
	}

	// Create new leader controller
	lc, err := lead.NewLeaderController(s.config, namespace, shardId, s.replicationRpcProvider, s.walFactory, s.kvFactory, newTermOptions)
	if err != nil {
		return nil, err
	}

	s.leaders[shardId] = lc
	s.leadersCounter.Inc()
	return lc, nil
}

func (s *shardsDirector) GetOrCreateFollower(namespace string, shardId int64, term int64, newTermOptions *proto.NewTermOptions) (follow.FollowerController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, constant.ErrAlreadyClosed
	}

	if follower, ok := s.followers[shardId]; ok {
		// There is already a follower controller for this shard
		return follower, nil
	} else if leader, ok := s.leaders[shardId]; ok {
		// There is an existing leader controller
		if term >= 0 && term != leader.Term() {
			// We should not close the existing leader because of a late request
			return nil, constant.ErrInvalidTerm
		}

		// If we are in the right term, let's close the leader and reopen as a follower controller
		if err := leader.Close(); err != nil {
			return nil, err
		}

		if _, ok := s.leaders[shardId]; ok {
			s.leadersCounter.Dec()
			delete(s.leaders, shardId)
		}
	}

	// Create new follower controller
	fc, err := follow.NewFollowerController(s.config, namespace, shardId, s.walFactory, s.kvFactory, newTermOptions)
	if err != nil {
		return nil, err
	}

	s.followers[shardId] = fc
	s.followersCounter.Inc()
	return fc, nil
}

func (s *shardsDirector) DeleteShard(req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
	s.Lock()
	defer s.Unlock()

	if leader, ok := s.leaders[req.Shard]; ok {
		resp, err := leader.DeleteShard(req)
		if err != nil {
			return nil, err
		}

		delete(s.leaders, req.Shard)
		s.leadersCounter.Dec()
		return resp, nil
	}

	if follower, ok := s.followers[req.Shard]; ok {
		resp, err := follower.DeleteShard(req)
		if err != nil {
			return nil, err
		}

		delete(s.followers, req.Shard)
		s.followersCounter.Dec()
		return resp, nil
	}

	fc, err := follow.NewFollowerController(s.config, req.Namespace, req.Shard, s.walFactory, s.kvFactory, nil)
	if err != nil {
		return nil, err
	}
	return fc.DeleteShard(req)
}

func (s *shardsDirector) Close() error {
	s.Lock()
	defer s.Unlock()

	s.closed = true
	var err error

	for _, leader := range s.leaders {
		err = multierr.Append(err, leader.Close())
	}

	for _, follower := range s.followers {
		err = multierr.Append(err, follower.Close())
	}

	return err
}
