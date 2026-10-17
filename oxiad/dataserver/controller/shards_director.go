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

package controller

import (
	"errors"
	"io"
	"log/slog"
	"sync"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/oxiad/dataserver/controller/follow"
	"github.com/oxia-db/oxia/oxiad/dataserver/controller/lead"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
	dataserverrpc "github.com/oxia-db/oxia/oxiad/dataserver/rpc"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
)

var ErrNodeIsNotFollower = errors.New("oxia: node is not follower")

type ShardsDirector interface {
	io.Closer

	GetLeader(shardId int64) (lead.LeaderController, error)
	GetFollower(shardId int64) (follow.FollowerController, error)

	GetOrCreateLeader(namespace string, shardId int64, newTermOptions *proto.NewTermOptions) (lead.LeaderController, error)
	GetOrCreateFollower(namespace string, shardId int64, term int64, newTermOptions *proto.NewTermOptions) (follow.FollowerController, error)

	DeleteShard(req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error)

	GetAllLeaders() []lead.LeaderController

	// Checkpoint takes a checkpoint of the database of a shard hosted here, as
	// its leader or follower, or returns constant.ErrNodeIsNotMember.
	Checkpoint(shardId int64) (*database.Checkpoint, error)
	// CommitOffset returns the last entry applied to the database of a shard
	// hosted here, or constant.ErrNodeIsNotMember.
	CommitOffset(shardId int64) (int64, error)
	// WithDatabase runs fn with the directory of the database of a shard
	// hosted here while it is still the database a checkpoint of the given
	// generation was taken from, see lead.LeaderController.WithDatabase, or
	// returns constant.ErrNodeIsNotMember.
	WithDatabase(shardId int64, generation uint64, fn func(dir string) error) error
	// DatabaseDir returns the directory of the database of a shard.
	DatabaseDir(namespace string, shardId int64) string
}

type shardsDirector struct {
	sync.RWMutex
	kvFactory              kvstore.Factory
	walFactory             wal.Factory
	replicationRpcProvider dataserverrpc.ReplicationRpcProvider
	storageOptions         *option.StorageOptions
	log                    *slog.Logger

	closed bool

	leaders   map[int64]lead.LeaderController
	followers map[int64]follow.FollowerController

	leadersCounter   metric.UpDownCounter
	followersCounter metric.UpDownCounter
}

func NewShardsDirector(storageOptions *option.StorageOptions, walFactory wal.Factory, kvFactory kvstore.Factory, provider dataserverrpc.ReplicationRpcProvider) ShardsDirector {
	sd := &shardsDirector{
		storageOptions:         storageOptions,
		walFactory:             walFactory,
		kvFactory:              kvFactory,
		leaders:                make(map[int64]lead.LeaderController),
		followers:              make(map[int64]follow.FollowerController),
		replicationRpcProvider: provider,
		log: slog.With(
			slog.String("component", "shards-director"),
		),

		leadersCounter: metric.NewUpDownCounter("oxia_server_leaders_count",
			"The number of leader controller in a dataserver", "count", map[string]any{}),
		followersCounter: metric.NewUpDownCounter("oxia_server_followers_count",
			"The number of follower controller in a dataserver", "count", map[string]any{}),
	}

	return sd
}

func (s *shardsDirector) GetLeader(shardId int64) (lead.LeaderController, error) {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, constant.ErrResourceUnavailable
	}

	if leader, ok := s.leaders[shardId]; ok {
		// There is already a leader controller for this shard
		return leader, nil
	}

	s.log.Debug(
		"This node is not hosting shard",
		slog.Int64("shard", shardId),
	)
	return nil, constant.ErrNodeIsNotLeader
}

func (s *shardsDirector) GetFollower(shardId int64) (follow.FollowerController, error) {
	s.RLock()
	defer s.RUnlock()

	if s.closed {
		return nil, constant.ErrResourceUnavailable
	}

	if follower, ok := s.followers[shardId]; ok {
		// There is already a follower controller for this shard
		return follower, nil
	}

	s.log.Debug(
		"This node is not hosting shard",
		slog.Int64("shard", shardId),
	)
	return nil, ErrNodeIsNotFollower
}

func (s *shardsDirector) GetOrCreateLeader(namespace string, shardId int64, newTermOptions *proto.NewTermOptions) (lead.LeaderController, error) {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return nil, constant.ErrResourceUnavailable
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
	lc, err := lead.NewLeaderController(s.storageOptions, namespace, shardId, s.replicationRpcProvider, s.walFactory, s.kvFactory, newTermOptions)
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
		return nil, constant.ErrResourceUnavailable
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
	fc, err := follow.NewFollowerController(s.storageOptions, namespace, shardId, s.walFactory, s.kvFactory, newTermOptions)
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
		resp, err := follower.Delete(req)
		if err != nil {
			return nil, err
		}

		delete(s.followers, req.Shard)
		s.followersCounter.Dec()
		return resp, nil
	}

	fc, err := follow.NewFollowerController(s.storageOptions, req.Namespace, req.Shard, s.walFactory, s.kvFactory, nil)
	if err != nil {
		return nil, err
	}
	return fc.Delete(req)
}

func (s *shardsDirector) Checkpoint(shardId int64) (*database.Checkpoint, error) {
	s.RLock()
	leader, isLeader := s.leaders[shardId]
	follower, isFollower := s.followers[shardId]
	closed := s.closed
	s.RUnlock()
	switch {
	case closed:
		return nil, constant.ErrResourceUnavailable
	case isLeader:
		return leader.Checkpoint()
	case isFollower:
		return follower.Checkpoint()
	default:
		return nil, constant.ErrNodeIsNotMember
	}
}

func (s *shardsDirector) CommitOffset(shardId int64) (int64, error) {
	s.RLock()
	defer s.RUnlock()
	switch {
	case s.closed:
		return 0, constant.ErrResourceUnavailable
	case s.leaders[shardId] != nil:
		return s.leaders[shardId].DatabaseCommitOffset(), nil
	case s.followers[shardId] != nil:
		return s.followers[shardId].CommitOffset(), nil
	default:
		return 0, constant.ErrNodeIsNotMember
	}
}

func (s *shardsDirector) WithDatabase(shardId int64, generation uint64, fn func(dir string) error) error {
	s.RLock()
	leader, isLeader := s.leaders[shardId]
	follower, isFollower := s.followers[shardId]
	closed := s.closed
	s.RUnlock()
	switch {
	case closed:
		return constant.ErrResourceUnavailable
	case isLeader:
		return leader.WithDatabase(generation, fn)
	case isFollower:
		return follower.WithDatabase(generation, fn)
	default:
		return constant.ErrNodeIsNotMember
	}
}

func (s *shardsDirector) DatabaseDir(namespace string, shardId int64) string {
	return s.kvFactory.DatabaseDir(namespace, shardId)
}

func (s *shardsDirector) GetAllLeaders() []lead.LeaderController {
	s.RLock()
	defer s.RUnlock()

	leaders := make([]lead.LeaderController, 0, len(s.leaders))
	for _, leader := range s.leaders {
		leaders = append(leaders, leader)
	}
	return leaders
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
