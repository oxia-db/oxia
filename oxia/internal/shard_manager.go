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

package internal

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/proto"
)

type ShardManager interface {
	io.Closer
	Get(key string) int64
	GetAll() []int64
	Leader(shardId int64) string
}

type shardManagerImpl struct {
	sync.RWMutex
	updatedWg common.WaitGroup

	shardStrategy  ShardStrategy
	clientPool     common.ClientPool
	serviceAddress string
	namespace      string
	shards         map[int64]Shard
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *slog.Logger
	requestTimeout time.Duration
}

func NewShardManager(shardStrategy ShardStrategy, clientPool common.ClientPool,
	serviceAddress string, namespace string, requestTimeout time.Duration) (ShardManager, error) {
	sm := &shardManagerImpl{
		namespace:      namespace,
		shardStrategy:  shardStrategy,
		clientPool:     clientPool,
		serviceAddress: serviceAddress,
		shards:         make(map[int64]Shard),
		requestTimeout: requestTimeout,
		logger: slog.With(
			slog.String("component", "shardManager"),
		),
	}

	sm.updatedWg = common.NewWaitGroup(1)
	sm.ctx, sm.cancel = context.WithCancel(context.Background())

	if err := sm.start(); err != nil {
		return nil, errors.Wrap(err, "oxia: failed to retrieve the initial list of shard assignments")
	}

	return sm, nil
}

func (s *shardManagerImpl) Close() error {
	s.cancel()
	return nil
}

func (s *shardManagerImpl) start() error {
	s.Lock()

	go common.DoWithLabels(
		s.ctx,
		map[string]string{
			"oxia": "receive-shard-updates",
		},
		s.receiveWithRecovery,
	)

	ctx, cancel := context.WithTimeout(s.ctx, s.requestTimeout)
	defer cancel()

	s.Unlock()
	return s.updatedWg.Wait(ctx)
}

func (s *shardManagerImpl) Get(key string) int64 {
	s.RLock()
	defer s.RUnlock()

	predicate := s.shardStrategy.Get(key)

	for _, shard := range s.shards {
		if predicate(shard) {
			return shard.Id
		}
	}
	panic("shard not found")
}

func (s *shardManagerImpl) GetAll() []int64 {
	s.RLock()
	defer s.RUnlock()

	shardIds := make([]int64, 0, len(s.shards))
	for shardId := range s.shards {
		shardIds = append(shardIds, shardId)
	}
	return shardIds
}

func (s *shardManagerImpl) Leader(shardId int64) string {
	s.RLock()
	defer s.RUnlock()

	if shard, ok := s.shards[shardId]; ok {
		return shard.Leader
	}
	panic("shard not found")
}

func (s *shardManagerImpl) isClosed() bool {
	return s.ctx.Err() != nil
}

func (s *shardManagerImpl) receiveWithRecovery() {
	backOff := common.NewBackOff(s.ctx)
	err := backoff.RetryNotify(
		func() error {
			err := s.receive(backOff)
			if s.isClosed() {
				s.logger.Debug(
					"Closed",
					slog.Any("error", err),
				)
				return nil
			}

			if !isErrorRetryable(err) {
				return backoff.Permanent(err)
			}
			return err
		},
		backOff,
		func(err error, duration time.Duration) {
			if status.Code(err) != codes.Canceled {
				s.logger.Warn(
					"Failed receiving shard assignments, retrying later",
					slog.Any("error", err),
					slog.Duration("retry-after", duration),
				)
			}
		},
	)
	if err != nil {
		s.logger.Error(
			"Failed receiving shard assignments",
			slog.Any("error", err),
		)
		s.updatedWg.Fail(err)
	}
}

func (s *shardManagerImpl) receive(backOff backoff.BackOff) error {
	rpc, err := s.clientPool.GetClientRpc(s.serviceAddress)
	if err != nil {
		return err
	}

	request := proto.ShardAssignmentsRequest{Namespace: s.namespace}

	stream, err := rpc.GetShardAssignments(s.ctx, &request)
	if err != nil {
		return err
	}

	for {
		response, err := stream.Recv()
		if err != nil {
			return err
		}

		assignments, ok := response.Namespaces[s.namespace]
		if !ok {
			return errors.New("namespace not found in shards assignments")
		}

		shards := make([]Shard, len(assignments.Assignments))
		for i, assignment := range assignments.Assignments {
			shards[i] = toShard(assignment)
		}
		s.update(shards)
		backOff.Reset()
	}
}

func (s *shardManagerImpl) update(updates []Shard) {
	s.Lock()
	defer s.Unlock()

	for _, update := range updates {
		if _, ok := s.shards[update.Id]; !ok {
			// delete overlaps
			for shardId, existing := range s.shards {
				if overlap(update.HashRange, existing.HashRange) {
					s.logger.Info(
						"Deleting shard as it overlaps",
						slog.Any("Existing", existing),
						slog.Any("Update", update),
					)
					delete(s.shards, shardId)
				}
			}
		}
		s.shards[update.Id] = update
	}

	s.updatedWg.Done()
}

func overlap(a HashRange, b HashRange) bool {
	return !(a.MinInclusive > b.MaxInclusive || a.MaxInclusive < b.MinInclusive)
}

func isErrorRetryable(err error) bool {
	switch status.Code(err) {
	case common.CodeNamespaceNotFound:
		return false
	case codes.Unauthenticated:
		return false
	default:
		return true
	}
}
