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

package internal

import (
	"cmp"
	"context"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
)

type ShardManager interface {
	io.Closer
	Get(key string) int64
	GetAll() []int64
	Leader(shardId int64) string
	Exists(shardId int64) bool
}

type shardManagerImpl struct {
	sync.RWMutex
	updatedWg concurrent.WaitGroup

	shardStrategy  ShardStrategy
	rpcProvider    RpcProvider
	serviceAddress string
	namespace      string
	shards         map[int64]Shard
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *slog.Logger
	requestTimeout time.Duration
}

func NewShardManager(shardStrategy ShardStrategy, rpcProvider RpcProvider,
	serviceAddress string, namespace string, requestTimeout time.Duration) (ShardManager, error) {
	sm := &shardManagerImpl{
		namespace:      namespace,
		shardStrategy:  shardStrategy,
		rpcProvider:    rpcProvider,
		serviceAddress: serviceAddress,
		shards:         make(map[int64]Shard),
		requestTimeout: requestTimeout,
		logger: slog.With(
			slog.String("component", "shardManager"),
		),
	}

	sm.updatedWg = concurrent.NewWaitGroup(1)
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

	go process.DoWithLabels(
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

	shardIDs := make([]int64, 0, len(s.shards))
	for shardId := range s.shards {
		shardIDs = append(shardIDs, shardId)
	}
	return shardIDs
}

func (s *shardManagerImpl) Leader(shardId int64) string {
	s.RLock()
	defer s.RUnlock()

	if shard, ok := s.shards[shardId]; ok {
		return shard.Leader
	}
	return s.serviceAddress
}

func (s *shardManagerImpl) Exists(shardId int64) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.shards[shardId]
	return ok
}

func (s *shardManagerImpl) isClosed() bool {
	return s.ctx.Err() != nil
}

func (s *shardManagerImpl) receiveWithRecovery() {
	backOff := time2.NewBackOff(s.ctx)
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

			if errors.Is(err, constant.ErrNamespaceNotFound) || status.Code(err) == codes.Unauthenticated {
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
	request := proto.ShardAssignmentsRequest{Namespace: s.namespace}

	stream, err := s.rpcProvider.GetShardAssignments(s.ctx, s.serviceAddress, &request)
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

	// Older coordinators can publish a snapshot where a split child overlaps
	// its parent while the split completes. Applied shard by shard, it can
	// leave hashes without a shard. Every snapshot lists all the shards, so
	// skip it: the next one replaces it.
	if shard, overlapping, found := findOverlap(updates); found {
		s.logger.Warn(
			"Ignoring shard assignments with overlapping shards",
			slog.Any("shard", shard),
			slog.Any("overlapping-shard", overlapping),
			slog.Any("assignments", updates),
		)
		return
	}

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
	return a.MinInclusive <= b.MaxInclusive && a.MaxInclusive >= b.MinInclusive
}

// findOverlap returns two of the shards that overlap, if any.
func findOverlap(shards []Shard) (shard Shard, overlapping Shard, found bool) {
	sorted := slices.SortedFunc(slices.Values(shards), func(a, b Shard) int {
		return cmp.Compare(a.HashRange.MinInclusive, b.HashRange.MinInclusive)
	})
	for i := 1; i < len(sorted); i++ {
		if overlap(sorted[i-1].HashRange, sorted[i].HashRange) {
			return sorted[i-1], sorted[i], true
		}
	}
	return Shard{}, Shard{}, false
}
