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

package oxia

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/oxia/internal"
	"github.com/streamnative/oxia/proto"
)

type sequenceUpdates struct {
	prefixKey    string
	partitionKey string
	ch           chan string
	shardManager internal.ShardManager
	clientPool   common.ClientPool

	ctx     context.Context
	backoff backoff.BackOff
	log     *slog.Logger
}

func newSequenceUpdates(ctx context.Context, prefixKey string, partitionKey string,
	clientPool common.ClientPool, shardManager internal.ShardManager) <-chan string {
	su := &sequenceUpdates{
		prefixKey:    prefixKey,
		partitionKey: partitionKey,
		ch:           make(chan string),
		shardManager: shardManager,
		clientPool:   clientPool,
		ctx:          ctx,
		backoff:      common.NewBackOffWithInitialInterval(ctx, 1*time.Second),
		log: slog.With(
			slog.String("component", "oxia-get-sequence-updates"),
			slog.String("key", "key"),
		),
	}

	go common.DoWithLabels(
		su.ctx,
		map[string]string{
			"oxia":      "sequence-updates",
			"prefixKey": prefixKey,
		},
		su.getSequenceUpdatesWithRetries,
	)

	return su.ch
}

func (su *sequenceUpdates) getSequenceUpdatesWithRetries() { //nolint:revive
	_ = backoff.RetryNotify(su.getSequenceUpdates,
		su.backoff, func(err error, duration time.Duration) {
			if !errors.Is(err, context.Canceled) {
				su.log.Error(
					"Error while getting sequence updates",
					slog.Any("error", err),
					slog.Duration("retry-after", duration),
				)
			}
		})

	// Signal that the background go-routine is now done
	close(su.ch)
}

func (su *sequenceUpdates) getSequenceUpdates() error {
	shard := su.shardManager.Get(su.partitionKey)
	leader := su.shardManager.Leader(shard)

	rpc, err := su.clientPool.GetClientRpc(leader)
	if err != nil {
		return err
	}

	updates, err := rpc.GetSequenceUpdates(su.ctx, &proto.GetSequenceUpdatesRequest{
		Key: su.prefixKey,
	})
	if err != nil {
		if su.ctx.Err() != nil {
			return su.ctx.Err()
		}
		return err
	}

	su.backoff.Reset()

	for {
		res, err2 := updates.Recv()
		if err2 != nil {
			return err2
		}

		if res.HighestSequenceKey == "" {
			// Ignore first response if there are no sequences for the key
			continue
		}

		select {
		case su.ch <- res.HighestSequenceKey:
		case <-su.ctx.Done():
			return su.ctx.Err()
		}
	}
}
