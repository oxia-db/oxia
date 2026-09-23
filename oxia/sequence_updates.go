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

package oxia

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/process"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/internal"
)

// A server rejects a subscription (eg. the node is not the leader) as soon as
// it receives it, while an accepted subscription gets no response until a
// sequence key exists: a stream that stayed open for longer than this was
// accepted.
const sequenceUpdatesAcceptedAfter = 1 * time.Second

type sequenceUpdates struct {
	prefixKey    string
	partitionKey string
	ch           chan string
	shardManager internal.ShardManager
	rpcProvider  internal.RpcProvider

	ctx     context.Context
	backoff backoff.BackOff
	log     *slog.Logger
}

func newSequenceUpdates(ctx context.Context, clientCtx context.Context, prefixKey string, partitionKey string,
	rpcProvider internal.RpcProvider, shardManager internal.ShardManager) <-chan string {
	// The subscription ends when either the caller's context is done or the
	// client is closed
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(clientCtx, cancel)

	su := &sequenceUpdates{
		prefixKey:    prefixKey,
		partitionKey: partitionKey,
		ch:           make(chan string),
		shardManager: shardManager,
		rpcProvider:  rpcProvider,
		ctx:          ctx,
		backoff:      time2.NewBackOffWithInitialInterval(ctx, 1*time.Second),
		log: slog.With(
			slog.String("component", "oxia-get-sequence-updates"),
			slog.String("prefix-key", prefixKey),
			slog.String("partition-key", partitionKey),
		),
	}

	go process.DoWithLabels(
		su.ctx,
		map[string]string{
			"oxia":      "sequence-updates",
			"prefixKey": prefixKey,
		},
		func() {
			defer stop()
			su.getSequenceUpdatesWithRetries()
		},
	)

	return su.ch
}

func (su *sequenceUpdates) getSequenceUpdatesWithRetries() { //nolint:revive
	_ = backoff.RetryNotify(su.getSequenceUpdates,
		su.backoff, func(err error, duration time.Duration) {
			// The stream fails with Canceled when the client connection is
			// closing, eg. while the client itself is being closed
			if !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
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

	updates, err := su.rpcProvider.GetSequenceUpdates(su.ctx, leader, &proto.GetSequenceUpdatesRequest{
		Key: su.prefixKey,
	})
	if err != nil {
		if su.ctx.Err() != nil {
			return su.ctx.Err()
		}
		return err
	}

	openedAt := time.Now()
	for {
		res, err2 := updates.Recv()
		if err2 != nil {
			// The stream gets created even if the server rejects the
			// subscription. Reset the backoff only when an accepted one fails
			// (eg. on a leader change), so a persistent rejection keeps
			// escalating the retry delay.
			if time.Since(openedAt) >= sequenceUpdatesAcceptedAfter {
				su.backoff.Reset()
			}
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
