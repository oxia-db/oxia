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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/internal"
)

// errShardSplit ends the subscription to a shard that was split: the
// subscriptions to the shards that replaced it take over.
var errShardSplit = errors.New("oxia: the shard was split")

type notifications struct {
	multiplexCh  chan *Notification
	shardManager internal.ShardManager
	rpcProvider  internal.RpcProvider

	initWaitGroup concurrent.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc

	// The subscriptions to the shards: the user-facing channel is closed once
	// they all stopped
	running sync.WaitGroup

	ctxMultiplexChanClosed    context.Context
	cancelMultiplexChanClosed context.CancelFunc
}

func newNotifications(ctx context.Context, options clientOptions, rpcProvider internal.RpcProvider, shardManager internal.ShardManager) (*notifications, error) {
	nm := &notifications{
		multiplexCh:  make(chan *Notification, 100),
		shardManager: shardManager,
		rpcProvider:  rpcProvider,
	}

	nm.ctx, nm.cancel = context.WithCancel(ctx)
	nm.ctxMultiplexChanClosed, nm.cancelMultiplexChanClosed = context.WithCancel(context.Background())

	// Create a notification manager for each shard
	shards := shardManager.GetAll()
	nm.initWaitGroup = concurrent.NewWaitGroup(len(shards))

	nm.running.Add(len(shards))
	for _, shard := range shards {
		init := &shardInit{}
		init.pending.Store(1)
		newShardNotificationsManager(shard, nm, nil, init)
	}

	go process.DoWithLabels(
		nm.ctx,
		map[string]string{
			"oxia": "notifications-manager-close",
		},
		func() {
			// Wait until all the shards managers are done before
			// closing the user-facing channel
			nm.running.Wait()

			close(nm.multiplexCh)
			nm.cancelMultiplexChanClosed()
		},
	)

	// Wait for the notifications on all the shards to be initialized
	timeoutCtx, cancel := context.WithTimeout(nm.ctx, options.requestTimeout)
	defer cancel()

	if err := nm.initWaitGroup.Wait(timeoutCtx); err != nil {
		// Stop the per-shard managers that may still be retrying
		nm.cancel()
		return nil, err
	}

	return nm, nil
}

func (nm *notifications) Ch() <-chan *Notification {
	return nm.multiplexCh
}

func (nm *notifications) Close() error {
	// Interrupt the go-routines receiving notifications on all the shards
	nm.cancel()

	// Wait until the all the go-routines are stopped and the user-facing channel
	// is closed
	<-nm.ctxMultiplexChanClosed.Done()

	// Ensure the channel is empty, so that the user will not see any notifications
	// after the close
	for range nm.multiplexCh { //nolint:revive
	}

	return nil
}

// shardInit is the initialization of the subscription to a shard of the
// initial shard map, which newNotifications waits for: the subscription is
// established with the first batch it receives, or, when the shard is split
// before, with those of the subscriptions to the shards that replaced it.
type shardInit struct {
	// The subscriptions that haven't received their first batch yet
	pending atomic.Int64
}

// Manages the notifications for a specific shard.
type shardNotificationsManager struct {
	shard              int64
	ctx                context.Context
	nm                 *notifications
	backoff            backoff.BackOff
	lastOffsetReceived int64
	initialized        bool
	// The initialization that the subscription counts for, until it is
	// initialized
	init *shardInit
	log  *slog.Logger
}

// newShardNotificationsManager starts the subscription to a shard: from the
// batch after startOffsetExclusive, or from the batch that the subscription
// receives first, which only initializes it, when startOffsetExclusive is nil.
func newShardNotificationsManager(shard int64, nm *notifications, startOffsetExclusive *int64, init *shardInit) {
	snm := &shardNotificationsManager{
		shard:              shard,
		ctx:                nm.ctx,
		nm:                 nm,
		lastOffsetReceived: -1,
		init:               init,
		backoff:            time2.NewBackOffWithInitialInterval(nm.ctx, 1*time.Second),
		log: slog.With(
			slog.String("component", "oxia-notifications-manager"),
			slog.Int64("shard", shard),
		),
	}
	if startOffsetExclusive != nil {
		snm.lastOffsetReceived = *startOffsetExclusive
		snm.initialized = true
		snm.init = nil
	}

	go process.DoWithLabels(
		snm.ctx,
		map[string]string{
			"oxia":  "notifications-manager",
			"shard": fmt.Sprintf("%d", shard),
		},
		snm.getNotificationsWithRetries,
	)
}

func (snm *shardNotificationsManager) getNotificationsWithRetries() { //nolint:revive
	// A change of the shard map ends the wait for the next attempt: it can be
	// the split of the shard
	timer := &internal.ShardMapTimer{}
	attempt := func() error {
		timer.Changed = snm.nm.shardManager.Changed()
		return snm.getNotifications()
	}
	err := backoff.RetryNotifyWithTimer(attempt,
		snm.backoff, func(err error, duration time.Duration) {
			if !errors.Is(err, context.Canceled) {
				snm.log.Error(
					"Error while getting notifications",
					slog.Any("error", err),
					slog.Duration("retry-after", duration),
				)
			}

			// Retryable errors (eg. the server has not yet received its shard
			// assignments) are handled by the backoff policy: the overall
			// initialization is still bounded by the request timeout in
			// newNotifications.
			if !snm.initialized && !constant.IsRetryable(err) {
				snm.initialized = true
				snm.nm.initWaitGroup.Fail(err)
				snm.nm.cancel()
			}
		}, timer)

	if errors.Is(err, errShardSplit) {
		snm.followSplit()
	}

	// Signal that this shard notification manager is now closed
	snm.nm.running.Done()
}

// split reports whether the shard was split: it is no longer in the shard map,
// and other shards replaced it.
func (snm *shardNotificationsManager) split() bool {
	shardManager := snm.nm.shardManager
	return !shardManager.Exists(snm.shard) && len(currentSuccessors(shardManager, snm.shard)) > 0
}

// followSplit subscribes to the shards that replaced the shard after it was
// split, from the last batch received from it: each of them holds the batches
// of the shard up to its last one, and delivers their notifications on its side
// of the split, before its own.
func (snm *shardNotificationsManager) followSplit() {
	if snm.ctx.Err() != nil {
		return
	}
	successors := currentSuccessors(snm.nm.shardManager, snm.shard)
	snm.log.Info(
		"Following the split of the shard",
		slog.Any("shards", successors),
		slog.Int64("offset", snm.lastOffsetReceived),
	)

	var startOffsetExclusive *int64
	if snm.initialized {
		startOffsetExclusive = &snm.lastOffsetReceived
	} else {
		// The subscription had not received any batch: the subscriptions to the
		// shards that replaced the shard initialize it in its place
		snm.init.pending.Add(int64(len(successors) - 1))
	}
	// Added before this manager is done, for the user-facing channel to stay
	// open
	snm.nm.running.Add(len(successors))
	for _, successor := range successors {
		newShardNotificationsManager(successor, snm.nm, startOffsetExclusive, snm.init)
	}
}

// cancelOnSplit cancels the stream of the subscription when the shard map,
// from the one whose change closes changed, no longer has the shard because it
// was split: the stream doesn't end by itself, until the shard is deleted.
func (snm *shardNotificationsManager) cancelOnSplit(ctx context.Context, cancel context.CancelFunc,
	changed <-chan struct{}) {
	for {
		select {
		case <-changed:
		case <-ctx.Done():
			return
		}
		changed = snm.nm.shardManager.Changed()
		if snm.split() {
			cancel()
			return
		}
	}
}

func (snm *shardNotificationsManager) multiplexNotificationBatch(nb *proto.NotificationBatch) error {
	if !snm.initialized {
		snm.log.Debug("Initialized the notification manager")

		// We need to discard the very first notification, because it's only
		// needed to ensure that the notification cursor is created on the
		// server side.
		snm.initialized = true
		if snm.init.pending.Add(-1) == 0 {
			snm.nm.initWaitGroup.Done()
		}
		snm.init = nil
		snm.lastOffsetReceived = nb.Offset
		return nil
	}

	for _, entry := range nb.Notifications {
		if err := snm.deliver(convertNotification(entry.GetKey(), entry.Value)); err != nil {
			return err
		}
	}
	return nil
}

func (snm *shardNotificationsManager) deliver(notification *Notification) error {
	select {
	case snm.nm.multiplexCh <- notification:
		return nil

	// Unblock from channel write when we're closing down
	case <-snm.ctx.Done():
		return snm.ctx.Err()
	}
}

// streamError returns the error that ends an attempt whose stream failed with
// err.
func (snm *shardNotificationsManager) streamError(err error) error {
	if snm.ctx.Err() != nil {
		return snm.ctx.Err()
	}
	if snm.split() {
		return backoff.Permanent(errShardSplit)
	}
	return err
}

func (snm *shardNotificationsManager) getNotifications() error {
	// Taken before the shard is looked up: a split from then on ends the
	// stream
	changed := snm.nm.shardManager.Changed()
	if snm.split() {
		return backoff.Permanent(errShardSplit)
	}
	leader := snm.nm.shardManager.Leader(snm.shard)

	var startOffsetExclusive *int64
	if snm.initialized {
		startOffsetExclusive = new(snm.lastOffsetReceived)
	}

	streamCtx, cancel := context.WithCancel(snm.ctx)
	defer cancel()
	go snm.cancelOnSplit(streamCtx, cancel, changed)

	notifications, err := snm.nm.rpcProvider.GetNotifications(streamCtx, leader, &proto.NotificationsRequest{
		Shard:                snm.shard,
		StartOffsetExclusive: startOffsetExclusive,
	})
	if err != nil {
		return snm.streamError(err)
	}

	for first := true; ; first = false {
		nb, err := notifications.Recv()
		if err != nil {
			return snm.streamError(err)
		} else if nb == nil {
			if snm.ctx.Err() != nil {
				return snm.ctx.Err()
			}
			return io.EOF
		}

		if snm.log.Enabled(snm.ctx, slog.LevelDebug) {
			snm.log.Debug(
				"Received batch notification",
				slog.Int64("offset", nb.Offset),
				slog.Int("count", len(nb.Notifications)),
			)
		}

		// The stream gets created even when the server rejects the subscription:
		// the rejection is only reported by the first Recv(). The server confirms
		// an accepted subscription with a first empty batch, so receiving a batch
		// is the signal that it was accepted, and a persistent rejection never
		// reaches here and keeps escalating the retry delay.
		snm.backoff.Reset()

		// The first batch tells where the subscription starts: after the
		// batches that the retention deleted, if any is after the requested
		// offset
		if first && startOffsetExclusive != nil && nb.Offset > *startOffsetExclusive {
			if err := snm.notifyMissed(*startOffsetExclusive, nb.Offset); err != nil {
				return err
			}
		}

		if err := snm.multiplexNotificationBatch(nb); err != nil {
			return err
		}

		snm.lastOffsetReceived = nb.Offset
	}
}

// notifyMissed tells the user that the subscription missed notifications of
// the shard, after offset fromExclusive and up to offset toInclusive: the
// retention deleted them before the subscription could read them.
func (snm *shardNotificationsManager) notifyMissed(fromExclusive int64, toInclusive int64) error {
	snm.log.Warn(
		"Missed notifications that the retention deleted before they were read",
		slog.Int64("from-offset-exclusive", fromExclusive),
		slog.Int64("to-offset-inclusive", toInclusive),
	)
	return snm.deliver(&Notification{Type: NotificationsMissed, VersionId: -1})
}

func convertNotificationType(t proto.NotificationType) NotificationType {
	switch t {
	case proto.NotificationType_KEY_CREATED:
		return KeyCreated
	case proto.NotificationType_KEY_MODIFIED:
		return KeyModified
	case proto.NotificationType_KEY_DELETED:
		return KeyDeleted
	case proto.NotificationType_KEY_RANGE_DELETED:
		return KeyRangeRangeDeleted
	default:
		panic("Invalid notification type")
	}
}

func convertNotification(key string, n *proto.Notification) *Notification {
	versionId := int64(-1)
	if n.VersionId != nil {
		versionId = *n.VersionId
	}
	return &Notification{
		Type:        convertNotificationType(n.Type),
		Key:         key,
		VersionId:   versionId,
		KeyRangeEnd: n.GetKeyRangeLast(),
	}
}
