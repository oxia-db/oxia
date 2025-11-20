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

package oxia

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/rpc"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/oxia/internal"
	"github.com/oxia-db/oxia/proto"
)

type notifications struct {
	multiplexCh  chan *Notification
	closeCh      chan any
	shardManager internal.ShardManager
	clientPool   rpc.ClientPool

	initWaitGroup concurrent.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc

	ctxMultiplexChanClosed    context.Context
	cancelMultiplexChanClosed context.CancelFunc
}

func newNotifications(ctx context.Context, options clientOptions, clientPool rpc.ClientPool, shardManager internal.ShardManager) (*notifications, error) {
	nm := &notifications{
		multiplexCh:  make(chan *Notification, 100),
		closeCh:      make(chan any),
		shardManager: shardManager,
		clientPool:   clientPool,
	}

	nm.ctx, nm.cancel = context.WithCancel(ctx)
	nm.ctxMultiplexChanClosed, nm.cancelMultiplexChanClosed = context.WithCancel(context.Background())

	// Create a notification manager for each shard
	shards := shardManager.GetAll()
	nm.initWaitGroup = concurrent.NewWaitGroup(len(shards))

	for _, shard := range shards {
		newShardNotificationsManager(shard, nm)
	}

	go process.DoWithLabels(
		nm.ctx,
		map[string]string{
			"oxia": "notifications-manager-close",
		},
		func() {
			// Wait until all the shards managers are done before
			// closing the user-facing channel
			for i := 0; i < len(shards); i++ {
				<-nm.closeCh
			}

			close(nm.multiplexCh)
			nm.cancelMultiplexChanClosed()
		},
	)

	// Wait for the notifications on all the shards to be initialized
	timeoutCtx, cancel := context.WithTimeout(nm.ctx, options.requestTimeout)
	defer cancel()

	if err := nm.initWaitGroup.Wait(timeoutCtx); err != nil {
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

// Manages the notifications for a specific shard.
type shardNotificationsManager struct {
	shard              int64
	ctx                context.Context
	nm                 *notifications
	backoff            backoff.BackOff
	lastOffsetReceived int64
	initialized        bool
	log                *slog.Logger
}

func newShardNotificationsManager(shard int64, nm *notifications) *shardNotificationsManager {
	snm := &shardNotificationsManager{
		shard:              shard,
		ctx:                nm.ctx,
		nm:                 nm,
		lastOffsetReceived: -1,
		backoff:            time2.NewBackOffWithInitialInterval(nm.ctx, 1*time.Second),
		log: slog.With(
			slog.String("component", "oxia-notifications-manager"),
			slog.Int64("shard", shard),
		),
	}

	go process.DoWithLabels(
		snm.ctx,
		map[string]string{
			"oxia":  "notifications-manager",
			"shard": fmt.Sprintf("%d", shard),
		},
		snm.getNotificationsWithRetries,
	)

	return snm
}

func (snm *shardNotificationsManager) getNotificationsWithRetries() { //nolint:revive
	_ = backoff.RetryNotify(snm.getNotifications,
		snm.backoff, func(err error, duration time.Duration) {
			if !errors.Is(err, context.Canceled) {
				snm.log.Error(
					"Error while getting notifications",
					slog.Any("error", err),
					slog.Duration("retry-after", duration),
				)
			}

			if !snm.initialized {
				snm.initialized = true
				snm.nm.initWaitGroup.Fail(err)
				snm.nm.cancel()
			}
		})

	// Signal that this shard notification manager is now closed
	snm.nm.closeCh <- nil
}

func (snm *shardNotificationsManager) multiplexNotificationBatch(nb *proto.NotificationBatch) error {
	if !snm.initialized {
		snm.log.Debug("Initialized the notification manager")

		// We need to discard the very first notification, because it's only
		// needed to ensure that the notification cursor is created on the
		// server side.
		snm.initialized = true
		snm.nm.initWaitGroup.Done()
		snm.lastOffsetReceived = nb.Offset
		return nil
	}

	for key, n := range nb.Notifications {
		select {
		case snm.nm.multiplexCh <- convertNotification(key, n):

		// Unblock from channel write when we're closing down
		case <-snm.ctx.Done():
			return snm.ctx.Err()
		}
	}
	return nil
}

func (snm *shardNotificationsManager) multiplexNotificationBatchOnce(notifications proto.OxiaClient_GetNotificationsClient) error {
	nb, err := notifications.Recv()
	if err != nil {
		return err
	} else if nb == nil {
		if snm.ctx.Err() != nil {
			return snm.ctx.Err()
		}
		return io.EOF
	}

	snm.log.Debug(
		"Received batch notification",
		slog.Int64("offset", nb.Offset),
		slog.Int("count", len(nb.Notifications)),
	)

	err = snm.multiplexNotificationBatch(nb)
	if err != nil {
		return err
	}

	snm.lastOffsetReceived = nb.Offset
	return nil
}

func (snm *shardNotificationsManager) multiplexNotifications(notifications proto.OxiaClient_GetNotificationsClient) error {
	for {
		err := snm.multiplexNotificationBatchOnce(notifications)
		if err != nil {
			return err
		}
	}
}

func (snm *shardNotificationsManager) getNotifications() error {
	leader := snm.nm.shardManager.Leader(snm.shard)

	client, err := snm.nm.clientPool.GetClientRpc(leader)
	if err != nil {
		return err
	}

	var startOffsetExclusive *int64
	if snm.lastOffsetReceived >= 0 {
		startOffsetExclusive = &snm.lastOffsetReceived
	}

	notifications, err := client.GetNotifications(snm.ctx, &proto.NotificationsRequest{
		Shard:                snm.shard,
		StartOffsetExclusive: startOffsetExclusive,
	})
	if err != nil {
		if snm.ctx.Err() != nil {
			return snm.ctx.Err()
		}
		return err
	}

	snm.backoff.Reset()

	return snm.multiplexNotifications(notifications)
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
