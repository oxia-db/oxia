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

package kv

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/proto"
)

const (
	notificationsPrefix      = constant.InternalKeyPrefix + "notifications"
	maxNotificationBatchSize = 100
)

var (
	firstNotificationKey = notificationKey(0)
	lastNotificationKey  = notificationKey(math.MaxInt64)

	notificationsPrefixScanFormat = fmt.Sprintf("%s/%%016x", notificationsPrefix)
)

type Notifications struct {
	batch proto.NotificationBatch
}

func newNotifications(shardId int64, offset int64, timestamp uint64) *Notifications {
	return &Notifications{
		proto.NotificationBatch{
			Shard:         shardId,
			Offset:        offset,
			Timestamp:     timestamp,
			Notifications: map[string]*proto.Notification{},
		},
	}
}

func (n *Notifications) Modified(key string, versionId, modificationsCount int64) {
	if strings.HasPrefix(key, constant.InternalKeyPrefix) {
		return
	}
	nType := proto.NotificationType_KEY_CREATED
	if modificationsCount > 0 {
		nType = proto.NotificationType_KEY_MODIFIED
	}
	n.batch.Notifications[key] = &proto.Notification{
		Type:      nType,
		VersionId: &versionId,
	}
}

func (n *Notifications) Deleted(key string) {
	if strings.HasPrefix(key, constant.InternalKeyPrefix) {
		return
	}
	n.batch.Notifications[key] = &proto.Notification{
		Type: proto.NotificationType_KEY_DELETED,
	}
}

func (n *Notifications) DeletedRange(keyStartInclusive, keyEndExclusive string) {
	if strings.HasPrefix(keyStartInclusive, constant.InternalKeyPrefix) {
		return
	}
	n.batch.Notifications[keyStartInclusive] = &proto.Notification{
		Type:         proto.NotificationType_KEY_RANGE_DELETED,
		KeyRangeLast: &keyEndExclusive,
	}
}

func notificationKey(offset int64) string {
	return fmt.Sprintf("%s/%016x", notificationsPrefix, offset)
}

func parseNotificationKey(key string) (offset int64, err error) {
	if _, err = fmt.Sscanf(key, notificationsPrefixScanFormat, &offset); err != nil {
		return offset, err
	}
	return offset, nil
}

type notificationsTracker struct {
	sync.Mutex
	cond       concurrent.ConditionContext
	shard      int64
	lastOffset atomic.Int64
	closed     atomic.Bool
	kv         KV
	log        *slog.Logger

	ctx       context.Context
	cancel    context.CancelFunc
	waitClose concurrent.WaitGroup

	readCounter      metric.Counter
	readBatchCounter metric.Counter
	readBytesCounter metric.Counter
}

func newNotificationsTracker(namespace string, shard int64, lastOffset int64, kv KV, notificationRetentionTime time.Duration, clock time2.Clock) *notificationsTracker {
	labels := metric.LabelsForShard(namespace, shard)
	nt := &notificationsTracker{
		shard:     shard,
		kv:        kv,
		waitClose: concurrent.NewWaitGroup(1),
		log: slog.With(
			slog.String("component", "notifications-tracker"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shard),
		),
		readCounter: metric.NewCounter("oxia_server_notifications_read",
			"The total number of notifications", "count", labels),
		readBatchCounter: metric.NewCounter("oxia_server_notifications_read_batches",
			"The total number of notification batches", "count", labels),
		readBytesCounter: metric.NewCounter("oxia_server_notifications_read",
			"The total size in bytes of notifications reads", metric.Bytes, labels),
	}
	nt.lastOffset.Store(lastOffset)
	nt.cond = concurrent.NewConditionContext(nt)
	nt.ctx, nt.cancel = context.WithCancel(context.Background())
	newNotificationsTrimmer(nt.ctx, namespace, shard, kv, notificationRetentionTime, nt.waitClose, clock)
	return nt
}

func (nt *notificationsTracker) UpdatedCommitOffset(offset int64) {
	nt.lastOffset.Store(offset)
	nt.cond.Broadcast()
}

func (nt *notificationsTracker) waitForNotifications(ctx context.Context, startOffset int64) error {
	nt.Lock()
	defer nt.Unlock()

	for startOffset > nt.lastOffset.Load() && !nt.closed.Load() {
		nt.log.Debug(
			"Waiting for notification to be available",
			slog.Int64("start-offset", startOffset),
			slog.Int64("last-committed-offset", nt.lastOffset.Load()),
		)

		if err := nt.cond.Wait(ctx); err != nil {
			return err
		}
	}

	if nt.closed.Load() {
		return constant.ErrAlreadyClosed
	}

	return nil
}

func (nt *notificationsTracker) ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error) {
	if err := nt.waitForNotifications(ctx, startOffset); err != nil {
		return nil, err
	}

	it, err := nt.kv.RangeScan(notificationKey(startOffset), lastNotificationKey)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var res []*proto.NotificationBatch

	totalCount := 0
	totalSize := 0

	for count := 0; count < maxNotificationBatchSize && it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return nil, errors.Wrap(err, "failed to read notification batch")
		}

		nb := &proto.NotificationBatch{}
		if err := nb.UnmarshalVT(value); err != nil {
			return nil, errors.Wrap(err, "failed to Deserialize notification batch")
		}
		res = append(res, nb)

		totalSize += len(value)
		totalCount += len(nb.Notifications)
	}

	nt.readBatchCounter.Add(len(res))
	nt.readBytesCounter.Add(totalSize)
	nt.readCounter.Add(totalCount)
	return res, nil
}

func (nt *notificationsTracker) Close() error {
	select {
	case <-nt.ctx.Done():
		return nil
	default:
		nt.cancel()
		nt.closed.Store(true)
		nt.cond.Broadcast()
		return nt.waitClose.Wait(context.Background())
	}
}
