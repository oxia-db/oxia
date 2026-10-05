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

package database

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
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
	// The sealed batch in the wire format, when it is kept for the
	// notifications cache, or nil
	encoded []byte
	// The notifications recorded by add, in the order of their operations.
	// Their keys can alias the WAL entry that the write was decoded from:
	// nothing may keep them past the write.
	pending []pendingNotification
	// When set, Deleted notifies the deletion of a record only if it reports
	// true for its key (see DeferredSplitFilter)
	notifiedDeletion func(key string) bool
}

// pendingNotification is a notification recorded by Notifications.add, held
// by value: the entry that seal builds for it points into it.
type pendingNotification struct {
	key             string
	keyRangeLast    string
	versionId       int64
	nType           proto.NotificationType
	hasVersionId    bool
	hasKeyRangeLast bool
}

// newNotifications creates the notifications of the batch at offset, with
// room for expectedCount of them: add only has to grow the slice that holds
// them past that count.
func newNotifications(shardId int64, offset int64, timestamp uint64, expectedCount int) *Notifications {
	return &Notifications{
		batch: proto.NotificationBatch{
			Shard:     shardId,
			Offset:    offset,
			Timestamp: timestamp,
		},
		pending: make([]pendingNotification, 0, expectedCount),
	}
}

// add records the operation on key that the notification describes. It keeps
// neither the notification nor its fields, but copies them, so that they can
// stay on the stack of the caller.
func (n *Notifications) add(key string, notification *proto.Notification) {
	p := pendingNotification{key: key, nType: notification.Type}
	if notification.VersionId != nil {
		p.versionId, p.hasVersionId = *notification.VersionId, true
	}
	if notification.KeyRangeLast != nil {
		p.keyRangeLast, p.hasKeyRangeLast = *notification.KeyRangeLast, true
	}
	n.pending = append(n.pending, p)
}

// seal builds the batch entries from the recorded operations, sorts them and
// deduplicates them, keeping the last operation recorded for each key. The
// sorted order makes the generated marshal deterministic — the serialized
// batch feeds the replicated checksum — and the sort must be stable so that
// "last within a run of equal keys" still means "last operation applied".
func (n *Notifications) seal() *proto.NotificationBatch {
	// One allocation for each kind of object, whatever the number of entries:
	// the entries point into the pending notifications for their keys,
	// version ids and range ends
	entries := make([]*proto.NotificationEntry, len(n.pending))
	entryStore := make([]proto.NotificationEntry, len(n.pending))
	notificationStore := make([]proto.Notification, len(n.pending))
	for i := range n.pending {
		p := &n.pending[i]
		notification := &notificationStore[i]
		notification.Type = p.nType
		if p.hasVersionId {
			notification.VersionId = &p.versionId
		}
		if p.hasKeyRangeLast {
			notification.KeyRangeLast = &p.keyRangeLast
		}
		entry := &entryStore[i]
		entry.Key = &p.key
		entry.Value = notification
		entries[i] = entry
	}

	slices.SortStableFunc(entries, func(a, b *proto.NotificationEntry) int {
		return strings.Compare(a.GetKey(), b.GetKey())
	})

	deduped := entries[:0]
	for i, entry := range entries {
		if i+1 < len(entries) && entries[i+1].GetKey() == entry.GetKey() {
			// A later operation on the same key supersedes this one
			continue
		}
		deduped = append(deduped, entry)
	}
	n.batch.Notifications = deduped
	return &n.batch
}

func (n *Notifications) Modified(key string, versionId, modificationsCount int64) {
	if strings.HasPrefix(key, constant.InternalKeyPrefix) {
		return
	}
	nType := proto.NotificationType_KEY_CREATED
	if modificationsCount > 0 {
		nType = proto.NotificationType_KEY_MODIFIED
	}
	n.add(key, &proto.Notification{
		Type:      nType,
		VersionId: &versionId,
	})
}

// Deleted notifies the deletion of the record at key. A split child that still
// holds records outside its hash range reads it from the batch, to skip the
// ones it doesn't hold for its clients: the caller notifies the deletion before
// deleting a record that the child may not keep.
func (n *Notifications) Deleted(key string) {
	if strings.HasPrefix(key, constant.InternalKeyPrefix) {
		return
	}
	if n.notifiedDeletion != nil && !n.notifiedDeletion(key) {
		return
	}
	n.add(key, &proto.Notification{
		Type: proto.NotificationType_KEY_DELETED,
	})
}

func (n *Notifications) DeletedRange(keyStartInclusive, keyEndExclusive string) {
	// With FEATURE_DELETE_RANGE_NOTIFICATION_RECORDS, a range covers either
	// internal keys or regular ones, and its start tells which: one that covers
	// both is rejected before its notification
	if strings.HasPrefix(keyStartInclusive, constant.InternalKeyPrefix) {
		return
	}
	n.add(keyStartInclusive, &proto.Notification{
		Type:         proto.NotificationType_KEY_RANGE_DELETED,
		KeyRangeLast: &keyEndExclusive,
	})
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

var (
	notificationBatchFields        = (&proto.NotificationBatch{}).ProtoReflect().Descriptor().Fields()
	notificationBatchOffsetField   = notificationBatchFields.ByName("offset").Number()
	notificationBatchNotifications = notificationBatchFields.ByName("notifications").Number()
)

// parseNotificationBatch reads the offset of an encoded notification batch,
// and counts its notifications, without decoding them.
func parseNotificationBatch(data []byte) (offset int64, notifications int, err error) {
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return 0, 0, protowire.ParseError(n)
		}
		data = data[n:]
		if number == notificationBatchOffsetField && wireType == protowire.VarintType {
			value, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return 0, 0, protowire.ParseError(n)
			}
			offset = int64(value)
			data = data[n:]
			continue
		}
		if n = protowire.ConsumeFieldValue(number, wireType, data); n < 0 {
			return 0, 0, protowire.ParseError(n)
		}
		if number == notificationBatchNotifications {
			notifications++
		}
		data = data[n:]
	}
	return offset, notifications, nil
}

type notificationsTracker struct {
	sync.Mutex
	cond       concurrent.ConditionContext
	shard      int64
	lastOffset atomic.Int64
	closed     atomic.Bool
	kv         kvstore.KV
	log        *slog.Logger

	// The first read creates the cache: until then, which is forever on a
	// follower, the commits don't pay for it
	cache   *notificationsCache
	caching atomic.Bool

	ctx       context.Context
	cancel    context.CancelFunc
	waitClose concurrent.WaitGroup

	readCounter      metric.Counter
	readBatchCounter metric.Counter
	readBytesCounter metric.Counter
}

func newNotificationsTracker(namespace string, shard int64, lastOffset int64, kv kvstore.KV, notificationRetentionTime time.Duration, clock time2.Clock) *notificationsTracker {
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
	newNotificationsTrimmer(nt.ctx, namespace, shard, kv, notificationRetentionTime, nt.waitClose, clock, nt.trimmed)
	return nt
}

// Caching reports whether the batches get cached, encoded in
// Notifications.encoded.
func (nt *notificationsTracker) Caching() bool {
	return nt.caching.Load()
}

// Committed makes the batch of notifications of a commit visible to the
// readers, once it is in the db.
func (nt *notificationsTracker) Committed(notifications *Notifications) {
	offset := notifications.batch.Offset
	// The offset must be updated while holding the lock the waiters check it
	// under, or a waiter that has just found it too low can miss the Broadcast
	// and stay parked until the next commit
	nt.Lock()
	if nt.cache != nil {
		if notifications.encoded != nil {
			nt.cache.add(proto.EncodedNotificationBatch{Offset: offset, Data: notifications.encoded},
				len(notifications.batch.Notifications))
		} else {
			// The first read created the cache after the batch was stored
			// without the bytes for it: the cache starts after it
			nt.cache.restart(offset + 1)
		}
	}
	nt.lastOffset.Store(offset)
	nt.Unlock()
	nt.cond.Broadcast()
}

// trimmed drops the batches up to offset, included, which the trimmer deleted
// from the db.
func (nt *notificationsTracker) trimmed(offset int64) {
	nt.Lock()
	defer nt.Unlock()
	if nt.cache != nil {
		nt.cache.dropUpTo(offset)
	}
}

// RecordsDeleted drops all the cached batches, as a delete range over the
// internal keys deletes notification records.
func (nt *notificationsTracker) RecordsDeleted() {
	nt.Lock()
	defer nt.Unlock()
	if nt.cache != nil {
		nt.cache.restart(nt.lastOffset.Load() + 1)
	}
}

// waitForNotifications waits until there are batches from startOffset on. It
// returns them when they are cached, with the number of notifications they
// carry, or nil.
func (nt *notificationsTracker) waitForNotifications(ctx context.Context, startOffset int64) (
	[]proto.EncodedNotificationBatch, int, error) {
	nt.Lock()
	defer nt.Unlock()

	if nt.cache == nil {
		// The first read: from now on, the batches get cached as they commit
		nt.cache = newNotificationsCache(nt.lastOffset.Load() + 1)
		nt.caching.Store(true)
	}

	for startOffset > nt.lastOffset.Load() && !nt.closed.Load() {
		if nt.log.Enabled(ctx, slog.LevelDebug) {
			nt.log.Debug(
				"Waiting for notification to be available",
				slog.Int64("start-offset", startOffset),
				slog.Int64("last-notification-offset", nt.lastOffset.Load()),
			)
		}

		if err := nt.cond.Wait(ctx); err != nil {
			return nil, 0, err
		}
	}

	if nt.closed.Load() {
		return nil, 0, constant.ErrResourceUnavailable
	}

	res, notifications := nt.cache.read(startOffset)
	return res, notifications, nil
}

// ReadNextNotifications returns the next retained batches from startOffset
// onwards, waiting until there is at least one: it never returns an empty
// result. A negative startOffset reads from the first retained batch, like 0.
func (nt *notificationsTracker) ReadNextNotifications(ctx context.Context, startOffset int64) (
	[]proto.EncodedNotificationBatch, error) {
	for {
		res, notifications, err := nt.waitForNotifications(ctx, startOffset)
		if err != nil {
			return nil, err
		}

		// Load before the scan: Committed runs after the batch commit, so the
		// scan sees every batch up to lastOffset not yet trimmed
		lastOffset := nt.lastOffset.Load()
		if res == nil {
			// The cache doesn't go back to startOffset
			if res, notifications, err = nt.scanNotifications(startOffset); err != nil {
				return nil, err
			}
		}
		if len(res) > 0 {
			size := 0
			for i := range res {
				size += len(res[i].Data)
			}
			nt.readBatchCounter.Add(len(res))
			nt.readBytesCounter.Add(size)
			nt.readCounter.Add(notifications)
			return res, nil
		}

		// Nothing is retained from startOffset onwards: the trimmer deleted
		// those batches, or startOffset is negative on a shard without any.
		// Wait for the next batch instead of returning the empty result, which
		// the caller would retry at once, busy-spinning.
		startOffset = max(startOffset, lastOffset+1)
	}
}

// scanNotifications reads the batches from startOffset on from the db, with
// the number of notifications they carry.
func (nt *notificationsTracker) scanNotifications(startOffset int64) ([]proto.EncodedNotificationBatch, int, error) {
	it, err := nt.kv.RangeScan(notificationKey(startOffset), lastNotificationKey, kvstore.ShowInternalKeys)
	if err != nil {
		return nil, 0, err
	}
	defer it.Close()

	var res []proto.EncodedNotificationBatch
	totalCount := 0

	for ; len(res) < maxNotificationBatchSize && it.Valid(); it.Next() {
		value, err := it.Value()
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed to read notification batch")
		}

		offset, count, err := parseNotificationBatch(value)
		if err != nil {
			return nil, 0, errors.Wrap(err, "failed to Deserialize notification batch")
		}
		// The value is only valid until the iterator moves
		res = append(res, proto.EncodedNotificationBatch{Offset: offset, Data: slices.Clone(value)})
		totalCount += count
	}

	// The iteration also stops when a read fails. Returning what was read
	// until then could be an empty result, which ReadNextNotifications takes
	// for trimmed batches and skips.
	if err := it.Error(); err != nil {
		return nil, 0, errors.Wrap(err, "failed to read notification batches")
	}
	return res, totalCount, nil
}

func (nt *notificationsTracker) Close() error {
	select {
	case <-nt.ctx.Done():
		return nil
	default:
		nt.cancel()
		// Like the offset, the closed flag must be set under the waiters' lock
		nt.Lock()
		nt.closed.Store(true)
		nt.Unlock()
		nt.cond.Broadcast()
		return nt.waitClose.Wait(context.Background())
	}
}
