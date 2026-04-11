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

package database

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/pkg/errors"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/proto"
)

const (
	minNotificationTrimmingInterval = 500 * time.Millisecond
	maxNotificationTrimmingInterval = 5 * time.Minute
)

type notificationsTrimmer struct {
	ctx                        context.Context
	waitClose                  concurrent.WaitGroup
	kv                         kvstore.KV
	interval                   time.Duration
	notificationsRetentionTime time.Duration
	clock                      time2.Clock
	log                        *slog.Logger
}

func newNotificationsTrimmer(ctx context.Context, namespace string, shardId int64, kv kvstore.KV, notificationRetentionTime time.Duration, waitClose concurrent.WaitGroup, clock time2.Clock) *notificationsTrimmer {
	interval := notificationRetentionTime / 10
	if interval < minNotificationTrimmingInterval {
		interval = minNotificationTrimmingInterval
	}
	if interval > maxNotificationTrimmingInterval {
		interval = maxNotificationTrimmingInterval
	}

	t := &notificationsTrimmer{
		ctx:                        ctx,
		waitClose:                  waitClose,
		kv:                         kv,
		interval:                   interval,
		notificationsRetentionTime: notificationRetentionTime,
		clock:                      clock,
		log: slog.With(
			slog.String("component", "db-notifications-trimmer"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
		),
	}

	go process.DoWithLabels(
		t.ctx,
		map[string]string{
			"oxia":      "notifications-trimmer",
			"namespace": namespace,
			"shard":     fmt.Sprintf("%d", shardId),
		},
		t.run,
	)

	return t
}

func (t *notificationsTrimmer) run() {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := t.trimNotifications(); err != nil {
				t.log.Warn("Failed to trim notifications", slog.Any("error", err))
			}

		case <-t.ctx.Done():
			t.waitClose.Done()
			return
		}
	}
}

func (t *notificationsTrimmer) trimNotifications() error {
	first, last, err := t.getFirstLast()
	if err != nil {
		return err
	}

	t.log.Debug(
		"Starting notifications trimming",
		slog.Int64("first-offset", first),
		slog.Int64("last-offset", last),
		slog.Time("current-time", t.clock.Now()),
		slog.Duration("retention-time", t.notificationsRetentionTime),
	)

	if last == constant.I64NegativeOne {
		return nil
	}

	cutoffTime := t.clock.Now().Add(-t.notificationsRetentionTime)

	_, tsFirst, err := t.readFrom(first)
	if errors.Is(err, kvstore.ErrKeyNotFound) {
		t.log.Warn(
			"First notification entry disappeared after range scan",
			slog.Int64("first-offset", first),
			slog.Int64("last-offset", last),
		)
		return nil
	}
	if err != nil {
		return err
	}

	t.log.Debug(
		"Starting notifications trimming",
		slog.Time("timestamp-first-entry", tsFirst),
		slog.Time("cutoff-time", cutoffTime),
	)

	if cutoffTime.Before(tsFirst) {
		return nil
	}

	trimOffset, err := t.binarySearch(first, last, cutoffTime)
	if err != nil {
		return errors.Wrap(err, "failed to perform binary search")
	}

	wb := t.kv.NewWriteBatch()
	if err = wb.DeleteRange(notificationKey(first), notificationKey(trimOffset+1)); err != nil {
		return err
	}

	if err = wb.Commit(); err != nil {
		return err
	}

	if err = wb.Close(); err != nil {
		return err
	}

	t.log.Debug(
		"Successfully trimmed the notification",
		slog.Int64("trimmed-offset", trimOffset),
		slog.Int64("first-offset", first),
		slog.Int64("last-offset", last),
	)
	return nil
}

func (t *notificationsTrimmer) getFirstLast() (first, last int64, err error) {
	it1, err := t.kv.KeyRangeScan(firstNotificationKey, lastNotificationKey, kvstore.ShowInternalKeys)
	if err != nil {
		return constant.I64NegativeOne, constant.I64NegativeOne, err
	}
	defer it1.Close()
	if !it1.Valid() {
		// There are no entries in DB
		return constant.I64NegativeOne, constant.I64NegativeOne, nil
	}

	if first, err = parseNotificationKey(it1.Key()); err != nil {
		return first, last, err
	}

	it2, err := t.kv.KeyRangeScanReverse(firstNotificationKey, lastNotificationKey, kvstore.ShowInternalKeys)
	if err != nil {
		return constant.I64NegativeOne, constant.I64NegativeOne, err
	}
	defer it2.Close()
	if !it2.Valid() {
		// There are no entries in DB
		return constant.I64NegativeOne, constant.I64NegativeOne, nil
	}

	if last, err = parseNotificationKey(it2.Key()); err != nil {
		return first, last, err
	}

	return first, last, nil
}

func (t *notificationsTrimmer) binarySearch(firstOffset, lastOffset int64, cutoffTime time.Time) (int64, error) {
	for firstOffset < lastOffset {
		midProbe := firstOffset + (lastOffset-firstOffset+1)/2

		actualMid, tsMid, err := t.readFrom(midProbe)
		// No notification exists in [midProbe, lastOffset]: either the ceiling
		// lookup found no entry at all (e.g. concurrent FilterDBForSplit deleted
		// the rest of the namespace), or it landed on an offset above our
		// current upper bound. In both cases we shrink the window and retry.
		if errors.Is(err, kvstore.ErrKeyNotFound) || (err == nil && actualMid > lastOffset) {
			lastOffset = midProbe - 1
			continue
		}
		if err != nil {
			return constant.I64NegativeOne, err
		}

		if cutoffTime.Before(tsMid) {
			lastOffset = midProbe - 1
		} else {
			firstOffset = actualMid
		}
	}

	return firstOffset, nil
}

func (t *notificationsTrimmer) readFrom(offset int64) (int64, time.Time, error) {
	returnedKey, value, closer, err := t.kv.Get(notificationKey(offset), kvstore.ComparisonCeiling, kvstore.ShowInternalKeys)
	if err != nil {
		return constant.I64NegativeOne, time.Time{}, err
	}
	defer closer.Close()

	if !strings.HasPrefix(returnedKey, notificationsPrefix+"/") {
		return constant.I64NegativeOne, time.Time{}, kvstore.ErrKeyNotFound
	}

	actualOffset, err := parseNotificationKey(returnedKey)
	if err != nil {
		return constant.I64NegativeOne, time.Time{}, err
	}

	nb := &proto.NotificationBatch{}
	if err := pb.Unmarshal(value, nb); err != nil {
		return constant.I64NegativeOne, time.Time{}, errors.Wrap(err, "failed to deserialize notification batch")
	}
	return actualOffset, time.UnixMilli(int64(nb.Timestamp)), nil
}
