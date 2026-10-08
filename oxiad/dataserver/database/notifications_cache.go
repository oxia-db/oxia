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
	"sort"

	"github.com/oxia-db/oxia/common/proto"
)

const (
	// The cache keeps the batches of the latest commits, for the subscribers
	// that keep up with them. Those further behind read from the db.
	maxCachedNotificationBatches = 1024
	maxCachedNotificationBytes   = 1024 * 1024
)

type cachedNotificationBatch struct {
	batch proto.EncodedNotificationBatch
	// The number of notifications in the batch, for the read metrics
	notifications int
}

// notificationsCache keeps the latest notification batches, encoded, so that
// reading one costs neither a db iterator nor a decode. It holds every batch
// from offset `from` on, within maxCachedNotificationBatches batches and
// maxCachedNotificationBytes bytes: to stay within them, it drops the oldest
// batches, and `from` moves past them.
type notificationsCache struct {
	ring  []cachedNotificationBatch
	head  int
	size  int
	bytes int
	from  int64
}

func newNotificationsCache(from int64) *notificationsCache {
	return &notificationsCache{
		ring: make([]cachedNotificationBatch, maxCachedNotificationBatches),
		from: from,
	}
}

// at returns the i-th oldest cached batch.
func (c *notificationsCache) at(i int) *cachedNotificationBatch {
	return &c.ring[(c.head+i)%len(c.ring)]
}

// add caches the batch following the cached ones.
func (c *notificationsCache) add(batch proto.EncodedNotificationBatch, notifications int) {
	if c.size == len(c.ring) {
		c.dropOldest()
	}
	*c.at(c.size) = cachedNotificationBatch{batch: batch, notifications: notifications}
	c.size++
	c.bytes += len(batch.Data)
	for c.bytes > maxCachedNotificationBytes {
		c.dropOldest()
	}
}

func (c *notificationsCache) dropOldest() {
	oldest := c.at(0)
	c.from = oldest.batch.Offset + 1
	c.bytes -= len(oldest.batch.Data)
	*oldest = cachedNotificationBatch{}
	c.head = (c.head + 1) % len(c.ring)
	c.size--
}

// dropUpTo drops the batches up to offset, included.
func (c *notificationsCache) dropUpTo(offset int64) {
	for c.size > 0 && c.at(0).batch.Offset <= offset {
		c.dropOldest()
	}
}

// restart drops all the batches, and caches the ones from offset `from` on.
func (c *notificationsCache) restart(from int64) {
	for c.size > 0 {
		c.dropOldest()
	}
	c.from = from
}

// read returns the cached batches from startOffset on, at most
// maxNotificationBatchSize of them, with the number of notifications they
// carry. It returns nil when the cache doesn't go back to startOffset.
func (c *notificationsCache) read(startOffset int64) ([]proto.EncodedNotificationBatch, int) {
	if startOffset < c.from {
		return nil, 0
	}
	first := sort.Search(c.size, func(i int) bool {
		return c.at(i).batch.Offset >= startOffset
	})
	res := make([]proto.EncodedNotificationBatch, min(c.size-first, maxNotificationBatchSize))
	notifications := 0
	for i := range res {
		cached := c.at(first + i)
		res[i] = cached.batch
		notifications += cached.notifications
	}
	return res, notifications
}
