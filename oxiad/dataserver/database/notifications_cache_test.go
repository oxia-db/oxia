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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

func encodedBatch(offset int64, size int) proto.EncodedNotificationBatch {
	return proto.EncodedNotificationBatch{Offset: offset, Data: make([]byte, size)}
}

func batchOffsets(batches []proto.EncodedNotificationBatch) []int64 {
	offsets := make([]int64, 0, len(batches))
	for _, batch := range batches {
		offsets = append(offsets, batch.Offset)
	}
	return offsets
}

func TestNotificationsCache_Read(t *testing.T) {
	c := newNotificationsCache(1)
	// Only the commits with notifications have a batch
	for _, offset := range []int64{1, 3, 4, 7} {
		c.add(encodedBatch(offset, 10), int(offset))
	}

	res, _ := c.read(0)
	assert.Nil(t, res)

	res, notifications := c.read(1)
	assert.Equal(t, []int64{1, 3, 4, 7}, batchOffsets(res))
	assert.Equal(t, 15, notifications)

	res, notifications = c.read(2)
	assert.Equal(t, []int64{3, 4, 7}, batchOffsets(res))
	assert.Equal(t, 14, notifications)

	res, _ = c.read(7)
	assert.Equal(t, []int64{7}, batchOffsets(res))
}

func TestNotificationsCache_ReadLimit(t *testing.T) {
	c := newNotificationsCache(0)
	for offset := range int64(maxNotificationBatchSize + 10) {
		c.add(encodedBatch(offset, 1), 1)
	}

	res, notifications := c.read(0)
	require.Len(t, res, maxNotificationBatchSize)
	assert.Equal(t, maxNotificationBatchSize, notifications)
	assert.EqualValues(t, maxNotificationBatchSize-1, res[len(res)-1].Offset)
}

func TestNotificationsCache_DropsOldestBeyondCount(t *testing.T) {
	c := newNotificationsCache(0)
	total := int64(maxCachedNotificationBatches + 10)
	for offset := range total {
		c.add(encodedBatch(offset, 1), 1)
	}
	assert.Equal(t, maxCachedNotificationBatches, c.size)

	// The reads from the dropped batches go to the db
	res, _ := c.read(9)
	assert.Nil(t, res)

	// Across the end of the ring
	res, _ = c.read(total - 20)
	expected := make([]int64, 0, 20)
	for offset := total - 20; offset < total; offset++ {
		expected = append(expected, offset)
	}
	assert.Equal(t, expected, batchOffsets(res))
}

func TestNotificationsCache_DropsOldestBeyondBytes(t *testing.T) {
	c := newNotificationsCache(0)
	for offset := range int64(6) {
		c.add(encodedBatch(offset, maxCachedNotificationBytes/4), 1)
	}

	res, _ := c.read(1)
	assert.Nil(t, res)
	res, _ = c.read(2)
	assert.Equal(t, []int64{2, 3, 4, 5}, batchOffsets(res))
	assert.Equal(t, maxCachedNotificationBytes, c.bytes)

	// A batch larger than the cache is not kept
	c.add(encodedBatch(6, maxCachedNotificationBytes+1), 1)
	res, _ = c.read(6)
	assert.Nil(t, res)
	assert.Zero(t, c.bytes)

	c.add(encodedBatch(7, 1), 1)
	res, _ = c.read(7)
	assert.Equal(t, []int64{7}, batchOffsets(res))
}

func TestNotificationsCache_DropUpTo(t *testing.T) {
	c := newNotificationsCache(0)
	for _, offset := range []int64{0, 2, 4, 6} {
		c.add(encodedBatch(offset, 1), 1)
	}

	c.dropUpTo(3)
	res, _ := c.read(2)
	assert.Nil(t, res)
	// There is no batch at 3: from there on, the cache still has them all
	res, _ = c.read(3)
	assert.Equal(t, []int64{4, 6}, batchOffsets(res))
}

func TestNotificationsCache_Restart(t *testing.T) {
	c := newNotificationsCache(0)
	c.add(encodedBatch(0, 1), 1)
	c.add(encodedBatch(1, 1), 1)

	c.restart(5)
	res, _ := c.read(1)
	assert.Nil(t, res)
	assert.Zero(t, c.size)
	assert.Zero(t, c.bytes)

	c.add(encodedBatch(5, 1), 1)
	res, _ = c.read(5)
	assert.Equal(t, []int64{5}, batchOffsets(res))
}
