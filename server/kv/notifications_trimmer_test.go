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

package kv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/compare"

	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/proto"
)

func TestNotificationsTrimmer(t *testing.T) {
	clock := &time2.MockedClock{}

	factory, err := NewPebbleKVFactory(testKVOptions)
	assert.NoError(t, err)
	dbx, err := NewDB(constant.DefaultNamespace, 1, factory, compare.EncoderNatural, 10*time.Millisecond, clock)
	assert.NoError(t, err)
	defer dbx.Close()

	for i := int64(0); i < 100; i++ {
		_, err = dbx.ProcessWrite(&proto.WriteRequest{
			Puts: []*proto.PutRequest{{
				Key:   fmt.Sprintf("key-%d", i),
				Value: []byte("0"),
			}},
		}, i, uint64(i), NoOpCallback)
		assert.NoError(t, err)
	}

	time.Sleep(1 * time.Second)
	// No entries should have been trimmed
	assert.EqualValues(t, 0, firstNotification(t, dbx))

	// Clock has advanced, though not enough to have the trimming started
	clock.Set(3)

	time.Sleep(1 * time.Second)
	// No entries should have been trimmed
	assert.EqualValues(t, 0, firstNotification(t, dbx))

	clock.Set(15)

	assert.Eventually(t, func() bool {
		return firstNotification(t, dbx) == 6
	}, 10*time.Second, 1*time.Second)

	clock.Set(75)

	assert.Eventually(t, func() bool {
		return firstNotification(t, dbx) == 66
	}, 10*time.Second, 1*time.Second)

	clock.Set(120)

	assert.Eventually(t, func() bool {
		return firstNotification(t, dbx) == -1
	}, 10*time.Second, 1*time.Second)
}

func firstNotification(t *testing.T, db DB) int64 {
	t.Helper()

	nextNotifications, err := db.ReadNextNotifications(context.Background(), 0)
	assert.NoError(t, err)

	if len(nextNotifications) == 0 {
		return -1
	}

	return nextNotifications[0].Offset
}
