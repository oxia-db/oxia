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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/logging"
)

// BenchmarkNotificationsTracker measures what it costs to deliver the
// notifications of each commit to subscribers that keep up with the writes.
// Each subscriber loops on ReadNextNotifications like the dispatcher of a
// GetNotifications stream, and the writer waits for all of them to receive a
// batch before committing the next one, so every commit wakes every
// subscriber, which reads back exactly that batch. Each commit overwrites the
// same keys, with one notification per key. Results are per commit, write
// included: subscribers=0 is the cost of the write alone.
func BenchmarkNotificationsTracker(b *testing.B) {
	// The logs go to stdout, where they'd break the result lines, and opening
	// the db logs at the info level
	logging.LogLevel = slog.LevelWarn
	logging.ConfigureLogger()

	for _, subscribers := range []int{0, 1, 10, 100} {
		for _, keys := range []int{1, 100} {
			b.Run(fmt.Sprintf("subscribers=%d/keys=%d", subscribers, keys), func(b *testing.B) {
				benchmarkNotificationsTracker(b, subscribers, keys)
			})
		}
	}
}

func benchmarkNotificationsTracker(b *testing.B, subscribers int, keys int) {
	b.Helper()
	factory, err := kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{DataDir: b.TempDir()})
	require.NoError(b, err)
	defer factory.Close()
	// Like the default retention, long enough that no batch is trimmed before
	// all the subscribers have read it
	db, err := NewDB(constant.DefaultNamespace, 1, factory, proto.KeySortingType_NATURAL, time.Hour, time2.SystemClock)
	require.NoError(b, err)
	defer db.Close()

	write := &proto.WriteRequest{}
	for i := range keys {
		write.Puts = append(write.Puts, &proto.PutRequest{Key: fmt.Sprintf("key-%03d", i), Value: []byte("value")})
	}

	ctx, cancel := context.WithCancel(context.Background())
	received := sync.WaitGroup{}
	subscribersDone := sync.WaitGroup{}
	// The subscribers stop before the db is closed under them
	defer func() {
		cancel()
		subscribersDone.Wait()
	}()
	for range subscribers {
		subscribersDone.Go(func() {
			// Each subscriber has its own context, like the stream it reads for:
			// sharing one would add contention on its done channel
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			offset := int64(-1)
			for {
				batches, err := db.ReadNextNotifications(ctx, offset+1)
				if err != nil {
					return
				}
				for _, nb := range batches {
					offset = nb.Offset
					received.Done()
				}
			}
		})
	}

	timestamp := uint64(time.Now().UnixMilli())
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		received.Add(subscribers)
		if _, err := db.ProcessWrite(write, int64(i), timestamp, NoOpCallback); err != nil {
			b.Fatal(err)
		}
		received.Wait()
	}
	b.StopTimer()
}
